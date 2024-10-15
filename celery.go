// Package celery helps to work with Celery (place tasks in queues and execute them).
package celery

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/marselester/gopher-celery/protocol"
	"github.com/marselester/gopher-celery/redis"
)

// TaskF represents a Celery task implemented by the client.
// The error doesn't affect anything, it's logged though.
type TaskF func(ctx context.Context, p *TaskParam) error

// Middleware is a chainable behavior modifier for tasks.
// For example, a caller can collect task metrics.
type Middleware func(next TaskF) TaskF

// DefaultQueue is the default queue name used by the app.
var DefaultQueue = "celery"

// DefaultHeatbeat is the default heartbeat interval used by the app.
var DefaultHeatbeat = 15

// Broker is responsible for receiving and sending task messages.
// For example, it knows how to read a message from a given queue in Redis.
// The messages can be in defferent formats depending on Celery protocol version.
type Broker interface {
	// Send puts a message to a queue.
	// Note, the method is safe to call concurrently.
	Send(msg []byte, queue string) error
	// Receive returns a raw message from one of the queues.
	// It blocks until there is a message available for consumption.
	// Note, the method is not concurrency safe.
	Receive(queue string) ([]byte, error)
	// Ack acknowledges a task message. If messages are not acked,
	// they will be redelivered after the visibility timeout.
	// Note, the method is safe to call concurrently.
	Ack(queue string, message []byte) error
	// Reject rejects a task message. It puts the message back to the queue.
	// Note, the method is safe to call concurrently.
	Reject(queue string, message []byte) error
	// SendFanout puts a message to a fanout queue.
	// Note, the method is safe to call concurrently.
	SendFanout(msg []byte, queue string, routingKey string) error
}

// Backend is responsible for storing and retrieving task results.
type Backend interface {
	// Store saves a task result.
	Store(key string, value []byte) error
	// Load loads a task result.
	Load(key string) ([]byte, error)
}

// AsyncParam represents parameters for sending a task message.
type AsyncParam struct {
	// Args is a list of arguments.
	// It will be an empty list if not provided.
	Args []interface{}
	// Kwargs is a dictionary of keyword arguments.
	// It will be an empty dictionary if not provided.
	Kwargs map[string]interface{}
	// Expires is an expiration date.
	// If not provided the message will never expire.
	Expires time.Time
}

// NewApp creates a Celery app.
// The default broker is Redis assumed to run on localhost.
// When producing tasks the default message serializer is json and protocol is v2.
func NewApp(options ...Option) *App {
	app := App{
		conf: Config{
			logger:     log.NewNopLogger(),
			registry:   protocol.NewSerializerRegistry(),
			mime:       protocol.MimeJSON,
			protocol:   protocol.V2,
			maxWorkers: DefaultMaxWorkers,
			queue:      DefaultQueue,
			heartbeat:  DefaultHeatbeat,
		},
		task: make(map[string]TaskF),
	}

	for _, opt := range options {
		opt(&app.conf)
	}

	app.sem = make(chan struct{}, app.conf.maxWorkers)

	if app.conf.broker == nil {
		app.conf.broker = redis.NewBroker()
	}

	return &app
}

// App is a Celery app to produce or consume tasks asynchronously.
type App struct {
	// conf represents app settings.
	conf Config

	// task maps a Celery task path to a task itself, e.g.,
	// "myproject.apps.myapp.tasks.mytask": TaskF.
	task map[string]TaskF

	// sem is a semaphore that limits number of workers.
	sem chan struct{}

	// counters for stats
	started atomic.Int32
	stopped atomic.Int32
}

// Register associates the task with given Python path.
// For example, when "myproject.apps.myapp.tasks.mytask"
// is seen, the TaskF task is executed.
//
// Note, the method is not concurrency safe.
// The tasks mustn't be registered after the app starts processing tasks.
func (a *App) Register(path string, task TaskF) {
	a.task[path] = task
}

// Bind associates the task with given Python path.
// For example, when "myproject.apps.myapp.tasks.mytask"
// is seen, the taskFunc is executed.
//
// taskFunc must be a method that returns an error
// as the last argument. It may contain context.Context
// as the first argument.
//
// Note, the method is not concurrency safe.
// The tasks mustn't be registered after the app starts processing tasks.
func (a *App) Bind(path string, taskFunc interface{}) {
	a.task[path] = Wrap(taskFunc)
}

// ApplyAsync sends a task message.
func (a *App) ApplyAsync(path, queue string, p *AsyncParam) error {
	m := protocol.Task{
		ID:      uuid.NewString(),
		Name:    path,
		Args:    p.Args,
		Kwargs:  p.Kwargs,
		Expires: p.Expires,
	}

	rawMsg, err := a.conf.registry.Encode(queue, a.conf.mime, a.conf.protocol, &m)
	if err != nil {
		return fmt.Errorf("failed to encode task message: %w", err)
	}

	if err = a.conf.broker.Send(rawMsg, queue); err != nil {
		return fmt.Errorf("failed to send task message to broker: %w", err)
	}

	return nil
}

// Delay is a shortcut to send a task message,
// i.e., it places the task associated with given Python path into queue.
func (a *App) Delay(path, queue string, args ...interface{}) error {
	m := protocol.Task{
		ID:   uuid.NewString(),
		Name: path,
		Args: args,
	}

	rawMsg, err := a.conf.registry.Encode(queue, a.conf.mime, a.conf.protocol, &m)
	if err != nil {
		return fmt.Errorf("failed to encode task message: %w", err)
	}

	if err = a.conf.broker.Send(rawMsg, queue); err != nil {
		return fmt.Errorf("failed to send task message to broker: %w", err)
	}

	return nil
}

// Run launches the workers that process the tasks received from the broker.
// The call is blocking until ctx is cancelled.
// The caller mustn't register any new tasks at this point.
func (a *App) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	level.Debug(a.conf.logger).Log("msg", "observing queue", "queue", a.conf.queue)

	msgs := make(chan *protocol.Task, 1)

	g.Go(func() error {
		return a.heartbeatLoop(ctx)
	})

	g.Go(func() error {
		defer close(msgs)

		// One goroutine fetching and decoding tasks from queues
		// shouldn't be a bottleneck since the worker goroutines
		// usually take seconds/minutes to complete.
		for {
			select {
			// Acquire a semaphore by sending a token.
			case a.sem <- struct{}{}:
			// Stop processing tasks.
			case <-ctx.Done():
				return ctx.Err()
			}

			raw, msg, err := a.receive(ctx)
			if err != nil {
				return err
			}

			g.Go(func() error {
				// Release a semaphore by discarding a token.
				defer func() { <-a.sem }()

				a.dispatch("task-started", struct {
					ID string `json:"uuid"`
				}{
					ID: msg.ID,
				})

				a.started.Add(1)

				start := time.Now()

				if result, err := a.executeTask(ctx, msg); err != nil {
					a.dispatch("task-failed", struct {
						ID      string  `json:"uuid"`
						Runtime float64 `json:"runtime"`
						Result  string  `json:"result"`
					}{
						ID:      msg.ID,
						Runtime: time.Since(start).Seconds(),
						Result:  err.Error(),
					})

					level.Error(a.conf.logger).Log("msg", "task failed", "name", msg.Name, "err", err)
				} else {
					a.dispatch("task-succeeded", struct {
						ID      string  `json:"uuid"`
						Runtime float64 `json:"runtime"`
						Result  string  `json:"result"`
					}{
						ID:      msg.ID,
						Runtime: time.Since(start).Seconds(),
						Result:  python(result),
					})
				}

				a.stopped.Add(1)

				return a.conf.broker.Ack(a.conf.queue, raw)
			})
		}
	})

	return g.Wait()
}

func (a *App) receive(ctx context.Context) ([]byte, *protocol.Task, error) {
	for {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}

		raw, err := a.conf.broker.Receive(a.conf.queue)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to receive a raw task message: %w", err)
		}

		if raw == nil {
			continue
		}

		msg, err := a.conf.registry.Decode(raw)
		if err != nil {
			level.Error(a.conf.logger).Log("msg", "failed to decode task message", "rawmsg", raw, "err", err)

			if err1 := a.conf.broker.Ack(a.conf.queue, raw); err1 != nil {
				return nil, nil, err1
			}

			continue
		}

		if a.task[msg.Name] == nil {
			level.Debug(a.conf.logger).Log("msg", "unregistered task", "name", msg.Name)

			if err1 := a.conf.broker.Ack(a.conf.queue, raw); err1 != nil {
				return nil, nil, err1
			}

			continue
		}

		if msg.IsExpired() {
			level.Debug(a.conf.logger).Log("msg", "task message expired", "name", msg.Name)

			if err1 := a.conf.broker.Ack(a.conf.queue, raw); err1 != nil {
				return nil, nil, err1
			}

			continue
		}

		event := struct {
			ID       string  `json:"uuid"`
			Name     string  `json:"name"`
			Args     string  `json:"args"`
			Kwargs   string  `json:"kwargs"`
			Expires  *string `json:"expires"`
			RootID   string  `json:"root_id"`
			ParentID string  `json:"parent_id"`
			Retries  int     `json:"retries"`
		}{
			ID:       msg.ID,
			Name:     msg.Name,
			Args:     python(msg.Args),
			Kwargs:   python(msg.Kwargs),
			RootID:   msg.RootID,
			ParentID: msg.ParentID,
			Retries:  msg.Retries,
		}

		if !msg.Expires.IsZero() {
			s := msg.Expires.Format(time.RFC3339)
			event.Expires = &s
		}

		a.dispatch("task-received", event)

		return raw, msg, nil
	}
}

func python(obj interface{}) string {
	data, err := json.Marshal(obj)
	if err != nil {
		return "nil"
	}

	if data[0] == '[' {
		data[0] = '('
		data[len(data)-1] = ')'
	}

	return string(data)
}

func (a *App) dispatch(event string, obj interface{}) {
	if a.conf.eventChannel == "" {
		return
	}

	payload, err := a.conf.registry.Event(event, a.conf.eventChannel, "task.multi", obj)
	if err != nil {
		level.Error(a.conf.logger).Log("msg", "failed to encode event message", "event", event, "err", err)

		return
	}

	if err := a.conf.broker.SendFanout(payload, a.conf.eventChannel, "task.multi"); err != nil {
		level.Error(a.conf.logger).Log("msg", "failed to send event message", "event", event, "err", err)
	}
}

func (a *App) heartbeatLoop(ctx context.Context) error {
	if a.conf.eventChannel == "" {
		return nil
	}

	timer := time.NewTimer(time.Duration(a.conf.heartbeat) * time.Second)

	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}

		if err := a.heartbeatOnce(); err != nil {
			return err
		}
	}
}

const si_load_shift = 16

func (a *App) heartbeatOnce() error {
	stopped := a.stopped.Load()
	started := a.started.Load()

	var info syscall.Sysinfo_t

	err := syscall.Sysinfo(&info)
	if err != nil {
		return err
	}

	obj := struct {
		Freq             float64    `json:"freq"`
		Active           int32      `json:"active"`
		Processed        int32      `json:"processed"`
		LoadAverage      [3]float64 `json:"loadavg"`
		SoftwareID       string     `json:"sw_ident"`
		SoftwareVersion  string     `json:"sw_ver"`
		SoftwarePlatform string     `json:"sw_sys"`
	}{
		Freq:      float64(a.conf.heartbeat),
		Active:    started - stopped,
		Processed: stopped,
		LoadAverage: [3]float64{
			math.Round(float64(info.Loads[0])/float64(1<<si_load_shift)*100) / 100,
			math.Round(float64(info.Loads[1])/float64(1<<si_load_shift)*100) / 100,
			math.Round(float64(info.Loads[2])/float64(1<<si_load_shift)*100) / 100,
		},
		SoftwareID:       "go",
		SoftwareVersion:  runtime.Version(),
		SoftwarePlatform: runtime.GOOS,
	}

	payload, err := a.conf.registry.Event("worker-heartbeat", a.conf.eventChannel, "worker.heartbeat", obj)
	if err != nil {
		return err
	}

	return a.conf.broker.SendFanout(payload, a.conf.eventChannel, "worker.heartbeat")
}

type contextKey int

const (
	// ContextKeyTaskName is a context key to access task names.
	ContextKeyTaskName contextKey = iota

	// ContextKeyTaskID is a context key to access task IDs.
	ContextKeyTaskID

	// ContextKeyUpdateStateCallback is a context key to retrieve a callback to update the state.
	ContextKeyUpdateStateCallback
)

// executeTask calls the task function with args and kwargs from the message.
// If the task panics, the stack trace is returned as an error.
func (a *App) executeTask(ctx context.Context, m *protocol.Task) (interface{}, error) {
	task := a.task[m.Name]

	// Use middlewares if a client provided them.
	if a.conf.chain != nil {
		task = a.conf.chain(task)
	}

	ctx = context.WithValue(ctx, ContextKeyTaskName, m.Name)
	ctx = context.WithValue(ctx, ContextKeyTaskID, m.ID)

	p := NewTaskParam(m.Args, m.Kwargs)

	err := task(ctx, p)

	return p.result, err
}
