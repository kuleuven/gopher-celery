// Package celery helps to work with Celery (place tasks in queues and execute them).
package celery

import (
	"context"
	"fmt"
	"runtime/debug"
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

// Broker is responsible for receiving and sending task messages.
// For example, it knows how to read a message from a given queue in Redis.
// The messages can be in defferent formats depending on Celery protocol version.
type Broker interface {
	// Send puts a message to a queue.
	// Note, the method is safe to call concurrently.
	Send(msg []byte, queue string, delivery_tag, exchange, routing_key string) error
	// Observe sets the queues from which the tasks should be received.
	// Note, the method is not concurrency safe.
	Observe(queues []string)
	// Receive returns a raw message from one of the queues.
	// It blocks until there is a message available for consumption.
	// Note, the method is not concurrency safe.
	Receive() ([]byte, error)
	// Ack acknowledges a task message. If messages are not acked,
	// they will be redelivered after the visibility timeout.
	Ack(tag string) error
	// ExtendLifetime makes sure the visibility timeout of a task message
	// is extended during processing. It returns a cancel function. This
	// function returns true if the refresh loop is stopped, false if
	// it is was already stopped.
	RefreshLifetime(tag string) func() bool
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
		},
		task:      make(map[string]TaskF),
		taskQueue: make(map[string]string),
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
	// taskQueue helps to determine which queue a task belongs to, e.g.,
	// "myproject.apps.myapp.tasks.mytask": "important".
	taskQueue map[string]string
	// sem is a semaphore that limits number of workers.
	sem chan struct{}
}

// Register associates the task with given Python path and queue.
// For example, when "myproject.apps.myapp.tasks.mytask"
// is seen in "important" queue, the TaskF task is executed.
//
// Note, the method is not concurrency safe.
// The tasks mustn't be registered after the app starts processing tasks.
func (a *App) Register(path, queue string, task TaskF) {
	a.task[path] = task
	a.taskQueue[path] = queue
}

// ApplyAsync sends a task message.
func (a *App) ApplyAsync(path, queue string, p *AsyncParam) error {
	m := protocol.Task{
		ID:          uuid.NewString(),
		Name:        path,
		Args:        p.Args,
		Kwargs:      p.Kwargs,
		Expires:     p.Expires,
		DeliveryTag: uuid.NewString(),
	}
	rawMsg, err := a.conf.registry.Encode(queue, a.conf.mime, a.conf.protocol, &m)
	if err != nil {
		return fmt.Errorf("failed to encode task message: %w", err)
	}

	if err = a.conf.broker.Send(rawMsg, queue, m.DeliveryTag, queue, queue); err != nil {
		return fmt.Errorf("failed to send task message to broker: %w", err)
	}
	return nil
}

// Delay is a shortcut to send a task message,
// i.e., it places the task associated with given Python path into queue.
func (a *App) Delay(path, queue string, args ...interface{}) error {
	m := protocol.Task{
		ID:          uuid.NewString(),
		Name:        path,
		Args:        args,
		DeliveryTag: uuid.NewString(),
	}
	rawMsg, err := a.conf.registry.Encode(queue, a.conf.mime, a.conf.protocol, &m)
	if err != nil {
		return fmt.Errorf("failed to encode task message: %w", err)
	}

	if err = a.conf.broker.Send(rawMsg, queue, m.DeliveryTag, queue, queue); err != nil {
		return fmt.Errorf("failed to send task message to broker: %w", err)
	}

	return nil
}

// Run launches the workers that process the tasks received from the broker.
// The call is blocking until ctx is cancelled.
// The caller mustn't register any new tasks at this point.
func (a *App) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	qq := make([]string, 0, len(a.taskQueue))
	for k := range a.taskQueue {
		qq = append(qq, a.taskQueue[k])
	}
	a.conf.broker.Observe(qq)
	level.Debug(a.conf.logger).Log("msg", "observing queues", "queues", qq)

	msgs := make(chan *protocol.Task, 1)
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
				return nil
			}

			select {
			case <-ctx.Done():
				return nil
			default:
				rawMsg, err := a.conf.broker.Receive()
				if err != nil {
					return fmt.Errorf("failed to receive a raw task message: %w", err)
				}
				// No messages in the broker so far.
				if rawMsg == nil {
					<-a.sem
					continue
				}

				m, err := a.conf.registry.Decode(rawMsg)
				if err != nil {
					level.Error(a.conf.logger).Log("msg", "failed to decode task message", "rawmsg", rawMsg, "err", err)
					<-a.sem
					continue
				}

				msgs <- m
			}
		}
	})

	go func() {
		// Start a worker when there is a task.
		for m := range msgs {
			level.Debug(a.conf.logger).Log("msg", "task received", "name", m.Name)

			if a.task[m.Name] == nil {
				level.Debug(a.conf.logger).Log("msg", "unregistered task", "name", m.Name)

				<-a.sem

				continue
			}

			if m.IsExpired() {
				level.Debug(a.conf.logger).Log("msg", "task message expired", "name", m.Name)

				<-a.sem

				continue
			}

			m := m
			g.Go(func() error {
				// Release a semaphore by discarding a token.
				defer func() { <-a.sem }()

				if err := a.executeTask(ctx, m); err != nil {
					level.Error(a.conf.logger).Log("msg", "task failed", "taskmsg", m, "err", err)
				} else {
					level.Debug(a.conf.logger).Log("msg", "task succeeded", "name", m.Name)
				}

				return a.conf.broker.Ack(m.DeliveryTag)
			})
		}
	}()

	return g.Wait()
}

type contextKey int

const (
	// ContextKeyTaskName is a context key to access task names.
	ContextKeyTaskName contextKey = iota
)

// executeTask calls the task function with args and kwargs from the message.
// If the task panics, the stack trace is returned as an error.
func (a *App) executeTask(ctx context.Context, m *protocol.Task) (err error) {
	cancel := a.conf.broker.RefreshLifetime(m.DeliveryTag)

	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("unexpected task error: %v: %s", r, debug.Stack())
		}
	}()

	task := a.task[m.Name]
	// Use middlewares if a client provided them.
	if a.conf.chain != nil {
		task = a.conf.chain(task)
	}

	ctx = context.WithValue(ctx, ContextKeyTaskName, m.Name)
	p := NewTaskParam(m.Args, m.Kwargs)

	return task(ctx, p)
}
