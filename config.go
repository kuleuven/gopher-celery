package celery

import (
	"fmt"

	"github.com/go-kit/log"

	"github.com/marselester/gopher-celery/protocol"
)

// DefaultMaxWorkers is the default upper limit of goroutines
// allowed to process Celery tasks.
// Note, the workers are launched only when there are tasks to process.
//
// Let's say it takes ~5s to process a task on average,
// so 1000 goroutines should be able to handle 200 tasks per second
// (X = N / R = 1000 / 5) according to Little's law N = X * R.
const DefaultMaxWorkers = 1000

// Option sets up a Config.
type Option func(*Config)

// WithCustomTaskSerializer registers a custom serializer where
// mime is the mime-type describing the serialized structure, e.g., application/json,
// and encoding is the content encoding which is usually utf-8 or binary.
func WithCustomTaskSerializer(serializer protocol.Serializer, mime, encoding string) Option {
	return func(c *Config) {
		c.registry.Register(serializer, mime, encoding)
	}
}

// WithTaskSerializer sets a serializer mime-type, e.g.,
// the message's body is encoded in JSON when a task is sent to the broker.
// It is equivalent to CELERY_TASK_SERIALIZER in Python.
func WithTaskSerializer(mime string) Option {
	return func(c *Config) {
		switch mime {
		case protocol.MimeJSON:
			c.mime = mime
		default:
			c.mime = protocol.MimeJSON
		}
	}
}

// WithTaskProtocol sets the default task message protocol version used to send tasks.
// It is equivalent to CELERY_TASK_PROTOCOL in Python.
func WithTaskProtocol(version int) Option {
	return func(c *Config) {
		switch version {
		case protocol.V1, protocol.V2:
			c.protocol = version
		default:
			c.protocol = protocol.V2
		}
	}
}

// WithBroker allows a caller to replace the default broker.
func WithBroker(broker Broker) Option {
	return func(c *Config) {
		c.broker = broker
	}
}

// WithLogger sets a structured logger.
func WithLogger(logger log.Logger) Option {
	return func(c *Config) {
		c.logger = logger
	}
}

// WithQueue sets the name of the Celery queue.
func WithQueue(name string) Option {
	return func(c *Config) {
		if name == "" {
			name = DefaultQueue
		}

		c.queue = name

		// If events are enabled, work with an ingest queue.
		// This way, the task-received event can be sent early
		// and does not block until a worker has free capacity.
		if c.ingestQueue != "" {
			c.ingestQueue = name
			c.queue = fmt.Sprintf("%s-recv", name)
		}
	}
}

// WithMaxWorkers sets an upper limit of goroutines
// allowed to process Celery tasks.
func WithMaxWorkers(n int) Option {
	return func(c *Config) {
		c.maxWorkers = n
	}
}

// WithMiddlewares sets a chain of task middlewares.
// The first middleware passed as argument is treated as the outermost middleware.
// If middlewares were configured previously, the new ones will be innermost.
func WithMiddlewares(chain ...Middleware) Option {
	return func(c *Config) {
		prev := c.chain

		if prev == nil {
			prev = func(next TaskF) TaskF {
				return next
			}
		}

		c.chain = func(next TaskF) TaskF {
			for i := len(chain) - 1; i >= 0; i-- {
				next = chain[i](next)
			}

			return prev(next)
		}
	}
}

var DefaultEventChannel = "celeryev"

// WithEvents enables task events.
// The name of the Celery queue where events are sent, should be provided.
// If no name is provided, events are disabled.
func WithEvents(channel string) Option {
	return func(c *Config) {
		c.eventChannel = channel
	}
}

// WithEventWorkerIdentifier sets the worker identifier.
func WithEventWorkerIdentifier(id string) Option {
	return func(c *Config) {
		c.registry.SetID(id)
	}
}

// WithIngestQueue enables the use of an ingest queue.
// This queue is used to receive tasks early and immediately
// send the task-received event, instead of waiting for a
// worker that has free capacity. Only useful in combination
// with WithEvents.
func WithIngestQueue() Option {
	return func(c *Config) {
		c.ingestQueue = c.queue
		c.queue = fmt.Sprintf("%s-recv", c.queue)
	}
}

// WithHeartbeat sets the heartbeat interval in seconds.
func WithHeartbeat(interval int) Option {
	return func(c *Config) {
		c.heartbeat = interval
	}
}

// WithBackend enables a celery Backend and the recover middleware.
func WithBackend(backend Backend) Option {
	return WithMiddlewares(BackendMiddleware(backend), RecoverMiddleware)
}

// Config represents Celery settings.
type Config struct {
	logger       log.Logger
	broker       Broker
	queue        string
	ingestQueue  string
	registry     *protocol.SerializerRegistry
	mime         string
	protocol     int
	maxWorkers   int
	chain        Middleware
	eventChannel string
	heartbeat    int
}
