// Package goredis implements a Celery broker using Redis
// and https://github.com/redis/go-redis.
package goredis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/marselester/gopher-celery/internal/broker"
)

// DefaultReceiveTimeout defines how many seconds the broker's Receive command
// should block waiting for results from Redis.
const DefaultReceiveTimeout = 5

// DefaultVisibilityTimeout defines how long the broker's visibility timeout
// is set to in seconds.
const DefaultVisibilityTimeout = 3600

// BrokerOption sets up a Broker.
type BrokerOption func(*Broker)

// WithReceiveTimeout sets a timeout of how long the broker's Receive command
// should block waiting for results from Redis.
// Larger the timeout, longer the client will have to wait for Celery app to exit.
// Smaller the timeout, more BRPOP commands would have to be sent to Redis.
func WithReceiveTimeout(timeout time.Duration) BrokerOption {
	return func(br *Broker) {
		br.receiveTimeout = timeout
	}
}

// WithVisibilityTimeout sets how long the broker's visibility timeout
// is set to in seconds.
func WithVisibilityTimeout(timeout time.Duration) BrokerOption {
	return func(br *Broker) {
		br.visibilityTimeout = timeout
	}
}

// WithClient sets Redis client representing a pool of connections.
func WithClient(c *redis.Client) BrokerOption {
	return func(br *Broker) {
		br.pool = c
	}
}

// WithAcknowledgements sets Redis ack support.
func WithAcknowledgements() BrokerOption {
	return func(br *Broker) {
		br.ack = true
	}
}

// NewBroker creates a broker backed by Redis.
// By default, it connects to localhost.
func NewBroker(options ...BrokerOption) *Broker {
	br := Broker{
		receiveTimeout:    DefaultReceiveTimeout * time.Second,
		visibilityTimeout: DefaultVisibilityTimeout * time.Second,
		ctx:               context.Background(),
	}
	for _, opt := range options {
		opt(&br)
	}

	if br.pool == nil {
		br.pool = redis.NewClient(&redis.Options{})
	}
	return &br
}

// Broker is a Redis broker that sends/receives messages from specified queues.
type Broker struct {
	pool              *redis.Client
	queues            []string
	receiveTimeout    time.Duration
	visibilityTimeout time.Duration
	ack               bool
	ctx               context.Context
}

// Send inserts the specified message at the head of the queue using LPUSH command.
// Note, the method is safe to call concurrently.
func (br *Broker) Send(m []byte, q string, delivery_tag, exchange, routing_key string) error {
	payload, err := json.Marshal([]interface{}{
		m,
		exchange,
		routing_key,
	})
	if err != nil {
		return err
	}

	pipe := br.pool.Pipeline()

	pipe.LPush(br.ctx, q, m)

	if br.ack {
		pipe.ZAdd(br.ctx, "unacked_index", redis.Z{
			Score:  float64(time.Now().Unix()),
			Member: delivery_tag,
		})

		pipe.HSet(br.ctx, "unacked", delivery_tag, string(payload))
	}

	_, err = pipe.Exec(br.ctx)

	return err
}

// Observe sets the queues from which the tasks should be received.
// Note, the method is not concurrency safe.
func (br *Broker) Observe(queues []string) {
	br.queues = queues
}

// Receive fetches a Celery task message from a tail of one of the queues in Redis.
// After a timeout it returns nil, nil.
//
// Celery relies on BRPOP command to process messages fairly, see https://github.com/celery/kombu/issues/166.
// Redis BRPOP is a blocking list pop primitive.
// It blocks the connection when there are no elements to pop from any of the given lists.
// An element is popped from the tail of the first list that is non-empty,
// with the given keys being checked in the order that they are given,
// see https://redis.io/commands/brpop/.
//
// Note, the method is not concurrency safe.
func (br *Broker) Receive() ([]byte, error) {
	res := br.pool.BRPop(br.ctx, br.receiveTimeout, br.queues...)
	err := res.Err()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to BRPOP %v: %w", br.queues, err)
	}

	// Put the Celery queue name to the end of the slice for fair processing.
	q := res.Val()[0]
	b := res.Val()[1]
	broker.Move2back(br.queues, q)
	return []byte(b), nil
}

func (br *Broker) Ack(tag string) error {
	if !br.ack {
		return nil
	}

	pipe := br.pool.Pipeline()

	pipe.ZRem(br.ctx, "unacked_index", tag)
	pipe.HDel(br.ctx, "unacked", tag)

	_, err := pipe.Exec(br.ctx)
	if err != nil {
		return err
	}

	return nil
}

func (br *Broker) ExtendLifetime(tag string) error {
	if !br.ack {
		return nil
	}

	return br.pool.ZAdd(br.ctx, "unacked_index", redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: tag,
	}).Err()
}

func (br *Broker) RefreshLifetime(tag string) func() bool {
	timer := time.NewTimer(br.visibilityTimeout / 2)

	go func() {
		defer timer.Stop()

		for range timer.C {
			if err := br.ExtendLifetime(tag); err != nil {
				return
			}
		}
	}()

	return timer.Stop
}
