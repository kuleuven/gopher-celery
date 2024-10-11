// Package goredis implements a Celery broker using Redis
// and https://github.com/redis/go-redis.
package goredis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultReceiveTimeout defines how many seconds the broker's Receive command
// should block waiting for results from Redis.
const DefaultReceiveTimeout = 5

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

// WithClient sets Redis client representing a pool of connections.
func WithClient(c *redis.Client) BrokerOption {
	return func(br *Broker) {
		br.pool = c
	}
}

// WithAcknowledgements makes sure tasks are not lost from redis if the worker
// crashes during execution. A unique worker ID is required for this to work.
// If multiple parallel workers are used, they should use different worker IDs.
// Note that if a worker is gone, an external mechanism must be used to
// restart the worker.
func WithAcknowledgements(id int) BrokerOption {
	return func(br *Broker) {
		br.ack = true
		br.workerID = id
	}
}

// NewBroker creates a broker backed by Redis.
// By default, it connects to localhost.
func NewBroker(options ...BrokerOption) *Broker {
	br := Broker{
		receiveTimeout: DefaultReceiveTimeout * time.Second,
		ctx:            context.Background(),
		restored:       make(map[string][]string),
	}

	for _, opt := range options {
		opt(&br)
	}

	if br.pool == nil {
		br.pool = redis.NewClient(&redis.Options{})
	}

	if !br.ack {
		return &br
	}

	return &br
}

// Broker is a Redis broker that sends/receives messages from specified queues.
type Broker struct {
	pool           *redis.Client
	receiveTimeout time.Duration
	ack            bool
	workerID       int
	ctx            context.Context
	restored       map[string][]string
}

// Send inserts the specified message at the head of the queue using LPUSH command.
// Note, the method is safe to call concurrently.
func (br *Broker) Send(m []byte, q string) error {
	return br.pool.LPush(br.ctx, q, m).Err()
}

// Receive fetches a Celery task message from a tail of one of the queues in Redis.
// After a timeout it returns nil, nil.
//
// Note, the method is not concurrency safe.
func (br *Broker) Receive(queue string) ([]byte, error) {
	if br.ack {
		if item, err := br.MaybeRestore(queue); err != nil || item != nil {
			return item, err
		}

		res := br.pool.BLMove(br.ctx, queue, fmt.Sprintf("%s-unacked-%d", queue, br.workerID), "LEFT", "RIGHT", br.receiveTimeout)
		if err := res.Err(); err == redis.Nil {
			return nil, nil
		} else if err != nil {
			return nil, err
		}

		return []byte(res.Val()), nil
	}

	res := br.pool.BRPop(br.ctx, br.receiveTimeout, queue)
	if err := res.Err(); err == redis.Nil {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	if len(res.Val()) != 2 {
		return nil, fmt.Errorf("unexpected BRPOP result: %v", res.Val())
	}

	return []byte(res.Val()[1]), nil
}

func (br *Broker) MaybeRestore(queue string) ([]byte, error) {
	if _, restored := br.restored[queue]; !restored {
		values, err := br.pool.LRange(br.ctx, fmt.Sprintf("%s-unacked-%d", queue, br.workerID), 0, -1).Result()
		if err != nil {
			return nil, err
		}

		br.restored[queue] = values
	}

	if len(br.restored[queue]) == 0 {
		return nil, nil
	}

	msg := br.restored[queue][0]

	br.restored[queue] = br.restored[queue][1:]

	return []byte(msg), nil
}

func (br *Broker) Ack(queue string, message []byte) error {
	if !br.ack {
		return nil
	}

	return br.pool.LRem(br.ctx, fmt.Sprintf("%s-unacked-%d", queue, br.workerID), 1, message).Err()
}

func (br *Broker) Reject(queue string, message []byte) error {
	if !br.ack {
		return nil
	}

	pipe := br.pool.Pipeline()
	pipe.LRem(br.ctx, fmt.Sprintf("%s-unacked-%d", queue, br.workerID), 1, message)
	pipe.LPush(br.ctx, queue, message)

	_, err := pipe.Exec(br.ctx)

	return err
}
