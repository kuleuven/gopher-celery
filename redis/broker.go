// Package redis implements a Celery broker using Redis
// and github.com/gomodule/redigo.
package redis

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/gomodule/redigo/redis"
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
//
// Note, the read timeout you specified with redis.DialReadTimeout() method
// should be bigger than the receive timeout.
// Otherwise redigo would return i/o timeout error.
func WithReceiveTimeout(timeout time.Duration) BrokerOption {
	return func(br *Broker) {
		sec := int(timeout.Seconds())
		if sec <= 0 {
			sec = 1
		}
		br.receiveTimeout = sec
	}
}

// WithPool sets Redis connection pool.
func WithPool(pool *redis.Pool) BrokerOption {
	return func(br *Broker) {
		br.pool = pool
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
// By default it connects to localhost.
func NewBroker(options ...BrokerOption) *Broker {
	br := Broker{
		receiveTimeout: DefaultReceiveTimeout,
		restored:       make(map[string][][]byte),
		db:             -1,
	}

	for _, opt := range options {
		opt(&br)
	}

	if br.pool == nil {
		br.pool = &redis.Pool{
			Dial: func() (redis.Conn, error) {
				return redis.DialURL("redis://localhost")
			},
		}
	}

	return &br
}

// Broker is a Redis broker that sends/receives messages from specified queues.
type Broker struct {
	pool           *redis.Pool
	receiveTimeout int
	ack            bool
	workerID       int
	restored       map[string][][]byte
	db             int
}

// Send inserts the specified message at the head of the topic queue using LPUSH command.
// Note, the method is safe to call concurrently.
func (br *Broker) Send(m []byte, q string) error {
	conn := br.pool.Get()
	defer conn.Close()

	_, err := conn.Do("LPUSH", q, m)

	return err
}

// SendFanout inserts the specified message at the head of the fanout queue using PUBLISH command.
// Note, the method is safe to call concurrently.
func (br *Broker) SendFanout(m []byte, q string, routingKey string) error {
	if err := br.fetchDB(); err != nil {
		return err
	}

	conn := br.pool.Get()
	defer conn.Close()

	name := fmt.Sprintf("/%d.%s/%s", br.db, q, routingKey)

	if routingKey == "" {
		name = fmt.Sprintf("/%d.%s", br.db, q)
	}

	_, err := conn.Do("PUBLISH", name, m)

	return err
}

func (br *Broker) fetchDB() error {
	if br.db >= 0 {
		return nil
	}

	conn := br.pool.Get()
	defer conn.Close()

	reply, err := redis.Bytes(conn.Do("CLIENT", "INFO"))
	if err != nil {
		return err
	}

	parts := bytes.Split(reply, []byte(" "))

	for _, part := range parts {
		if bytes.HasPrefix(part, []byte("db=")) {
			br.db, _ = strconv.Atoi(string(part[3:]))

			return nil
		}
	}

	return fmt.Errorf("unable to fetch DB number")
}

// Receive fetches a Celery task message from a tail of one of the topic queues in Redis.
// After a timeout it returns nil, nil.
// Note, the method is not concurrency safe.
func (br *Broker) Receive(queue string) ([]byte, error) {
	conn := br.pool.Get()
	defer conn.Close()

	if br.ack {
		if item, err := br.MaybeRestore(queue); err != nil || item != nil {
			return item, err
		}

		res, err := redis.Bytes(conn.Do(
			"BLMOVE",
			redis.Args{}.Add(queue, fmt.Sprintf("%s-unacked-%d", queue, br.workerID), "LEFT", "RIGHT").Add(br.receiveTimeout)...,
		))
		if err == redis.ErrNil {
			return nil, nil
		}

		return res, err
	}

	// See the discussion regarding timeout and Context cancellation
	// https://github.com/gomodule/redigo/issues/207#issuecomment-283815775.
	res, err := redis.ByteSlices(conn.Do(
		"BRPOP",
		redis.Args{}.Add(queue).Add(br.receiveTimeout)...,
	))
	if err == redis.ErrNil {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	return res[1], nil
}

func (br *Broker) MaybeRestore(queue string) ([]byte, error) {
	if _, restored := br.restored[queue]; !restored {
		conn := br.pool.Get()
		defer conn.Close()

		values, err := redis.ByteSlices(conn.Do("LRANGE", fmt.Sprintf("%s-unacked-%d", queue, br.workerID), 0, -1))
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

	return msg, nil
}

func (br *Broker) Ack(queue string, message []byte) error {
	if !br.ack {
		return nil
	}

	conn := br.pool.Get()
	defer conn.Close()

	_, err := conn.Do("LREM", fmt.Sprintf("%s-unacked-%d", queue, br.workerID), 1, message)

	return err
}

func (br *Broker) Reject(queue string, message []byte) error {
	if !br.ack {
		return nil
	}

	conn := br.pool.Get()
	defer conn.Close()

	if err := conn.Send("LREM", fmt.Sprintf("%s-unacked-%d", queue, br.workerID), 1, message); err != nil {
		return err
	}

	if err := conn.Send("LPUSH", queue, message); err != nil {
		return err
	}

	if err := conn.Flush(); err != nil {
		return err
	}

	_, err := conn.Receive()

	return err
}

func (br *Broker) SubscribeFanout(ctx context.Context, queue string, routingKey string, ch chan<- []byte) error {
	if err := br.fetchDB(); err != nil {
		return err
	}

	conn := br.pool.Get()
	defer conn.Close()

	psc := redis.PubSubConn{Conn: conn}

	name := fmt.Sprintf("/%d.%s/%s", br.db, queue, routingKey)

	if routingKey == "" {
		name = fmt.Sprintf("/%d.%s", br.db, queue)
	}

	if err := psc.Subscribe(name); err != nil {
		return err
	}

	defer psc.Unsubscribe()

	for {
		switch v := psc.ReceiveContext(ctx).(type) {
		case error:
			return v
		case redis.Message:
			ch <- v.Data
		}
	}
}
