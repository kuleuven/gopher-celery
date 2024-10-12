package redis

import (
	"fmt"

	"github.com/gomodule/redigo/redis"
)

func (br *Broker) Store(key string, value []byte) error {
	conn := br.pool.Get()
	defer conn.Close()

	if err := conn.Send("SETEX", fmt.Sprintf("celery-task-meta-%s", key), value, 86400); err != nil {
		return err
	}

	if err := conn.Send("PUBLISH", fmt.Sprintf("celery-task-meta-%s", key), value); err != nil {
		return err
	}

	if err := conn.Flush(); err != nil {
		return err
	}

	_, err := conn.Receive()

	return err
}

func (br *Broker) Load(key string) ([]byte, error) {
	conn := br.pool.Get()
	defer conn.Close()

	return redis.Bytes(conn.Do(
		"GET",
		redis.Args{}.AddFlat(fmt.Sprintf("celery-task-meta-%s", key)),
	))
}
