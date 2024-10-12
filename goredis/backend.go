package goredis

import (
	"fmt"
	"time"
)

func (br *Broker) Store(key string, value []byte) error {
	pipe := br.pool.Pipeline()

	pipe.Set(br.ctx, fmt.Sprintf("celery-task-meta-%s", key), value, 24*time.Hour)
	pipe.Publish(br.ctx, fmt.Sprintf("celery-task-meta-%s", key), value)

	_, err := pipe.Exec(br.ctx)

	return err
}

func (br *Broker) Load(key string) ([]byte, error) {
	return br.pool.Get(br.ctx, fmt.Sprintf("celery-task-meta-%s", key)).Bytes()
}
