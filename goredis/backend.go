package goredis

import (
	"fmt"
	"time"
)

func (br *Broker) Store(key string, value []byte) error {
	return br.pool.Set(br.ctx, fmt.Sprintf("celery-task-meta-%s", key), value, 24*time.Hour).Err()
}

func (br *Broker) Load(key string) ([]byte, error) {
	return br.pool.Get(br.ctx, fmt.Sprintf("celery-task-meta-%s", key)).Bytes()
}
