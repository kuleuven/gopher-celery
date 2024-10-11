package celery

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/marselester/gopher-celery/protocol"
)

func WithBackend(backend Backend) Option {
	return WithMiddlewares(func(f TaskF) TaskF {
		return func(ctx context.Context, p *TaskParam) error {
			var meta map[string]interface{}

			if err := setResult(ctx, backend, protocol.STARTED, nil, meta); err != nil {
				return err
			}

			ctx = context.WithValue(ctx, ContextKeyMetaCallback, func(m map[string]interface{}) error {
				meta = m

				return setResult(ctx, backend, protocol.STARTED, nil, meta)
			})

			err := f(ctx, p)

			if err == nil {
				return setResult(ctx, backend, protocol.SUCCESS, "All good", meta)
			}

			if err1 := setResult(ctx, backend, protocol.FAILURE, err, meta); err1 != nil {
				err = fmt.Errorf("%w, %s", err, err1)
			}

			return err
		}
	})
}

func setResult(ctx context.Context, backend Backend, status protocol.Status, result interface{}, meta map[string]interface{}) error {
	taskID, ok := ctx.Value(ContextKeyTaskID).(string)
	if !ok {
		return nil
	}

	msg := &protocol.Result{
		ID:        taskID,
		Status:    string(status),
		Traceback: nil,
		Result:    result,
		Children:  []interface{}{},
		DateDone:  time.Now().UTC().Format(time.RFC3339Nano),
		Meta:      meta,
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return backend.Store(taskID, payload)
}

var ErrTypeAssertion = fmt.Errorf("type assertion failed")

func SetMeta(ctx context.Context, m map[string]interface{}) error {
	cb, ok := ctx.Value(ContextKeyMetaCallback).(func(m map[string]interface{}) error)
	if !ok {
		return ErrTypeAssertion
	}

	return cb(m)
}
