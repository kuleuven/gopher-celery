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
			if err := setResult(ctx, backend, protocol.STARTED, nil); err != nil {
				return err
			}

			ctx = context.WithValue(ctx, ContextKeyUpdateStateCallback, func(status protocol.Status, meta map[string]interface{}) error {
				return setResult(ctx, backend, status, meta)
			})

			err := call(ctx, f, p)

			if err == nil {
				return setResult(ctx, backend, protocol.SUCCESS, p.result)
			}

			if err1 := setResult(ctx, backend, protocol.FAILURE, err.Error()); err1 != nil {
				err = fmt.Errorf("%w, %s", err, err1)
			}

			return err
		}
	})
}

func setResult(ctx context.Context, backend Backend, status protocol.Status, result interface{}) error {
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
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return backend.Store(taskID, payload)
}

var ErrTypeAssertion = fmt.Errorf("type assertion failed")

func UpdateState(ctx context.Context, status protocol.Status, meta map[string]interface{}) error {
	cb, ok := ctx.Value(ContextKeyUpdateStateCallback).(func(protocol.Status, map[string]interface{}) error)
	if !ok {
		return ErrTypeAssertion
	}

	return cb(status, meta)
}
