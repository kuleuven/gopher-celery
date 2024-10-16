package celery

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/marselester/gopher-celery/protocol"
)

func BackendMiddleware(backend Backend) func(next TaskF) TaskF {
	return func(f TaskF) TaskF {
		return func(ctx context.Context, p *TaskParam) error {
			if err := setResult(ctx, backend, protocol.STARTED, nil); err != nil {
				return err
			}

			prev := ctx.Value(ContextKeyUpdateStateCallback).(func(status protocol.Status, meta map[string]interface{}) error)

			ctx = context.WithValue(ctx, ContextKeyUpdateStateCallback, func(status protocol.Status, meta map[string]interface{}) error {
				if err := prev(status, meta); err != nil {
					return err
				}

				return setResult(ctx, backend, status, meta)
			})

			err := f(ctx, p)

			if err == nil {
				return setResult(ctx, backend, protocol.SUCCESS, p.result)
			}

			if err1 := setResult(ctx, backend, protocol.FAILURE, packError(err.Error())); err1 != nil {
				err = fmt.Errorf("%w, %s", err, err1)
			}

			return err
		}
	}
}

func packError(err string) interface{} {
	return map[string]interface{}{
		"exc_type":    "GopherError",
		"exc_message": err,
	}
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
