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
			if err := SetResult(ctx, backend, protocol.STARTED, nil); err != nil {
				return err
			}

			err := f(ctx, p)

			if err == nil {
				return SetResult(ctx, backend, protocol.SUCCESS, "All good")
			}

			if err1 := SetResult(ctx, backend, protocol.FAILURE, err); err1 != nil {
				err = fmt.Errorf("%w, %s", err, err1)
			}

			return err
		}
	})
}

func SetResult(ctx context.Context, backend Backend, status protocol.Status, result interface{}) error {
	taskID, ok := ctx.Value(ContextKeyTaskName).(string)
	if !ok {
		return nil
	}

	msg := &protocol.Result{
		ID:        taskID,
		Status:    string(status),
		Traceback: nil,
		Result:    result,
		Children:  nil,
		DateDone:  time.Now().UTC().Format(time.RFC3339Nano),
		Meta:      nil,
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return backend.Store(taskID, payload)
}
