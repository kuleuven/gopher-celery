package celery

import (
	"context"
	"fmt"
	"runtime/debug"
)

func RecoverMiddleware(next TaskF) TaskF {
	return func(ctx context.Context, p *TaskParam) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("unexpected task error: %v: %s", r, debug.Stack())
			}
		}()

		return next(ctx, p)
	}
}
