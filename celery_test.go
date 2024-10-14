package celery

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"

	"github.com/marselester/gopher-celery/goredis"
	"github.com/marselester/gopher-celery/protocol"
)

func TestExecuteTaskPanic(t *testing.T) {
	app := NewApp(WithMiddlewares(RecoverMiddleware))
	app.Register(
		"myproject.apps.myapp.tasks.mytask",
		func(ctx context.Context, p *TaskParam) error {
			_ = p.Args()[100]
			return nil
		},
	)

	m := protocol.Task{
		ID:   "0ad73c66-f4c9-4600-bd20-96746e720eed",
		Name: "myproject.apps.myapp.tasks.mytask",
		Args: []interface{}{"fizz"},
		Kwargs: map[string]interface{}{
			"b": "bazz",
		},
	}

	want := "unexpected task error"
	_, err := app.executeTask(context.Background(), &m)
	if !strings.HasPrefix(err.Error(), want) {
		t.Errorf("expected %q got %q", want, err)
	}
}

func TestExecuteTaskMiddlewares(t *testing.T) {
	// The middlewares are called in the order they were defined, e.g., A -> B -> task.
	tests := map[string]struct {
		middlewares []Middleware
		want        string
	}{
		"A-B-task": {
			middlewares: []Middleware{
				func(next TaskF) TaskF {
					return func(ctx context.Context, p *TaskParam) error {
						err := next(ctx, p)
						return fmt.Errorf("A -> %w", err)
					}
				},
				func(next TaskF) TaskF {
					return func(ctx context.Context, p *TaskParam) error {
						err := next(ctx, p)
						return fmt.Errorf("B -> %w", err)
					}
				},
			},
			want: "A -> B -> task",
		},
		"A-task": {
			middlewares: []Middleware{
				func(next TaskF) TaskF {
					return func(ctx context.Context, p *TaskParam) error {
						err := next(ctx, p)
						return fmt.Errorf("A -> %w", err)
					}
				},
			},
			want: "A -> task",
		},
		"empty chain": {
			middlewares: []Middleware{},
			want:        "task",
		},
		"nil chain": {
			middlewares: nil,
			want:        "task",
		},
	}

	ctx := context.Background()
	m := protocol.Task{
		ID:   "0ad73c66-f4c9-4600-bd20-96746e720eed",
		Name: "myproject.apps.myapp.tasks.mytask",
		Args: []interface{}{"fizz"},
		Kwargs: map[string]interface{}{
			"b": "bazz",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			app := NewApp(
				WithMiddlewares(tc.middlewares...),
			)
			app.Register(
				"myproject.apps.myapp.tasks.mytask",
				func(ctx context.Context, p *TaskParam) error {
					return fmt.Errorf("task")
				},
			)

			_, err := app.executeTask(ctx, &m)
			if !strings.HasPrefix(err.Error(), tc.want) {
				t.Errorf("expected %q got %q", tc.want, err)
			}
		})
	}
}

func TestProduceAndConsume(t *testing.T) {
	app := NewApp(WithLogger(log.NewJSONLogger(os.Stderr)))
	err := app.Delay(
		"myproject.apps.myapp.tasks.mytask",
		DefaultQueue,
		2,
		3,
	)
	if err != nil {
		t.Fatal(err)
	}

	// The test finishes either when ctx times out or the task finishes.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	var sum int
	app.Register(
		"myproject.apps.myapp.tasks.mytask",
		func(ctx context.Context, p *TaskParam) error {
			defer cancel()

			p.NameArgs("a", "b")
			sum = p.MustInt("a") + p.MustInt("b")
			return nil
		},
	)
	if err := app.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		t.Error(err)
	}

	want := 5
	if want != sum {
		t.Errorf("expected sum %d got %d", want, sum)
	}
}

func TestProduceAndConsume100times(t *testing.T) {
	app := NewApp(WithLogger(log.NewJSONLogger(os.Stderr)))
	for i := 0; i < 100; i++ {
		err := app.Delay(
			"myproject.apps.myapp.tasks.mytask",
			DefaultQueue,
			2,
			3,
		)
		if err != nil {
			t.Fatal(err)
		}
	}

	// The test finishes either when ctx times out or all the tasks finish.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	var sum int32
	app.Register(
		"myproject.apps.myapp.tasks.mytask",
		func(ctx context.Context, p *TaskParam) error {
			p.NameArgs("a", "b")
			atomic.AddInt32(
				&sum,
				int32(p.MustInt("a")+p.MustInt("b")),
			)
			return nil
		},
	)
	if err := app.Run(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Error(err)
	}

	var want int32 = 500
	if want != sum {
		t.Errorf("expected sum %d got %d", want, sum)
	}
}

func TestGoredisProduceAndConsume100times(t *testing.T) {
	app := NewApp(
		WithBroker(goredis.NewBroker()),
		WithLogger(log.NewJSONLogger(os.Stderr)),
	)
	for i := 0; i < 100; i++ {
		err := app.Delay(
			"myproject.apps.myapp.tasks.mytask",
			DefaultQueue,
			2,
			3,
		)
		if err != nil {
			t.Fatal(err)
		}
	}

	// The test finishes either when ctx times out or all the tasks finish.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	var sum int32
	app.Register(
		"myproject.apps.myapp.tasks.mytask",
		func(ctx context.Context, p *TaskParam) error {
			p.NameArgs("a", "b")
			atomic.AddInt32(
				&sum,
				int32(p.MustInt("a")+p.MustInt("b")),
			)
			return nil
		},
	)
	if err := app.Run(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Error(err)
	}

	var want int32 = 500
	if want != sum {
		t.Errorf("expected sum %d got %d", want, sum)
	}
}

func TestBackend(t *testing.T) {
	r := goredis.NewBroker()

	app := NewApp(
		WithBroker(r),
		WithLogger(log.NewJSONLogger(os.Stderr)),
		WithBackend(r),
	)

	err := app.Delay(
		"myproject.apps.myapp.tasks.mytask",
		DefaultQueue,
		2,
		3,
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	var taskID string

	app.Register(
		"myproject.apps.myapp.tasks.mytask",
		func(ctx context.Context, p *TaskParam) error {
			taskID = ctx.Value(ContextKeyTaskID).(string)

			panic("panic")

			return nil
		},
	)
	if err := app.Run(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Error(err)
	}

	_, err = r.Load(taskID)
	if err != nil {
		t.Error(err)
	}
}
