package celery

import (
	"context"
	"errors"
	"testing"
)

func TestWrap(t *testing.T) {
	fail := errors.New("fail")

	f := func(a, b string) (int, error) {
		if a == "fail" {
			return 0, fail
		}

		return len(a) - len(b), nil
	}

	w := Wrap(f)

	err := w(context.Background(), &TaskParam{
		args: []interface{}{"foo", "bar"},
	})
	if err != nil {
		t.Error(err)
	}

	err = w(context.Background(), &TaskParam{
		args: []interface{}{"fail", "bar"},
	})
	if err != fail {
		t.Error(err)
	}
}

func TestWrapContext(t *testing.T) {
	fail := errors.New("fail")

	f := func(ctx context.Context, a, b string) (int, error) {
		if a == "fail" {
			return 0, fail
		}

		return len(a) - len(b), nil
	}

	w := Wrap(f)

	err := w(context.Background(), &TaskParam{
		args: []interface{}{"foo", "bar"},
	})
	if err != nil {
		t.Error(err)
	}

	err = w(context.Background(), &TaskParam{
		args: []interface{}{"fail", "bar"},
	})
	if err != fail {
		t.Error(err)
	}
}
