package celery

import (
	"context"
	"errors"
	"reflect"
)

var ErrWrongNumberOfArguments = errors.New("wrong number of arguments")

var ErrLastReturnValueMustBeError = errors.New("last return value must be an error")

// Wrap returns a TaskF that wraps the given function.
//
// The function can have any number of arguments, but the last return value must be an error.
// The function may accept a context as the first argument, in that case it is passed.
// The arguments are passed from the task's Args fields. The Kwargs fields are ignored.
// The result is passed to the task's SetResult method.
func Wrap(taskFunc interface{}) TaskF {
	return func(ctx context.Context, p *TaskParam) error {
		args, err := arguments(ctx, p.args, taskFunc)
		if err != nil {
			return err
		}

		results := reflect.ValueOf(taskFunc).Call(args)

		if len(results) == 0 {
			return ErrLastReturnValueMustBeError
		}

		lastResult := results[len(results)-1]

		if !lastResult.IsNil() {
			errorInterface := reflect.TypeOf((*error)(nil)).Elem()

			if !lastResult.Type().Implements(errorInterface) {
				return ErrLastReturnValueMustBeError
			}

			return lastResult.Interface().(error)
		}

		result := make([]interface{}, len(results)-1)

		for i := 0; i < len(results)-1; i++ {
			result[i] = results[i].Interface()
		}

		p.SetResult(result...)

		return nil
	}
}

func arguments(ctx context.Context, arguments []interface{}, taskFunc interface{}) ([]reflect.Value, error) {
	f := reflect.TypeOf(taskFunc)

	result := make([]reflect.Value, f.NumIn())

	ctxType := reflect.TypeOf((*context.Context)(nil)).Elem()

	for i := 0; i < f.NumIn(); i++ {
		if f.In(i) == ctxType {
			result[i] = reflect.ValueOf(ctx)

			continue
		}

		if len(arguments) == 0 {
			return nil, ErrWrongNumberOfArguments
		}

		result[i] = cast(arguments[0], f.In(i))
		arguments = arguments[1:]
	}

	if len(arguments) > 0 {
		return nil, ErrWrongNumberOfArguments
	}

	return result, nil
}

func cast(v interface{}, t reflect.Type) reflect.Value {
	return reflect.ValueOf(v).Convert(t)
}
