package goredis

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
)

func TestReceive(t *testing.T) {
	q := "goredisq"
	br := NewBroker(WithReceiveTimeout(time.Second))
	br.Observe([]string{q})

	tests := map[string]struct {
		input []string
		want  []byte
		err   error
	}{
		"timeout": {
			input: nil,
			want:  nil,
			err:   nil,
		},
		"one-msg": {
			input: []string{"{}"},
			want:  []byte("{}"),
			err:   nil,
		},
		"oldest-msg": {
			input: []string{"1", "2"},
			want:  []byte("1"),
			err:   nil,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Cleanup(func() {
				br.pool.Del(context.Background(), q)
			})

			for _, m := range tc.input {
				if err := br.Send([]byte(m), q, uuid.NewString(), q, q); err != nil {
					t.Fatal(err)
				}
			}

			got, err := br.Receive()
			if err != tc.err {
				t.Fatal(err)
			}

			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Error(diff, string(got))
			}
		})
	}
}
