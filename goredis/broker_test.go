package goredis

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestReceive(t *testing.T) {
	q := "goredisq"
	br := NewBroker(WithReceiveTimeout(time.Second))

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
				if err := br.Send([]byte(m), q); err != nil {
					t.Fatal(err)
				}
			}

			got, err := br.Receive(q)
			if err != tc.err {
				t.Fatal(err)
			}

			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Error(diff, string(got))
			}
		})
	}
}

func TestSendFanout(t *testing.T) {
	q := "redigoq"
	br := NewBroker(WithReceiveTimeout(time.Second))

	if err := br.SendFanout([]byte("{}"), q, "foo"); err != nil {
		t.Fatal(err)
	}
}
