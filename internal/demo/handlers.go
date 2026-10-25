// Package demo has the handlers dtaskd registers out of the box, enough to
// try the cluster without writing any code.
package demo

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	pb "github.com/monaaoua9/dtask/api/gen/dtask/v1"
	"github.com/monaaoua9/dtask/internal/worker"
)

func Handlers() map[string]worker.Handler {
	return map[string]worker.Handler{
		"echo":  echo,
		"sleep": sleep,
		"fail":  alwaysFail,
		"flaky": flaky,
		"bad":   badInput,
	}
}

func echo(_ context.Context, t *pb.Task) ([]byte, error) { return t.Payload, nil }

// payload is a duration like "250ms" or a bare number of milliseconds
func sleep(ctx context.Context, t *pb.Task) ([]byte, error) {
	s := strings.TrimSpace(string(t.Payload))
	d, err := time.ParseDuration(s)
	if err != nil {
		ms, err2 := strconv.Atoi(s)
		if err2 != nil {
			return nil, worker.Permanent(errors.New("payload must be a duration or milliseconds"))
		}
		d = time.Duration(ms) * time.Millisecond
	}
	select {
	case <-time.After(d):
		return []byte("slept " + d.String()), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func alwaysFail(context.Context, *pb.Task) ([]byte, error) {
	return nil, errors.New("this handler always fails")
}

// fails until attempt N (payload) to show retries with backoff
func flaky(_ context.Context, t *pb.Task) ([]byte, error) {
	n, _ := strconv.Atoi(strings.TrimSpace(string(t.Payload)))
	if int(t.Attempt) <= n {
		return nil, errors.New("not yet, attempt " + strconv.Itoa(int(t.Attempt)))
	}
	return []byte("worked on attempt " + strconv.Itoa(int(t.Attempt))), nil
}

func badInput(context.Context, *pb.Task) ([]byte, error) {
	return nil, worker.Permanent(errors.New("input can never be valid"))
}
