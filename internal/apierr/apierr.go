// Package apierr holds the error values that cross package and network
// boundaries, so a worker behaves the same whether it talks to the cluster
// in-process or over gRPC.
package apierr

import "errors"

var (
	ErrNotFound   = errors.New("task not found")
	ErrFenced     = errors.New("stale lease token")
	ErrWrongState = errors.New("task is not in a state that allows this")
	ErrInvalid    = errors.New("invalid argument")
	// ErrUnavailable covers "no leader right now" and transport trouble: retry.
	ErrUnavailable = errors.New("cluster unavailable")
)

// Permanent reports whether retrying the same call can never succeed.
func Permanent(err error) bool {
	return errors.Is(err, ErrNotFound) || errors.Is(err, ErrFenced) ||
		errors.Is(err, ErrWrongState) || errors.Is(err, ErrInvalid)
}
