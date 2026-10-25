package fsm

import (
	"errors"

	"github.com/monaaoua9/dtask/internal/apierr"
)

// These come back from Apply as the command's result, they're not storage errors.
var (
	ErrNotFound   = apierr.ErrNotFound
	ErrFenced     = apierr.ErrFenced
	ErrWrongState = apierr.ErrWrongState
	ErrExists     = errors.New("task id already exists")
	ErrBadCommand = errors.New("malformed command")
)
