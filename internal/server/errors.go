package server

import (
	"context"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/monaaoua9/dtask/internal/apierr"
	"github.com/monaaoua9/dtask/internal/raft"
)

// trailer keys the client library looks at
const (
	TrailerLeaderID      = "dtask-leader-id"
	TrailerLeaderAddr    = "dtask-leader-addr"
	TrailerOutcomeUnsure = "dtask-outcome-unknown"
)

// ToStatus maps domain errors onto gRPC codes. Unavailable always means
// "retry, probably somewhere else"; the trailer says where.
func ToStatus(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}

	var nl *raft.NotLeaderError
	switch {
	case errors.As(err, &nl):
		_ = grpc.SetTrailer(ctx, metadata.Pairs(TrailerLeaderID, string(nl.LeaderID), TrailerLeaderAddr, nl.LeaderAddr))
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, raft.ErrNotLeader), errors.Is(err, raft.ErrLeaderNotReady),
		errors.Is(err, raft.ErrConfigInProgress), errors.Is(err, raft.ErrStopped), errors.Is(err, raft.ErrTransferring):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, raft.ErrLeadershipLost):
		_ = grpc.SetTrailer(ctx, metadata.Pairs(TrailerOutcomeUnsure, "1"))
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, apierr.ErrInvalid), errors.Is(err, raft.ErrLastVoter):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, apierr.ErrNotFound), errors.Is(err, raft.ErrUnknownServer):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, apierr.ErrFenced):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, apierr.ErrWrongState):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}
