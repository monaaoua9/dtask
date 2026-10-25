// Package client is the Go client for a dtask cluster. It finds the leader,
// follows redirects when leadership moves and retries transient failures with
// jittered backoff. Submit attaches an idempotency key when you don't, so a
// retry after a lost response can't enqueue the task twice.
package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pb "github.com/monaaoua9/dtask/api/gen/dtask/v1"
	"github.com/monaaoua9/dtask/internal/apierr"
	"github.com/monaaoua9/dtask/internal/backoff"
)

var (
	ErrNotFound    = apierr.ErrNotFound
	ErrFenced      = apierr.ErrFenced
	ErrWrongState  = apierr.ErrWrongState
	ErrInvalid     = apierr.ErrInvalid
	ErrUnavailable = apierr.ErrUnavailable
)

const (
	trailerLeaderAddr = "dtask-leader-addr"
	maxMsg            = 256 << 20
)

type Option func(*Client)

func WithCredentials(c credentials.TransportCredentials) Option {
	return func(cl *Client) { cl.creds = c }
}

// WithMaxAttempts caps how many attempts one call may burn through when its
// context has no deadline. With a deadline, the deadline is the limit.
func WithMaxAttempts(n int) Option { return func(cl *Client) { cl.attempts = n } }

type Client struct {
	seeds    []string
	creds    credentials.TransportCredentials
	attempts int
	rng      *backoff.Rand

	mu     sync.Mutex
	conns  map[string]*grpc.ClientConn
	leader string
	next   int
}

func Dial(seeds []string, opts ...Option) (*Client, error) {
	if len(seeds) == 0 {
		return nil, errors.New("client: at least one seed address is required")
	}
	c := &Client{
		seeds: seeds, creds: insecure.NewCredentials(), attempts: 8,
		rng: backoff.NewRand(time.Now().UnixNano()), conns: map[string]*grpc.ClientConn{},
	}
	for _, o := range opts {
		o(c)
	}
	return c, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var first error
	for _, cc := range c.conns {
		if err := cc.Close(); err != nil && first == nil {
			first = err
		}
	}
	c.conns = map[string]*grpc.ClientConn{}
	return first
}

func (c *Client) conn(addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cc, ok := c.conns[addr]; ok {
		return cc, nil
	}
	cc, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(c.creds),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsg)),
	)
	if err != nil {
		return nil, err
	}
	c.conns[addr] = cc
	return cc, nil
}

func (c *Client) target() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leader != "" {
		return c.leader
	}
	t := c.seeds[c.next%len(c.seeds)]
	c.next++
	return t
}

func (c *Client) setLeader(addr string) {
	c.mu.Lock()
	c.leader = addr
	c.mu.Unlock()
}

// do runs fn against the believed leader, chasing redirects and retrying
// Unavailable answers. Everything else is the caller's problem.
func (c *Client) do(ctx context.Context, fn func(cc *grpc.ClientConn, opts ...grpc.CallOption) error) error {
	pol := backoff.Policy{Base: 25 * time.Millisecond, Max: time.Second}
	var last error
	// with a deadline the caller decides how long we keep trying; without
	// one, give up after a bounded number of attempts
	_, hasDeadline := ctx.Deadline()
	for attempt := 0; hasDeadline || attempt < c.attempts; attempt++ {
		addr := c.target()
		cc, err := c.conn(addr)
		if err != nil {
			return err
		}
		var trailer metadata.MD
		err = fn(cc, grpc.Trailer(&trailer))
		if err == nil {
			c.setLeader(addr)
			return nil
		}
		last = err
		st, _ := status.FromError(err)
		if st.Code() != codes.Unavailable && st.Code() != codes.DeadlineExceeded {
			return convert(err)
		}
		if ctx.Err() != nil {
			return convert(err)
		}

		// follow the hint if there is one, otherwise forget and rotate seeds
		if hint := trailer.Get(trailerLeaderAddr); len(hint) > 0 && hint[0] != "" && hint[0] != addr {
			c.setLeader(hint[0])
			continue // a redirect isn't a failure, no sleep
		}
		c.setLeader("")
		t := time.NewTimer(pol.Delay(attempt, c.rng) + 10*time.Millisecond)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return convert(last)
		}
	}
	return convert(last)
}

func convert(err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	var sentinel error
	switch st.Code() {
	case codes.NotFound:
		sentinel = ErrNotFound
	case codes.Aborted:
		sentinel = ErrFenced
	case codes.FailedPrecondition:
		sentinel = ErrWrongState
	case codes.InvalidArgument:
		sentinel = ErrInvalid
	case codes.Unavailable, codes.DeadlineExceeded:
		sentinel = ErrUnavailable
	default:
		return err
	}
	return fmt.Errorf("%w: %s", sentinel, st.Message())
}

func newKey() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func (c *Client) Submit(ctx context.Context, req *pb.SubmitRequest) (*pb.SubmitResponse, error) {
	if req.IdempotencyKey == "" {
		req = proto.Clone(req).(*pb.SubmitRequest)
		req.IdempotencyKey = newKey()
	}
	var out *pb.SubmitResponse
	err := c.do(ctx, func(cc *grpc.ClientConn, o ...grpc.CallOption) (err error) {
		out, err = pb.NewTaskServiceClient(cc).Submit(ctx, req, o...)
		return
	})
	return out, err
}

func (c *Client) GetTask(ctx context.Context, req *pb.GetTaskRequest) (*pb.GetTaskResponse, error) {
	var out *pb.GetTaskResponse
	err := c.do(ctx, func(cc *grpc.ClientConn, o ...grpc.CallOption) (err error) {
		out, err = pb.NewTaskServiceClient(cc).GetTask(ctx, req, o...)
		return
	})
	return out, err
}

func (c *Client) ListTasks(ctx context.Context, req *pb.ListTasksRequest) (*pb.ListTasksResponse, error) {
	var out *pb.ListTasksResponse
	err := c.do(ctx, func(cc *grpc.ClientConn, o ...grpc.CallOption) (err error) {
		out, err = pb.NewTaskServiceClient(cc).ListTasks(ctx, req, o...)
		return
	})
	return out, err
}

func (c *Client) CancelTask(ctx context.Context, req *pb.CancelTaskRequest) (*pb.CancelTaskResponse, error) {
	var out *pb.CancelTaskResponse
	err := c.do(ctx, func(cc *grpc.ClientConn, o ...grpc.CallOption) (err error) {
		out, err = pb.NewTaskServiceClient(cc).CancelTask(ctx, req, o...)
		return
	})
	return out, err
}

func (c *Client) Lease(ctx context.Context, req *pb.LeaseRequest) (*pb.LeaseResponse, error) {
	var out *pb.LeaseResponse
	err := c.do(ctx, func(cc *grpc.ClientConn, o ...grpc.CallOption) (err error) {
		out, err = pb.NewTaskServiceClient(cc).Lease(ctx, req, o...)
		return
	})
	return out, err
}

func (c *Client) ExtendLease(ctx context.Context, req *pb.ExtendLeaseRequest) (*pb.ExtendLeaseResponse, error) {
	var out *pb.ExtendLeaseResponse
	err := c.do(ctx, func(cc *grpc.ClientConn, o ...grpc.CallOption) (err error) {
		out, err = pb.NewTaskServiceClient(cc).ExtendLease(ctx, req, o...)
		return
	})
	return out, err
}

func (c *Client) Complete(ctx context.Context, req *pb.CompleteRequest) (*pb.CompleteResponse, error) {
	var out *pb.CompleteResponse
	err := c.do(ctx, func(cc *grpc.ClientConn, o ...grpc.CallOption) (err error) {
		out, err = pb.NewTaskServiceClient(cc).Complete(ctx, req, o...)
		return
	})
	return out, err
}

func (c *Client) Fail(ctx context.Context, req *pb.FailRequest) (*pb.FailResponse, error) {
	var out *pb.FailResponse
	err := c.do(ctx, func(cc *grpc.ClientConn, o ...grpc.CallOption) (err error) {
		out, err = pb.NewTaskServiceClient(cc).Fail(ctx, req, o...)
		return
	})
	return out, err
}

func (c *Client) Release(ctx context.Context, req *pb.ReleaseRequest) (*pb.ReleaseResponse, error) {
	var out *pb.ReleaseResponse
	err := c.do(ctx, func(cc *grpc.ClientConn, o ...grpc.CallOption) (err error) {
		out, err = pb.NewTaskServiceClient(cc).Release(ctx, req, o...)
		return
	})
	return out, err
}

func (c *Client) QueueStats(ctx context.Context) (*pb.QueueStatsResponse, error) {
	var out *pb.QueueStatsResponse
	err := c.do(ctx, func(cc *grpc.ClientConn, o ...grpc.CallOption) (err error) {
		out, err = pb.NewAdminServiceClient(cc).QueueStats(ctx, &pb.QueueStatsRequest{}, o...)
		return
	})
	return out, err
}

func (c *Client) SetQueueConfig(ctx context.Context, cfg *pb.QueueConfig) error {
	return c.do(ctx, func(cc *grpc.ClientConn, o ...grpc.CallOption) error {
		_, err := pb.NewAdminServiceClient(cc).SetQueueConfig(ctx, &pb.SetQueueConfigRequest{Config: cfg}, o...)
		return err
	})
}

func (c *Client) AddServer(ctx context.Context, id, addr string) error {
	return c.do(ctx, func(cc *grpc.ClientConn, o ...grpc.CallOption) error {
		_, err := pb.NewAdminServiceClient(cc).AddServer(ctx, &pb.AddServerRequest{Id: id, Addr: addr}, o...)
		return err
	})
}

func (c *Client) RemoveServer(ctx context.Context, id string) error {
	return c.do(ctx, func(cc *grpc.ClientConn, o ...grpc.CallOption) error {
		_, err := pb.NewAdminServiceClient(cc).RemoveServer(ctx, &pb.RemoveServerRequest{Id: id}, o...)
		return err
	})
}

// NodeStatus asks one specific node, followers included, how it sees the cluster.
func (c *Client) NodeStatus(ctx context.Context, addr string) (*pb.StatusResponse, error) {
	cc, err := c.conn(addr)
	if err != nil {
		return nil, err
	}
	resp, err := pb.NewAdminServiceClient(cc).Status(ctx, &pb.StatusRequest{})
	return resp, convert(err)
}
