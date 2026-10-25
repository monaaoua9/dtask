// Package server wires the gRPC and HTTP surfaces: peer RPCs, the task and
// admin APIs, health checks and the Prometheus endpoint.
package server

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime/debug"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	pb "github.com/monaaoua9/dtask/api/gen/dtask/v1"
	"github.com/monaaoua9/dtask/internal/cluster"
	"github.com/monaaoua9/dtask/internal/raft"
	"github.com/monaaoua9/dtask/internal/raft/grpctransport"
)

type GRPC struct {
	srv    *grpc.Server
	health *health.Server
	lis    net.Listener
	log    *slog.Logger
}

type Options struct {
	Listen   string
	Creds    credentials.TransportCredentials
	Handler  raft.RPCHandler
	Service  *cluster.Service
	Registry prometheus.Registerer
	Logger   *slog.Logger
}

func NewGRPC(o Options) (*GRPC, error) {
	lis, err := net.Listen("tcp", o.Listen)
	if err != nil {
		return nil, err
	}
	m := newRPCMetrics(o.Registry)

	sopts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(recoverInterceptor(o.Logger), instrument(m, o.Logger)),
		grpc.MaxRecvMsgSize(256 << 20),
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 30 * time.Second, Timeout: 5 * time.Second}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 5 * time.Second, PermitWithoutStream: true}),
	}
	if o.Creds != nil {
		sopts = append(sopts, grpc.Creds(o.Creds))
	}
	srv := grpc.NewServer(sopts...)

	pb.RegisterRaftServiceServer(srv, &grpctransport.Server{H: o.Handler})
	pb.RegisterTaskServiceServer(srv, o.Service)
	pb.RegisterAdminServiceServer(srv, o.Service)
	hs := health.NewServer()
	grpc_health_v1.RegisterHealthServer(srv, hs)
	reflection.Register(srv)

	return &GRPC{srv: srv, health: hs, lis: lis, log: o.Logger}, nil
}

func (g *GRPC) Addr() string { return g.lis.Addr().String() }

func (g *GRPC) Serve() error { return g.srv.Serve(g.lis) }

func (g *GRPC) SetServing(ok bool) {
	st := grpc_health_v1.HealthCheckResponse_NOT_SERVING
	if ok {
		st = grpc_health_v1.HealthCheckResponse_SERVING
	}
	g.health.SetServingStatus("", st)
}

// GracefulStop lets in-flight RPCs finish; past the deadline it cuts them off.
func (g *GRPC) GracefulStop(ctx context.Context) {
	done := make(chan struct{})
	go func() { g.srv.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		g.srv.Stop()
	}
}

func (g *GRPC) Stop() { g.srv.Stop() }

type rpcMetrics struct {
	calls   *prometheus.CounterVec
	latency *prometheus.HistogramVec
}

func newRPCMetrics(reg prometheus.Registerer) *rpcMetrics {
	m := &rpcMetrics{
		calls: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "dtask", Subsystem: "grpc", Name: "requests_total", Help: "RPCs served by method and code."}, []string{"method", "code"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "dtask", Subsystem: "grpc", Name: "request_seconds", Help: "RPC latency by method.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 16),
		}, []string{"method"}),
	}
	if reg != nil {
		reg.MustRegister(m.calls, m.latency)
	}
	return m
}

func recoverInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				log.Error("panic in rpc handler", "method", info.FullMethod, "panic", r, "stack", string(debug.Stack()))
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return h(ctx, req)
	}
}

func instrument(m *rpcMetrics, log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := h(ctx, req)
		err = ToStatus(ctx, err)
		code := status.Code(err)
		m.calls.WithLabelValues(info.FullMethod, code.String()).Inc()
		m.latency.WithLabelValues(info.FullMethod).Observe(time.Since(start).Seconds())
		if code == codes.Internal {
			log.Error("rpc failed", "method", info.FullMethod, "err", err)
		}
		return resp, err
	}
}

// HTTP serves /metrics, /healthz (process is up) and /readyz (this node has
// a leader it can reach, so it can take traffic).
type HTTP struct {
	srv *http.Server
	lis net.Listener
}

func NewHTTP(addr string, gatherer prometheus.Gatherer, ready func() bool, debugHandlers bool) (*HTTP, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready() {
			http.Error(w, "not ready\n", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ready\n"))
	})
	if debugHandlers {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	return &HTTP{srv: &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}, lis: lis}, nil
}

func (h *HTTP) Addr() string { return h.lis.Addr().String() }

func (h *HTTP) Serve() error { return h.srv.Serve(h.lis) }

func (h *HTTP) Shutdown(ctx context.Context) error { return h.srv.Shutdown(ctx) }
