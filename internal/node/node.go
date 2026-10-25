// Package node assembles one cluster member: Raft, the task state machine,
// the leader-side service, scheduler loops, the worker pool and the network
// servers, and owns their startup and shutdown order.
package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"google.golang.org/grpc/credentials"

	"github.com/monaaoua9/dtask/internal/cluster"
	"github.com/monaaoua9/dtask/internal/fsm"
	"github.com/monaaoua9/dtask/internal/raft"
	"github.com/monaaoua9/dtask/internal/raft/grpctransport"
	"github.com/monaaoua9/dtask/internal/raft/wal"
	"github.com/monaaoua9/dtask/internal/scheduler"
	"github.com/monaaoua9/dtask/internal/server"
	"github.com/monaaoua9/dtask/internal/tlsutil"
	"github.com/monaaoua9/dtask/internal/worker"
	"github.com/monaaoua9/dtask/pkg/client"
)

type Config struct {
	ID raft.NodeID
	// AdvertiseAddr is the gRPC address peers and clients use to reach us
	AdvertiseAddr string
	ListenAddr    string // empty: no network servers (in-process tests)
	HTTPAddr      string
	DataDir       string // empty: everything in memory
	Bootstrap     []raft.Server
	// Join asks the existing cluster (via Seeds) to add this node once it's up
	Join  bool
	Seeds []string

	Queues       []string
	Concurrency  int
	LeaseTTL     time.Duration
	PollInterval time.Duration
	NoWorkers    bool
	Handlers     map[string]worker.Handler

	Heartbeat         time.Duration
	ElectionMin       time.Duration
	ElectionMax       time.Duration
	SnapshotThreshold uint64
	Scheduler         scheduler.Config
	Cluster           cluster.Config
	NoFsync           bool
	Debug             bool
	TLS               tlsutil.Files

	// hooks for tests that run many nodes in one process
	Transport raft.Transport
	WorkerAPI worker.API
	Logger    *slog.Logger
}

type Node struct {
	cfg Config
	log *slog.Logger
	reg *prometheus.Registry

	fsm   *fsm.FSM
	raft  *raft.Node
	svc   *cluster.Service
	sched *scheduler.Scheduler
	pool  *worker.Pool

	grpc  *server.GRPC
	http  *server.HTTP
	trans *grpctransport.Transport
	cli   *client.Client

	bg       context.CancelFunc
	wg       sync.WaitGroup
	draining atomic.Bool
	stopOnce sync.Once
}

func New(cfg Config) (*Node, error) {
	if cfg.ID == "" {
		return nil, errors.New("node id is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if len(cfg.Queues) == 0 {
		cfg.Queues = []string{"default"}
	}
	log := cfg.Logger.With("node", string(cfg.ID))
	n := &Node{cfg: cfg, log: log, reg: prometheus.NewRegistry(), fsm: fsm.New()}
	n.reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	var serverCreds, clientCreds credentials.TransportCredentials
	if cfg.TLS.Enabled() {
		var err error
		if serverCreds, err = cfg.TLS.Server(); err != nil {
			return nil, err
		}
		if clientCreds, err = cfg.TLS.Client(); err != nil {
			return nil, err
		}
	}

	var store raft.Store = raft.NewMemStore()
	if cfg.DataDir != "" {
		s, err := wal.Open(filepath.Join(cfg.DataDir, "raft"), wal.Options{NoSync: cfg.NoFsync})
		if err != nil {
			return nil, fmt.Errorf("open raft store: %w", err)
		}
		store = s
	}

	trans := cfg.Transport
	if trans == nil {
		n.trans = grpctransport.NewTransport(clientCreds)
		trans = n.trans
	}

	rn, err := raft.New(raft.Config{
		ID: cfg.ID, Addr: cfg.AdvertiseAddr, Bootstrap: cfg.Bootstrap,
		Store: store, Transport: trans, FSM: n.fsm, Logger: cfg.Logger, Metrics: raft.NewMetrics(n.reg),
		HeartbeatInterval: cfg.Heartbeat, ElectionTimeoutMin: cfg.ElectionMin, ElectionTimeoutMax: cfg.ElectionMax,
		SnapshotThreshold: cfg.SnapshotThreshold,
	})
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	n.raft = rn

	ccfg := cfg.Cluster
	ccfg.NodeID = cfg.ID
	ccfg.Metrics = cluster.NewMetrics(n.reg)
	ccfg.Health = func(id raft.NodeID) (float64, bool) {
		if n.sched == nil {
			return 0, true
		}
		return n.sched.Health(id)
	}
	n.svc = cluster.NewService(ccfg, rn, n.fsm)
	n.reg.MustRegister(cluster.NewFSMCollector(n.fsm, ccfg.Clock))

	scfg := cfg.Scheduler
	scfg.Logger = log
	if scfg.HeartbeatInterval == 0 {
		scfg.HeartbeatInterval = cfg.Heartbeat
	}
	n.sched = scheduler.New(scfg, rn, n.svc, n.fsm)

	api := cfg.WorkerAPI
	if api == nil && !cfg.NoWorkers {
		seeds := cfg.Seeds
		if len(seeds) == 0 {
			seeds = []string{cfg.AdvertiseAddr}
		}
		var opts []client.Option
		if clientCreds != nil {
			opts = append(opts, client.WithCredentials(clientCreds))
		}
		n.cli, err = client.Dial(seeds, opts...)
		if err != nil {
			return nil, err
		}
		api = n.cli
	}
	if !cfg.NoWorkers {
		n.pool = worker.New(worker.Config{
			NodeID: string(cfg.ID), Queues: cfg.Queues, Concurrency: cfg.Concurrency,
			LeaseTTL: cfg.LeaseTTL, PollInterval: cfg.PollInterval,
			Handlers: cfg.Handlers, API: api, Logger: log, Metrics: worker.NewMetrics(n.reg),
		})
	}

	if cfg.ListenAddr != "" {
		n.grpc, err = server.NewGRPC(server.Options{
			Listen: cfg.ListenAddr, Creds: serverCreds, Handler: rn, Service: n.svc, Registry: n.reg, Logger: log,
		})
		if err != nil {
			return nil, err
		}
		if cfg.HTTPAddr != "" {
			n.http, err = server.NewHTTP(cfg.HTTPAddr, n.reg, n.Ready, cfg.Debug)
			if err != nil {
				return nil, err
			}
		}
	}
	return n, nil
}

func (n *Node) Raft() *raft.Node          { return n.raft }
func (n *Node) Service() *cluster.Service { return n.svc }
func (n *Node) FSM() *fsm.FSM             { return n.fsm }
func (n *Node) ID() raft.NodeID           { return n.cfg.ID }

// GRPCAddr is the address actually bound, useful when ListenAddr used port 0.
func (n *Node) GRPCAddr() string {
	if n.grpc == nil {
		return ""
	}
	return n.grpc.Addr()
}

func (n *Node) HTTPAddr() string {
	if n.http == nil {
		return ""
	}
	return n.http.Addr()
}

// Ready means this node knows a leader and isn't on its way out.
func (n *Node) Ready() bool {
	return !n.draining.Load() && n.raft.Status().LeaderID != ""
}

func (n *Node) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	n.bg = cancel

	n.raft.Start()
	if n.grpc != nil {
		n.goRun("grpc", n.grpc.Serve)
		n.grpc.SetServing(true)
	}
	if n.http != nil {
		n.goRun("http", n.http.Serve)
	}
	n.wg.Add(1)
	go func() { defer n.wg.Done(); n.sched.Run(ctx) }()
	if n.pool != nil {
		n.pool.Start()
	}
	if n.cfg.Join {
		n.wg.Add(1)
		go func() { defer n.wg.Done(); n.joinCluster(ctx) }()
	}
	n.log.Info("node started", "grpc", n.GRPCAddr(), "http", n.HTTPAddr())
}

func (n *Node) goRun(name string, fn func() error) {
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		if err := fn(); err != nil && !errors.Is(err, context.Canceled) {
			n.log.Debug("server exited", "server", name, "err", err)
		}
	}()
}

func (n *Node) joinCluster(ctx context.Context) {
	cli, err := client.Dial(n.cfg.Seeds)
	if err != nil {
		n.log.Error("cannot join: bad seeds", "err", err)
		return
	}
	defer cli.Close()
	for ctx.Err() == nil {
		cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		err := cli.AddServer(cctx, string(n.cfg.ID), n.cfg.AdvertiseAddr)
		cancel()
		if err == nil {
			n.log.Info("joined cluster")
			return
		}
		n.log.Warn("join attempt failed, retrying", "err", err)
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
		}
	}
}

// Shutdown stops the node without dropping work: it refuses new work, lets
// running tasks finish (or hands them back once ctx runs out), and only then
// takes down the cluster-facing pieces.
func (n *Node) Shutdown(ctx context.Context) error {
	var err error
	n.stopOnce.Do(func() {
		n.log.Info("shutting down")
		n.draining.Store(true)
		if n.grpc != nil {
			n.grpc.SetServing(false)
		}

		// workers go first: they still need the cluster to report results
		if n.pool != nil {
			err = n.pool.Shutdown(ctx)
		}

		// give leadership away so the cluster doesn't sit through an election timeout
		if n.raft.IsLeader() {
			tctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			if terr := n.raft.TransferLeadership(tctx); terr != nil {
				n.log.Debug("leadership transfer skipped", "err", terr)
			}
			cancel()
		}

		n.bg()
		if n.grpc != nil {
			n.grpc.GracefulStop(ctx)
		}
		n.raft.Stop()
		if n.http != nil {
			_ = n.http.Shutdown(ctx)
		}
		n.wg.Wait()
		n.closeClients()
		n.log.Info("shutdown complete")
	})
	return err
}

// Crash drops everything on the floor, like kill -9. Tests only.
func (n *Node) Crash() {
	n.stopOnce.Do(func() {
		if n.pool != nil {
			n.pool.Kill()
		}
		if n.bg != nil {
			n.bg()
		}
		if n.grpc != nil {
			n.grpc.Stop()
		}
		n.raft.Stop()
		if n.http != nil {
			_ = n.http.Shutdown(context.Background())
		}
		n.wg.Wait()
		n.closeClients()
	})
}

func (n *Node) closeClients() {
	if n.trans != nil {
		n.trans.Close()
	}
	if n.cli != nil {
		_ = n.cli.Close()
	}
}
