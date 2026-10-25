// dtaskd is one node of the cluster.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/monaaoua9/dtask/internal/demo"
	"github.com/monaaoua9/dtask/internal/node"
	"github.com/monaaoua9/dtask/internal/raft"
	"github.com/monaaoua9/dtask/internal/scheduler"
	"github.com/monaaoua9/dtask/internal/tlsutil"
)

func main() {
	var (
		id          = flag.String("id", env("DTASK_ID", ""), "unique node id, e.g. n1")
		listen      = flag.String("listen", env("DTASK_LISTEN", ":7000"), "gRPC listen address")
		advertise   = flag.String("advertise", env("DTASK_ADVERTISE", ""), "gRPC address other nodes and clients use (defaults to listen)")
		httpAddr    = flag.String("http", env("DTASK_HTTP", ":9090"), "metrics and health listen address, empty to disable")
		dataDir     = flag.String("data-dir", env("DTASK_DATA_DIR", "./data"), "where the raft log and snapshots live")
		peers       = flag.String("peers", env("DTASK_PEERS", ""), "initial cluster for first boot: id=host:port,id=host:port,...")
		join        = flag.Bool("join", false, "start empty and ask the cluster in --seeds to add this node")
		seeds       = flag.String("seeds", env("DTASK_SEEDS", ""), "comma separated gRPC addresses of existing nodes (defaults to the --peers addresses)")
		queues      = flag.String("queues", env("DTASK_QUEUES", "default"), "queues this node's workers pull from")
		concurrency = flag.Int("concurrency", 4, "worker goroutines on this node, 0 disables workers")
		leaseTTL    = flag.Duration("lease-ttl", 15*time.Second, "how long a worker may hold a task without heartbeating")
		snapEvery   = flag.Uint64("snapshot-threshold", 10000, "applied entries between log compactions")
		retention   = flag.Duration("retention", time.Hour, "keep finished tasks this long")
		heartbeat   = flag.Duration("heartbeat", 100*time.Millisecond, "raft heartbeat interval")
		electionMin = flag.Duration("election-timeout", time.Second, "minimum election timeout (max is twice this)")
		drain       = flag.Duration("shutdown-timeout", 30*time.Second, "how long to wait for running tasks on SIGTERM")
		noFsync     = flag.Bool("unsafe-no-fsync", false, "skip fsync, only for benchmarks")
		debug       = flag.Bool("debug", false, "serve pprof on the http address and log at debug level")
		logJSON     = flag.Bool("log-json", false, "log as JSON")
		tlsCert     = flag.String("tls-cert", env("DTASK_TLS_CERT", ""), "node certificate (enables mutual TLS)")
		tlsKey      = flag.String("tls-key", env("DTASK_TLS_KEY", ""), "node private key")
		tlsCA       = flag.String("tls-ca", env("DTASK_TLS_CA", ""), "CA that signs every node and client certificate")
	)
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	hopts := &slog.HandlerOptions{Level: level}
	var h slog.Handler = slog.NewTextHandler(os.Stderr, hopts)
	if *logJSON {
		h = slog.NewJSONHandler(os.Stderr, hopts)
	}
	log := slog.New(h)

	if *id == "" {
		fatal(log, "--id is required")
	}
	adv := *advertise
	if adv == "" {
		adv = *listen
	}
	boot, err := parsePeers(*peers)
	if err != nil {
		fatal(log, err.Error())
	}
	seedList := splitList(*seeds)
	if len(seedList) == 0 {
		for _, s := range boot {
			seedList = append(seedList, s.Addr)
		}
	}
	if *join && len(seedList) == 0 {
		fatal(log, "--join needs --seeds")
	}

	n, err := node.New(node.Config{
		ID: raft.NodeID(*id), AdvertiseAddr: adv, ListenAddr: *listen, HTTPAddr: *httpAddr,
		DataDir: *dataDir, Bootstrap: boot, Join: *join, Seeds: seedList,
		Queues: splitList(*queues), Concurrency: *concurrency, NoWorkers: *concurrency == 0,
		LeaseTTL: *leaseTTL, Handlers: demo.Handlers(),
		Heartbeat: *heartbeat, ElectionMin: *electionMin, ElectionMax: 2 * *electionMin,
		SnapshotThreshold: *snapEvery, NoFsync: *noFsync, Debug: *debug,
		Scheduler: scheduler.Config{Retention: *retention},
		TLS:       tlsutil.Files{Cert: *tlsCert, Key: *tlsKey, CA: *tlsCA},
		Logger:    log,
	})
	if err != nil {
		fatal(log, "cannot start: "+err.Error())
	}
	n.Start()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	stop() // a second signal gets the default behaviour: die now

	log.Info("signal received, draining", "timeout", drain.String())
	sctx, cancel := context.WithTimeout(context.Background(), *drain)
	defer cancel()
	if err := n.Shutdown(sctx); err != nil {
		log.Warn("shutdown hit its deadline, running tasks were handed back", "err", err)
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func parsePeers(s string) ([]raft.Server, error) {
	var out []raft.Server
	for _, p := range splitList(s) {
		id, addr, ok := strings.Cut(p, "=")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("bad --peers entry %q, want id=host:port", p)
		}
		out = append(out, raft.Server{ID: raft.NodeID(id), Addr: addr, Voter: true})
	}
	return out, nil
}

func fatal(log *slog.Logger, msg string) {
	log.Error(msg)
	os.Exit(2)
}
