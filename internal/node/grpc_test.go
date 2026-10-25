package node_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	pb "github.com/monaaoua9/dtask/api/gen/dtask/v1"
	"github.com/monaaoua9/dtask/internal/node"
	"github.com/monaaoua9/dtask/internal/raft"
	"github.com/monaaoua9/dtask/internal/tlsutil"
	"github.com/monaaoua9/dtask/internal/worker"
	"github.com/monaaoua9/dtask/pkg/client"
)

type netCluster struct {
	t     *testing.T
	nodes map[raft.NodeID]*node.Node
	addrs []string
	ids   []raft.NodeID
	runs  atomic.Int64
	tls   tlsutil.Files
}

func freeAddrs(t *testing.T, n int) []string {
	t.Helper()
	var out []string
	var ls []net.Listener
	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		ls = append(ls, l)
		out = append(out, l.Addr().String())
	}
	for _, l := range ls {
		l.Close()
	}
	return out
}

func newNetCluster(t *testing.T, n int, tlsFiles tlsutil.Files) *netCluster {
	t.Helper()
	nc := &netCluster{t: t, nodes: map[raft.NodeID]*node.Node{}, addrs: freeAddrs(t, n), tls: tlsFiles}
	var boot []raft.Server
	for i, a := range nc.addrs {
		id := raft.NodeID(fmt.Sprintf("n%d", i+1))
		nc.ids = append(nc.ids, id)
		boot = append(boot, raft.Server{ID: id, Addr: a, Voter: true})
	}
	for i, id := range nc.ids {
		nc.startNode(id, nc.addrs[i], boot, false)
	}
	t.Cleanup(func() {
		for _, nd := range nc.nodes {
			nd.Crash()
		}
	})
	return nc
}

func (nc *netCluster) startNode(id raft.NodeID, addr string, boot []raft.Server, join bool) *node.Node {
	nd, err := node.New(node.Config{
		ID: id, AdvertiseAddr: addr, ListenAddr: addr, HTTPAddr: "127.0.0.1:0",
		DataDir: nc.t.TempDir(), NoFsync: true, Bootstrap: boot, Join: join, Seeds: nc.addrs[:3],
		Queues: []string{"default"}, Concurrency: 2, LeaseTTL: 5 * time.Second, PollInterval: 20 * time.Millisecond,
		Heartbeat: 25 * time.Millisecond, ElectionMin: 250 * time.Millisecond, ElectionMax: 500 * time.Millisecond,
		Handlers: map[string]worker.Handler{"count": func(ctx context.Context, t *pb.Task) ([]byte, error) {
			nc.runs.Add(1)
			return []byte("ok"), nil
		}},
		TLS:    nc.tls,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(nc.t, err)
	nd.Start()
	nc.nodes[id] = nd
	return nd
}

func (nc *netCluster) leader() *node.Node {
	nc.t.Helper()
	var l *node.Node
	require.Eventually(nc.t, func() bool {
		for _, n := range nc.nodes {
			if n.Raft().IsLeader() {
				l = n
				return true
			}
		}
		return false
	}, 15*time.Second, 20*time.Millisecond)
	return l
}

func (nc *netCluster) client(opts ...client.Option) *client.Client {
	c, err := client.Dial(nc.addrs[:3], opts...)
	require.NoError(nc.t, err)
	nc.t.Cleanup(func() { c.Close() })
	return c
}

func submitMany(t *testing.T, c *client.Client, prefix string, n int) []string {
	t.Helper()
	var ids []string
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		resp, err := c.Submit(ctx, &pb.SubmitRequest{Queue: "default", Type: "count", IdempotencyKey: fmt.Sprint(prefix, i)})
		cancel()
		require.NoError(t, err)
		ids = append(ids, resp.Task.Id)
	}
	return ids
}

func waitSucceeded(t *testing.T, c *client.Client, ids []string) {
	t.Helper()
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, id := range ids {
			r, err := c.GetTask(ctx, &pb.GetTaskRequest{Id: id})
			if err != nil || r.Task.State != pb.TaskState_TASK_STATE_SUCCEEDED {
				return false
			}
		}
		return true
	}, 40*time.Second, 100*time.Millisecond)
}

func TestGRPCClusterSurvivesLeaderKill(t *testing.T) {
	testGRPCCluster(t, tlsutil.Files{})
}

func TestGRPCClusterWithMutualTLS(t *testing.T) {
	testGRPCCluster(t, makeCerts(t))
}

func testGRPCCluster(t *testing.T, tlsFiles tlsutil.Files) {
	nc := newNetCluster(t, 3, tlsFiles)
	nc.leader()

	var opts []client.Option
	if tlsFiles.Enabled() {
		creds, err := tlsFiles.Client()
		require.NoError(t, err)
		opts = append(opts, client.WithCredentials(creds))
	}
	c := nc.client(opts...)

	first := submitMany(t, c, "a", 30)
	waitSucceeded(t, c, first)

	// kill the leader hard; the client must find the new one by itself
	old := nc.leader()
	oldID := old.ID()
	old.Crash()
	delete(nc.nodes, oldID)

	second := submitMany(t, c, "b", 30)
	waitSucceeded(t, c, second)
	require.GreaterOrEqual(t, nc.runs.Load(), int64(60))

	// idempotent resubmission returns the original task
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dup, err := c.Submit(ctx, &pb.SubmitRequest{Queue: "default", Type: "count", IdempotencyKey: "a0"})
	require.NoError(t, err)
	require.True(t, dup.Duplicate)
	require.Equal(t, first[0], dup.Task.Id)

	stats, err := c.QueueStats(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(60), stats.Queues[0].Succeeded)

	_, err = c.GetTask(ctx, &pb.GetTaskRequest{Id: "does-not-exist"})
	require.ErrorIs(t, err, client.ErrNotFound)

	_, err = c.Submit(ctx, &pb.SubmitRequest{Queue: "bad queue name!", Type: "x"})
	require.ErrorIs(t, err, client.ErrInvalid)

	// pagination walks everything exactly once
	seen := map[string]bool{}
	cursor := ""
	for {
		page, err := c.ListTasks(ctx, &pb.ListTasksRequest{PageSize: 7, Cursor: cursor})
		require.NoError(t, err)
		for _, tk := range page.Tasks {
			require.False(t, seen[tk.Id], "task listed twice")
			seen[tk.Id] = true
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	require.Len(t, seen, 60)

	if !tlsFiles.Enabled() {
		checkHTTP(t, nc)
	}
}

func checkHTTP(t *testing.T, nc *netCluster) {
	t.Helper()
	for _, nd := range nc.nodes {
		base := "http://" + nd.HTTPAddr()
		for path, want := range map[string]int{"/healthz": 200, "/readyz": 200} {
			resp, err := http.Get(base + path)
			require.NoError(t, err)
			resp.Body.Close()
			require.Equal(t, want, resp.StatusCode, path)
		}
		resp, err := http.Get(base + "/metrics")
		require.NoError(t, err)
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		text := string(body)
		for _, m := range []string{"dtask_raft_term", "dtask_raft_commit_index", `dtask_tasks{queue="default",state="succeeded"}`, "dtask_grpc_requests_total", "dtask_worker_executions_total"} {
			require.True(t, strings.Contains(text, m), "metric %s missing from /metrics on %s", m, nd.ID())
		}
		return // one node is enough
	}
}

func TestJoinNewNodeOverGRPC(t *testing.T) {
	nc := newNetCluster(t, 3, tlsutil.Files{})
	nc.leader()
	c := nc.client()
	ids := submitMany(t, c, "j", 10)
	waitSucceeded(t, c, ids)

	extra := freeAddrs(t, 1)[0]
	nc.addrs = append(nc.addrs, extra)
	nd := nc.startNode("n4", extra, nil, true)
	require.Eventually(t, func() bool {
		return nd.Raft().Status().Config.IsVoter("n4") && nd.Raft().Status().LastApplied >= 10
	}, 30*time.Second, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := c.NodeStatus(ctx, extra)
	require.NoError(t, err)
	require.Len(t, st.Servers, 4)
}

func TestGracefulShutdownOverGRPC(t *testing.T) {
	nc := newNetCluster(t, 3, tlsutil.Files{})
	l := nc.leader()
	c := nc.client()
	ids := submitMany(t, c, "g", 20)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start := time.Now()
	require.NoError(t, l.Shutdown(ctx))
	delete(nc.nodes, l.ID())
	t.Logf("leader shutdown took %v", time.Since(start))

	// leadership was handed over, not abandoned
	nl := nc.leader()
	require.NotEqual(t, l.ID(), nl.ID())
	waitSucceeded(t, c, ids)
}

// --- throwaway CA + per-node certificate for the mTLS test ---

func makeCerts(t *testing.T) tlsutil.Files {
	t.Helper()
	dir := t.TempDir()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "dtask-test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "dtask-node"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caCert, &leafKey.PublicKey, caKey)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(leafKey)
	require.NoError(t, err)

	write := func(name, typ string, der []byte) string {
		p := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600))
		return p
	}
	return tlsutil.Files{
		CA:   write("ca.pem", "CERTIFICATE", caDER),
		Cert: write("node.pem", "CERTIFICATE", leafDER),
		Key:  write("node-key.pem", "EC PRIVATE KEY", keyDER),
	}
}
