// dtaskctl talks to a running cluster.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	pb "github.com/monaaoua9/dtask/api/gen/dtask/v1"
	"github.com/monaaoua9/dtask/internal/tlsutil"
	"github.com/monaaoua9/dtask/pkg/client"
)

const usage = `usage: dtaskctl [--addr host:port,...] [--json] [--timeout 10s] <command> [flags]

commands:
  submit        --queue Q --type T [--payload S] [--priority N] [--delay D] [--max-attempts N] [--key K]
  get           <task-id>
  list          [--queue Q] [--state pending|leased|succeeded|dead|canceled] [--limit N]
  cancel        <task-id>
  status        [--node host:port]
  queues
  queue-config  --name Q [--rate PER_SEC] [--burst N] [--max-in-flight N]
  add-server    --id ID --addr host:port
  remove-server --id ID
`

type app struct {
	cli     *client.Client
	asJSON  bool
	timeout time.Duration
}

func main() {
	global := flag.NewFlagSet("dtaskctl", flag.ExitOnError)
	addr := global.String("addr", env("DTASK_ADDR", "127.0.0.1:7000"), "comma separated cluster addresses")
	asJSON := global.Bool("json", false, "print protobuf JSON instead of tables")
	timeout := global.Duration("timeout", 10*time.Second, "per-command deadline")
	tlsCert := global.String("tls-cert", env("DTASK_TLS_CERT", ""), "client certificate")
	tlsKey := global.String("tls-key", env("DTASK_TLS_KEY", ""), "client key")
	tlsCA := global.String("tls-ca", env("DTASK_TLS_CA", ""), "CA certificate")
	global.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	_ = global.Parse(os.Args[1:])
	args := global.Args()
	if len(args) == 0 {
		global.Usage()
		os.Exit(2)
	}

	var opts []client.Option
	if files := (tlsutil.Files{Cert: *tlsCert, Key: *tlsKey, CA: *tlsCA}); files.Enabled() {
		creds, err := files.Client()
		die(err)
		opts = append(opts, client.WithCredentials(creds))
	}
	cli, err := client.Dial(strings.Split(*addr, ","), opts...)
	die(err)
	defer cli.Close()

	a := &app{cli: cli, asJSON: *asJSON, timeout: *timeout}
	die(a.run(args[0], args[1:]))
}

func (a *app) run(cmd string, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), a.timeout)
	defer cancel()
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)

	switch cmd {
	case "submit":
		queue := fs.String("queue", "default", "")
		typ := fs.String("type", "", "")
		payload := fs.String("payload", "", "")
		prio := fs.Int("priority", 0, "")
		delay := fs.Duration("delay", 0, "")
		attempts := fs.Uint("max-attempts", 0, "")
		key := fs.String("key", "", "")
		_ = fs.Parse(args)
		resp, err := a.cli.Submit(ctx, &pb.SubmitRequest{
			Queue: *queue, Type: *typ, Payload: []byte(*payload), Priority: int32(*prio),
			DelayMs: delay.Milliseconds(), IdempotencyKey: *key,
			Retry: &pb.RetryPolicy{MaxAttempts: uint32(*attempts)},
		})
		if err != nil {
			return err
		}
		if a.asJSON {
			return printJSON(resp)
		}
		note := ""
		if resp.Duplicate {
			note = " (duplicate, returning the original)"
		}
		fmt.Println(resp.Task.Id + note)

	case "get":
		_ = fs.Parse(args)
		if fs.NArg() != 1 {
			return errors.New("get needs a task id")
		}
		resp, err := a.cli.GetTask(ctx, &pb.GetTaskRequest{Id: fs.Arg(0)})
		if err != nil {
			return err
		}
		if a.asJSON {
			return printJSON(resp.Task)
		}
		printTasks([]*pb.Task{resp.Task})

	case "list":
		queue := fs.String("queue", "", "")
		state := fs.String("state", "", "")
		limit := fs.Uint("limit", 50, "")
		_ = fs.Parse(args)
		st, err := parseState(*state)
		if err != nil {
			return err
		}
		resp, err := a.cli.ListTasks(ctx, &pb.ListTasksRequest{Queue: *queue, State: st, PageSize: uint32(*limit)})
		if err != nil {
			return err
		}
		if a.asJSON {
			return printJSON(resp)
		}
		printTasks(resp.Tasks)
		if resp.NextCursor != "" {
			fmt.Fprintf(os.Stderr, "more results exist, raise --limit\n")
		}

	case "cancel":
		_ = fs.Parse(args)
		if fs.NArg() != 1 {
			return errors.New("cancel needs a task id")
		}
		resp, err := a.cli.CancelTask(ctx, &pb.CancelTaskRequest{Id: fs.Arg(0)})
		if err != nil {
			return err
		}
		printTasks([]*pb.Task{resp.Task})

	case "status":
		node := fs.String("node", "", "ask this node instead of the first reachable one")
		_ = fs.Parse(args)
		return a.status(ctx, *node)

	case "queues":
		resp, err := a.cli.QueueStats(ctx)
		if err != nil {
			return err
		}
		if a.asJSON {
			return printJSON(resp)
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
		fmt.Fprintln(w, "QUEUE\tPENDING\tDELAYED\tLEASED\tDONE\tDEAD\tRATE/S\tMAX-IN-FLIGHT")
		for _, q := range resp.Queues {
			fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%d\t%d\t%g\t%d\n", q.Name, q.Pending, q.Delayed, q.Leased, q.Succeeded, q.Dead, q.Config.GetRatePerSec(), q.Config.GetMaxInFlight())
		}
		return w.Flush()

	case "queue-config":
		name := fs.String("name", "", "")
		rate := fs.Float64("rate", 0, "tasks per second, 0 = unlimited")
		burst := fs.Uint("burst", 1, "")
		inflight := fs.Uint("max-in-flight", 0, "")
		_ = fs.Parse(args)
		return a.cli.SetQueueConfig(ctx, &pb.QueueConfig{Name: *name, RatePerSec: *rate, Burst: uint32(*burst), MaxInFlight: uint32(*inflight)})

	case "add-server":
		id := fs.String("id", "", "")
		addr := fs.String("addr", "", "")
		_ = fs.Parse(args)
		return a.cli.AddServer(ctx, *id, *addr)

	case "remove-server":
		id := fs.String("id", "", "")
		_ = fs.Parse(args)
		return a.cli.RemoveServer(ctx, *id)

	default:
		fmt.Fprint(os.Stderr, usage)
		return fmt.Errorf("unknown command %q", cmd)
	}
	return nil
}

func (a *app) status(ctx context.Context, node string) error {
	if node == "" {
		node = strings.Split(env("DTASK_ADDR", "127.0.0.1:7000"), ",")[0]
	}
	st, err := a.cli.NodeStatus(ctx, node)
	if err != nil {
		return err
	}
	if a.asJSON {
		return printJSON(st)
	}
	fmt.Printf("answered by %s (%s), term %d, leader %s\n", st.NodeId, st.Role, st.Term, orDash(st.LeaderId))
	fmt.Printf("log: last=%d commit=%d applied=%d\n\n", st.LastLogIndex, st.CommitIndex, st.LastApplied)
	w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tADDRESS\tVOTER\tHEALTHY\tPHI")
	for _, s := range st.Servers {
		health, phi := "-", "-"
		if st.Role == "leader" || s.Id == st.NodeId {
			health, phi = fmt.Sprint(s.Healthy), fmt.Sprintf("%.1f", s.Phi)
		}
		fmt.Fprintf(w, "%s\t%s\t%v\t%s\t%s\n", s.Id, s.Addr, s.Voter, health, phi)
	}
	return w.Flush()
}

func printTasks(ts []*pb.Task) {
	w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tQUEUE\tTYPE\tSTATE\tATTEMPT\tWORKER\tERROR")
	for _, t := range ts {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d/%d\t%s\t%s\n", t.Id, t.Queue, t.Type,
			strings.ToLower(strings.TrimPrefix(t.State.String(), "TASK_STATE_")),
			t.Attempt, t.Retry.GetMaxAttempts(), orDash(t.LeaseWorker), orDash(t.LastError))
	}
	_ = w.Flush()
}

func parseState(s string) (pb.TaskState, error) {
	if s == "" {
		return pb.TaskState_TASK_STATE_UNSPECIFIED, nil
	}
	v, ok := pb.TaskState_value["TASK_STATE_"+strings.ToUpper(s)]
	if !ok {
		return 0, fmt.Errorf("unknown state %q", s)
	}
	return pb.TaskState(v), nil
}

func printJSON(m proto.Message) error {
	b, err := protojson.MarshalOptions{Multiline: true, EmitUnpopulated: false}.Marshal(m)
	if err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func die(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
