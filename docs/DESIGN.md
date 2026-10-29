# dtask: design decisions

The short list of choices behind dtask. Layers, Raft notes, failure matrix and the full trade-off list: [ARCHITECTURE.md](ARCHITECTURE.md).

## Scope

A distributed task queue and scheduler. Clients submit tasks, workers lease them, the cluster guarantees a recorded outcome comes from the execution that held the winning lease. Delivery is at-least-once. The Raft implementation is written from scratch.

## Decisions

**Raft replicates opaque bytes; the task logic is a separate deterministic state machine.** `raft` knows nothing about tasks and `fsm` knows nothing about networks. `cluster.Service` is the only place "now" and "random" enter the system, and it writes both into the command before it's replicated.

**The state machine is a pure function of (state, command).** No `time.Now()` inside `Apply`; each command carries `now_ns` and the FSM keeps `clock = max(clock, now_ns)` so a new leader with a slower clock can't move time backwards. Retry delays (backoff and jitter) are computed by the leader and sent as `retry_at_ns`. Rate limiting uses integer milli-tokens, no floats. A test applies random command streams to two FSMs, rebuilds one from a snapshot halfway through, and compares every result and the final SHA-256 digest.

**Leases carry fencing tokens, checked inside the state machine.** A wall-clock TTL alone can't stop a paused worker from acting after its lease expired. Every `Lease` stamps the task with an increasing token and every later call (`Extend`, `Complete`, `Fail`, `Release`) must present it. The token is checked where the result is recorded, so a zombie can still burn CPU or cause external side effects (that's what at-least-once means) but can't overwrite the stored outcome. `Complete` with the same token is idempotent, so a lost response can be retried.

**Phi-accrual failure detection, fed by Raft heartbeats.** The leader turns each peer's heartbeat spacing into a suspicion level instead of using a fixed timeout, so a jittery link isn't declared dead and a regular one is noticed fast. Crossing the threshold proposes `ExpireNode`, which requeues every task leased to that node in one command. Wrongly declaring a live node dead is safe (its work is re-run and fenced); both detectors failing only costs speed, not correctness.

**Group-committed proposals.** A loop drains whatever queued up and appends it with one fsync, so concurrent clients share the disk cost.

**Retries count lease expiries as attempts.** A task that keeps killing its worker ends up dead-lettered instead of looping forever.

## Not done on purpose

- **One Raft group.** Throughput is bounded by the leader. Fix: partition queues across groups.
- **Whole FSM in memory, snapshots in one gRPC message.** Fine up to a few hundred thousand live tasks.
- **Single-server membership changes**, not joint consensus. Safe because the code serialises them.
- **Home-made Raft.** It has seen far less production time than etcd/raft or hashicorp/raft. For something you operate, use one of those.

## Checked by

Chaos tests with partitions and crashes, the replica-equivalence test above, and `TestZombieWorkerAfterPartitionIsFenced`, which asserts that the stored result of every task came from the execution that held the winning token. See sections 6 and 7 of ARCHITECTURE.md.
