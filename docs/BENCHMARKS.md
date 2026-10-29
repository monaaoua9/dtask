# dtask: benchmarks

Measured 2026-10-29.

| | |
|---|---|
| CPU | Intel i5-8365U, 4 cores / 8 threads, 1.6 GHz base |
| RAM / disk | 8 GB, SSD |
| OS | Windows 11 |
| Toolchain | Go 1.27.0 |

Reproduce:

```
go test -run xxx -bench . -benchmem -benchtime 3s -count 3 ./internal/raft
```

Both benchmarks measure the Raft layer only (`Propose` until the entry is committed), with a 32-byte or 16-byte payload and 16 parallel proposers per CPU. Three runs; median with min–max in brackets.

| benchmark | commits/s |
|---|---:|
| 3 in-process nodes, memory stores | 193,758 (179,148 – 214,469) |
| 1 node, real WAL with fsync | 39,845 (38,572 – 45,922) |

Allocations per op: 14–15 (about 3.1 KB) for the first, 8 (about 1.1 KB) for the second.

## Reading these numbers

- The first row is the protocol and its batching with no disk: three nodes in one process over a simulated network. It shows the ceiling of the code path, not what a real cluster does.
- The second row is the one that matters for durability. It is one node, so there's no network round trip and no replication, and group commit is what makes ~40k commits/s possible with an fsync behind each batch. A real 3-node cluster adds network latency and follower fsyncs on top.
- This is not end-to-end task throughput. A task needs more than one committed command (at least submit and complete, plus the lease), so tasks/s is a fraction of commits/s. I haven't measured it.
- The README mentioned ~28k and ~139k commits/s from an earlier run on the same laptop. This run is higher on both and I haven't investigated; it's a laptop, so a 10–20% spread between runs of the same code is normal here.
