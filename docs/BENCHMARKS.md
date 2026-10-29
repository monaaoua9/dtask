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
go test -run xxx -bench . -benchmem -benchtime 3s ./internal/raft
```

Both benchmarks measure the Raft layer only (`Propose` until the entry is committed), with a 32-byte or 16-byte payload and 16 parallel proposers per CPU.

| benchmark | commits/s | ns/op | allocs/op |
|---|---:|---:|---:|
| 3 in-process nodes, memory stores | 176,127 | 5,678 | 15 |
| 1 node, real WAL with fsync | 43,184 | 23,157 | 8 |

## Reading these numbers

- The first row is the protocol and its batching with no disk: three nodes in one process over a simulated network. It shows the ceiling of the code path, not what a real cluster does.
- The second row is the one that matters for durability. It is one node, so there's no network round trip and no replication, and group commit is what makes ~43k commits/s possible with an fsync behind each batch. A real 3-node cluster adds network latency and follower fsyncs on top.
- This is not end-to-end task throughput. A task needs more than one committed command (at least submit and complete, plus the lease), so tasks/s is a fraction of commits/s. I haven't measured it.
- The README mentions ~28k and ~139k commits/s from an earlier run on the same laptop. This run is higher on both. I haven't investigated; it's a laptop, so don't read much into the difference.
