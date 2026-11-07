# raftline

A fault-tolerant key-value store built on my own implementation of the Raft consensus algorithm, in Go.

Five servers keep identical copies of the data. Any two of them can crash, restart or be cut off from the network and the store keeps serving reads and writes. Every write is either applied exactly once on all servers or not at all.

**Status:** in progress (Nov 15 – Dec 15, 2026). See [docs/plan.md](docs/plan.md) for what is done.

## What's here

| Package | What it does |
|---|---|
| `raft/` | Raft: leader election, log replication, persistence |
| `simnet/` | An in-process network that drops, delays and reorders messages and simulates partitions, so failures can be tested in seconds |
| `kv/` | The key-value service on top of Raft (Get / Put / Append, duplicate detection, linearizable reads) |

## How it's tested

The tests run real clusters of 3 to 7 servers over `simnet` and break them on purpose:

- leaders disconnected mid-term, minorities and majorities partitioned
- servers crashed and restarted from their saved state
- 10% of requests and replies dropped, replies reordered by up to 2 seconds
- the Figure 8 scenario from the paper, where a leader crashes right after appending

While the tests run, a checker records every command each server applies and fails at once if two servers ever apply different commands at the same index or apply out of order.

The tests are gated by milestone (`raft/milestone.go`), so each part is finished and passing before the next starts.

```bash
go test ./... -race            # everything unlocked so far
go test ./raft -run Election   # one group
go test ./raft -count=10       # repeat to shake out rare bugs
```

## Design

[docs/design.md](docs/design.md) covers the state each server keeps, the two RPCs, the rules that make Raft safe, and how the key-value layer gets exactly-once writes.

## Results

Filled in as each milestone is measured: test pass rate over 100 runs, time to elect a new leader after a failure, and throughput of the key-value store with 1 to 5 servers.

## Reference

Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus Algorithm (Extended Version)*, 2014.
