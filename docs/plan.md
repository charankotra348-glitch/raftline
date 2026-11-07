# Plan (Nov 15 – Dec 15, 2026)

Timeboxed: on Dec 15 whatever is finished gets written up and the project stops. About 2.5 hours a day.

Before Nov 15: read the Raft paper sections 1–5 and 8 twice, and draw Figure 2 from memory.

## Week 1 (Nov 15–21): leader election, milestone 1

- [ ] `ticker`: random election timeout, start an election when it runs out
- [ ] `RequestVote` handler: the term and vote rules
- [ ] Collect votes in parallel; become leader on a majority
- [ ] Heartbeats from the leader every 100 ms
- [ ] Step down whenever a reply or request carries a higher term
- [ ] Set `Milestone = 1`; `go test ./raft -race -count=20` passes

## Week 2 (Nov 22–28): log replication, milestone 2

- [ ] `Start` appends to the leader's log
- [ ] `AppendEntries` receiver rules 2–5
- [ ] The vote's up-to-date check (section 5.4.1)
- [ ] Leader advances `commitIndex` (current-term entries only)
- [ ] Applier goroutine sends committed entries on `applyCh` in order
- [ ] Fast backup with `ConflictTerm` / `ConflictIndex`
- [ ] Set `Milestone = 2`; `-count=20` passes

## Week 3 (Nov 29 – Dec 5): persistence, milestone 3

- [ ] `persist` / `readPersist` with gob
- [ ] Call `persist` after every change to term, vote or log
- [ ] Unreliable-network and Figure 8 tests pass
- [ ] Set `Milestone = 3`; `scripts/stress.sh 100` passes 100/100

## Week 4 (Dec 6–12): key-value store

- [ ] Clerk with retries and leader tracking
- [ ] Server: Start, wait for the index, detect lost leadership
- [ ] Duplicate table per clerk
- [ ] Tests: basic ops, concurrent clients, partitions, crashes, linearizability check

## Dec 13–15: write-up

- [ ] Measure: pass rate over 100 runs, re-election time, ops/sec
- [ ] Fill in the README results with the real numbers
- [ ] One diagram of the RPC flow
- [ ] Pin the repo; add it to the resume

## Stretch, only if ahead

- Snapshots (InstallSnapshot RPC) so the log stops growing
- A real gRPC transport and a 3-process demo on localhost

## Debugging habits

- Run with `-race` every time.
- Hold `rf.mu` while reading or writing any field; never hold it while making an RPC or sending on `applyCh`.
- After every reply, check that the term and role are still what they were when the request was sent; if not, ignore the reply.
- Add a `debug(format, args...)` helper that prints `time server term role message`, switched on by an environment variable.
