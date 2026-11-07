# Design

## The problem

One server holding the data is simple but fails with that machine. Several copies survive failures, but only if every copy applies the same writes in the same order, even when messages are lost and machines crash mid-operation. Raft solves this: the servers agree on one ordered log of commands, and each server applies that log to its own copy of the data.

## Roles and terms

Each server is a **follower**, a **candidate** or the **leader**. Time is divided into **terms**, numbered 1, 2, 3, and so on. Each term has at most one leader.

- Followers do nothing on their own; they answer the leader and candidates.
- If a follower hears nothing from a leader for an election timeout (random, 300–600 ms), it becomes a candidate, starts a new term and asks the others for votes.
- A candidate that gets votes from a majority becomes leader and sends heartbeats every 100 ms to keep the others from starting elections.
- Any server that sees a higher term than its own adopts it and becomes a follower. This one rule removes stale leaders.

The timeout is random so that two servers rarely time out together and split the vote.

## State on each server

| Field | Saved to disk? | Meaning |
|---|---|---|
| `currentTerm` | yes | latest term this server has seen |
| `votedFor` | yes | who it voted for in `currentTerm` (-1 if nobody) |
| `log[]` | yes | the commands, each tagged with the term it was created in |
| `commitIndex` | no | highest log index known to be committed |
| `lastApplied` | no | highest index applied to the key-value data |
| `nextIndex[]`, `matchIndex[]` | no, leader only | what the leader believes each follower has |

The three saved fields must be written before the server answers any RPC that changed them. If a server voted and then forgot it after a crash, it could vote twice in one term and allow two leaders.

## The two RPCs

**RequestVote**, sent by candidates. A server grants its vote if the candidate's term is not behind its own, it has not voted for someone else this term, and the candidate's log is at least as up to date as its own (higher last term, or the same last term and an equal or longer log). The last check guarantees a new leader already holds every committed entry.

**AppendEntries**, sent by the leader (with no entries, it is the heartbeat). It carries new entries plus the index and term of the entry just before them. The follower accepts only if its log has a matching entry there; that check, repeated at every step, means two logs that agree at one index agree on everything before it. On a mismatch the follower reports the conflicting term and where it starts, so the leader can skip back a term at a time instead of one entry per round trip.

## When an entry is committed

The leader marks an entry committed once a majority hold it **and** it is from the leader's current term. Entries from earlier terms are committed only indirectly, when a later entry from the current term commits. Without this rule, an entry stored on a majority can still be overwritten (Figure 8 in the paper); `TestPersistFigure8` recreates that case.

Once committed, an entry is sent on `applyCh`, in order, exactly once per server.

## Safety properties the tests check

1. **Election safety:** at most one leader per term.
2. **Log matching:** if two logs hold an entry with the same index and term, they are identical up to that index.
3. **Leader completeness:** a committed entry is in the log of every later leader.
4. **State machine safety:** no two servers apply different commands at the same index.

The test harness checks 4 on every applied message; 1 is checked whenever the tests look for a leader.

## Key-value layer (week 4)

- **Operations.** `Get(key)`, `Put(key, value)`, `Append(key, arg)`. Every operation, reads included, goes through the Raft log, so a read never returns data older than a write that has finished.
- **Clerks.** Clients (Clerks) send each request to the server they think is leader and retry on another server after a timeout or a "wrong leader" reply.
- **Waiting for the result.** A server calls `Start(op)`, then waits until that log index is applied. If a different operation appears at that index, its leadership was lost and the clerk retries.
- **Exactly once.** A retry can put the same `Append` in the log twice. Each clerk has a random id and numbers its requests; every server keeps the highest request number applied per clerk and skips duplicates. This table is part of the replicated state, so all servers skip the same ones.

## Out of scope

Snapshots and log compaction, membership changes, and a real network transport (gRPC). The log grows without bound, which is fine for the test workloads. These are the natural next steps and are listed in the plan as stretch goals.
