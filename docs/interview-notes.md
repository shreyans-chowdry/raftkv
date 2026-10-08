# Interview notes

Answers to the questions this project invites, each pointing at the code
that implements it, so you can walk an interviewer from the idea to the
line. Numbers come from `benchmarks/results/` and `benchmarks/results/chaos-500.txt`.

## Why are election timeouts randomized?

So that one follower usually times out first, wins, and heartbeats everyone
else before they start their own elections. With equal timeouts every
follower would become a candidate at once and votes would split forever.

- `raft/node.go` `resetTimeout`: a fresh random timeout in
  `[ElectionTicksMin, ElectionTicksMax]` (12-24 ticks of 25 ms = 300-600 ms
  in deployment) every time the timer is reset.
- `TestElectionTimerBounds` drives a node with the manual clock and checks
  it never campaigns before the minimum and always has by the maximum.

## When is the election timer reset, and why only then?

Only when (1) granting a vote, (2) receiving AppendEntries or
InstallSnapshot from the current leader, (3) starting an election. The
comment above `handleAppend` explains why: a follower that keeps hearing
from a candidate it refuses (stale log) must still time out and run itself,
otherwise a node that keeps rejoining with higher terms can block every
up-to-date node from ever campaigning. `TestTimerNotResetByRejectedCandidate`
sends a stale-log candidate's RequestVote every tick and checks the
follower still campaigns.

## Why can't a leader commit an old-term entry just because a majority stores it?

Figure 8 of the paper. Suppose an entry from term 2 sits on a majority. A
server without it, whose last entry is from term 3, still looks
"up to date" to voters and can win term 4, then overwrite the term-2 entry
everywhere. Once the leader has an entry from its *own* term on a majority,
no server lacking it can win, so that entry and everything before it are
safe. Rule: count replicas only for current-term entries; older ones commit
indirectly.

- `raft/node.go` `maybeCommit` (the comment there is the explanation in full).
- `TestFigure8CommitRule` builds exactly that situation with fake peers:
  acks for the term-2 entry do not move the commit index; acks for the
  term-4 entry commit both. Removing the term check makes this test fail
  (checked by mutation while building the project).
- New leaders append a no-op (`becomeLeader`) so old entries get committed
  promptly and ReadIndex has a current-term entry to wait for.

## What must be persisted, and when?

`currentTerm`, `votedFor` and the log, durable before any message that
depends on them leaves the node. Otherwise a restarted node could vote twice
in one term (two leaders) or forget entries it acknowledged (a committed
entry lost).

- `raft/persister.go`: append-only `raft.log` of `[len][crc32c][payload]`
  records; buffered until `Sync`, which writes and fsyncs once.
- `raft/node.go` `flush`: every event-loop iteration ends by syncing, then
  sending queued replies. That is also group commit: one fsync covers every
  vote, append and truncation of the iteration.
- A leader's own AppendEntries go out *before* its fsync (thesis 10.2.1):
  they promise nothing about the leader's disk, and the leader counts itself
  toward a majority only after the fsync (`selfMatch`).
- Torn writes: `Load` stops at the first short or bad-CRC record and
  truncates there (`TestTornWrite`).
- Mutation check: sending replies before the fsync made
  `TestCrashFigure8ManySeeds` report a leader-completeness violation.

## How do you avoid applying a retried Append twice?

Each clerk has a random 64-bit ID and numbers its requests. The state
machine keeps, per client, the last applied sequence number and its result
(`kv/store.go` `Apply`); a request with `seq <= last` returns the saved
result without executing. The table is part of every snapshot
(`Store.Snapshot`), or a retry arriving after a snapshot install would apply
twice. `TestDuplicateAppendAppliedOnce` drops half of all replies so the
clerk keeps retrying requests that already executed, and checks every
append landed exactly once.

## How are reads linearizable?

ReadIndex (thesis 6.4), `raft/node.go` `addRead` / `registerRead` /
`checkReads` and `kv/server.go` `readIndexGet`:

1. The leader records its commit index as the read index (after its no-op
   from this term has committed, so the commit index is up to date).
2. It confirms it is still leader: heartbeats carry a round number (`Seq`);
   once a majority has echoed a round that started after the read arrived,
   no other leader can have existed at the time of the read.
3. It waits until its state machine has applied the read index, then reads
   locally.

Lease reads would skip step 2 but depend on bounded clock drift; this
project doesn't use them. `TestNoStaleReadFromDeposedLeader` cuts a leader
off from the other servers (but not from clients), commits a new value
through the new leader, and asks the old one: it must refuse rather than
return the old value. Skipping step 2 makes that test fail and makes chaos
seeds 9 and 22 non-linearizable.

## 5 nodes split 3/2. What happens?

The 3 side elects a leader (or keeps it) and keeps committing; the 2 side
can't commit anything, and a leader stuck there steps down after an
election timeout without hearing from a majority (`checkQuorum`). On heal,
the minority's uncommitted entries are overwritten by the majority's
(`TestRejoinOverwritesUncommitted`, `TestBackupFast`). The dashboard GIF
shows the minority side cycling through ever higher terms; that's the
missing Pre-Vote extension (see the README's limitations).

## Why snapshots, and what does InstallSnapshot do?

To bound log size and restart time. The KV server snapshots when
`raft.log` passes `MaxRaftState` (`kv/server.go` `applyLoop`); Raft then
drops entries up to the snapshot index (`compact`) and rewrites its files
atomically (snapshot file first, then the log; `Persister.SaveSnapshot`).
A follower whose next needed entry was compacted away gets the leader's
snapshot instead (`sendSnapshot` / `handleSnapshot`); it keeps any part of
its own log that follows the snapshot. `TestInstallSnapshotCatchUp` cuts a
follower off for 1,000 operations with a 1 KB threshold and watches it come
back through a snapshot.

## How did you test correctness?

Layers, cheapest first:

1. Unit tests: sim network determinism, persister round trips and torn
   writes, the Figure 8 rule with fake peers, timer rules with a manual clock.
2. Invariant checker (`raft/invariants.go`) attached to every test cluster:
   election safety, leader completeness, commit agreement, plus log matching
   at the end of each test.
3. Seeded scenario suites: 1,000 election runs under random partitions;
   300 crash runs (Figure 8 with crashes on a 20%-loss reordering network).
4. Chaos (`chaos/`): 5 nodes, 5 clients on 5 keys, a fault every 200-800 ms
   (partitions, isolating the leader, crash/restart, 0-30% loss, delay,
   reordering, lost client requests and replies), every history checked by
   Porcupine. 500 seeds, 0 failures, about a million operations checked.
5. Mutation checks: I broke the code on purpose (Figure 8 rule, fsync
   before reply, ReadIndex leadership check) and confirmed the tests catch
   each one. A test suite that has never failed hasn't shown it can.

Replay any chaos seed with `go test ./chaos -run 'TestChaos$' -seed=N -v`.
The seed fixes the fault schedule, workload and network randomness;
goroutine scheduling isn't controlled, so the exact interleaving can vary.

## What limited throughput, and what helped most?

See `benchmarks/results/summary.md` for the ladder. The biggest single
win wasn't on the pack's list: profiling showed the leader doing ~1 fsync
per entry even with group commit on, because the KV server held its mutex
across `raft.Start`, so only one proposal reached the Raft loop per
iteration. Registering waiters by `(clientID, seq)` before `Start` and
dropping the lock let many proposals queue up, so one iteration batches
them into one AppendEntries and one fsync: in a short probe that took 64
clients from about 3,760 to 10,475 ops/s and leader fsyncs per entry from
0.99 to 0.15.

## Raft vs Paxos?

Same safety guarantees. Raft decomposes the problem (leader election, log
replication, safety) and relies on a strong leader through which all
entries flow, which makes it easier to implement and to reason about.
Multi-Paxos can be made equivalent but leaves more of those decisions to
the implementer.
