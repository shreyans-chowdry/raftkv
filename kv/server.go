package kv

import (
	"context"
	"sync"
	"time"

	"github.com/shreyans-chowdry/raftkv/raft"
)

// Err is the outcome of a client request.
type Err string

const (
	OK             Err = ""
	ErrWrongLeader Err = "ErrWrongLeader" // try another server (see LeaderHint)
	ErrTimeout     Err = "ErrTimeout"     // outcome unknown; retry with the same seq
	ErrShutdown    Err = "ErrShutdown"    // server is stopping; try another
)

// GetArgs is a Get request.
type GetArgs struct {
	Key      string
	ClientID uint64
	Seq      uint64
}

// GetReply is a Get response.
type GetReply struct {
	Err        Err
	Value      string
	LeaderHint raft.NodeID
}

// PutAppendArgs is a Put or Append request.
type PutAppendArgs struct {
	Key      string
	Value    string
	Append   bool
	ClientID uint64
	Seq      uint64
}

// PutAppendReply is a Put/Append response.
type PutAppendReply struct {
	Err        Err
	LeaderHint raft.NodeID
}

// Config tunes a Server.
type Config struct {
	// MaxRaftState: snapshot when the Raft log file grows past this many
	// bytes. <= 0 disables snapshots.
	MaxRaftState int64
	// ReadIndex serves Gets with the ReadIndex protocol instead of putting
	// them in the log.
	ReadIndex bool
	// OpTimeout bounds how long a request waits to be committed (default 2s).
	OpTimeout time.Duration
}

type result struct {
	err   Err
	value string
}

// opKey identifies one client request.
type opKey struct{ client, seq uint64 }

// waiter is a client request waiting for its op to be applied.
type waiter struct {
	key   opKey
	index uint64 // log index Raft gave it (0 until Start returns)
	ch    chan result
}

// Server is one replica of the KV service.
type Server struct {
	rf  *raft.Node
	cfg Config

	mu          sync.Mutex
	applied     *sync.Cond // signalled whenever lastApplied moves
	store       *Store
	lastApplied uint64
	lastSnap    uint64
	waiters     map[opKey]*waiter  // by request
	byIndex     map[uint64]*waiter // by the log index Start returned
	killed      bool
	done        chan struct{}
}

// NewServer starts a KV server that consumes rf's apply channel.
func NewServer(rf *raft.Node, applyCh <-chan raft.ApplyMsg, cfg Config) *Server {
	if cfg.OpTimeout == 0 {
		cfg.OpTimeout = 2 * time.Second
	}
	s := &Server{rf: rf, cfg: cfg, store: NewStore(), waiters: map[opKey]*waiter{},
		byIndex: map[uint64]*waiter{}, done: make(chan struct{})}
	s.applied = sync.NewCond(&s.mu)
	go s.applyLoop(applyCh)
	return s
}

// Raft returns the underlying Raft node.
func (s *Server) Raft() *raft.Node { return s.rf }

// Kill stops serving. Requests in progress return ErrShutdown. It does not
// stop the Raft node; the caller owns that.
func (s *Server) Kill() {
	s.mu.Lock()
	s.killed = true
	s.failAll(ErrShutdown)
	s.mu.Unlock()
	s.applied.Broadcast()
}

// resolve answers w (if still waiting) and forgets it. Caller holds mu.
func (s *Server) resolve(w *waiter, r result) {
	if s.waiters[w.key] != w {
		return // already answered or replaced
	}
	delete(s.waiters, w.key)
	if w.index != 0 && s.byIndex[w.index] == w {
		delete(s.byIndex, w.index)
	}
	w.ch <- r
}

func (s *Server) failAll(e Err) {
	for _, w := range s.waiters {
		s.resolve(w, result{err: e})
	}
}

// Done is closed when the apply loop exits (after the Raft node stops).
func (s *Server) Done() <-chan struct{} { return s.done }

// Get returns the value for a key ("" if absent).
func (s *Server) Get(ctx context.Context, args *GetArgs) *GetReply {
	if s.cfg.ReadIndex {
		return s.readIndexGet(ctx, args)
	}
	r := s.submit(ctx, Op{Type: OpGet, Key: args.Key, ClientID: args.ClientID, Seq: args.Seq})
	return &GetReply{Err: r.err, Value: r.value, LeaderHint: s.rf.LeaderHint()}
}

// PutAppend executes a Put or Append.
func (s *Server) PutAppend(ctx context.Context, args *PutAppendArgs) *PutAppendReply {
	op := Op{Type: OpPut, Key: args.Key, Value: args.Value, ClientID: args.ClientID, Seq: args.Seq}
	if args.Append {
		op.Type = OpAppend
	}
	r := s.submit(ctx, op)
	return &PutAppendReply{Err: r.err, LeaderHint: s.rf.LeaderHint()}
}

// submit puts op in the log and waits until it is applied. If a different
// op shows up at the index Raft gave us, or our term changes, we lost
// leadership and this op may never commit here: the client must retry
// elsewhere (dedup makes the retry safe even if this attempt did commit).
//
// Waiters are keyed by (clientID, seq) and registered before Start, so the
// apply loop can't miss them, and mu is NOT held across Start. That matters
// for throughput: many submits can be inside Start at once, so the Raft
// loop finds many proposals waiting and puts them in one batch with one
// fsync. (Holding a lock across Start would let only one proposal through
// per loop iteration.)
func (s *Server) submit(ctx context.Context, op Op) result {
	w := &waiter{key: opKey{op.ClientID, op.Seq}, ch: make(chan result, 1)}
	s.mu.Lock()
	if s.killed {
		s.mu.Unlock()
		return result{err: ErrShutdown}
	}
	if old := s.waiters[w.key]; old != nil {
		// A retry of a request we're still waiting on: the old attempt
		// gives up and the client gets its answer through this one.
		s.resolve(old, result{err: ErrTimeout})
	}
	s.waiters[w.key] = w
	s.mu.Unlock()

	idx, term, isLeader := s.rf.Start(op.Encode())
	s.mu.Lock()
	if !isLeader {
		s.resolve(w, result{err: ErrWrongLeader})
	} else if s.waiters[w.key] == w {
		w.index = idx
		if old := s.byIndex[idx]; old != nil {
			s.resolve(old, result{err: ErrWrongLeader})
		}
		s.byIndex[idx] = w
	}
	s.mu.Unlock()

	timer := time.NewTimer(s.cfg.OpTimeout)
	defer timer.Stop()
	check := time.NewTicker(25 * time.Millisecond)
	defer check.Stop()
	for {
		select {
		case r := <-w.ch:
			return r
		case <-ctx.Done():
			s.giveUp(w, ErrTimeout)
		case <-timer.C:
			s.giveUp(w, ErrTimeout)
		case <-check.C:
			if t, _ := s.rf.GetState(); t != term {
				s.giveUp(w, ErrWrongLeader)
			}
		}
	}
}

// giveUp answers w with e unless the apply loop got there first; either
// way the next receive on w.ch returns.
func (s *Server) giveUp(w *waiter, e Err) {
	s.mu.Lock()
	s.resolve(w, result{err: e})
	s.mu.Unlock()
}

// readIndexGet serves a read without writing to the log: get a read index
// from Raft (which confirms we are still leader), wait until our state
// machine has applied at least that far, then read locally.
func (s *Server) readIndexGet(ctx context.Context, args *GetArgs) *GetReply {
	timeout := s.cfg.OpTimeout
	if dl, ok := ctx.Deadline(); ok && time.Until(dl) < timeout {
		timeout = time.Until(dl)
	}
	idx, err := s.rf.ReadIndex(timeout)
	if err != nil {
		return &GetReply{Err: ErrWrongLeader, LeaderHint: s.rf.LeaderHint()}
	}
	deadline := time.Now().Add(timeout)
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.lastApplied < idx && !s.killed {
		if time.Now().After(deadline) {
			return &GetReply{Err: ErrTimeout}
		}
		waitWithTimeout(s.applied, deadline)
	}
	if s.killed {
		return &GetReply{Err: ErrShutdown}
	}
	return &GetReply{Value: s.store.Get(args.Key)}
}

// waitWithTimeout waits on c (whose lock is held) until signalled or until
// the deadline, whichever comes first.
func waitWithTimeout(c *sync.Cond, deadline time.Time) {
	t := time.AfterFunc(time.Until(deadline), c.Broadcast)
	c.Wait()
	t.Stop()
}

func (s *Server) applyLoop(ch <-chan raft.ApplyMsg) {
	defer close(s.done)
	for m := range ch {
		s.mu.Lock()
		switch {
		case m.SnapshotValid:
			if m.SnapshotIndex > s.lastApplied {
				if err := s.store.Restore(m.Snapshot); err != nil {
					panic(err)
				}
				s.lastApplied = m.SnapshotIndex
				s.lastSnap = m.SnapshotIndex
				// Waiters at or below the snapshot can't learn their result
				// here; the client retries and dedup answers it.
				for idx, w := range s.byIndex {
					if idx <= m.SnapshotIndex {
						s.resolve(w, result{err: ErrWrongLeader})
					}
				}
			}
		case m.CommandValid && m.CommandIndex > s.lastApplied:
			s.lastApplied = m.CommandIndex
			atIndex := s.byIndex[m.CommandIndex]
			if m.Command == nil { // a leader's no-op
				if atIndex != nil {
					s.resolve(atIndex, result{err: ErrWrongLeader})
				}
				break
			}
			op, err := DecodeOp(m.Command)
			if err != nil {
				panic(err)
			}
			v := s.store.Apply(op)
			key := opKey{op.ClientID, op.Seq}
			// Whoever is waiting for this request gets the result, even if
			// it was waiting at a different index (e.g. an earlier attempt
			// through another leader committed it).
			if w := s.waiters[key]; w != nil {
				s.resolve(w, result{value: v})
			}
			// Whoever expected something else at this index lost out.
			if atIndex != nil && atIndex.key != key {
				s.resolve(atIndex, result{err: ErrWrongLeader})
			}
		}
		s.applied.Broadcast()
		if s.cfg.MaxRaftState > 0 && s.lastApplied > s.lastSnap && s.rf.StateSize() >= s.cfg.MaxRaftState {
			// Raft returns once the snapshot is durable and the log is
			// compacted, so StateSize drops before we check again.
			s.lastSnap = s.lastApplied
			s.rf.Snapshot(s.lastApplied, s.store.Snapshot())
		}
		s.mu.Unlock()
	}
	s.mu.Lock()
	s.killed = true
	s.failAll(ErrShutdown)
	s.mu.Unlock()
	s.applied.Broadcast()
}
