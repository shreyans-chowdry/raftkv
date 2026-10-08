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

// waiter is a client request waiting for its log index to be applied.
type waiter struct {
	clientID, seq uint64
	term          uint64
	ch            chan result
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
	waiters     map[uint64]*waiter
	killed      bool
	done        chan struct{}
}

// NewServer starts a KV server that consumes rf's apply channel.
func NewServer(rf *raft.Node, applyCh <-chan raft.ApplyMsg, cfg Config) *Server {
	if cfg.OpTimeout == 0 {
		cfg.OpTimeout = 2 * time.Second
	}
	s := &Server{rf: rf, cfg: cfg, store: NewStore(), waiters: map[uint64]*waiter{}, done: make(chan struct{})}
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
	for idx, w := range s.waiters {
		w.ch <- result{err: ErrShutdown}
		delete(s.waiters, idx)
	}
	s.mu.Unlock()
	s.applied.Broadcast()
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

// submit puts op in the log and waits until it is applied at the index Raft
// gave it. If something else shows up at that index, or our term changes,
// we lost leadership and the op may never commit here: the client must
// retry elsewhere (dedup makes the retry safe even if this one did commit).
func (s *Server) submit(ctx context.Context, op Op) result {
	s.mu.Lock()
	if s.killed {
		s.mu.Unlock()
		return result{err: ErrShutdown}
	}
	// Holding mu across Start is deliberate: the apply loop needs mu to
	// apply anything, so the entry cannot be applied before we register the
	// waiter. Raft's event loop never takes mu, so this can't deadlock.
	idx, term, isLeader := s.rf.Start(op.Encode())
	if !isLeader {
		s.mu.Unlock()
		return result{err: ErrWrongLeader}
	}
	w := &waiter{clientID: op.ClientID, seq: op.Seq, term: term, ch: make(chan result, 1)}
	if old, ok := s.waiters[idx]; ok {
		old.ch <- result{err: ErrWrongLeader}
	}
	s.waiters[idx] = w
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
			s.dropWaiter(idx, w)
			return result{err: ErrTimeout}
		case <-timer.C:
			s.dropWaiter(idx, w)
			return result{err: ErrTimeout}
		case <-check.C:
			if t, _ := s.rf.GetState(); t != term {
				s.dropWaiter(idx, w)
				return result{err: ErrWrongLeader}
			}
		}
	}
}

func (s *Server) dropWaiter(idx uint64, w *waiter) {
	s.mu.Lock()
	if s.waiters[idx] == w {
		delete(s.waiters, idx)
	}
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
				// Waiters at or below the snapshot can't learn their result.
				for idx, w := range s.waiters {
					if idx <= m.SnapshotIndex {
						w.ch <- result{err: ErrWrongLeader}
						delete(s.waiters, idx)
					}
				}
			}
		case m.CommandValid && m.CommandIndex > s.lastApplied:
			s.lastApplied = m.CommandIndex
			w := s.waiters[m.CommandIndex]
			delete(s.waiters, m.CommandIndex)
			if m.Command == nil {
				if w != nil {
					w.ch <- result{err: ErrWrongLeader}
				}
				break
			}
			op, err := DecodeOp(m.Command)
			if err != nil {
				panic(err)
			}
			v := s.store.Apply(op)
			if w != nil {
				if w.clientID == op.ClientID && w.seq == op.Seq {
					w.ch <- result{value: v}
				} else {
					w.ch <- result{err: ErrWrongLeader}
				}
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
	for idx, w := range s.waiters {
		w.ch <- result{err: ErrShutdown}
		delete(s.waiters, idx)
	}
	s.mu.Unlock()
	s.applied.Broadcast()
}
