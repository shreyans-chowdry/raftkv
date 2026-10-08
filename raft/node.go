// Package raft is a from-scratch implementation of the Raft consensus
// algorithm, following the extended paper (Ongaro & Ousterhout, 2014) and,
// for client reads, chapter 6 of Ongaro's thesis.
//
// Concurrency model: each Node runs one event-loop goroutine that owns all
// Raft state. Incoming messages, timer ticks, client proposals, snapshot and
// read requests reach it through channels. Nothing inside the loop takes a
// lock or waits on the network, so there is no lock ordering to get wrong and
// no way to block the loop while sending an RPC.
//
// Every loop iteration ends with flush(): first make buffered state durable
// (one fsync for everything changed in that iteration, which is group
// commit), then send the messages queued during the iteration. Because
// replies are sent only after the fsync, no reply ever claims something that
// isn't on disk yet. (A leader's AppendEntries are the exception: they
// promise nothing about the leader's disk, so they go out before its fsync.)
package raft

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shreyans-chowdry/raftkv/transport"
)

// NodeID re-exports transport.NodeID.
type NodeID = transport.NodeID

// Role is a node's current role.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "?"
}

// ErrNotLeader is returned by ReadIndex when this node is not (or stops being)
// the leader before the read can be confirmed.
var ErrNotLeader = errors.New("raft: not leader")

// ErrStopped is returned once the node has been stopped.
var ErrStopped = errors.New("raft: node stopped")

// ApplyMsg is delivered on the apply channel, in log order, exactly once per
// committed entry (or snapshot) per node incarnation.
type ApplyMsg struct {
	CommandValid bool
	Command      []byte // nil for the leader's no-op entries
	CommandIndex uint64
	CommandTerm  uint64

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex uint64
	SnapshotTerm  uint64
}

// Config configures a Node. Zero values get the defaults noted below.
type Config struct {
	ID    NodeID
	Peers []NodeID // every member of the cluster, including ID

	Transport transport.Transport
	Storage   *Persister
	Clock     Clock // default RealClock

	// TickInterval is the length of one tick (default 25ms). Election timeout
	// is randomized in [ElectionTicksMin, ElectionTicksMax] ticks (default
	// 12..24, i.e. 300-600ms); the leader heartbeats every HeartbeatTicks
	// (default 3, i.e. 75ms).
	TickInterval     time.Duration
	ElectionTicksMin int
	ElectionTicksMax int
	HeartbeatTicks   int

	// Seed for the randomized election timeout (default: derived from time).
	Seed int64

	// MaxBatchBytes caps the payload of one AppendEntries (default 1 MiB).
	MaxBatchBytes int
	// MaxInflight is the pipelining window: how many AppendEntries a leader
	// may have outstanding per follower (default 64; 1 = no pipelining).
	MaxInflight int
	// DisableBatching sends at most one entry per AppendEntries. Only used
	// to measure the benefit of batching.
	DisableBatching bool
	// DisableGroupCommit fsyncs after every individual state change instead
	// of once per loop iteration. Only used to measure group commit.
	DisableGroupCommit bool

	// SerialLeaderWrite makes the leader fsync new entries before sending
	// them to followers. By default it sends AppendEntries first and syncs
	// its own disk in parallel (thesis 10.2.1); that is safe because the
	// leader only counts itself toward a majority after the fsync. Only
	// used to measure the difference.
	SerialLeaderWrite bool

	// ApplyCh receives committed entries and snapshots. The node closes it
	// when stopped. Required.
	ApplyCh chan<- ApplyMsg

	// DisableCheckQuorum keeps a leader that has lost contact with a
	// majority in office until it hears of a higher term. Safety does not
	// depend on check-quorum; tests turn it off to widen the window in which
	// a deposed leader still believes it leads.
	DisableCheckQuorum bool

	// Observer, if set, is told about elections and commits so tests can
	// check safety invariants while the cluster runs.
	Observer Observer

	// Logger for debug output (nil = silent).
	Logger *log.Logger
}

func (c *Config) setDefaults() {
	if c.Clock == nil {
		c.Clock = RealClock{}
	}
	if c.TickInterval == 0 {
		c.TickInterval = 25 * time.Millisecond
	}
	if c.ElectionTicksMin == 0 {
		c.ElectionTicksMin = 12
	}
	if c.ElectionTicksMax < c.ElectionTicksMin {
		c.ElectionTicksMax = 2 * c.ElectionTicksMin
	}
	if c.HeartbeatTicks == 0 {
		c.HeartbeatTicks = 3
	}
	if c.Seed == 0 {
		c.Seed = time.Now().UnixNano() + int64(c.ID)
	}
	if c.MaxBatchBytes == 0 {
		c.MaxBatchBytes = 1 << 20
	}
	if c.MaxInflight == 0 {
		c.MaxInflight = 64
	}
}

// progress is the leader's view of one follower.
type progress struct {
	match uint64 // highest index known to be replicated on the follower
	next  uint64 // next index to send
	state progressState

	// probeSent: in probe state, one AppendEntries is outstanding; wait for
	// its reply (or the next heartbeat) before sending another.
	probeSent bool
	// inflight holds the last index of each pipelined AppendEntries not yet
	// acknowledged (replicate state).
	inflight []uint64
	// pendingSnap is the snapshot index being sent (snapshot state).
	pendingSnap uint64
	snapTicks   int

	ackedSeq     uint64 // highest heartbeat Seq the follower has echoed
	recentActive bool   // heard from the follower since the last quorum check
	madeProgress bool   // match advanced since the last heartbeat
}

type progressState int

const (
	// stateProbe: we don't know where the follower's log matches ours; send
	// one AppendEntries at a time and back up on rejection.
	stateProbe progressState = iota
	// stateReplicate: the follower is in sync; stream entries optimistically.
	stateReplicate
	// stateSnapshot: the follower needs entries we compacted; an
	// InstallSnapshot is in flight.
	stateSnapshot
)

func (p *progress) becomeProbe() {
	p.state = stateProbe
	p.next = p.match + 1
	p.probeSent = false
	p.inflight = p.inflight[:0]
}

func (p *progress) becomeReplicate() {
	p.state = stateReplicate
	p.next = p.match + 1
	p.inflight = p.inflight[:0]
}

type proposal struct {
	data  []byte
	reply chan proposeResult
}

type proposeResult struct {
	index, term uint64
	isLeader    bool
}

type readReq struct {
	index uint64
	seq   uint64
	done  chan readResult
}

type readResult struct {
	index uint64
	err   error
}

type snapReq struct {
	index uint64
	data  []byte
	done  chan struct{}
}

// Node is one Raft peer.
type Node struct {
	cfg    Config
	id     NodeID
	others []NodeID
	quorum int
	rng    *rand.Rand
	logger *log.Logger

	// Persistent state (Figure 2), mirrored to cfg.Storage.
	term uint64
	vote NodeID
	log  raftLog
	// snapData is the latest snapshot's service data (sent to followers that
	// fall behind the start of the log).
	snapData []byte

	// Volatile state.
	role         Role
	leader       NodeID
	commit       uint64
	queued       uint64 // highest index handed to the applier (lastApplied)
	selfMatch    uint64 // highest index durable on the leader's own disk
	electionTick int
	ticks        uint64
	timeout      int // randomized election timeout, in ticks
	hbTick       int
	quorumTick   int
	votes        map[NodeID]bool
	prs          map[NodeID]*progress

	// ReadIndex state.
	hbSeq        uint64
	reads        []*readReq // waiting for heartbeat acks, in seq order
	readsWaiting []*readReq // waiting for the leader to commit in its term
	readHB       bool       // broadcast a heartbeat at the end of this iteration

	outbox []transport.Message

	// Channels into the loop.
	proposeC chan proposal
	readC    chan *readReq
	snapC    chan snapReq
	statusC  chan chan Status
	debugC   chan chan LogSnapshot
	stopC    chan struct{}
	doneC    chan struct{}
	stopOnce sync.Once

	applyQ *applyQueue

	// Lock-free mirrors of term/role for GetState.
	stTerm   atomic.Uint64
	stLeader atomic.Uint64 // leader id as known by this node
	stIsLead atomic.Bool
}

// NewNode loads persisted state from cfg.Storage and starts the node.
func NewNode(cfg Config) (*Node, error) {
	cfg.setDefaults()
	if cfg.ApplyCh == nil || cfg.Storage == nil || cfg.Transport == nil {
		return nil, errors.New("raft: ApplyCh, Storage and Transport are required")
	}
	st, err := cfg.Storage.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: loading state: %w", err)
	}
	n := &Node{
		cfg:      cfg,
		id:       cfg.ID,
		rng:      rand.New(rand.NewSource(cfg.Seed)),
		logger:   cfg.Logger,
		term:     st.Term,
		vote:     st.Vote,
		log:      raftLog{snapIndex: st.SnapIndex, snapTerm: st.SnapTerm, entries: st.Entries},
		snapData: st.Snapshot,
		proposeC: make(chan proposal, 1024),
		readC:    make(chan *readReq, 1024),
		snapC:    make(chan snapReq),
		statusC:  make(chan chan Status),
		debugC:   make(chan chan LogSnapshot),
		stopC:    make(chan struct{}),
		doneC:    make(chan struct{}),
	}
	for _, p := range cfg.Peers {
		if p != cfg.ID {
			n.others = append(n.others, p)
		}
	}
	n.quorum = len(cfg.Peers)/2 + 1
	n.commit = st.SnapIndex
	n.queued = st.SnapIndex
	n.applyQ = newApplyQueue(cfg.ApplyCh)
	if st.SnapIndex > 0 {
		n.applyQ.push(ApplyMsg{SnapshotValid: true, Snapshot: st.Snapshot,
			SnapshotIndex: st.SnapIndex, SnapshotTerm: st.SnapTerm})
	}
	n.resetTimeout()
	n.publish()
	go n.applyQ.run()
	go n.run()
	return n, nil
}

func (n *Node) logf(format string, args ...interface{}) {
	if n.logger != nil {
		n.logger.Printf("[n%d t%d %v] %s", n.id, n.term, n.role, fmt.Sprintf(format, args...))
	}
}

// ---------------------------------------------------------------------------
// Public API (safe to call from any goroutine).

// Start proposes command for the log. If this node is the leader it returns
// the index the command will occupy if it commits, the current term and
// true; it returns immediately, without waiting for the commit.
func (n *Node) Start(command []byte) (index, term uint64, isLeader bool) {
	if !n.stIsLead.Load() {
		return 0, n.stTerm.Load(), false
	}
	p := proposal{data: command, reply: make(chan proposeResult, 1)}
	select {
	case n.proposeC <- p:
	case <-n.stopC:
		return 0, 0, false
	}
	select {
	case r := <-p.reply:
		return r.index, r.term, r.isLeader
	case <-n.stopC:
		return 0, 0, false
	}
}

// GetState returns the current term and whether this node believes it is the
// leader.
func (n *Node) GetState() (term uint64, isLeader bool) {
	return n.stTerm.Load(), n.stIsLead.Load()
}

// LeaderHint returns the leader this node last heard from (0 if unknown).
func (n *Node) LeaderHint() NodeID { return NodeID(n.stLeader.Load()) }

// StateSize returns the size of the persisted Raft log in bytes.
func (n *Node) StateSize() int64 { return n.cfg.Storage.Size() }

// Snapshot tells Raft that the service has captured its state up to and
// including index in data, so log entries up to index can be discarded. It
// returns once the snapshot is durable (or ignored as stale).
func (n *Node) Snapshot(index uint64, data []byte) {
	r := snapReq{index: index, data: data, done: make(chan struct{})}
	select {
	case n.snapC <- r:
	case <-n.stopC:
		return
	}
	select {
	case <-r.done:
	case <-n.stopC:
	}
}

// ReadIndex implements the read path from section 6.4 of the Raft thesis.
// It returns an index such that a state machine that has applied at least
// that index reflects every write that completed before ReadIndex was
// called. The leader records its commit index, then confirms it is still the
// leader by collecting heartbeat acknowledgements from a majority.
func (n *Node) ReadIndex(timeout time.Duration) (uint64, error) {
	if !n.stIsLead.Load() {
		return 0, ErrNotLeader
	}
	r := &readReq{done: make(chan readResult, 1)}
	select {
	case n.readC <- r:
	case <-n.stopC:
		return 0, ErrStopped
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case res := <-r.done:
		return res.index, res.err
	case <-t.C:
		return 0, ErrNotLeader
	case <-n.stopC:
		return 0, ErrStopped
	}
}

// Status is a snapshot of a node's state for debugging and /status.
type Status struct {
	ID          NodeID            `json:"id"`
	Role        string            `json:"role"`
	Term        uint64            `json:"term"`
	Leader      NodeID            `json:"leader"`
	CommitIndex uint64            `json:"commitIndex"`
	LastApplied uint64            `json:"lastApplied"`
	LastIndex   uint64            `json:"lastIndex"`
	SnapIndex   uint64            `json:"snapshotIndex"`
	LogLength   int               `json:"logLength"`
	StateBytes  int64             `json:"stateBytes"`
	Fsyncs      uint64            `json:"fsyncs"`
	Match       map[NodeID]uint64 `json:"match,omitempty"`
	Ticks       uint64            `json:"ticks"`
}

// Status returns the node's current state.
func (n *Node) Status() Status {
	c := make(chan Status, 1)
	select {
	case n.statusC <- c:
		return <-c
	case <-n.stopC:
		return Status{ID: n.id, Role: "stopped"}
	}
}

// LogSnapshot is a copy of a node's log, for invariant checks in tests.
type LogSnapshot struct {
	ID        NodeID
	SnapIndex uint64
	SnapTerm  uint64
	Commit    uint64
	Entries   []Entry
}

// DebugLog returns a copy of the node's log.
func (n *Node) DebugLog() (LogSnapshot, bool) {
	c := make(chan LogSnapshot, 1)
	select {
	case n.debugC <- c:
		return <-c, true
	case <-n.stopC:
		return LogSnapshot{}, false
	}
}

// Stop shuts the node down without syncing anything not yet durable (so it
// doubles as a crash in tests). The apply channel is closed afterwards.
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		close(n.stopC)
		<-n.doneC
		n.applyQ.stop()
	})
}

// ---------------------------------------------------------------------------
// Event loop.

func (n *Node) run() {
	defer close(n.doneC)
	ticker := n.cfg.Clock.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()
	recv := n.cfg.Transport.Recv()
	for {
		select {
		case <-n.stopC:
			return
		case m := <-recv:
			n.step(m)
			n.drain(recv)
		case p := <-n.proposeC:
			n.propose(p)
			n.drain(recv)
		case <-ticker.C():
			n.tick()
		case r := <-n.readC:
			n.addRead(r)
		case r := <-n.snapC:
			n.compact(r)
		case c := <-n.statusC:
			c <- n.status()
		case c := <-n.debugC:
			c <- LogSnapshot{ID: n.id, SnapIndex: n.log.snapIndex, SnapTerm: n.log.snapTerm,
				Commit: n.commit, Entries: append([]Entry(nil), n.log.entries...)}
		}
		n.flush()
	}
}

// drain handles whatever else is already waiting, so that one fsync covers
// as many updates as possible (group commit) and the leader can pack many
// proposals into one AppendEntries (batching).
func (n *Node) drain(recv <-chan transport.Message) {
	for i := 0; i < 256; i++ {
		select {
		case m := <-recv:
			n.step(m)
		case p := <-n.proposeC:
			n.propose(p)
		case r := <-n.readC:
			n.addRead(r)
		default:
			return
		}
	}
}

// flush makes buffered state durable, then sends queued messages.
func (n *Node) flush() {
	if n.role == Leader {
		// Proposals appended in this iteration go out to followers now.
		n.broadcastAppend()
		if n.readHB {
			n.broadcastHeartbeat()
		}
	}
	n.readHB = false
	if n.role == Leader && !n.cfg.SerialLeaderWrite {
		// AppendEntries and InstallSnapshot from a leader promise nothing
		// about the leader's own disk, so they can go out before the fsync;
		// the disk write and the network round trip then overlap. Replies
		// (votes, acks) still wait for the fsync below.
		k := 0
		for _, m := range n.outbox {
			if m.Type == transport.MsgApp || m.Type == transport.MsgSnap {
				n.cfg.Transport.Send(m.To, m)
			} else {
				n.outbox[k] = m
				k++
			}
		}
		n.outbox = n.outbox[:k]
	}
	if err := n.cfg.Storage.Sync(); err != nil {
		// Losing durability silently would break safety; better to stop.
		panic(fmt.Sprintf("raft n%d: fsync failed: %v", n.id, err))
	}
	if n.role == Leader && n.selfMatch < n.log.lastIndex() {
		// Our own entries are now on disk, so they count toward a majority.
		n.selfMatch = n.log.lastIndex()
		n.maybeCommit()
	}
	for _, m := range n.outbox {
		n.cfg.Transport.Send(m.To, m)
	}
	n.outbox = n.outbox[:0]
	n.publish()
}

// persisted is called after each change to persistent state. With group
// commit (the default) it does nothing and flush() syncs once per iteration.
func (n *Node) persisted() {
	if n.cfg.DisableGroupCommit {
		if err := n.cfg.Storage.Sync(); err != nil {
			panic(fmt.Sprintf("raft n%d: fsync failed: %v", n.id, err))
		}
	}
}

func (n *Node) publish() {
	n.stTerm.Store(n.term)
	n.stIsLead.Store(n.role == Leader)
	n.stLeader.Store(uint64(n.leader))
}

func (n *Node) send(m transport.Message) {
	m.From = n.id
	m.Term = n.term
	n.outbox = append(n.outbox, m)
}

func (n *Node) saveHardState() {
	n.cfg.Storage.SaveHardState(n.term, n.vote)
	n.persisted()
}

func (n *Node) resetTimeout() {
	lo, hi := n.cfg.ElectionTicksMin, n.cfg.ElectionTicksMax
	n.timeout = lo + n.rng.Intn(hi-lo+1)
}

// ---------------------------------------------------------------------------
// Timers.

func (n *Node) tick() {
	n.ticks++
	if n.role == Leader {
		n.hbTick++
		if n.hbTick >= n.cfg.HeartbeatTicks {
			n.hbTick = 0
			n.broadcastHeartbeat()
		}
		n.quorumTick++
		if n.quorumTick >= n.cfg.ElectionTicksMax && !n.cfg.DisableCheckQuorum {
			n.quorumTick = 0
			n.checkQuorum()
		}
		return
	}
	n.electionTick++
	if n.electionTick >= n.timeout {
		n.campaign()
	}
}

// checkQuorum makes a leader that can no longer reach a majority step down,
// so clients stuck on the minority side of a partition get redirected
// instead of waiting on a leader that can never commit.
func (n *Node) checkQuorum() {
	active := 1
	for _, p := range n.others {
		if n.prs[p].recentActive {
			active++
		}
		n.prs[p].recentActive = false
	}
	if active < n.quorum {
		n.logf("lost contact with a majority, stepping down")
		n.becomeFollower(n.term, 0)
	}
}

// ---------------------------------------------------------------------------
// Role transitions.

func (n *Node) becomeFollower(term uint64, leader NodeID) {
	if term > n.term {
		n.term = term
		n.vote = 0
		n.saveHardState()
	}
	if n.role == Leader {
		n.failReads(ErrNotLeader)
	}
	n.role = Follower
	n.leader = leader
	n.prs = nil
}

func (n *Node) campaign() {
	n.role = Candidate
	n.term++
	n.vote = n.id
	n.leader = 0
	n.saveHardState()
	// Reset case 3 of 3: starting an election.
	n.electionTick = 0
	n.resetTimeout()
	n.votes = map[NodeID]bool{n.id: true}
	n.logf("starting election")
	if n.quorum == 1 {
		n.becomeLeader()
		return
	}
	for _, p := range n.others {
		n.send(transport.Message{Type: transport.MsgVote, To: p,
			LastLogIndex: n.log.lastIndex(), LastLogTerm: n.log.lastTerm()})
	}
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.leader = n.id
	n.hbTick = 0
	n.quorumTick = 0
	n.selfMatch = 0
	n.prs = map[NodeID]*progress{}
	for _, p := range n.others {
		n.prs[p] = &progress{next: n.log.lastIndex() + 1, recentActive: true}
	}
	// Append a no-op so entries from earlier terms get committed (they can
	// only commit indirectly, see maybeCommit) and so ReadIndex has an entry
	// from this term to wait for.
	e := Entry{Index: n.log.lastIndex() + 1, Term: n.term, Type: transport.EntryNoop}
	n.log.append(e)
	n.cfg.Storage.AppendEntries([]Entry{e})
	n.persisted()
	n.logf("became leader, lastIndex=%d", n.log.lastIndex())
	if n.cfg.Observer != nil {
		n.cfg.Observer.LeaderElected(n.id, n.term, &n.log)
	}
	n.broadcastHeartbeat()
}

// ---------------------------------------------------------------------------
// Message handling.

func (n *Node) step(m transport.Message) {
	switch {
	case m.Term > n.term:
		// Any message from a higher term means we are out of date.
		lead := NodeID(0)
		if m.Type == transport.MsgApp || m.Type == transport.MsgSnap {
			lead = m.From
		}
		n.becomeFollower(m.Term, lead)
	case m.Term < n.term:
		// Stale sender. Tell requesters our term so they step down; drop
		// stale replies (acting on them could corrupt progress tracking).
		switch m.Type {
		case transport.MsgVote:
			n.send(transport.Message{Type: transport.MsgVoteResp, To: m.From})
		case transport.MsgApp:
			n.send(transport.Message{Type: transport.MsgAppResp, To: m.From, RejectIndex: m.PrevLogIndex})
		case transport.MsgSnap:
			n.send(transport.Message{Type: transport.MsgSnapResp, To: m.From})
		}
		return
	}

	switch m.Type {
	case transport.MsgVote:
		n.handleVote(m)
	case transport.MsgVoteResp:
		if n.role == Candidate {
			n.votes[m.From] = m.Granted
			granted := 0
			for _, g := range n.votes {
				if g {
					granted++
				}
			}
			if granted >= n.quorum {
				n.becomeLeader()
			}
		}
	case transport.MsgApp:
		if n.role == Leader {
			panic(fmt.Sprintf("raft n%d: two leaders in term %d (%d and %d)", n.id, n.term, n.id, m.From))
		}
		n.handleAppend(m)
	case transport.MsgAppResp:
		if n.role == Leader {
			n.handleAppendResp(m)
		}
	case transport.MsgSnap:
		if n.role == Leader {
			panic(fmt.Sprintf("raft n%d: two leaders in term %d", n.id, n.term))
		}
		n.handleSnapshot(m)
	case transport.MsgSnapResp:
		if n.role == Leader {
			n.handleSnapshotResp(m)
		}
	}
}

func (n *Node) handleVote(m transport.Message) {
	canVote := n.vote == 0 || n.vote == m.From
	// Election restriction (5.4.1): only vote for a candidate whose log is at
	// least as up-to-date as ours, so a leader always has every committed entry.
	upToDate := m.LastLogTerm > n.log.lastTerm() ||
		(m.LastLogTerm == n.log.lastTerm() && m.LastLogIndex >= n.log.lastIndex())
	grant := canVote && upToDate && n.role == Follower
	if grant {
		n.vote = m.From
		n.saveHardState()
		// Reset case 1 of 3: granting a vote.
		n.electionTick = 0
	}
	n.send(transport.Message{Type: transport.MsgVoteResp, To: m.From, Granted: grant})
}

// Why the election timer is reset only in three cases (granting a vote,
// AppendEntries/InstallSnapshot from the current leader, starting an
// election) and not on every message: a node that keeps hearing from a
// candidate it refuses to vote for (say, one with a stale log) must still be
// able to time out and run itself; otherwise a disconnected node that keeps
// rejoining with higher terms can stop the up-to-date nodes from ever
// starting an election, and the cluster livelocks with no leader.

func (n *Node) handleAppend(m transport.Message) {
	n.role = Follower
	n.leader = m.From
	// Reset case 2 of 3: AppendEntries from the current term's leader.
	n.electionTick = 0

	reply := transport.Message{Type: transport.MsgAppResp, To: m.From, Seq: m.Seq, RejectIndex: m.PrevLogIndex}
	prev, prevTerm, ents := m.PrevLogIndex, m.PrevLogTerm, m.Entries

	if prev < n.log.snapIndex {
		// The start of this message is already in our snapshot, and
		// everything in a snapshot is committed, so it matches any leader's
		// log. Skip the covered part.
		skip := n.log.snapIndex - prev
		if uint64(len(ents)) <= skip {
			ents = nil
		} else {
			ents = ents[skip:]
		}
		prev, prevTerm = n.log.snapIndex, n.log.snapTerm
	}

	if prev > n.log.lastIndex() {
		reply.ConflictIndex = n.log.lastIndex() + 1
		n.send(reply)
		return
	}
	if t, _ := n.log.term(prev); t != prevTerm {
		reply.ConflictTerm = t
		reply.ConflictIndex = n.log.firstIndexOfTerm(t, prev)
		n.send(reply)
		return
	}

	// Consistency check passed. Append whatever is new, truncating only on
	// an actual conflict: a delayed duplicate of an older AppendEntries must
	// never delete entries we already accepted.
	for i, e := range ents {
		if e.Index > n.log.lastIndex() {
			n.log.append(ents[i:]...)
			n.cfg.Storage.AppendEntries(ents[i:])
			n.persisted()
			break
		}
		if t, _ := n.log.term(e.Index); t != e.Term {
			if e.Index <= n.commit {
				panic(fmt.Sprintf("raft n%d: leader %d conflicts with committed entry %d", n.id, m.From, e.Index))
			}
			n.log.truncateFrom(e.Index)
			n.cfg.Storage.Truncate(e.Index)
			n.log.append(ents[i:]...)
			n.cfg.Storage.AppendEntries(ents[i:])
			n.persisted()
			break
		}
	}
	lastNew := prev + uint64(len(ents))
	if m.LeaderCommit > n.commit {
		// Only up to the last entry this message vouched for: beyond that our
		// log might still hold stale entries from an old term.
		c := m.LeaderCommit
		if c > lastNew {
			c = lastNew
		}
		n.advanceCommit(c)
	}
	reply.Success = true
	reply.MatchIndex = lastNew
	n.send(reply)
}

func (n *Node) handleAppendResp(m transport.Message) {
	pr := n.prs[m.From]
	if pr == nil {
		return
	}
	pr.recentActive = true
	if m.Seq > pr.ackedSeq {
		pr.ackedSeq = m.Seq
		n.checkReads()
	}

	if m.Success {
		if m.MatchIndex > pr.match {
			pr.match = m.MatchIndex
			pr.madeProgress = true
		}
		if pr.next < pr.match+1 {
			pr.next = pr.match + 1
		}
		// Release pipeline slots that this ack covers.
		k := 0
		for _, last := range pr.inflight {
			if last > pr.match {
				pr.inflight[k] = last
				k++
			}
		}
		pr.inflight = pr.inflight[:k]
		switch pr.state {
		case stateProbe:
			pr.becomeReplicate()
		case stateSnapshot:
			if pr.match >= pr.pendingSnap {
				pr.becomeReplicate()
			}
		}
		n.maybeCommit()
		n.sendAppend(m.From)
		return
	}

	// Rejected. Ignore it if it answers an older request than the one we
	// care about (it can arrive late or out of order).
	if m.RejectIndex < pr.match {
		return
	}
	switch pr.state {
	case stateReplicate:
		if m.RejectIndex <= pr.match {
			return
		}
	case stateProbe:
		if m.RejectIndex != pr.next-1 {
			return
		}
	case stateSnapshot:
		return
	}
	next := m.ConflictIndex
	if m.ConflictTerm != 0 {
		if last, ok := n.log.lastIndexOfTerm(m.ConflictTerm); ok {
			next = last + 1
		}
	}
	if next > m.RejectIndex {
		next = m.RejectIndex
	}
	if next <= pr.match {
		next = pr.match + 1
	}
	if next < 1 {
		next = 1
	}
	pr.becomeProbe()
	pr.next = next
	n.sendAppend(m.From)
}

func (n *Node) handleSnapshot(m transport.Message) {
	n.role = Follower
	n.leader = m.From
	n.electionTick = 0 // counts as hearing from the current leader
	reply := transport.Message{Type: transport.MsgSnapResp, To: m.From, MatchIndex: m.SnapIndex}
	if m.SnapIndex <= n.commit {
		// We already have (and have committed) everything it covers.
		n.send(reply)
		return
	}
	if t, ok := n.log.term(m.SnapIndex); ok && t == m.SnapTerm && m.SnapIndex <= n.log.lastIndex() {
		// Keep the part of our log that follows the snapshot.
		n.log.compactTo(m.SnapIndex, m.SnapTerm)
	} else {
		n.log = raftLog{snapIndex: m.SnapIndex, snapTerm: m.SnapTerm}
	}
	n.snapData = m.Snapshot
	n.commit = m.SnapIndex
	if err := n.cfg.Storage.Sync(); err != nil {
		panic(err)
	}
	if err := n.cfg.Storage.SaveSnapshot(m.SnapIndex, m.SnapTerm, m.Snapshot, n.term, n.vote, n.log.entries); err != nil {
		panic(fmt.Sprintf("raft n%d: saving snapshot: %v", n.id, err))
	}
	n.queued = m.SnapIndex
	n.applyQ.push(ApplyMsg{SnapshotValid: true, Snapshot: m.Snapshot,
		SnapshotIndex: m.SnapIndex, SnapshotTerm: m.SnapTerm})
	n.logf("installed snapshot at %d", m.SnapIndex)
	n.send(reply)
}

func (n *Node) handleSnapshotResp(m transport.Message) {
	pr := n.prs[m.From]
	if pr == nil {
		return
	}
	pr.recentActive = true
	if m.MatchIndex > pr.match {
		pr.match = m.MatchIndex
	}
	pr.becomeProbe()
	n.maybeCommit()
	n.sendAppend(m.From)
}

// ---------------------------------------------------------------------------
// Leader: replication.

func (n *Node) propose(p proposal) {
	if n.role != Leader {
		p.reply <- proposeResult{term: n.term}
		return
	}
	e := Entry{Index: n.log.lastIndex() + 1, Term: n.term, Data: p.data}
	n.log.append(e)
	n.cfg.Storage.AppendEntries([]Entry{e})
	n.persisted()
	p.reply <- proposeResult{index: e.Index, term: n.term, isLeader: true}
}

func (n *Node) broadcastAppend() {
	for _, p := range n.others {
		n.sendAppend(p)
	}
}

func (n *Node) maxEntries() int {
	if n.cfg.DisableBatching {
		return 1
	}
	return 1 << 16
}

// sendAppend sends entries to follower `to` according to its progress state.
func (n *Node) sendAppend(to NodeID) {
	pr := n.prs[to]
	for {
		switch pr.state {
		case stateSnapshot:
			return
		case stateProbe:
			if pr.probeSent {
				return
			}
		case stateReplicate:
			if len(pr.inflight) >= n.cfg.MaxInflight || pr.next > n.log.lastIndex() {
				return
			}
		}
		prev := pr.next - 1
		prevTerm, ok := n.log.term(prev)
		if !ok {
			// The entries this follower needs were compacted away.
			n.sendSnapshot(to)
			return
		}
		ents := n.log.batchFrom(pr.next, n.maxEntries(), n.cfg.MaxBatchBytes)
		n.send(transport.Message{Type: transport.MsgApp, To: to, PrevLogIndex: prev,
			PrevLogTerm: prevTerm, Entries: ents, LeaderCommit: n.commit, Seq: n.hbSeq})
		if pr.state == stateProbe {
			pr.probeSent = true
			return
		}
		if len(ents) == 0 {
			return
		}
		last := ents[len(ents)-1].Index
		pr.next = last + 1
		pr.inflight = append(pr.inflight, last)
	}
}

func (n *Node) sendSnapshot(to NodeID) {
	pr := n.prs[to]
	pr.state = stateSnapshot
	pr.pendingSnap = n.log.snapIndex
	pr.snapTicks = 0
	pr.inflight = pr.inflight[:0]
	n.send(transport.Message{Type: transport.MsgSnap, To: to, SnapIndex: n.log.snapIndex,
		SnapTerm: n.log.snapTerm, Snapshot: n.snapData})
}

// broadcastHeartbeat sends every follower an AppendEntries that is cheap and
// always useful:
//   - in sync (replicate): an empty AppendEntries at its match index, which
//     always succeeds and carries the commit index. If the follower made no
//     progress over a whole heartbeat interval while entries were in flight,
//     assume they were lost and fall back to probing.
//   - probing: resend the probe (the last one may have been lost).
//   - snapshot: resend the snapshot every few heartbeats if still no reply.
//
// Each heartbeat round has a new Seq, used to confirm leadership for reads.
func (n *Node) broadcastHeartbeat() {
	n.hbSeq++
	for _, to := range n.others {
		pr := n.prs[to]
		switch pr.state {
		case stateReplicate:
			if len(pr.inflight) > 0 && !pr.madeProgress {
				pr.becomeProbe()
				n.sendAppend(to)
				break
			}
			prevTerm, ok := n.log.term(pr.match)
			if !ok {
				n.sendSnapshot(to)
				break
			}
			commit := n.commit
			if commit > pr.match {
				commit = pr.match
			}
			n.send(transport.Message{Type: transport.MsgApp, To: to, PrevLogIndex: pr.match,
				PrevLogTerm: prevTerm, LeaderCommit: commit, Seq: n.hbSeq})
		case stateProbe:
			pr.probeSent = false
			n.sendAppend(to)
		case stateSnapshot:
			pr.snapTicks++
			if pr.snapTicks >= 4 {
				n.sendSnapshot(to)
			} else {
				// An empty append at index 0 always matches: it confirms
				// leadership (for reads) without disturbing the snapshot.
				n.send(transport.Message{Type: transport.MsgApp, To: to, Seq: n.hbSeq})
			}
		}
		pr.madeProgress = false
	}
}

// maybeCommit advances the commit index to the highest index stored on a
// majority, but only if that entry is from the current term.
//
// Figure 8, in short: suppose an entry from an old term sits on a majority
// of servers. That does NOT make it safe. A server that never received it
// could still win an election (its last entry may have a higher term than
// the old entry, so voters see it as up to date) and then overwrite the old
// entry everywhere. Once the leader has an entry from its *own* term on a
// majority, no server lacking that entry can win an election any more, so
// that entry and everything before it are safe. Hence: count replicas only
// for current-term entries; older entries commit indirectly with them.
func (n *Node) maybeCommit() {
	matches := make([]uint64, 0, len(n.others)+1)
	matches = append(matches, n.selfMatch)
	for _, p := range n.others {
		matches = append(matches, n.prs[p].match)
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	cand := matches[n.quorum-1]
	if cand <= n.commit {
		return
	}
	if t, _ := n.log.term(cand); t != n.term {
		return
	}
	n.advanceCommit(cand)
	n.promoteReads()
}

func (n *Node) advanceCommit(c uint64) {
	if c <= n.commit {
		return
	}
	if n.cfg.Observer != nil {
		for i := n.commit + 1; i <= c; i++ {
			if i > n.log.snapIndex {
				n.cfg.Observer.Committed(n.id, i, n.log.entryAt(i).Term)
			}
		}
	}
	n.commit = c
	if n.queued < n.log.snapIndex {
		n.queued = n.log.snapIndex
	}
	if n.queued < c {
		msgs := make([]ApplyMsg, 0, c-n.queued)
		for i := n.queued + 1; i <= c; i++ {
			e := n.log.entryAt(i)
			am := ApplyMsg{CommandValid: true, CommandIndex: i, CommandTerm: e.Term}
			if e.Type == transport.EntryNormal {
				am.Command = e.Data
			}
			msgs = append(msgs, am)
		}
		n.applyQ.push(msgs...)
		n.queued = c
	}
}

// ---------------------------------------------------------------------------
// Snapshots requested by the service.

func (n *Node) compact(r snapReq) {
	defer close(r.done)
	if r.index <= n.log.snapIndex || r.index > n.queued {
		return
	}
	t, _ := n.log.term(r.index)
	n.log.compactTo(r.index, t)
	n.snapData = r.data
	if err := n.cfg.Storage.Sync(); err != nil {
		panic(err)
	}
	if err := n.cfg.Storage.SaveSnapshot(r.index, t, r.data, n.term, n.vote, n.log.entries); err != nil {
		panic(fmt.Sprintf("raft n%d: saving snapshot: %v", n.id, err))
	}
}

// ---------------------------------------------------------------------------
// ReadIndex.

func (n *Node) addRead(r *readReq) {
	if n.role != Leader {
		r.done <- readResult{err: ErrNotLeader}
		return
	}
	if t, _ := n.log.term(n.commit); t != n.term {
		// A new leader doesn't know the true commit index until it commits
		// an entry from its own term (its no-op). Wait for that.
		n.readsWaiting = append(n.readsWaiting, r)
		return
	}
	n.registerRead(r)
}

func (n *Node) registerRead(r *readReq) {
	r.index = n.commit
	if n.quorum == 1 {
		r.done <- readResult{index: r.index}
		return
	}
	// The heartbeat sent at the end of this iteration (or any later one)
	// carries a Seq >= r.seq. Acks for it prove we were still leader after
	// the read arrived.
	r.seq = n.hbSeq + 1
	n.reads = append(n.reads, r)
	n.readHB = true
}

func (n *Node) promoteReads() {
	if len(n.readsWaiting) == 0 {
		return
	}
	if t, _ := n.log.term(n.commit); t != n.term {
		return
	}
	w := n.readsWaiting
	n.readsWaiting = nil
	for _, r := range w {
		n.registerRead(r)
	}
}

func (n *Node) checkReads() {
	for len(n.reads) > 0 {
		r := n.reads[0]
		acks := 1
		for _, p := range n.others {
			if n.prs[p].ackedSeq >= r.seq {
				acks++
			}
		}
		if acks < n.quorum {
			return
		}
		r.done <- readResult{index: r.index}
		n.reads = n.reads[1:]
	}
}

func (n *Node) failReads(err error) {
	for _, r := range n.reads {
		r.done <- readResult{err: err}
	}
	for _, r := range n.readsWaiting {
		r.done <- readResult{err: err}
	}
	n.reads, n.readsWaiting = nil, nil
}

// ---------------------------------------------------------------------------

func (n *Node) status() Status {
	s := Status{
		ID: n.id, Role: n.role.String(), Term: n.term, Leader: n.leader,
		CommitIndex: n.commit, LastApplied: n.applyQ.applied(), LastIndex: n.log.lastIndex(),
		SnapIndex: n.log.snapIndex, LogLength: len(n.log.entries),
		StateBytes: n.cfg.Storage.Size(), Fsyncs: n.cfg.Storage.SyncCount(), Ticks: n.ticks,
	}
	if n.role == Leader {
		s.Match = map[NodeID]uint64{n.id: n.selfMatch}
		for _, p := range n.others {
			s.Match[p] = n.prs[p].match
		}
	}
	return s
}
