// Package transport defines the messages Raft nodes exchange and the
// interface the raft package uses to send and receive them.
//
// Messages are one-way and best effort: Send may drop a message, deliver it
// late, or deliver it out of order. Raft is designed to tolerate all three, so
// the raft package never waits for a "reply" to a send. A reply is just
// another message that may or may not arrive later.
//
// Two implementations exist: transport/sim (in-memory, with fault injection,
// used by tests) and transport/grpc (real network, used by cmd/raftkv-server).
package transport

import "fmt"

// NodeID identifies a Raft node. IDs start at 1; 0 means "no node".
type NodeID uint64

// MsgType says which Raft RPC (or reply) a Message carries.
type MsgType uint8

const (
	// MsgVote is a RequestVote request (Raft paper, Figure 2).
	MsgVote MsgType = iota + 1
	// MsgVoteResp is the reply to MsgVote.
	MsgVoteResp
	// MsgApp is an AppendEntries request; with no entries it is a heartbeat.
	MsgApp
	// MsgAppResp is the reply to MsgApp.
	MsgAppResp
	// MsgSnap is an InstallSnapshot request (Raft paper, section 7).
	MsgSnap
	// MsgSnapResp is the reply to MsgSnap.
	MsgSnapResp
)

func (t MsgType) String() string {
	switch t {
	case MsgVote:
		return "Vote"
	case MsgVoteResp:
		return "VoteResp"
	case MsgApp:
		return "App"
	case MsgAppResp:
		return "AppResp"
	case MsgSnap:
		return "Snap"
	case MsgSnapResp:
		return "SnapResp"
	}
	return fmt.Sprintf("MsgType(%d)", uint8(t))
}

// EntryType distinguishes client commands from internal entries.
type EntryType uint8

const (
	// EntryNormal carries a command for the state machine.
	EntryNormal EntryType = iota
	// EntryNoop is the empty entry a new leader appends at the start of its
	// term so that it can commit (and learn the commit index) quickly.
	EntryNoop
)

// Entry is one Raft log entry. Index and Term are absolute.
// Entries are treated as immutable once created: receivers may share Data
// with the sender, so nobody writes into it.
type Entry struct {
	Index uint64
	Term  uint64
	Type  EntryType
	Data  []byte
}

// Message is a union of all Raft RPC requests and replies. Only the fields
// relevant to Type are set. One flat struct keeps the transports simple: the
// sim network passes it by value and the gRPC transport maps it to one
// protobuf message.
type Message struct {
	Type MsgType
	From NodeID
	To   NodeID
	// Term is the sender's current term (every Raft message carries it).
	Term uint64

	// RequestVote fields.
	LastLogIndex uint64
	LastLogTerm  uint64
	Granted      bool // MsgVoteResp

	// AppendEntries fields.
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64

	// AppendEntries reply fields.
	Success bool
	// MatchIndex is the highest index the follower now knows matches the
	// leader's log (MsgAppResp on success, and MsgSnapResp).
	MatchIndex uint64
	// ConflictIndex/ConflictTerm implement the "fast backup" optimisation:
	// on a rejected AppendEntries the follower tells the leader which term
	// conflicted and where that term starts, so the leader can skip a whole
	// term per round trip instead of one entry.
	ConflictIndex uint64
	ConflictTerm  uint64
	// RejectIndex echoes the PrevLogIndex of the rejected request, so the
	// leader can recognise and ignore stale rejections.
	RejectIndex uint64

	// Seq is the leader's heartbeat round number. Followers echo it in
	// MsgAppResp; the leader uses the echoes to confirm it is still leader
	// before serving a ReadIndex read.
	Seq uint64

	// InstallSnapshot fields.
	SnapIndex uint64
	SnapTerm  uint64
	Snapshot  []byte
}

func (m Message) String() string {
	return fmt.Sprintf("%v %d->%d t%d", m.Type, m.From, m.To, m.Term)
}

// Transport moves Raft messages between nodes. Every node owns one.
type Transport interface {
	// Send queues m for delivery to node `to`. It sets m.To, never blocks for
	// long and gives no delivery guarantee.
	Send(to NodeID, m Message)
	// Recv returns the channel on which messages addressed to this node arrive.
	Recv() <-chan Message
}
