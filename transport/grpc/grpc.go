// Package grpctransport carries Raft messages and client requests over gRPC.
//
// Raft traffic: each node keeps one client-streaming RPC open to every peer
// and pushes messages down it; replies come back on the peer's own stream to
// us. Each peer has its own bounded send queue and goroutine, so a slow or
// dead peer never blocks the Raft event loop (Send never waits). If the
// stream breaks, the sender redials with backoff; messages queued meanwhile
// may be dropped, which Raft tolerates.
//
// The package also has admin switches used by the live cluster view to
// simulate failures in a real deployment: Isolate (drop all Raft traffic in
// and out) and Block (drop traffic to and from chosen peers).
package grpctransport

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	pb "github.com/shreyans-chowdry/raftkv/proto/raftkvpb"
	"github.com/shreyans-chowdry/raftkv/transport"
)

// MaxMsgSize bounds a single gRPC message (snapshots travel in one message).
const MaxMsgSize = 512 << 20

// Transport implements transport.Transport over gRPC.
type Transport struct {
	pb.UnimplementedRaftServer

	id    transport.NodeID
	recv  chan transport.Message
	peers map[transport.NodeID]*peer

	mu       sync.Mutex
	blocked  map[transport.NodeID]bool
	isolated atomic.Bool

	closeOnce sync.Once
	closed    chan struct{}
	wg        sync.WaitGroup
}

type peer struct {
	id   transport.NodeID
	addr string
	q    chan transport.Message
}

// New creates the transport for node id. peers maps every other node's ID to
// its gRPC address. Call Register on the node's gRPC server so peers can
// reach it.
func New(id transport.NodeID, peers map[transport.NodeID]string) *Transport {
	t := &Transport{
		id:      id,
		recv:    make(chan transport.Message, 4096),
		peers:   map[transport.NodeID]*peer{},
		blocked: map[transport.NodeID]bool{},
		closed:  make(chan struct{}),
	}
	for pid, addr := range peers {
		if pid == id {
			continue
		}
		p := &peer{id: pid, addr: addr, q: make(chan transport.Message, 4096)}
		t.peers[pid] = p
		t.wg.Add(1)
		go t.sender(p)
	}
	return t
}

// Register exposes the Raft service on s.
func (t *Transport) Register(s *grpc.Server) { pb.RegisterRaftServer(s, t) }

// Send queues m for peer `to`. It never blocks: if the queue is full the
// message is dropped.
func (t *Transport) Send(to transport.NodeID, m transport.Message) {
	m.From, m.To = t.id, to
	if t.dropped(to) {
		return
	}
	p := t.peers[to]
	if p == nil {
		return
	}
	select {
	case p.q <- m:
	default:
	}
}

// Recv returns incoming messages.
func (t *Transport) Recv() <-chan transport.Message { return t.recv }

// Isolate drops all Raft traffic to and from this node while on.
func (t *Transport) Isolate(on bool) { t.isolated.Store(on) }

// Isolated reports whether Isolate is on.
func (t *Transport) Isolated() bool { return t.isolated.Load() }

// Block drops traffic to and from the given peers (replacing any previous
// list). Block() with no arguments unblocks everyone.
func (t *Transport) Block(ids ...transport.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.blocked = map[transport.NodeID]bool{}
	for _, id := range ids {
		t.blocked[id] = true
	}
}

// Blocked returns the currently blocked peers.
func (t *Transport) Blocked() []transport.NodeID {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []transport.NodeID
	for id := range t.blocked {
		out = append(out, id)
	}
	return out
}

func (t *Transport) dropped(peer transport.NodeID) bool {
	if t.isolated.Load() {
		return true
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.blocked[peer]
}

// Close stops the sender goroutines.
func (t *Transport) Close() {
	t.closeOnce.Do(func() { close(t.closed) })
	t.wg.Wait()
}

// Stream receives one peer's messages (server side of the Raft service).
func (t *Transport) Stream(stream pb.Raft_StreamServer) error {
	for {
		in, err := stream.Recv()
		if err != nil {
			return err
		}
		m := FromPB(in)
		if t.dropped(m.From) {
			continue
		}
		select {
		case t.recv <- m:
		case <-t.closed:
			return nil
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

// DialOptions are the client options used for peer and client connections.
func DialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxMsgSize), grpc.MaxCallSendMsgSize(MaxMsgSize)),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 10 * time.Second, Timeout: 3 * time.Second}),
	}
}

// ServerOptions are the options a node's gRPC server should use.
func ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.MaxRecvMsgSize(MaxMsgSize),
		grpc.MaxSendMsgSize(MaxMsgSize),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 5 * time.Second, PermitWithoutStream: true}),
	}
}

// sender owns the stream to one peer: dial, stream, redial on failure.
func (t *Transport) sender(p *peer) {
	defer t.wg.Done()
	backoff := 25 * time.Millisecond
	for {
		select {
		case <-t.closed:
			return
		default:
		}
		conn, err := grpc.NewClient(p.addr, DialOptions()...)
		if err == nil {
			ctx, cancel := context.WithCancel(context.Background())
			var stream pb.Raft_StreamClient
			stream, err = pb.NewRaftClient(conn).Stream(ctx)
			if err == nil {
				backoff = 25 * time.Millisecond
				err = t.pump(p, stream)
			}
			cancel()
			conn.Close()
			if err == nil { // closed
				return
			}
		}
		// Peer unreachable: drop what's queued (it's stale by the time the
		// peer is back; Raft will resend what matters) and retry later.
		t.drain(p)
		select {
		case <-t.closed:
			return
		case <-time.After(backoff):
		}
		// Cap the backoff well below the election timeout: a restarted peer
		// that doesn't hear from the leader within its timeout will start an
		// election and needlessly depose a healthy leader.
		if backoff < 100*time.Millisecond {
			backoff *= 2
		}
	}
}

func (t *Transport) pump(p *peer, stream pb.Raft_StreamClient) error {
	for {
		select {
		case <-t.closed:
			stream.CloseAndRecv()
			return nil
		case m := <-p.q:
			if t.dropped(p.id) {
				continue
			}
			if err := stream.Send(ToPB(m)); err != nil {
				return err
			}
		}
	}
}

func (t *Transport) drain(p *peer) {
	for {
		select {
		case <-p.q:
		default:
			return
		}
	}
}

// ToPB converts a Raft message to its protobuf form.
func ToPB(m transport.Message) *pb.RaftMessage {
	out := &pb.RaftMessage{
		Type: uint32(m.Type), From: uint64(m.From), To: uint64(m.To), Term: m.Term,
		LastLogIndex: m.LastLogIndex, LastLogTerm: m.LastLogTerm, Granted: m.Granted,
		PrevLogIndex: m.PrevLogIndex, PrevLogTerm: m.PrevLogTerm, LeaderCommit: m.LeaderCommit,
		Success: m.Success, MatchIndex: m.MatchIndex, ConflictIndex: m.ConflictIndex,
		ConflictTerm: m.ConflictTerm, RejectIndex: m.RejectIndex, Seq: m.Seq,
		SnapIndex: m.SnapIndex, SnapTerm: m.SnapTerm, Snapshot: m.Snapshot,
	}
	if len(m.Entries) > 0 {
		out.Entries = make([]*pb.Entry, len(m.Entries))
		for i, e := range m.Entries {
			out.Entries[i] = &pb.Entry{Index: e.Index, Term: e.Term, Type: uint32(e.Type), Data: e.Data}
		}
	}
	return out
}

// FromPB converts a protobuf message back to a Raft message.
func FromPB(in *pb.RaftMessage) transport.Message {
	m := transport.Message{
		Type: transport.MsgType(in.Type), From: transport.NodeID(in.From), To: transport.NodeID(in.To),
		Term: in.Term, LastLogIndex: in.LastLogIndex, LastLogTerm: in.LastLogTerm, Granted: in.Granted,
		PrevLogIndex: in.PrevLogIndex, PrevLogTerm: in.PrevLogTerm, LeaderCommit: in.LeaderCommit,
		Success: in.Success, MatchIndex: in.MatchIndex, ConflictIndex: in.ConflictIndex,
		ConflictTerm: in.ConflictTerm, RejectIndex: in.RejectIndex, Seq: in.Seq,
		SnapIndex: in.SnapIndex, SnapTerm: in.SnapTerm, Snapshot: in.Snapshot,
	}
	if len(in.Entries) > 0 {
		m.Entries = make([]transport.Entry, len(in.Entries))
		for i, e := range in.Entries {
			m.Entries[i] = transport.Entry{Index: e.Index, Term: e.Term, Type: transport.EntryType(e.Type), Data: e.Data}
		}
	}
	return m
}
