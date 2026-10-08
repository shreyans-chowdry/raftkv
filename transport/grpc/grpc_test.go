package grpctransport

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/shreyans-chowdry/raftkv/kv"
	"github.com/shreyans-chowdry/raftkv/raft"
	"github.com/shreyans-chowdry/raftkv/transport"
)

func TestProtoRoundTrip(t *testing.T) {
	m := transport.Message{Type: transport.MsgApp, From: 1, To: 2, Term: 3, LastLogIndex: 4, LastLogTerm: 5,
		Granted: true, PrevLogIndex: 6, PrevLogTerm: 7, LeaderCommit: 8, Success: true, MatchIndex: 9,
		ConflictIndex: 10, ConflictTerm: 11, RejectIndex: 12, Seq: 13, SnapIndex: 14, SnapTerm: 15,
		Snapshot: []byte("snap"),
		Entries: []transport.Entry{{Index: 1, Term: 2, Type: transport.EntryNoop}, {Index: 2, Term: 2, Data: []byte("x")}}}
	got := FromPB(ToPB(m))
	if !reflect.DeepEqual(got, m) {
		t.Fatalf("round trip:\n got %+v\nwant %+v", got, m)
	}
}

type node struct {
	gs    *grpc.Server
	tr    *Transport
	rf    *raft.Node
	srv   *kv.Server
	store *raft.Persister
}

func (n *node) kill() {
	n.gs.Stop()
	n.srv.Kill()
	n.rf.Stop()
	n.tr.Close()
	n.store.Close()
}

// TestClusterOverGRPC runs 3 real nodes over gRPC on localhost, writes,
// kills one node, and checks the other two keep serving.
func TestClusterOverGRPC(t *testing.T) {
	const n = 3
	lis := make([]net.Listener, n)
	addrs := map[raft.NodeID]string{}
	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lis[i] = l
		addrs[raft.NodeID(i+1)] = l.Addr().String()
	}
	nodes := make([]*node, n)
	for i := 0; i < n; i++ {
		id := raft.NodeID(i + 1)
		st, _ := raft.OpenPersister(t.TempDir())
		tr := New(id, addrs)
		ch := make(chan raft.ApplyMsg, 256)
		rf, err := raft.NewNode(raft.Config{ID: id, Peers: []raft.NodeID{1, 2, 3}, Transport: tr, Storage: st,
			TickInterval: 10 * time.Millisecond, ElectionTicksMin: 15, ElectionTicksMax: 30, HeartbeatTicks: 5,
			ApplyCh: ch})
		if err != nil {
			t.Fatal(err)
		}
		srv := kv.NewServer(rf, ch, kv.Config{ReadIndex: true, MaxRaftState: 4096})
		gs := grpc.NewServer(ServerOptions()...)
		tr.Register(gs)
		RegisterKV(gs, srv)
		go gs.Serve(lis[i])
		nodes[i] = &node{gs: gs, tr: tr, rf: rf, srv: srv, store: st}
	}
	defer func() {
		for _, x := range nodes {
			if x != nil {
				x.kill()
			}
		}
	}()

	conns, closeAll, err := DialCluster(addrs)
	if err != nil {
		t.Fatal(err)
	}
	defer closeAll()
	ck := kv.NewClerk(conns)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := 0; i < 50; i++ {
		if err := ck.Append(ctx, "k", fmt.Sprint(i%10)); err != nil {
			t.Fatal(err)
		}
	}
	// Kill the leader.
	var leader int
	for i, x := range nodes {
		if _, l := x.rf.GetState(); l {
			leader = i
		}
	}
	nodes[leader].kill()
	nodes[leader] = nil
	if err := ck.Append(ctx, "k", "!"); err != nil {
		t.Fatal(err)
	}
	v, err := ck.Get(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	want := ""
	for i := 0; i < 50; i++ {
		want += fmt.Sprint(i % 10)
	}
	want += "!"
	if v != want {
		t.Fatalf("got %q want %q", v, want)
	}
}

func TestParsePeers(t *testing.T) {
	p, err := ParsePeers("1=a:1, 2=b:2,3=c:3")
	if err != nil || len(p) != 3 || p[2] != "b:2" {
		t.Fatalf("%v %v", p, err)
	}
	for _, bad := range []string{"", "x=a", "1a:1", "0=a:1"} {
		if _, err := ParsePeers(bad); err == nil {
			t.Fatalf("ParsePeers(%q) should fail", bad)
		}
	}
}
