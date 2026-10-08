// Package chaos runs RaftKV clusters in-process on the simulated network,
// injects faults while clients run, and checks the recorded histories for
// linearizability with Porcupine.
package chaos

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/shreyans-chowdry/raftkv/kv"
	"github.com/shreyans-chowdry/raftkv/raft"
	"github.com/shreyans-chowdry/raftkv/transport/sim"
)

// Options configures a Cluster.
type Options struct {
	N    int   // number of nodes (default 5)
	Seed int64 // drives the network, elections and client-network faults

	// Raft timing (defaults: 10ms ticks, 15-30 tick elections, 5 tick heartbeat).
	Tick             time.Duration
	ElectionTicksMin int
	ElectionTicksMax int
	HeartbeatTicks   int

	MaxRaftState int64 // KV snapshot threshold in bytes (<= 0: never)
	ReadIndex    bool  // serve Gets with ReadIndex instead of the log
	// NoCheckQuorum lets deposed leaders keep believing they lead.
	NoCheckQuorum bool

	// OpTimeout for the KV servers (default 1s).
	OpTimeout time.Duration
}

// Cluster is N RaftKV nodes in one process.
type Cluster struct {
	opt     Options
	Net     *sim.Network
	Checker *raft.InvariantChecker

	mu      sync.Mutex
	rng     *rand.Rand // client-network faults
	members []*member
	// Client-side faults: probability a request, or a reply, is lost.
	reqDrop, replyDrop float64
	dir                string
}

type member struct {
	id    raft.NodeID
	dir   string
	store *raft.Persister
	rf    *raft.Node
	kv    *kv.Server
	up    bool
	boots int
}

// NewCluster starts a cluster. Call Shutdown when done.
func NewCluster(opt Options) (*Cluster, error) {
	if opt.N == 0 {
		opt.N = 5
	}
	if opt.Tick == 0 {
		opt.Tick = 10 * time.Millisecond
	}
	if opt.ElectionTicksMin == 0 {
		opt.ElectionTicksMin, opt.ElectionTicksMax = 15, 30
	}
	if opt.HeartbeatTicks == 0 {
		opt.HeartbeatTicks = 5
	}
	if opt.OpTimeout == 0 {
		opt.OpTimeout = time.Second
	}
	dir, err := os.MkdirTemp("", fmt.Sprintf("raftkv-chaos-%d-", opt.Seed))
	if err != nil {
		return nil, err
	}
	c := &Cluster{
		opt:     opt,
		Net:     sim.New(opt.Seed),
		Checker: raft.NewInvariantChecker(),
		rng:     rand.New(rand.NewSource(opt.Seed ^ 0x5eed)),
		dir:     dir,
	}
	for i := 1; i <= opt.N; i++ {
		id := raft.NodeID(i)
		c.Net.Endpoint(id)
		c.members = append(c.members, &member{id: id, dir: fmt.Sprintf("%s/n%d", dir, i)})
	}
	c.Net.Start(time.Millisecond)
	for _, m := range c.members {
		if err := c.boot(m); err != nil {
			c.Shutdown()
			return nil, err
		}
	}
	return c, nil
}

func (c *Cluster) peers() []raft.NodeID {
	var ps []raft.NodeID
	for _, m := range c.members {
		ps = append(ps, m.id)
	}
	return ps
}

// boot starts a member from its on-disk state. Caller must not hold c.mu.
func (c *Cluster) boot(m *member) error {
	st, err := raft.OpenPersister(m.dir)
	if err != nil {
		return err
	}
	applyCh := make(chan raft.ApplyMsg, 256)
	m.boots++
	rf, err := raft.NewNode(raft.Config{
		ID: m.id, Peers: c.peers(), Transport: c.Net.Restart(m.id), Storage: st,
		TickInterval: c.opt.Tick, ElectionTicksMin: c.opt.ElectionTicksMin,
		ElectionTicksMax: c.opt.ElectionTicksMax, HeartbeatTicks: c.opt.HeartbeatTicks,
		Seed: c.opt.Seed*1000 + int64(m.id)*10 + int64(m.boots), ApplyCh: applyCh, Observer: c.Checker,
		DisableCheckQuorum: c.opt.NoCheckQuorum,
	})
	if err != nil {
		st.Close()
		return err
	}
	srv := kv.NewServer(rf, applyCh, kv.Config{MaxRaftState: c.opt.MaxRaftState,
		ReadIndex: c.opt.ReadIndex, OpTimeout: c.opt.OpTimeout})
	c.mu.Lock()
	m.store, m.rf, m.kv, m.up = st, rf, srv, true
	c.mu.Unlock()
	return nil
}

// Crash stops node id abruptly: anything not fsync'd is lost, in-flight
// messages to it are dropped, its clients get errors.
func (c *Cluster) Crash(id raft.NodeID) {
	m := c.member(id)
	c.mu.Lock()
	if !m.up {
		c.mu.Unlock()
		return
	}
	m.up = false
	c.mu.Unlock()
	c.Net.Crash(id)
	m.kv.Kill()
	m.rf.Stop()
	m.store.Close()
	<-m.kv.Done()
}

// Restart boots a crashed node from its disk.
func (c *Cluster) Restart(id raft.NodeID) error {
	m := c.member(id)
	c.mu.Lock()
	up := m.up
	c.mu.Unlock()
	if up {
		return nil
	}
	return c.boot(m)
}

// Up reports whether node id is running.
func (c *Cluster) Up(id raft.NodeID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.member(id).up
}

// NumUp returns how many nodes are running.
func (c *Cluster) NumUp() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, m := range c.members {
		if m.up {
			n++
		}
	}
	return n
}

func (c *Cluster) member(id raft.NodeID) *member { return c.members[id-1] }

// IDs returns all node IDs.
func (c *Cluster) IDs() []raft.NodeID { return c.peers() }

// Node returns the running raft node for id (nil if down).
func (c *Cluster) Node(id raft.NodeID) *raft.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.member(id)
	if !m.up {
		return nil
	}
	return m.rf
}

// Server returns the running KV server for id (nil if down).
func (c *Cluster) Server(id raft.NodeID) *kv.Server {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.member(id)
	if !m.up {
		return nil
	}
	return m.kv
}

// Store returns the persister of node id (nil if down).
func (c *Cluster) Store(id raft.NodeID) *raft.Persister {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.member(id)
	if !m.up {
		return nil
	}
	return m.store
}

// Leader returns a node that currently believes it is leader, if any.
func (c *Cluster) Leader() (raft.NodeID, bool) {
	var best raft.NodeID
	var bestTerm uint64
	for _, id := range c.peers() {
		if n := c.Node(id); n != nil {
			if t, ok := n.GetState(); ok && t >= bestTerm {
				best, bestTerm = id, t
			}
		}
	}
	return best, best != 0
}

// SetClientFaults sets the probability that a client request or reply is
// lost between a clerk and a server.
func (c *Cluster) SetClientFaults(reqDrop, replyDrop float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reqDrop, c.replyDrop = reqDrop, replyDrop
}

// Clerk returns a new client of the cluster.
func (c *Cluster) Clerk(clientID uint64) *kv.Clerk {
	conns := map[raft.NodeID]kv.Conn{}
	for _, id := range c.peers() {
		conns[id] = &conn{c: c, id: id}
	}
	ck := kv.NewClerkWithID(conns, clientID, c.opt.Seed*7919+int64(clientID))
	ck.RPCTimeout = 2 * c.opt.OpTimeout
	return ck
}

// Shutdown stops every node and removes the data directory.
func (c *Cluster) Shutdown() {
	for _, m := range c.members {
		c.mu.Lock()
		up := m.up
		c.mu.Unlock()
		if up {
			c.Crash(m.id)
		}
	}
	c.Net.Stop()
	os.RemoveAll(c.dir)
}

// AllLogs returns copies of the logs of running nodes.
func (c *Cluster) AllLogs() []raft.LogSnapshot {
	var out []raft.LogSnapshot
	for _, id := range c.peers() {
		if n := c.Node(id); n != nil {
			if l, ok := n.DebugLog(); ok {
				out = append(out, l)
			}
		}
	}
	return out
}

var errLost = errors.New("chaos: message lost")

// conn is the in-process client "network" between a clerk and one server.
type conn struct {
	c  *Cluster
	id raft.NodeID
}

func (x *conn) server() (*kv.Server, bool, bool) {
	x.c.mu.Lock()
	defer x.c.mu.Unlock()
	m := x.c.member(x.id)
	dropReq := x.c.reqDrop > 0 && x.c.rng.Float64() < x.c.reqDrop
	dropRep := x.c.replyDrop > 0 && x.c.rng.Float64() < x.c.replyDrop
	if !m.up {
		return nil, false, false
	}
	return m.kv, dropReq, dropRep
}

// lost simulates waiting for a reply that never comes (shortened so tests
// stay fast; the clerk can't tell the difference).
func lost(ctx context.Context) error {
	select {
	case <-time.After(20 * time.Millisecond):
	case <-ctx.Done():
	}
	return errLost
}

func (x *conn) Get(ctx context.Context, args *kv.GetArgs) (*kv.GetReply, error) {
	s, dropReq, dropRep := x.server()
	if s == nil || dropReq {
		return nil, lost(ctx)
	}
	r := s.Get(ctx, args)
	if dropRep {
		return nil, lost(ctx)
	}
	return r, nil
}

func (x *conn) PutAppend(ctx context.Context, args *kv.PutAppendArgs) (*kv.PutAppendReply, error) {
	s, dropReq, dropRep := x.server()
	if s == nil || dropReq {
		return nil, lost(ctx)
	}
	r := s.PutAppend(ctx, args)
	if dropRep {
		return nil, lost(ctx)
	}
	return r, nil
}

// Partition/Heal etc. pass through to the network.

// Partition splits the cluster into the given groups.
func (c *Cluster) Partition(groups ...[]raft.NodeID) { c.Net.Partition(groups...) }

// Heal removes partitions and isolation.
func (c *Cluster) Heal() { c.Net.Heal() }

// Isolate cuts one node off from all others.
func (c *Cluster) Isolate(id raft.NodeID) { c.Net.Disconnect(id) }

// SetLink sets drop/delay/reorder on all links.
func (c *Cluster) SetLink(l sim.LinkConfig) { c.Net.SetLinkConfig(l) }
