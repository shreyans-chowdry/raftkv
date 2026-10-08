// Package sim is an in-memory network for tests. It can drop, delay and
// reorder messages, partition the cluster and isolate or crash nodes.
//
// Determinism: every random decision (drop or not, how long to delay) comes
// from one math/rand source seeded at construction, and messages that become
// due at the same tick are delivered in send order. So for the same seed and
// the same sequence of Send calls, the network makes the same decisions and
// delivers in the same order. (Goroutine scheduling in the code *using* the
// network is not controlled, so a whole cluster run is reproducible in its
// fault schedule, not instruction by instruction.)
//
// Time inside the network is counted in ticks. Call Start to advance ticks in
// the background from a real-time ticker, or call Advance from a test to step
// time by hand.
package sim

import (
	"container/heap"
	"math/rand"
	"sync"
	"time"

	"github.com/shreyans-chowdry/raftkv/transport"
)

// LinkConfig describes how unreliable the network is.
type LinkConfig struct {
	// DropProb is the probability in [0,1] that a message is lost.
	DropProb float64
	// MinDelay and MaxDelay bound the delivery delay, in ticks.
	MinDelay, MaxDelay int
	// Reorder allows a later message on a link to overtake an earlier one.
	// When false, each link (from, to) is FIFO.
	Reorder bool
}

// Stats counts what happened to messages.
type Stats struct {
	Sent, Delivered, Dropped uint64
}

// InboxSize is the capacity of each node's receive channel. If a node falls
// that far behind, further messages to it are dropped, as a real overloaded
// socket buffer would.
const InboxSize = 4096

type pending struct {
	at  uint64
	seq uint64
	msg transport.Message
	// dst is the receiver's inbox at send time. If the receiver crashes and
	// restarts before delivery, its inbox changes and the message is lost,
	// just as packets to a dead process are.
	dst chan transport.Message
}

type pq []pending

func (q pq) Len() int { return len(q) }
func (q pq) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}
func (q pq) Swap(i, j int)       { q[i], q[j] = q[j], q[i] }
func (q *pq) Push(x interface{}) { *q = append(*q, x.(pending)) }
func (q *pq) Pop() interface{} {
	old := *q
	it := old[len(old)-1]
	*q = old[:len(old)-1]
	return it
}

type link struct{ from, to transport.NodeID }

// Network is the simulated network. It is safe for concurrent use.
type Network struct {
	mu      sync.Mutex
	rng     *rand.Rand
	now     uint64
	seq     uint64
	queue   pq
	inbox   map[transport.NodeID]chan transport.Message
	crashed map[transport.NodeID]bool
	isolate map[transport.NodeID]bool
	group   map[transport.NodeID]int // nil = no partition
	cfg     LinkConfig
	lastAt  map[link]uint64
	stats   Stats

	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
}

// New returns a reliable network (no drops, no delay) whose random choices
// are driven by seed.
func New(seed int64) *Network {
	return &Network{
		rng:     rand.New(rand.NewSource(seed)),
		inbox:   map[transport.NodeID]chan transport.Message{},
		crashed: map[transport.NodeID]bool{},
		isolate: map[transport.NodeID]bool{},
		lastAt:  map[link]uint64{},
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
}

// Start advances network time by one tick every `resolution` of real time in a
// background goroutine, until Stop is called.
func (n *Network) Start(resolution time.Duration) {
	go func() {
		defer close(n.done)
		t := time.NewTicker(resolution)
		defer t.Stop()
		for {
			select {
			case <-n.stop:
				return
			case <-t.C:
				n.Advance(1)
			}
		}
	}()
}

// Stop halts the background ticker started by Start.
func (n *Network) Stop() {
	n.stopOnce.Do(func() { close(n.stop) })
}

// Endpoint returns the transport for node id, creating its inbox if needed.
func (n *Network) Endpoint(id transport.NodeID) transport.Transport {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, ok := n.inbox[id]; !ok {
		n.inbox[id] = make(chan transport.Message, InboxSize)
	}
	return &endpoint{net: n, id: id, ch: n.inbox[id]}
}

type endpoint struct {
	net *Network
	id  transport.NodeID
	ch  chan transport.Message
}

func (e *endpoint) Send(to transport.NodeID, m transport.Message) {
	m.From = e.id
	m.To = to
	e.net.send(e, m)
}

func (e *endpoint) Recv() <-chan transport.Message { return e.ch }

// SetLinkConfig changes drop/delay/reorder behaviour for all links.
func (n *Network) SetLinkConfig(c LinkConfig) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if c.MaxDelay < c.MinDelay {
		c.MaxDelay = c.MinDelay
	}
	n.cfg = c
}

// LinkConfig returns the current link configuration.
func (n *Network) LinkConfig() LinkConfig {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.cfg
}

// Partition splits nodes into the given groups. Nodes in different groups
// cannot talk, in either direction. Nodes not listed form their own group.
func (n *Network) Partition(groups ...[]transport.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = map[transport.NodeID]int{}
	for i, g := range groups {
		for _, id := range g {
			n.group[id] = i + 1
		}
	}
}

// Heal removes any partition and reconnects isolated nodes.
func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = nil
	n.isolate = map[transport.NodeID]bool{}
}

// Disconnect isolates one node from everyone (both directions).
func (n *Network) Disconnect(id transport.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.isolate[id] = true
}

// Reconnect undoes Disconnect.
func (n *Network) Reconnect(id transport.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.isolate, id)
}

// Crash marks a node as down: messages to it are dropped, including ones
// already in flight, and its old endpoint stops receiving anything.
func (n *Network) Crash(id transport.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.crashed[id] = true
}

// Restart brings a crashed node back with a fresh, empty inbox and returns its
// new endpoint. Messages queued for the old incarnation are never delivered.
func (n *Network) Restart(id transport.NodeID) transport.Transport {
	n.mu.Lock()
	delete(n.crashed, id)
	n.inbox[id] = make(chan transport.Message, InboxSize)
	ch := n.inbox[id]
	n.mu.Unlock()
	return &endpoint{net: n, id: id, ch: ch}
}

// Stats returns message counters.
func (n *Network) Stats() Stats {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.stats
}

// connected reports whether a message can currently travel from a to b.
// Caller holds n.mu.
func (n *Network) connected(a, b transport.NodeID) bool {
	if n.crashed[a] || n.crashed[b] || n.isolate[a] || n.isolate[b] {
		return false
	}
	if n.group != nil && n.group[a] != n.group[b] {
		return false
	}
	return true
}

func (n *Network) send(e *endpoint, m transport.Message) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.stats.Sent++
	// A crashed node's old endpoint may still try to send; drop that.
	if n.inbox[e.id] != e.ch || !n.connected(m.From, m.To) {
		n.stats.Dropped++
		return
	}
	if n.cfg.DropProb > 0 && n.rng.Float64() < n.cfg.DropProb {
		n.stats.Dropped++
		return
	}
	delay := n.cfg.MinDelay
	if n.cfg.MaxDelay > n.cfg.MinDelay {
		delay += n.rng.Intn(n.cfg.MaxDelay - n.cfg.MinDelay + 1)
	}
	at := n.now + uint64(delay)
	if !n.cfg.Reorder {
		l := link{m.From, m.To}
		if at < n.lastAt[l] {
			at = n.lastAt[l]
		}
		n.lastAt[l] = at
	}
	dst, ok := n.inbox[m.To]
	if !ok {
		n.stats.Dropped++
		return
	}
	n.seq++
	heap.Push(&n.queue, pending{at: at, seq: n.seq, msg: m, dst: dst})
}

// Advance moves network time forward by ticks and delivers every message that
// has become due, in (due tick, send order) order.
func (n *Network) Advance(ticks int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i := 0; i < ticks; i++ {
		n.deliverDue()
		n.now++
	}
	n.deliverDue()
}

func (n *Network) deliverDue() {
	for n.queue.Len() > 0 && n.queue[0].at <= n.now {
		p := heap.Pop(&n.queue).(pending)
		m := p.msg
		ch := p.dst
		if n.inbox[m.To] != ch || !n.connected(m.From, m.To) {
			n.stats.Dropped++
			continue
		}
		select {
		case ch <- m:
			n.stats.Delivered++
		default:
			n.stats.Dropped++
		}
	}
}
