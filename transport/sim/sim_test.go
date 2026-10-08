package sim

import (
	"math"
	"testing"

	"github.com/shreyans-chowdry/raftkv/transport"
)

// drain returns every message currently waiting in a node's inbox.
func drain(t transport.Transport) []transport.Message {
	var out []transport.Message
	for {
		select {
		case m := <-t.Recv():
			out = append(out, m)
		default:
			return out
		}
	}
}

// deliveryOrder sends 500 messages from 3 senders to one receiver over a
// lossy, reordering network and returns the order they arrived in.
func deliveryOrder(seed int64) []uint64 {
	n := New(seed)
	n.SetLinkConfig(LinkConfig{DropProb: 0.1, MinDelay: 0, MaxDelay: 20, Reorder: true})
	a, b, c, r := n.Endpoint(1), n.Endpoint(2), n.Endpoint(3), n.Endpoint(4)
	senders := []transport.Transport{a, b, c}
	var got []uint64
	for i := 0; i < 500; i++ {
		senders[i%3].Send(4, transport.Message{Type: transport.MsgApp, Term: uint64(i)})
		if i%7 == 0 {
			n.Advance(1)
		}
		for _, m := range drain(r) {
			got = append(got, m.Term)
		}
	}
	n.Advance(50)
	for _, m := range drain(r) {
		got = append(got, m.Term)
	}
	return got
}

func TestDeterministicForSeed(t *testing.T) {
	x, y := deliveryOrder(42), deliveryOrder(42)
	if len(x) != len(y) {
		t.Fatalf("same seed delivered %d vs %d messages", len(x), len(y))
	}
	for i := range x {
		if x[i] != y[i] {
			t.Fatalf("same seed, different order at %d: %d vs %d", i, x[i], y[i])
		}
	}
	z := deliveryOrder(43)
	same := len(z) == len(x)
	for i := 0; same && i < len(x); i++ {
		same = x[i] == z[i]
	}
	if same {
		t.Fatalf("different seeds produced identical delivery; RNG is not being used")
	}
	// Reordering must actually happen.
	reordered := false
	for i := 1; i < len(x); i++ {
		if x[i] < x[i-1] {
			reordered = true
		}
	}
	if !reordered {
		t.Fatalf("no reordering observed with Reorder=true")
	}
}

func TestFIFOWithoutReorder(t *testing.T) {
	n := New(1)
	n.SetLinkConfig(LinkConfig{MinDelay: 0, MaxDelay: 30})
	a, r := n.Endpoint(1), n.Endpoint(2)
	for i := 0; i < 200; i++ {
		a.Send(2, transport.Message{Term: uint64(i)})
		n.Advance(1)
	}
	n.Advance(100)
	got := drain(r)
	if len(got) != 200 {
		t.Fatalf("delivered %d, want 200", len(got))
	}
	for i := range got {
		if got[i].Term != uint64(i) {
			t.Fatalf("FIFO violated at %d: got %d", i, got[i].Term)
		}
	}
}

func TestPartitionBlocksBothWaysAndHealRestores(t *testing.T) {
	n := New(7)
	e := map[transport.NodeID]transport.Transport{}
	for id := transport.NodeID(1); id <= 5; id++ {
		e[id] = n.Endpoint(id)
	}
	n.Partition([]transport.NodeID{1, 2}, []transport.NodeID{3, 4, 5})
	e[1].Send(3, transport.Message{})
	e[3].Send(1, transport.Message{})
	e[1].Send(2, transport.Message{})
	e[4].Send(5, transport.Message{})
	n.Advance(1)
	if got := len(drain(e[3])) + len(drain(e[1])); got != 0 {
		t.Fatalf("%d messages crossed the partition", got)
	}
	if len(drain(e[2])) != 1 || len(drain(e[5])) != 1 {
		t.Fatalf("messages inside a group were not delivered")
	}

	// A message in flight when the partition starts must not cross it.
	n.Heal()
	n.SetLinkConfig(LinkConfig{MinDelay: 5, MaxDelay: 5})
	e[1].Send(3, transport.Message{})
	n.Partition([]transport.NodeID{1}, []transport.NodeID{2, 3, 4, 5})
	n.Advance(10)
	if len(drain(e[3])) != 0 {
		t.Fatalf("in-flight message crossed a partition created after sending")
	}

	n.Heal()
	e[1].Send(3, transport.Message{})
	e[3].Send(1, transport.Message{})
	n.Advance(10)
	if len(drain(e[3])) != 1 || len(drain(e[1])) != 1 {
		t.Fatalf("heal did not restore traffic")
	}
}

func TestDisconnectAndCrash(t *testing.T) {
	n := New(3)
	a, b := n.Endpoint(1), n.Endpoint(2)
	n.Disconnect(2)
	a.Send(2, transport.Message{})
	b.Send(1, transport.Message{})
	n.Advance(1)
	if len(drain(a))+len(drain(b)) != 0 {
		t.Fatalf("disconnected node exchanged messages")
	}
	n.Reconnect(2)
	a.Send(2, transport.Message{})
	n.Advance(1)
	if len(drain(b)) != 1 {
		t.Fatalf("reconnect did not restore traffic")
	}

	// Crash with a message in flight: the new incarnation must not get it.
	n.SetLinkConfig(LinkConfig{MinDelay: 3, MaxDelay: 3})
	a.Send(2, transport.Message{Term: 99})
	n.Crash(2)
	b.Send(1, transport.Message{})
	b2 := n.Restart(2)
	n.Advance(5)
	if len(drain(b2)) != 0 {
		t.Fatalf("message sent before the crash reached the restarted node")
	}
	if len(drain(a)) != 0 {
		t.Fatalf("message from crashed node was delivered")
	}
	b.Send(1, transport.Message{}) // old incarnation's endpoint is dead
	b2.Send(1, transport.Message{})
	n.Advance(5)
	if got := len(drain(a)); got != 1 {
		t.Fatalf("got %d messages, want only the new incarnation's 1", got)
	}
}

func TestDropProbabilityRoughlyRespected(t *testing.T) {
	n := New(99)
	n.SetLinkConfig(LinkConfig{DropProb: 0.3})
	a, r := n.Endpoint(1), n.Endpoint(2)
	const total = 10000
	delivered := 0
	for i := 0; i < total; i++ {
		a.Send(2, transport.Message{})
		if i%1000 == 999 {
			n.Advance(1)
			delivered += len(drain(r))
		}
	}
	n.Advance(1)
	delivered += len(drain(r))
	frac := 1 - float64(delivered)/total
	// Binomial std dev for n=10000, p=0.3 is ~0.0046; allow 5 sigma.
	if math.Abs(frac-0.3) > 0.025 {
		t.Fatalf("drop fraction %.3f, want ~0.30", frac)
	}
	st := n.Stats()
	if st.Sent != total || st.Delivered+st.Dropped != total {
		t.Fatalf("stats don't add up: %+v", st)
	}
}
