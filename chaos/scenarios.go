package chaos

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/shreyans-chowdry/raftkv/raft"
	"github.com/shreyans-chowdry/raftkv/transport/sim"
)

// Scheduler injects one fault every 200-800ms, chosen by a seeded RNG:
// a random two-way partition, isolating the leader, crashing a node (and
// restarting it 0.5-2s later), changing message loss (0-30%), changing
// delay and reordering, or healing everything. It never lets more than a
// minority be down at once, so a majority can always make progress once the
// network allows it.
type Scheduler struct {
	c   *Cluster
	rng *rand.Rand
	log *EventLog

	restarts map[raft.NodeID]time.Time
}

// EventLog is a timestamped list of what the scheduler did; printed when a
// run fails so the failure can be read alongside the history.
type EventLog struct {
	mu     sync.Mutex
	start  time.Time
	events []string
}

// NewEventLog starts an empty log.
func NewEventLog() *EventLog { return &EventLog{start: time.Now()} }

// Addf records an event.
func (l *EventLog) Addf(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, fmt.Sprintf("%7.3fs  %s", time.Since(l.start).Seconds(), fmt.Sprintf(format, args...)))
}

// String returns all events, one per line.
func (l *EventLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.events, "\n")
}

// Len returns the number of events.
func (l *EventLog) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.events)
}

// NewScheduler returns a scheduler for c driven by seed.
func NewScheduler(c *Cluster, seed int64, log *EventLog) *Scheduler {
	return &Scheduler{c: c, rng: rand.New(rand.NewSource(seed)), log: log, restarts: map[raft.NodeID]time.Time{}}
}

// Run injects faults until stop is closed.
func (s *Scheduler) Run(stop <-chan struct{}) {
	for {
		wait := time.Duration(200+s.rng.Intn(601)) * time.Millisecond
		select {
		case <-stop:
			return
		case <-time.After(wait):
		}
		s.restartDue()
		s.step()
	}
}

func (s *Scheduler) restartDue() {
	now := time.Now()
	for id, at := range s.restarts {
		if !now.Before(at) {
			if err := s.c.Restart(id); err != nil {
				s.log.Addf("restart n%d FAILED: %v", id, err)
				continue
			}
			s.log.Addf("restart n%d", id)
			delete(s.restarts, id)
		}
	}
}

func (s *Scheduler) step() {
	ids := s.c.IDs()
	n := len(ids)
	switch s.rng.Intn(6) {
	case 0: // random partition into two groups
		perm := s.rng.Perm(n)
		cut := 1 + s.rng.Intn(n-1)
		var a, b []raft.NodeID
		for i, p := range perm {
			if i < cut {
				a = append(a, ids[p])
			} else {
				b = append(b, ids[p])
			}
		}
		s.c.Partition(a, b)
		s.log.Addf("partition %v | %v", a, b)
	case 1: // isolate the leader
		if l, ok := s.c.Leader(); ok {
			s.c.Isolate(l)
			s.log.Addf("isolate leader n%d", l)
		} else {
			s.log.Addf("isolate leader: no leader right now")
		}
	case 2: // crash a node, restart it later
		down := n - s.c.NumUp()
		if down >= (n-1)/2 {
			s.log.Addf("crash: skipped, %d already down", down)
			return
		}
		var cand []raft.NodeID
		for _, id := range ids {
			if s.c.Up(id) {
				cand = append(cand, id)
			}
		}
		id := cand[s.rng.Intn(len(cand))]
		s.c.Crash(id)
		after := time.Duration(500+s.rng.Intn(1500)) * time.Millisecond
		s.restarts[id] = time.Now().Add(after)
		s.log.Addf("crash n%d (restart in %v)", id, after)
	case 3: // message loss
		l := s.c.Net.LinkConfig()
		l.DropProb = float64(s.rng.Intn(31)) / 100
		s.c.SetLink(l)
		cd := l.DropProb / 3
		s.c.SetClientFaults(cd, cd)
		s.log.Addf("drop %.0f%% (client %.0f%%)", l.DropProb*100, cd*100)
	case 4: // delay and reordering
		l := s.c.Net.LinkConfig()
		l.MinDelay = 0
		l.MaxDelay = s.rng.Intn(21)
		l.Reorder = s.rng.Intn(2) == 0
		s.c.SetLink(l)
		s.log.Addf("delay 0-%d ticks, reorder=%v", l.MaxDelay, l.Reorder)
	case 5:
		s.c.Heal()
		s.log.Addf("heal")
	}
}

// Calm undoes every fault: heal, reliable links, no client loss, restart
// all nodes.
func (s *Scheduler) Calm() {
	s.c.Heal()
	s.c.SetLink(sim.LinkConfig{})
	s.c.SetClientFaults(0, 0)
	for _, id := range s.c.IDs() {
		if !s.c.Up(id) {
			s.c.Restart(id)
		}
	}
	s.restarts = map[raft.NodeID]time.Time{}
	s.log.Addf("calm: healed, reliable network, all nodes up")
}
