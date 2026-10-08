package raft

import "sync"

// applyQueue decouples the event loop from the service. The loop pushes
// committed entries (in order) without ever blocking; a separate goroutine
// delivers them on the apply channel at whatever pace the service consumes.
// If the loop waited on the service instead, a slow state machine would stall
// heartbeats and trigger needless elections.
type applyQueue struct {
	mu      sync.Mutex
	cond    *sync.Cond
	items   []ApplyMsg
	stopped bool
	last    uint64 // index of the last message delivered
	out     chan<- ApplyMsg
	quit    chan struct{}
	done    chan struct{}
}

func newApplyQueue(out chan<- ApplyMsg) *applyQueue {
	q := &applyQueue{out: out, quit: make(chan struct{}), done: make(chan struct{})}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *applyQueue) push(msgs ...ApplyMsg) {
	q.mu.Lock()
	q.items = append(q.items, msgs...)
	q.mu.Unlock()
	q.cond.Signal()
}

func (q *applyQueue) applied() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.last
}

func (q *applyQueue) run() {
	defer close(q.done)
	defer close(q.out)
	for {
		q.mu.Lock()
		for len(q.items) == 0 && !q.stopped {
			q.cond.Wait()
		}
		if q.stopped {
			q.mu.Unlock()
			return
		}
		batch := q.items
		q.items = nil
		q.mu.Unlock()
		for _, m := range batch {
			select {
			case q.out <- m:
			case <-q.quit:
				return
			}
			q.mu.Lock()
			if m.SnapshotValid {
				q.last = m.SnapshotIndex
			} else {
				q.last = m.CommandIndex
			}
			q.mu.Unlock()
		}
	}
}

// stop discards undelivered messages, ends the goroutine and closes the
// apply channel.
func (q *applyQueue) stop() {
	q.mu.Lock()
	q.stopped = true
	q.mu.Unlock()
	close(q.quit)
	q.cond.Broadcast()
	<-q.done
}
