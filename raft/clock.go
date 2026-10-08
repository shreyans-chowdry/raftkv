package raft

import (
	"sync"
	"time"
)

// Clock is where a Node gets its ticks from. Injecting it lets tests drive
// time by hand (ManualClock) instead of waiting on the wall clock.
//
// Raft only needs one thing from time: a periodic tick. Election timeouts and
// heartbeat intervals are counted in ticks, so the same code runs on a real
// clock in deployment and on a manual clock in tests.
type Clock interface {
	NewTicker(d time.Duration) Ticker
}

// Ticker delivers ticks on C.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// RealClock uses time.Ticker.
type RealClock struct{}

// NewTicker returns a wall-clock ticker.
func (RealClock) NewTicker(d time.Duration) Ticker { return realTicker{time.NewTicker(d)} }

type realTicker struct{ t *time.Ticker }

func (r realTicker) C() <-chan time.Time { return r.t.C }
func (r realTicker) Stop()               { r.t.Stop() }

// ManualClock only moves when Advance is called. Ticks are buffered (up to
// 1024 per ticker) rather than dropped, so a test that advances by N periods
// knows the node will see N ticks.
type ManualClock struct {
	mu      sync.Mutex
	now     time.Time
	tickers []*manualTicker
}

// NewManualClock returns a clock frozen at an arbitrary fixed instant.
func NewManualClock() *ManualClock {
	return &ManualClock{now: time.Unix(1_000_000, 0)}
}

type manualTicker struct {
	clock  *ManualClock
	c      chan time.Time
	period time.Duration
	next   time.Time
}

func (t *manualTicker) C() <-chan time.Time { return t.c }

func (t *manualTicker) Stop() {
	c := t.clock
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, x := range c.tickers {
		if x == t {
			c.tickers = append(c.tickers[:i], c.tickers[i+1:]...)
			return
		}
	}
}

// NewTicker registers a ticker that fires every d of manual time.
func (c *ManualClock) NewTicker(d time.Duration) Ticker {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &manualTicker{clock: c, c: make(chan time.Time, 1024), period: d, next: c.now.Add(d)}
	c.tickers = append(c.tickers, t)
	return t
}

// Now returns the manual time.
func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves time forward by d, firing every ticker once per period that
// elapsed.
func (c *ManualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	for _, t := range c.tickers {
		for !t.next.After(c.now) {
			select {
			case t.c <- t.next:
			default:
			}
			t.next = t.next.Add(t.period)
		}
	}
}
