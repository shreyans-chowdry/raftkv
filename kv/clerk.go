package kv

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	mrand "math/rand"
	"sync"
	"time"

	"github.com/shreyans-chowdry/raftkv/raft"
)

// Conn is how a Clerk reaches one server: in-process (tests, chaos harness)
// or over gRPC (transport/grpc). An error means the call's outcome is
// unknown (request or reply lost, server down).
type Conn interface {
	Get(ctx context.Context, args *GetArgs) (*GetReply, error)
	PutAppend(ctx context.Context, args *PutAppendArgs) (*PutAppendReply, error)
}

// Clerk is a client of the KV service. It hides leader discovery and
// retries: Get, Put and Append block until the operation has taken effect
// (or ctx expires). A Clerk issues one request at a time; it is safe for use
// from several goroutines, which simply take turns.
type Clerk struct {
	ids     []raft.NodeID
	servers map[raft.NodeID]Conn

	mu       sync.Mutex
	clientID uint64
	seq      uint64
	leader   int // index into ids of the last known leader
	rng      *mrand.Rand

	// RPCTimeout bounds each attempt (default 2.5s, a bit above the
	// server's own 2s op timeout).
	RPCTimeout time.Duration
}

// NewClerk returns a clerk with a random 64-bit client ID.
func NewClerk(servers map[raft.NodeID]Conn) *Clerk {
	var b [8]byte
	rand.Read(b[:])
	return NewClerkWithID(servers, binary.LittleEndian.Uint64(b[:]), int64(binary.LittleEndian.Uint64(b[:])))
}

// NewClerkWithID is NewClerk with a chosen client ID and RNG seed (tests).
func NewClerkWithID(servers map[raft.NodeID]Conn, clientID uint64, seed int64) *Clerk {
	c := &Clerk{servers: servers, clientID: clientID, rng: mrand.New(mrand.NewSource(seed)),
		RPCTimeout: 2500 * time.Millisecond}
	for id := range servers {
		c.ids = append(c.ids, id)
	}
	// Map iteration order is random; sort for reproducibility.
	for i := 1; i < len(c.ids); i++ {
		for j := i; j > 0 && c.ids[j] < c.ids[j-1]; j-- {
			c.ids[j], c.ids[j-1] = c.ids[j-1], c.ids[j]
		}
	}
	c.leader = c.rng.Intn(len(c.ids))
	return c
}

// ClientID returns this clerk's ID.
func (c *Clerk) ClientID() uint64 { return c.clientID }

// Get fetches the current value of key ("" if it doesn't exist).
func (c *Clerk) Get(ctx context.Context, key string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	args := &GetArgs{Key: key, ClientID: c.clientID, Seq: c.seq}
	var value string
	err := c.retry(ctx, func(ctx context.Context, s Conn) (Err, raft.NodeID, error) {
		r, err := s.Get(ctx, args)
		if err != nil {
			return "", 0, err
		}
		value = r.Value
		return r.Err, r.LeaderHint, nil
	})
	return value, err
}

// Put sets key to value.
func (c *Clerk) Put(ctx context.Context, key, value string) error {
	return c.putAppend(ctx, key, value, false)
}

// Append appends value to key's current value.
func (c *Clerk) Append(ctx context.Context, key, value string) error {
	return c.putAppend(ctx, key, value, true)
}

func (c *Clerk) putAppend(ctx context.Context, key, value string, isAppend bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	args := &PutAppendArgs{Key: key, Value: value, Append: isAppend, ClientID: c.clientID, Seq: c.seq}
	return c.retry(ctx, func(ctx context.Context, s Conn) (Err, raft.NodeID, error) {
		r, err := s.PutAppend(ctx, args)
		if err != nil {
			return "", 0, err
		}
		return r.Err, r.LeaderHint, nil
	})
}

// retry sends the same request (same seq) until a server accepts it. It
// follows leader hints when it gets one, otherwise tries servers in turn,
// backing off after each full round of failures.
func (c *Clerk) retry(ctx context.Context, call func(context.Context, Conn) (Err, raft.NodeID, error)) error {
	backoff := 10 * time.Millisecond
	failures := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		id := c.ids[c.leader]
		actx, cancel := context.WithTimeout(ctx, c.RPCTimeout)
		e, hint, err := call(actx, c.servers[id])
		cancel()
		if err == nil && e == OK {
			return nil
		}
		// Prefer a hint that names a different server.
		moved := false
		if err == nil && e == ErrWrongLeader && hint != 0 && hint != id {
			for i, x := range c.ids {
				if x == hint {
					c.leader = i
					moved = true
				}
			}
		}
		if !moved {
			c.leader = (c.leader + 1) % len(c.ids)
		}
		failures++
		if failures%len(c.ids) == 0 {
			jitter := time.Duration(c.rng.Int63n(int64(backoff)))
			select {
			case <-time.After(backoff + jitter):
			case <-ctx.Done():
				return ctx.Err()
			}
			if backoff < 200*time.Millisecond {
				backoff *= 2
			}
		}
	}
}
