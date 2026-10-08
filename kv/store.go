// Package kv is a linearizable key-value service on top of package raft.
//
// Writes (Put, Append) go through the Raft log. Every client has a random
// 64-bit ID and numbers its requests 1, 2, 3...; the state machine remembers
// the last sequence number and result per client, so a retried request that
// was already applied is answered from that table instead of being applied
// twice (exactly-once semantics). The table is part of every snapshot.
//
// Reads (Get) either go through the log too, or, with ReadIndex enabled, are
// served by the leader after confirming its leadership (Raft thesis 6.4).
package kv

import (
	"encoding/binary"
	"errors"
	"sort"
)

// OpType is the kind of operation.
type OpType uint8

const (
	OpGet OpType = iota + 1
	OpPut
	OpAppend
)

func (t OpType) String() string {
	switch t {
	case OpGet:
		return "Get"
	case OpPut:
		return "Put"
	case OpAppend:
		return "Append"
	}
	return "?"
}

// Op is a command in the Raft log.
type Op struct {
	Type     OpType
	Key      string
	Value    string
	ClientID uint64
	Seq      uint64
}

// Encode serialises the op for the Raft log.
func (o Op) Encode() []byte {
	b := make([]byte, 0, 1+8+8+8+len(o.Key)+len(o.Value))
	b = append(b, byte(o.Type))
	b = binary.LittleEndian.AppendUint64(b, o.ClientID)
	b = binary.LittleEndian.AppendUint64(b, o.Seq)
	b = binary.AppendUvarint(b, uint64(len(o.Key)))
	b = append(b, o.Key...)
	b = append(b, o.Value...)
	return b
}

// DecodeOp parses an encoded op.
func DecodeOp(b []byte) (Op, error) {
	if len(b) < 17 {
		return Op{}, errors.New("kv: op too short")
	}
	o := Op{Type: OpType(b[0]),
		ClientID: binary.LittleEndian.Uint64(b[1:]),
		Seq:      binary.LittleEndian.Uint64(b[9:])}
	rest := b[17:]
	kl, n := binary.Uvarint(rest)
	if n <= 0 || uint64(len(rest)-n) < kl {
		return Op{}, errors.New("kv: bad key length")
	}
	o.Key = string(rest[n : n+int(kl)])
	o.Value = string(rest[n+int(kl):])
	return o, nil
}

type dedupEntry struct {
	seq   uint64
	value string
}

// Store is the deterministic state machine. Given the same ops in the same
// order, every replica ends up in the same state. It is not safe for
// concurrent use; Server serialises access.
type Store struct {
	data  map[string]string
	dedup map[uint64]dedupEntry // clientID -> last applied seq and its result
}

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{data: map[string]string{}, dedup: map[uint64]dedupEntry{}}
}

// Apply executes op and returns its result (the value, for Get). A request
// whose sequence number is not newer than the client's last applied one is
// a duplicate: it is not executed again, and the saved result is returned.
//
// This relies on each client having at most one request outstanding, which
// Clerk guarantees.
func (s *Store) Apply(op Op) string {
	if d, ok := s.dedup[op.ClientID]; ok && op.Seq <= d.seq {
		return d.value
	}
	var result string
	switch op.Type {
	case OpGet:
		result = s.data[op.Key]
	case OpPut:
		s.data[op.Key] = op.Value
	case OpAppend:
		s.data[op.Key] += op.Value
	}
	s.dedup[op.ClientID] = dedupEntry{seq: op.Seq, value: result}
	return result
}

// Get reads a key without going through the log (used by ReadIndex reads).
func (s *Store) Get(key string) string { return s.data[key] }

// Len returns the number of keys.
func (s *Store) Len() int { return len(s.data) }

// Snapshot serialises the whole state, including the dedup table (without
// it, a request retried after a snapshot install would be applied twice).
// Keys are written in sorted order so equal states give equal bytes.
func (s *Store) Snapshot() []byte {
	var b []byte
	b = binary.AppendUvarint(b, uint64(len(s.data)))
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b = appendString(b, k)
		b = appendString(b, s.data[k])
	}
	b = binary.AppendUvarint(b, uint64(len(s.dedup)))
	ids := make([]uint64, 0, len(s.dedup))
	for id := range s.dedup {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		d := s.dedup[id]
		b = binary.LittleEndian.AppendUint64(b, id)
		b = binary.AppendUvarint(b, d.seq)
		b = appendString(b, d.value)
	}
	return b
}

// Restore replaces the state with a snapshot.
func (s *Store) Restore(b []byte) error {
	r := reader{b: b}
	n := r.uvarint()
	data := make(map[string]string, n)
	for i := uint64(0); i < n && r.err == nil; i++ {
		k := r.str()
		data[k] = r.str()
	}
	m := r.uvarint()
	dedup := make(map[uint64]dedupEntry, m)
	for i := uint64(0); i < m && r.err == nil; i++ {
		id := r.u64()
		seq := r.uvarint()
		dedup[id] = dedupEntry{seq: seq, value: r.str()}
	}
	if r.err != nil {
		return r.err
	}
	s.data, s.dedup = data, dedup
	return nil
}

func appendString(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

type reader struct {
	b   []byte
	err error
}

var errCorrupt = errors.New("kv: corrupt snapshot")

func (r *reader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.err = errCorrupt
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) u64() uint64 {
	if r.err != nil || len(r.b) < 8 {
		r.err = errCorrupt
		return 0
	}
	v := binary.LittleEndian.Uint64(r.b)
	r.b = r.b[8:]
	return v
}

func (r *reader) str() string {
	n := r.uvarint()
	if r.err != nil || uint64(len(r.b)) < n {
		r.err = errCorrupt
		return ""
	}
	s := string(r.b[:n])
	r.b = r.b[n:]
	return s
}
