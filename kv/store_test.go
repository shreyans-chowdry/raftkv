package kv

import "testing"

func TestOpRoundTrip(t *testing.T) {
	for _, o := range []Op{
		{Type: OpPut, Key: "k", Value: "v", ClientID: 1 << 60, Seq: 7},
		{Type: OpAppend, Key: "", Value: "x\x00y", ClientID: 0, Seq: 0},
		{Type: OpGet, Key: "a long key with spaces", ClientID: 42, Seq: 1 << 40},
	} {
		got, err := DecodeOp(o.Encode())
		if err != nil || got != o {
			t.Fatalf("round trip %+v -> %+v (%v)", o, got, err)
		}
	}
	if _, err := DecodeOp([]byte{1, 2}); err == nil {
		t.Fatalf("expected error for short op")
	}
}

func TestStoreDedup(t *testing.T) {
	s := NewStore()
	s.Apply(Op{Type: OpAppend, Key: "k", Value: "a", ClientID: 1, Seq: 1})
	s.Apply(Op{Type: OpAppend, Key: "k", Value: "a", ClientID: 1, Seq: 1}) // retry
	s.Apply(Op{Type: OpAppend, Key: "k", Value: "b", ClientID: 2, Seq: 1}) // other client
	s.Apply(Op{Type: OpAppend, Key: "k", Value: "c", ClientID: 1, Seq: 2})
	s.Apply(Op{Type: OpAppend, Key: "k", Value: "a", ClientID: 1, Seq: 1}) // very late duplicate
	if v := s.Get("k"); v != "abc" {
		t.Fatalf("got %q, want abc", v)
	}
	// A duplicate Get returns the value seen the first time.
	if v := s.Apply(Op{Type: OpGet, Key: "k", ClientID: 3, Seq: 1}); v != "abc" {
		t.Fatalf("get %q", v)
	}
	s.Apply(Op{Type: OpPut, Key: "k", Value: "z", ClientID: 2, Seq: 2})
	if v := s.Apply(Op{Type: OpGet, Key: "k", ClientID: 3, Seq: 1}); v != "abc" {
		t.Fatalf("duplicate get returned %q, want the original result abc", v)
	}
}

func TestStoreSnapshotRoundTrip(t *testing.T) {
	s := NewStore()
	for i := 0; i < 100; i++ {
		s.Apply(Op{Type: OpAppend, Key: string(rune('a' + i%7)), Value: "x", ClientID: uint64(i % 5), Seq: uint64(i/5 + 1)})
	}
	snap := s.Snapshot()
	r := NewStore()
	if err := r.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if string(r.Snapshot()) != string(snap) {
		t.Fatalf("restored store re-snapshots differently")
	}
	// The dedup table survived: a retried op is still recognised.
	before := r.Get("a")
	r.Apply(Op{Type: OpAppend, Key: "a", Value: "DUP", ClientID: 0, Seq: 20})
	if r.Get("a") != before {
		t.Fatalf("duplicate applied after restore")
	}
	if err := r.Restore(snap[:len(snap)-3]); err == nil {
		t.Fatalf("truncated snapshot restored without error")
	}
}
