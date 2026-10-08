package grpctransport

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/grpc"

	"github.com/shreyans-chowdry/raftkv/kv"
	pb "github.com/shreyans-chowdry/raftkv/proto/raftkvpb"
	"github.com/shreyans-chowdry/raftkv/raft"
)

// KVService exposes a kv.Server over gRPC.
type KVService struct {
	pb.UnimplementedKVServer
	srv *kv.Server
}

// RegisterKV exposes srv's client API on s.
func RegisterKV(s *grpc.Server, srv *kv.Server) {
	pb.RegisterKVServer(s, &KVService{srv: srv})
}

// Get implements the KV service.
func (s *KVService) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	r := s.srv.Get(ctx, &kv.GetArgs{Key: req.Key, ClientID: req.ClientId, Seq: req.Seq})
	return &pb.GetResponse{Err: string(r.Err), Value: r.Value, LeaderHint: uint64(r.LeaderHint)}, nil
}

// PutAppend implements the KV service.
func (s *KVService) PutAppend(ctx context.Context, req *pb.PutAppendRequest) (*pb.PutAppendResponse, error) {
	r := s.srv.PutAppend(ctx, &kv.PutAppendArgs{Key: req.Key, Value: req.Value, Append: req.Append,
		ClientID: req.ClientId, Seq: req.Seq})
	return &pb.PutAppendResponse{Err: string(r.Err), LeaderHint: uint64(r.LeaderHint)}, nil
}

// KVConn is a kv.Conn to one server over gRPC.
type KVConn struct {
	conn *grpc.ClientConn
	c    pb.KVClient
}

// DialKV returns a connection to the KV service at addr. Dialing is lazy:
// errors show up on the first call.
func DialKV(addr string) (*KVConn, error) {
	conn, err := grpc.NewClient(addr, DialOptions()...)
	if err != nil {
		return nil, err
	}
	return &KVConn{conn: conn, c: pb.NewKVClient(conn)}, nil
}

// Close closes the connection.
func (k *KVConn) Close() error { return k.conn.Close() }

// Get implements kv.Conn.
func (k *KVConn) Get(ctx context.Context, a *kv.GetArgs) (*kv.GetReply, error) {
	r, err := k.c.Get(ctx, &pb.GetRequest{Key: a.Key, ClientId: a.ClientID, Seq: a.Seq})
	if err != nil {
		return nil, err
	}
	return &kv.GetReply{Err: kv.Err(r.Err), Value: r.Value, LeaderHint: raft.NodeID(r.LeaderHint)}, nil
}

// PutAppend implements kv.Conn.
func (k *KVConn) PutAppend(ctx context.Context, a *kv.PutAppendArgs) (*kv.PutAppendReply, error) {
	r, err := k.c.PutAppend(ctx, &pb.PutAppendRequest{Key: a.Key, Value: a.Value, Append: a.Append,
		ClientId: a.ClientID, Seq: a.Seq})
	if err != nil {
		return nil, err
	}
	return &kv.PutAppendReply{Err: kv.Err(r.Err), LeaderHint: raft.NodeID(r.LeaderHint)}, nil
}

// DialCluster connects to every server and returns conns keyed by node ID.
func DialCluster(addrs map[raft.NodeID]string) (map[raft.NodeID]kv.Conn, func(), error) {
	conns := map[raft.NodeID]kv.Conn{}
	var all []*KVConn
	for id, a := range addrs {
		c, err := DialKV(a)
		if err != nil {
			for _, x := range all {
				x.Close()
			}
			return nil, nil, err
		}
		all = append(all, c)
		conns[id] = c
	}
	return conns, func() {
		for _, x := range all {
			x.Close()
		}
	}, nil
}

// ParsePeers parses "1=host:port,2=host:port,...".
func ParsePeers(s string) (map[raft.NodeID]string, error) {
	out := map[raft.NodeID]string{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("bad peer %q, want id=host:port", part)
		}
		n, err := strconv.ParseUint(id, 10, 64)
		if err != nil || n == 0 {
			return nil, fmt.Errorf("bad peer id %q", id)
		}
		out[raft.NodeID(n)] = addr
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no peers in %q", s)
	}
	return out, nil
}
