// Command raftkv-cli talks to a RaftKV cluster.
//
//	raftkv-cli --servers 1=localhost:7001,2=localhost:7002,3=localhost:7003 put k v
//	raftkv-cli --servers ... append k v
//	raftkv-cli --servers ... get k
//
// It follows leader hints and retries until the operation succeeds or
// --timeout expires.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/shreyans-chowdry/raftkv/kv"
	grpct "github.com/shreyans-chowdry/raftkv/transport/grpc"
)

func main() {
	servers := flag.String("servers", "1=localhost:7001,2=localhost:7002,3=localhost:7003,4=localhost:7004,5=localhost:7005",
		"cluster nodes: id=host:port,...")
	timeout := flag.Duration("timeout", 10*time.Second, "give up after this long")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: raftkv-cli [--servers ...] get KEY | put KEY VALUE | append KEY VALUE\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	args := flag.Args()
	if len(args) < 2 {
		flag.Usage()
		os.Exit(2)
	}
	addrs, err := grpct.ParsePeers(*servers)
	if err != nil {
		fail(err)
	}
	conns, closeAll, err := grpct.DialCluster(addrs)
	if err != nil {
		fail(err)
	}
	defer closeAll()
	ck := kv.NewClerk(conns)
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	switch args[0] {
	case "get":
		v, err := ck.Get(ctx, args[1])
		if err != nil {
			fail(err)
		}
		fmt.Println(v)
	case "put", "append":
		if len(args) != 3 {
			flag.Usage()
			os.Exit(2)
		}
		if args[0] == "put" {
			err = ck.Put(ctx, args[1], args[2])
		} else {
			err = ck.Append(ctx, args[1], args[2])
		}
		if err != nil {
			fail(err)
		}
		fmt.Println("OK")
	default:
		flag.Usage()
		os.Exit(2)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
