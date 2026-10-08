package chaos

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
)

// KVInput is one client operation as recorded in a history.
type KVInput struct {
	Op    string // "get", "put", "append"
	Key   string
	Value string
}

// KVOutput is what the client observed.
type KVOutput struct {
	Value string
}

// KVModel is the sequential specification Porcupine checks histories
// against: a map from keys to strings. Histories are partitioned by key,
// since operations on different keys never constrain each other; checking
// keys separately is exponentially cheaper than checking them together.
var KVModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(KVInput).Key
			byKey[k] = append(byKey[k], op)
		}
		keys := make([]string, 0, len(byKey))
		for k := range byKey {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var out [][]porcupine.Operation
		for _, k := range keys {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() interface{} { return "" },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		st := state.(string)
		in := input.(KVInput)
		switch in.Op {
		case "get":
			return output.(KVOutput).Value == st, st
		case "put":
			return true, in.Value
		default:
			return true, st + in.Value
		}
	},
	DescribeOperation: func(input, output interface{}) string {
		in := input.(KVInput)
		switch in.Op {
		case "get":
			return fmt.Sprintf("get(%q) -> %q", in.Key, short(output.(KVOutput).Value))
		case "put":
			return fmt.Sprintf("put(%q, %q)", in.Key, in.Value)
		default:
			return fmt.Sprintf("append(%q, %q)", in.Key, in.Value)
		}
	},
	DescribeState: func(state interface{}) string { return fmt.Sprintf("%q", short(state.(string))) },
}

func short(s string) string {
	if len(s) > 40 {
		return "…" + s[len(s)-40:]
	}
	return s
}

// History records client operations with invocation and return times.
type History struct {
	mu    sync.Mutex
	start time.Time
	ops   []porcupine.Operation
}

// NewHistory starts an empty history; times are measured from now.
func NewHistory() *History { return &History{start: time.Now()} }

// Now returns the current timestamp on the history's clock (monotonic).
func (h *History) Now() int64 { return int64(time.Since(h.start)) }

// Add records a completed operation.
func (h *History) Add(client int, in KVInput, out KVOutput, call, ret int64) {
	h.mu.Lock()
	h.ops = append(h.ops, porcupine.Operation{ClientId: client, Input: in, Output: out, Call: call, Return: ret})
	h.mu.Unlock()
}

// Ops returns the recorded operations.
func (h *History) Ops() []porcupine.Operation {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]porcupine.Operation(nil), h.ops...)
}

func checkOps(h *History) string {
	return string(porcupine.CheckOperationsTimeout(KVModel, h.Ops(), 10*time.Second))
}
