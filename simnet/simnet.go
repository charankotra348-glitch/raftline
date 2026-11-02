// Package simnet is an in-process network for testing distributed code.
//
// Every node registers a handler under an integer id. A Call from one node to
// another is serialized with encoding/gob (so the two sides never share
// memory, just like a real RPC) and can be dropped, delayed or reordered when
// the network is set to unreliable. Nodes can be disconnected and reconnected
// to create partitions.
//
// The network never runs your code on the caller's goroutine: each delivered
// request runs the handler on its own goroutine, and a lost request or reply
// looks the same to the caller as a timeout (Call returns false).
package simnet

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

// Handler serves one request. method names the RPC; args is a freshly
// decoded pointer of the type the caller sent. It returns the reply (a
// pointer), which is serialized back to the caller.
type Handler func(method string, args any) any

// Network is safe for concurrent use.
type Network struct {
	mu        sync.Mutex
	rng       *rand.Rand
	handlers  map[int]Handler
	connected map[int]bool
	reliable  bool
	longDelay bool // disconnected calls take up to 7s to fail instead of 100ms
	reorder   bool // occasionally delay replies a long time so they arrive late

	calls atomic.Int64
	bytes atomic.Int64
}

// New returns a reliable network. seed makes drops and delays repeatable.
func New(seed int64) *Network {
	return &Network{
		rng:       rand.New(rand.NewSource(seed)),
		handlers:  map[int]Handler{},
		connected: map[int]bool{},
		reliable:  true,
	}
}

// Register adds (or replaces) the handler for node id and connects it.
func (n *Network) Register(id int, h Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
	n.connected[id] = true
}

// Remove deletes node id, as if its process crashed. Calls to it fail.
func (n *Network) Remove(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.handlers, id)
	delete(n.connected, id)
}

// Connect sets whether node id can send and receive.
func (n *Network) Connect(id int, on bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.connected[id] = on
}

// SetReliable turns random drops and short delays off (true) or on (false).
func (n *Network) SetReliable(on bool) { n.mu.Lock(); n.reliable = on; n.mu.Unlock() }

// SetLongDelays makes calls to unreachable nodes take a long time to fail.
func (n *Network) SetLongDelays(on bool) { n.mu.Lock(); n.longDelay = on; n.mu.Unlock() }

// SetReordering makes some replies arrive much later than others.
func (n *Network) SetReordering(on bool) { n.mu.Lock(); n.reorder = on; n.mu.Unlock() }

// Calls is the number of requests sent so far (delivered or not).
func (n *Network) Calls() int64 { return n.calls.Load() }

// Bytes is the number of request bytes sent so far.
func (n *Network) Bytes() int64 { return n.bytes.Load() }

func (n *Network) intn(k int) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.rng.Intn(k)
}

// Call sends args (a pointer) from node `from` to node `to` and decodes the
// reply into reply (a pointer of the type the handler returns). It reports
// whether a reply arrived. A false result means the request or the reply was
// lost, or the peer is down; the caller cannot tell which.
func (n *Network) Call(from, to int, method string, args, reply any) bool {
	n.calls.Add(1)
	payload, err := encode(args)
	if err != nil {
		panic(fmt.Sprintf("simnet: cannot encode %s args: %v", method, err))
	}
	n.bytes.Add(int64(len(payload)))

	n.mu.Lock()
	reliable, longDelay, reorder := n.reliable, n.longDelay, n.reorder
	h, alive := n.handlers[to]
	up := n.connected[from] && n.connected[to] && alive
	n.mu.Unlock()

	if !up {
		wait := 100
		if longDelay {
			wait = 7000
		}
		time.Sleep(time.Duration(n.intn(wait)) * time.Millisecond)
		return false
	}
	if !reliable {
		time.Sleep(time.Duration(n.intn(27)) * time.Millisecond)
		if n.intn(1000) < 100 { // drop the request
			return false
		}
	}

	in, err := decodeLike(args, payload)
	if err != nil {
		panic(fmt.Sprintf("simnet: cannot decode %s args: %v", method, err))
	}
	done := make(chan []byte, 1)
	go func() {
		out, err := encode(h(method, in))
		if err != nil {
			panic(fmt.Sprintf("simnet: cannot encode %s reply: %v", method, err))
		}
		done <- out
	}()

	// While the handler runs, notice if the destination dies or is cut off.
	var out []byte
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
wait:
	for {
		select {
		case out = <-done:
			break wait
		case <-tick.C:
			n.mu.Lock()
			_, stillThere := n.handlers[to]
			ok := stillThere && n.connected[to] && n.connected[from]
			n.mu.Unlock()
			if !ok {
				return false
			}
		}
	}

	if !reliable && n.intn(1000) < 100 { // drop the reply
		return false
	}
	if reorder && n.intn(900) < 600 {
		time.Sleep(time.Duration(200+n.intn(1+n.intn(2000))) * time.Millisecond)
	}
	if err := gob.NewDecoder(bytes.NewReader(out)).Decode(reply); err != nil {
		panic(fmt.Sprintf("simnet: cannot decode %s reply: %v", method, err))
	}
	return true
}

func encode(v any) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(v)
	return buf.Bytes(), err
}

// decodeLike decodes payload into a new value of the same pointer type as like.
func decodeLike(like any, payload []byte) (any, error) {
	t := reflect.TypeOf(like)
	if t.Kind() != reflect.Pointer {
		return nil, fmt.Errorf("args must be a pointer, got %s", t)
	}
	v := reflect.New(t.Elem())
	err := gob.NewDecoder(bytes.NewReader(payload)).Decode(v.Interface())
	return v.Interface(), err
}
