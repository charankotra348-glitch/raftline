package simnet

import (
	"sync/atomic"
	"testing"
	"time"
)

type echoArgs struct{ N int }
type echoReply struct{ N int }

func echo(served *atomic.Int64) Handler {
	return func(method string, args any) any {
		served.Add(1)
		return &echoReply{N: args.(*echoArgs).N * 2}
	}
}

func TestDeliversAndCopies(t *testing.T) {
	net := New(1)
	var served atomic.Int64
	net.Register(0, echo(&served))
	net.Register(1, echo(&served))

	args := &echoArgs{N: 21}
	var reply echoReply
	if !net.Call(0, 1, "Echo", args, &reply) || reply.N != 42 {
		t.Fatalf("got ok reply %+v, want 42", reply)
	}
	if net.Calls() != 1 || net.Bytes() == 0 {
		t.Fatalf("counters not updated: calls=%d bytes=%d", net.Calls(), net.Bytes())
	}
}

func TestHandlerGetsItsOwnCopy(t *testing.T) {
	net := New(1)
	net.Register(0, func(string, any) any { return &echoReply{} })
	net.Register(1, func(_ string, a any) any {
		a.(*echoArgs).N = -1 // must not be visible to the caller
		return &echoReply{}
	})
	args := &echoArgs{N: 5}
	net.Call(0, 1, "Echo", args, &echoReply{})
	if args.N != 5 {
		t.Fatal("handler changed the caller's args: values are shared, not copied")
	}
}

func TestDisconnectedAndRemovedNodesFail(t *testing.T) {
	net := New(1)
	var served atomic.Int64
	net.Register(0, echo(&served))
	net.Register(1, echo(&served))

	net.Connect(1, false)
	if net.Call(0, 1, "Echo", &echoArgs{1}, &echoReply{}) {
		t.Fatal("call to a disconnected node succeeded")
	}
	net.Connect(1, true)
	net.Remove(1)
	if net.Call(0, 1, "Echo", &echoArgs{1}, &echoReply{}) {
		t.Fatal("call to a removed node succeeded")
	}
	if served.Load() != 0 {
		t.Fatalf("handler ran %d times for unreachable calls", served.Load())
	}
}

func TestUnreliableDropsSome(t *testing.T) {
	net := New(7)
	var served atomic.Int64
	net.Register(0, echo(&served))
	net.Register(1, echo(&served))
	net.SetReliable(false)

	ok := 0
	for i := 0; i < 200; i++ {
		if net.Call(0, 1, "Echo", &echoArgs{i}, &echoReply{}) {
			ok++
		}
	}
	// About 19% of calls should fail (10% request + 10% reply loss).
	if ok < 130 || ok > 190 {
		t.Fatalf("%d/200 calls succeeded on an unreliable network; expected roughly 160", ok)
	}
}

func TestCutDuringCallFails(t *testing.T) {
	net := New(1)
	release := make(chan struct{})
	net.Register(0, func(string, any) any { return &echoReply{} })
	net.Register(1, func(string, any) any { <-release; return &echoReply{} })

	go func() { time.Sleep(50 * time.Millisecond); net.Connect(1, false) }()
	start := time.Now()
	if net.Call(0, 1, "Slow", &echoArgs{}, &echoReply{}) {
		t.Fatal("call succeeded although the peer was cut off mid-call")
	}
	close(release)
	if time.Since(start) > time.Second {
		t.Fatal("caller stayed blocked long after the peer was cut off")
	}
}
