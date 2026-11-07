package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/charankotra348-glitch/raftline/simnet"
)

// need skips a test until Milestone reaches m.
func need(t *testing.T, m int) {
	t.Helper()
	if Milestone < m {
		t.Skipf("needs milestone %d (raft.Milestone is %d)", m, Milestone)
	}
}

// electionWait is long enough for any reasonable election to finish.
const electionWait = 1000 * time.Millisecond

// cluster runs n Raft servers over a simulated network and checks that
// every server applies the same command at each index.
type cluster struct {
	t   *testing.T
	n   int
	net *simnet.Network

	rngMu sync.Mutex
	rng   *rand.Rand

	mu         sync.Mutex
	rafts      []*Raft
	persisters []*Persister
	applied    []map[int]any // per server: index -> command it applied
	gen        []int         // bumped on every restart so old appliers stop recording
	failure    string        // first safety violation seen by an applier
	stops      []chan struct{}
	offline    []bool // disconnected from the network by the test
}

func newCluster(t *testing.T, n int, reliable bool) *cluster {
	t.Helper()
	c := &cluster{
		t:          t,
		n:          n,
		net:        simnet.New(time.Now().UnixNano()),
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
		rafts:      make([]*Raft, n),
		persisters: make([]*Persister, n),
		applied:    make([]map[int]any, n),
		gen:        make([]int, n),
		stops:      make([]chan struct{}, n),
		offline:    make([]bool, n),
	}
	c.net.SetReliable(reliable)
	for i := 0; i < n; i++ {
		c.persisters[i] = NewPersister()
		c.start(i)
	}
	t.Cleanup(c.cleanup)
	return c
}

type transport struct {
	net *simnet.Network
	me  int
}

func (tr transport) Call(to int, method string, args, reply any) bool {
	return tr.net.Call(tr.me, to, method, args, reply)
}

// randInt and intn are safe to call from several goroutines.
func (c *cluster) randInt() int {
	c.rngMu.Lock()
	defer c.rngMu.Unlock()
	return c.rng.Int()
}

func (c *cluster) intn(k int) int {
	c.rngMu.Lock()
	defer c.rngMu.Unlock()
	return c.rng.Intn(k)
}

// start boots server i on a copy of its saved state, as after a reboot.
func (c *cluster) start(i int) {
	c.crash(i)

	c.mu.Lock()
	c.persisters[i] = c.persisters[i].Copy()
	c.applied[i] = map[int]any{}
	c.gen[i]++
	gen := c.gen[i]
	stop := make(chan struct{})
	c.stops[i] = stop
	c.offline[i] = false
	c.mu.Unlock()

	applyCh := make(chan ApplyMsg)
	rf := Make(i, c.n, transport{c.net, i}, c.persisters[i], applyCh)

	c.mu.Lock()
	c.rafts[i] = rf
	c.mu.Unlock()
	c.net.Register(i, rf.Handle)

	go c.applier(i, gen, applyCh, stop)
}

// crash stops server i. Its persister keeps whatever it had saved.
func (c *cluster) crash(i int) {
	c.net.Remove(i)
	c.mu.Lock()
	rf := c.rafts[i]
	c.rafts[i] = nil
	if c.stops[i] != nil {
		close(c.stops[i])
		c.stops[i] = nil
	}
	if c.persisters[i] != nil {
		// Writes after this moment are lost, as if the disk went away with the process.
		c.persisters[i] = c.persisters[i].Copy()
	}
	c.mu.Unlock()
	if rf != nil {
		rf.Kill()
	}
}

func (c *cluster) applier(i, gen int, applyCh <-chan ApplyMsg, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case m := <-applyCh:
			if !m.CommandValid {
				continue
			}
			c.mu.Lock()
			if c.gen[i] != gen {
				c.mu.Unlock()
				return
			}
			c.check(i, m)
			c.mu.Unlock()
		}
	}
}

// check records an applied command and verifies the safety properties:
// commands arrive in index order with no gaps, and no two servers ever
// apply different commands at the same index. Called with c.mu held.
func (c *cluster) check(i int, m ApplyMsg) {
	if c.failure != "" {
		return
	}
	if m.CommandIndex > 1 {
		if _, ok := c.applied[i][m.CommandIndex-1]; !ok {
			c.failure = fmt.Sprintf("server %d applied index %d before index %d", i, m.CommandIndex, m.CommandIndex-1)
			return
		}
	}
	if _, dup := c.applied[i][m.CommandIndex]; dup {
		c.failure = fmt.Sprintf("server %d applied index %d twice", i, m.CommandIndex)
		return
	}
	for j, log := range c.applied {
		if other, ok := log[m.CommandIndex]; ok && other != m.Command {
			c.failure = fmt.Sprintf("index %d: server %d applied %v but server %d applied %v",
				m.CommandIndex, i, m.Command, j, other)
			return
		}
	}
	c.applied[i][m.CommandIndex] = m.Command
}

func (c *cluster) failIfUnsafe() {
	c.t.Helper()
	c.mu.Lock()
	f := c.failure
	c.mu.Unlock()
	if f != "" {
		c.t.Fatal(f)
	}
}

func (c *cluster) cleanup() {
	for i := 0; i < c.n; i++ {
		c.crash(i)
	}
}

func (c *cluster) disconnect(i int) {
	c.mu.Lock()
	c.offline[i] = true
	c.mu.Unlock()
	c.net.Connect(i, false)
}

func (c *cluster) connect(i int) {
	c.mu.Lock()
	c.offline[i] = false
	c.mu.Unlock()
	c.net.Connect(i, true)
}

func (c *cluster) raft(i int) *Raft {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rafts[i]
}

// isConnected reports whether server i is running and on the network.
func (c *cluster) isConnected(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rafts[i] != nil && !c.offline[i]
}

// checkOneLeader waits for exactly one leader among connected servers and returns it.
func (c *cluster) checkOneLeader() int {
	c.t.Helper()
	for iter := 0; iter < 10; iter++ {
		time.Sleep(time.Duration(450+c.intn(100)) * time.Millisecond)
		leaders := map[int][]int{}
		for i := 0; i < c.n; i++ {
			if c.isConnected(i) {
				if term, lead := c.raft(i).GetState(); lead {
					leaders[term] = append(leaders[term], i)
				}
			}
		}
		last := -1
		for term, ids := range leaders {
			if len(ids) > 1 {
				c.t.Fatalf("term %d has %d leaders: %v", term, len(ids), ids)
			}
			if term > last {
				last = term
			}
		}
		if last != -1 {
			return leaders[last][0]
		}
	}
	c.t.Fatal("no leader was elected")
	return -1
}

// checkTerms returns the term all connected servers agree on.
func (c *cluster) checkTerms() int {
	c.t.Helper()
	term := -1
	for i := 0; i < c.n; i++ {
		if c.isConnected(i) {
			t, _ := c.raft(i).GetState()
			if term == -1 {
				term = t
			} else if t != term {
				c.t.Fatalf("servers disagree on the term: %d vs %d", term, t)
			}
		}
	}
	return term
}

// checkNoLeader fails if any connected server thinks it is the leader.
func (c *cluster) checkNoLeader() {
	c.t.Helper()
	for i := 0; i < c.n; i++ {
		if c.isConnected(i) {
			if _, lead := c.raft(i).GetState(); lead {
				c.t.Fatalf("server %d is leader without a majority", i)
			}
		}
	}
}

// nCommitted returns how many servers have applied index, and the command there.
func (c *cluster) nCommitted(index int) (int, any) {
	c.t.Helper()
	c.failIfUnsafe()
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	var cmd any
	for i := range c.applied {
		if v, ok := c.applied[i][index]; ok {
			count++
			cmd = v
		}
	}
	return count, cmd
}

// wait waits for at least n servers to apply index, giving up if the term
// moves past startTerm (the entry may then never commit). Returns the command.
func (c *cluster) wait(index, n, startTerm int) any {
	c.t.Helper()
	pause := 10 * time.Millisecond
	for iter := 0; iter < 30; iter++ {
		if got, _ := c.nCommitted(index); got >= n {
			break
		}
		time.Sleep(pause)
		if pause < time.Second {
			pause *= 2
		}
		if startTerm > -1 {
			for i := 0; i < c.n; i++ {
				if rf := c.raft(i); rf != nil {
					if t, _ := rf.GetState(); t > startTerm {
						return -1 // someone moved on; the caller retries
					}
				}
			}
		}
	}
	got, cmd := c.nCommitted(index)
	if got < n {
		c.t.Fatalf("only %d servers applied index %d; wanted %d", got, index, n)
	}
	return cmd
}

// one submits cmd to whichever server is leader and waits until at least
// `expected` servers have applied it. With retry, it resubmits if the leader
// fails before the entry commits. Returns the index cmd was committed at.
func (c *cluster) one(cmd any, expected int, retry bool) int {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	starts := 0
	for time.Now().Before(deadline) {
		index := -1
		for k := 0; k < c.n; k++ {
			starts = (starts + 1) % c.n
			if c.isConnected(starts) {
				if i, _, ok := c.raft(starts).Start(cmd); ok {
					index = i
					break
				}
			}
		}
		if index != -1 {
			for t0 := time.Now(); time.Since(t0) < 2*time.Second; time.Sleep(20 * time.Millisecond) {
				got, got1 := c.nCommitted(index)
				if got > 0 && got >= expected && got1 == cmd {
					return index
				}
			}
			if !retry {
				c.t.Fatalf("command %v was not committed by %d servers", cmd, expected)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	c.t.Fatalf("command %v was not committed within 10s", cmd)
	return -1
}
