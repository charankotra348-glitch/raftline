package raft

import (
	"testing"
	"time"
)

// Milestone 3: persistence, crashes and unreliable networks.

func TestPersistRestartAll(t *testing.T) {
	need(t, 3)
	const n = 3
	c := newCluster(t, n, true)
	c.one(11, n, true)

	// Everyone crashes and comes back: committed entries must survive.
	for i := 0; i < n; i++ {
		c.start(i)
	}
	c.one(12, n, true)

	leader := c.checkOneLeader()
	c.start(leader)
	c.one(13, n, true)

	leader = c.checkOneLeader()
	c.crash(leader)
	c.one(14, n-1, true)
	c.start(leader)

	c.wait(4, n, -1) // the restarted server catches up
	c.one(15, n, true)
}

func TestPersistPartitionedRestarts(t *testing.T) {
	need(t, 3)
	const n = 5
	c := newCluster(t, n, true)

	index := 1
	for iter := 0; iter < 9; iter++ {
		c.one(10+index, n, true)
		index++

		leader := c.checkOneLeader()
		c.crash((leader + 1) % n)
		c.crash((leader + 2) % n)

		c.one(10+index, n-2, true)
		index++

		c.crash((leader + 0) % n)
		c.crash((leader + 3) % n)
		c.crash((leader + 4) % n)

		c.start((leader + 1) % n)
		c.start((leader + 2) % n)
		time.Sleep(electionWait)
		c.start((leader + 3) % n)

		c.one(10+index, n-2, true)
		index++

		c.start((leader + 4) % n)
		c.start((leader + 0) % n)
	}
	c.one(1000, n, true)
}

// Figure 8 of the paper: a leader must not count replicas of entries from
// earlier terms toward commitment. Here leaders crash right after appending.
func TestPersistFigure8(t *testing.T) {
	need(t, 3)
	const n = 5
	c := newCluster(t, n, true)
	c.one(c.randInt(), 1, true)

	up := n
	for iter := 0; iter < 1000; iter++ {
		leader := -1
		for i := 0; i < n; i++ {
			if rf := c.raft(i); rf != nil {
				if _, _, ok := rf.Start(c.randInt()); ok {
					leader = i
				}
			}
		}
		if c.intn(1000) < 100 {
			time.Sleep(time.Duration(c.intn(500)) * time.Millisecond)
		} else {
			time.Sleep(time.Duration(c.intn(13)) * time.Millisecond)
		}
		if leader != -1 {
			c.crash(leader)
			up--
		}
		if up < 3 {
			if s := c.intn(n); c.raft(s) == nil {
				c.start(s)
				up++
			}
		}
		if iter%100 == 0 {
			c.failIfUnsafe()
		}
	}
	for i := 0; i < n; i++ {
		if c.raft(i) == nil {
			c.start(i)
		}
	}
	c.one(c.randInt(), n, true)
}

// Many concurrent clients on a lossy, reordering network while servers are
// cut off and crash. Checks safety throughout and liveness at the end.
func TestPersistChurnUnreliable(t *testing.T) {
	need(t, 3)
	const n = 5
	c := newCluster(t, n, false)
	c.net.SetLongDelays(true)
	c.net.SetReordering(true)

	stop := make(chan struct{})
	done := make(chan []int)
	for client := 0; client < 3; client++ {
		go func(client int) {
			var committed []int
			defer func() { done <- committed }()
			for {
				select {
				case <-stop:
					return
				default:
				}
				cmd := c.randInt()
				for i := 0; i < n; i++ {
					rf := c.raft(i)
					if rf == nil {
						continue
					}
					if index, _, ok := rf.Start(cmd); ok {
						for wait := 0; wait < 40; wait++ {
							if got, v := c.nCommitted(index); got > 0 {
								if v == cmd {
									committed = append(committed, cmd)
								}
								break
							}
							time.Sleep(50 * time.Millisecond)
						}
						break
					}
				}
				time.Sleep(time.Duration(79+client*17) * time.Millisecond)
			}
		}(client)
	}

	for iter := 0; iter < 20; iter++ {
		if c.intn(1000) < 200 {
			c.disconnect(c.intn(n))
		}
		if c.intn(1000) < 500 {
			if i := c.intn(n); c.raft(i) == nil {
				c.start(i)
			}
			c.connect(c.intn(n))
		}
		if c.intn(1000) < 200 {
			if i := c.intn(n); c.raft(i) != nil {
				c.crash(i)
			}
		}
		// Keep a majority alive so the test can make progress.
		time.Sleep(time.Duration(c.intn(int(electionWait.Milliseconds())*7/10)) * time.Millisecond)
		alive := 0
		for i := 0; i < n; i++ {
			if c.raft(i) != nil {
				alive++
			}
		}
		for i := 0; alive < 3 && i < n; i++ {
			if c.raft(i) == nil {
				c.start(i)
				alive++
			}
		}
		c.failIfUnsafe()
	}

	close(stop)
	var all []int
	for client := 0; client < 3; client++ {
		all = append(all, <-done...)
	}

	c.net.SetReliable(true)
	c.net.SetLongDelays(false)
	c.net.SetReordering(false)
	for i := 0; i < n; i++ {
		if c.raft(i) == nil {
			c.start(i)
		}
		c.connect(i)
	}
	last := c.one(c.randInt(), n, true)

	// Every command a client saw committed must be in the final log.
	inLog := map[any]bool{}
	for index := 1; index <= last; index++ {
		if _, v := c.nCommitted(index); v != nil {
			inLog[v] = true
		}
	}
	for _, cmd := range all {
		if !inLog[cmd] {
			t.Fatalf("command %d was reported committed but is missing from the final log", cmd)
		}
	}
}
