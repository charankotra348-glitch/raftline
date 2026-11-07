package raft

import (
	"strings"
	"sync"
	"testing"
	"time"
)

// Milestone 2: log replication and commitment.

func TestAgreeBasic(t *testing.T) {
	need(t, 2)
	c := newCluster(t, 3, true)

	for index := 1; index <= 3; index++ {
		if got, _ := c.nCommitted(index); got > 0 {
			t.Fatalf("index %d committed before Start", index)
		}
		if at := c.one(index*100, 3, false); at != index {
			t.Fatalf("command %d landed at index %d, want %d", index*100, at, index)
		}
	}
}

// Each command should be sent to each follower about once.
func TestAgreeRPCBytes(t *testing.T) {
	need(t, 2)
	c := newCluster(t, 3, true)
	c.one(99, 3, false)
	before := c.net.Bytes()

	var sent int64
	for index := 2; index < 12; index++ {
		cmd := strings.Repeat("x", 5000) + string(rune('a'+index))
		if at := c.one(cmd, 3, false); at != index {
			t.Fatalf("command landed at index %d, want %d", at, index)
		}
		sent += int64(len(cmd))
	}
	got := c.net.Bytes() - before
	if limit := sent*2 + 50000; got > limit {
		t.Fatalf("sent %d bytes for %d bytes of commands; at most %d expected", got, sent, limit)
	}
}

func TestAgreeFollowerDown(t *testing.T) {
	need(t, 2)
	c := newCluster(t, 3, true)

	c.one(101, 3, false)
	leader := c.checkOneLeader()
	c.disconnect((leader + 1) % 3)

	// Two of three is a majority: the cluster keeps committing.
	c.one(102, 2, false)
	c.one(103, 2, false)
	time.Sleep(electionWait)
	c.one(104, 2, false)
	c.one(105, 2, false)

	// The follower comes back and catches up.
	c.connect((leader + 1) % 3)
	c.one(106, 3, true)
	time.Sleep(electionWait)
	c.one(107, 3, true)
}

func TestAgreeNoMajority(t *testing.T) {
	need(t, 2)
	const n = 5
	c := newCluster(t, n, true)
	c.one(10, n, false)

	leader := c.checkOneLeader()
	for k := 1; k <= 3; k++ {
		c.disconnect((leader + k) % n)
	}

	index, _, ok := c.raft(leader).Start(20)
	if !ok {
		t.Fatal("leader rejected Start")
	}
	if index != 2 {
		t.Fatalf("Start returned index %d, want 2", index)
	}
	time.Sleep(2 * electionWait)
	if got, _ := c.nCommitted(index); got > 0 {
		t.Fatalf("%d servers committed index %d without a majority", got, index)
	}

	for k := 1; k <= 3; k++ {
		c.connect((leader + k) % n)
	}
	// The disconnected majority may have elected someone; either way the
	// cluster must recover and commit new commands.
	leader2 := c.checkOneLeader()
	if index2, _, ok := c.raft(leader2).Start(30); ok && (index2 < 2 || index2 > 3) {
		t.Fatalf("unexpected index %d after healing", index2)
	}
	c.one(1000, n, true)
}

func TestAgreeConcurrentStarts(t *testing.T) {
	need(t, 2)
	c := newCluster(t, 3, true)

	for attempt := 0; attempt < 5; attempt++ {
		if attempt > 0 {
			time.Sleep(3 * time.Second)
		}
		leader := c.checkOneLeader()
		_, term, ok := c.raft(leader).Start(1)
		if !ok {
			continue
		}

		var wg sync.WaitGroup
		indices := make(chan int, 5)
		for k := 0; k < 5; k++ {
			wg.Add(1)
			go func(k int) {
				defer wg.Done()
				i, term1, ok := c.raft(leader).Start(100 + k)
				if ok && term1 == term {
					indices <- i
				}
			}(k)
		}
		wg.Wait()
		close(indices)

		changed := false
		for i := 0; i < 3; i++ {
			if t1, _ := c.raft(i).GetState(); t1 != term {
				changed = true
			}
		}
		if changed {
			continue // a new term started; try again
		}

		seen := map[any]bool{}
		for index := range indices {
			cmd := c.wait(index, 3, term)
			if cmd == -1 {
				changed = true
				break
			}
			seen[cmd] = true
		}
		if changed {
			continue
		}
		for k := 0; k < 5; k++ {
			if !seen[100+k] {
				t.Fatalf("command %d was not committed", 100+k)
			}
		}
		return
	}
	t.Fatal("the term kept changing; could not run the test")
}

// A leader that was partitioned away must not commit its stale entries.
func TestAgreeRejoinOldLeader(t *testing.T) {
	need(t, 2)
	c := newCluster(t, 3, true)
	c.one(101, 3, true)

	leader1 := c.checkOneLeader()
	c.disconnect(leader1)
	// The cut-off leader accepts entries it can never commit.
	c.raft(leader1).Start(102)
	c.raft(leader1).Start(103)
	c.raft(leader1).Start(104)

	c.one(103, 2, true) // the new leader commits index 2

	leader2 := c.checkOneLeader()
	c.disconnect(leader2)
	c.connect(leader1)
	c.one(104, 2, true) // leader1's stale entries must be overwritten

	c.connect(leader2)
	c.one(105, 3, true)
}

// Followers far behind, with conflicting entries, must catch up quickly
// (this is what ConflictTerm / ConflictIndex are for).
func TestAgreeBackup(t *testing.T) {
	need(t, 2)
	const n = 5
	c := newCluster(t, n, true)
	c.one(c.randInt(), n, true)

	// Leader and one follower get cut off with 50 uncommitted entries.
	leader1 := c.checkOneLeader()
	c.disconnect((leader1 + 2) % n)
	c.disconnect((leader1 + 3) % n)
	c.disconnect((leader1 + 4) % n)
	for i := 0; i < 50; i++ {
		c.raft(leader1).Start(c.randInt())
	}
	time.Sleep(electionWait / 2)
	c.disconnect(leader1)
	c.disconnect((leader1 + 1) % n)

	// The other three commit 50 different entries.
	c.connect((leader1 + 2) % n)
	c.connect((leader1 + 3) % n)
	c.connect((leader1 + 4) % n)
	for i := 0; i < 50; i++ {
		c.one(c.randInt(), 3, true)
	}

	// Now another leader with a follower missing, and 50 more uncommitted.
	leader2 := c.checkOneLeader()
	other := (leader1 + 2) % n
	if leader2 == other {
		other = (leader2 + 1) % n
	}
	c.disconnect(other)
	for i := 0; i < 50; i++ {
		c.raft(leader2).Start(c.randInt())
	}
	time.Sleep(electionWait / 2)

	// Bring the first group back with `other`.
	for i := 0; i < n; i++ {
		c.disconnect(i)
	}
	c.connect(leader1)
	c.connect((leader1 + 1) % n)
	c.connect(other)
	for i := 0; i < 50; i++ {
		c.one(c.randInt(), 3, true)
	}

	for i := 0; i < n; i++ {
		c.connect(i)
	}
	c.one(c.randInt(), n, true)
}

// Elections and agreement should not need an unreasonable number of RPCs.
func TestAgreeRPCCount(t *testing.T) {
	need(t, 2)
	c := newCluster(t, 3, true)
	leader := c.checkOneLeader()

	if calls := c.net.Calls(); calls > 30 || calls < 1 {
		t.Fatalf("%d RPCs to elect one leader; expected between 1 and 30", calls)
	}

	for attempt := 0; attempt < 5; attempt++ {
		if attempt > 0 {
			time.Sleep(3 * time.Second)
		}
		leader = c.checkOneLeader()
		before := c.net.Calls()
		start, term, ok := c.raft(leader).Start(1)
		if !ok {
			continue
		}
		const iters = 10
		var cmds []int
		retry := false
		for i := 1; i < iters+2; i++ {
			cmd := c.randInt()
			cmds = append(cmds, cmd)
			index, term1, ok := c.raft(leader).Start(cmd)
			if term1 != term || !ok {
				retry = true
				break
			}
			if index != start+i {
				t.Fatalf("Start returned index %d, want %d", index, start+i)
			}
		}
		if retry {
			continue
		}
		for i := 1; i < iters+1; i++ {
			got := c.wait(start+i, 3, term)
			if got == -1 {
				retry = true
				break
			}
			if got != cmds[i-1] {
				t.Fatalf("index %d holds %v, want %v", start+i, got, cmds[i-1])
			}
		}
		if retry {
			continue
		}
		if t1, _ := c.raft(leader).GetState(); t1 != term {
			continue
		}
		if used := c.net.Calls() - before; used > (iters+1+3)*3 {
			t.Fatalf("%d RPCs for %d commands; expected at most %d", used, iters, (iters+1+3)*3)
		}
		break
	}

	// An idle leader sends heartbeats only.
	time.Sleep(electionWait)
	before := c.net.Calls()
	time.Sleep(electionWait)
	if idle := c.net.Calls() - before; idle > 3*20 {
		t.Fatalf("%d RPCs in 1s of idleness; expected at most 60", idle)
	}
}
