package raft

import (
	"testing"
	"time"
)

// Milestone 1: leader election.

func TestElectionInitial(t *testing.T) {
	need(t, 1)
	c := newCluster(t, 3, true)

	c.checkOneLeader()
	time.Sleep(50 * time.Millisecond)
	term1 := c.checkTerms()
	if term1 < 1 {
		t.Fatalf("term is %d after an election; it should be at least 1", term1)
	}

	// With no failures, the leader's heartbeats must keep everyone quiet.
	time.Sleep(2 * electionWait)
	if term2 := c.checkTerms(); term2 != term1 {
		t.Fatalf("term changed from %d to %d with no failures: heartbeats are not suppressing elections", term1, term2)
	}
	c.checkOneLeader()
}

func TestElectionAfterLeaderFails(t *testing.T) {
	need(t, 1)
	c := newCluster(t, 3, true)

	leader1 := c.checkOneLeader()
	c.disconnect(leader1)
	c.checkOneLeader() // the other two elect a new leader

	// The old leader rejoins; it must not disturb the new one for good.
	c.connect(leader1)
	leader2 := c.checkOneLeader()

	// Without a majority, nobody can be leader.
	c.disconnect(leader2)
	c.disconnect((leader2 + 1) % 3)
	time.Sleep(2 * electionWait)
	c.checkNoLeader()

	// A majority again: a leader appears.
	c.connect((leader2 + 1) % 3)
	c.checkOneLeader()

	c.connect(leader2)
	c.checkOneLeader()
}

func TestElectionMany(t *testing.T) {
	need(t, 1)
	const n = 7
	c := newCluster(t, n, true)
	c.checkOneLeader()

	for iter := 0; iter < 10; iter++ {
		// Take down up to three servers; four of seven is still a majority.
		down := []int{c.intn(n), c.intn(n), c.intn(n)}
		for _, i := range down {
			c.disconnect(i)
		}
		c.checkOneLeader()
		for _, i := range down {
			c.connect(i)
		}
	}
	c.checkOneLeader()
}
