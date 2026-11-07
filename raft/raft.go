// Package raft implements the Raft consensus algorithm from "In Search of an
// Understandable Consensus Algorithm" (Ongaro and Ousterhout, 2014).
//
// A service (the key-value store in ../kv) creates one Raft per server, calls
// Start to propose a command, and reads committed commands from applyCh in
// log order. Raft never interprets commands.
//
// Figure 2 of the paper is the specification. Every rule there is required;
// the comments below say where each one is implemented.
package raft

import (
	"sync"
	"sync/atomic"
	"time"
)

// Timing. The test harness expects a new leader within about 5 seconds of a
// failure and allows at most about 10 heartbeats per second per leader.
const (
	HeartbeatInterval  = 100 * time.Millisecond
	ElectionTimeoutMin = 300 * time.Millisecond
	ElectionTimeoutMax = 600 * time.Millisecond
)

// Transport sends one RPC to a peer and reports whether a reply arrived.
// args and reply are pointers to the RPC types in types.go.
type Transport interface {
	Call(to int, method string, args, reply any) bool
}

// Raft is one server's view of the cluster.
type Raft struct {
	mu        sync.Mutex
	me        int // this server's index in [0, n)
	n         int // cluster size
	transport Transport
	persister *Persister
	applyCh   chan<- ApplyMsg
	dead      atomic.Bool

	// Persistent state (save before replying to any RPC that changed it).
	currentTerm int
	votedFor    int        // -1 when no vote in currentTerm
	log         []LogEntry // log[0] is a placeholder with Term 0

	// Volatile state on all servers.
	role        Role
	commitIndex int
	lastApplied int
	lastHeard   time.Time // last time a valid leader or a granted vote reset the election timer

	// Volatile state on leaders (reinitialized after each election).
	nextIndex  []int
	matchIndex []int
}

// Make creates a server and returns at once; long-running work happens on
// goroutines it starts. persister may already hold state from before a crash.
func Make(me, n int, transport Transport, persister *Persister, applyCh chan<- ApplyMsg) *Raft {
	rf := &Raft{
		me:        me,
		n:         n,
		transport: transport,
		persister: persister,
		applyCh:   applyCh,
		votedFor:  -1,
		log:       []LogEntry{{Term: 0}},
		role:      Follower,
		lastHeard: time.Now(),
	}
	rf.readPersist(persister.Read())

	go rf.ticker()
	// TODO(milestone 2): start the goroutine that sends committed entries on applyCh.
	return rf
}

// GetState returns the current term and whether this server thinks it is the leader.
func (rf *Raft) GetState() (term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// Start proposes command for the log. If this server is not the leader it
// returns isLeader=false. Otherwise it appends the entry and returns at once
// with the index the entry will have if it is ever committed. Commitment is
// not guaranteed: the leader may lose its job before replicating it.
func (rf *Raft) Start(command any) (index, term int, isLeader bool) {
	// TODO(milestone 2)
	return -1, -1, false
}

// Kill stops the server. Goroutines should check killed() and exit.
func (rf *Raft) Kill()        { rf.dead.Store(true) }
func (rf *Raft) killed() bool { return rf.dead.Load() }

// Handle dispatches an incoming RPC. The network calls it on its own goroutine.
func (rf *Raft) Handle(method string, args any) any {
	switch method {
	case "Raft.RequestVote":
		reply := &RequestVoteReply{}
		rf.RequestVote(args.(*RequestVoteArgs), reply)
		return reply
	case "Raft.AppendEntries":
		reply := &AppendEntriesReply{}
		rf.AppendEntries(args.(*AppendEntriesArgs), reply)
		return reply
	}
	panic("raft: unknown method " + method)
}

// RequestVote is the receiver side of Figure 2's RequestVote RPC.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// TODO(milestone 1): the two receiver rules, plus "Rules for Servers / All Servers".
	// TODO(milestone 2): the up-to-date check on LastLogTerm / LastLogIndex (section 5.4.1).
}

// AppendEntries is the receiver side of Figure 2's AppendEntries RPC.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// TODO(milestone 1): reject stale terms, step down on newer ones, reset the election timer.
	// TODO(milestone 2): receiver rules 2-5, and fill ConflictTerm / ConflictIndex on a mismatch.
}

// ticker starts an election when the timer runs out, and sends heartbeats
// while this server is the leader.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// TODO(milestone 1): pick a random timeout in [ElectionTimeoutMin, ElectionTimeoutMax),
		// and start an election if no leader or vote has reset lastHeard within it.
		time.Sleep(10 * time.Millisecond)
	}
}

// persist saves currentTerm, votedFor and log to the persister.
func (rf *Raft) persist() {
	// TODO(milestone 3): encode the three fields with encoding/gob and call rf.persister.Save.
}

// readPersist restores the state saved by persist.
func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 {
		return
	}
	// TODO(milestone 3): decode what persist wrote, in the same order.
}
