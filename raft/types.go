package raft

// Role is what a server currently believes it is.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	return [...]string{"follower", "candidate", "leader"}[r]
}

// LogEntry is one slot of the replicated log. Index 0 is a placeholder entry
// with term 0, so the first real command sits at index 1.
type LogEntry struct {
	Term    int
	Command any
}

// ApplyMsg is what Raft sends on applyCh once an entry is committed. The
// service above Raft (the key-value store) applies Command to its state.
type ApplyMsg struct {
	CommandValid bool
	Command      any
	CommandIndex int
	CommandTerm  int
}

// RequestVoteArgs and the other RPC types are sent over the network, so every
// field must be exported (capitalized) or gob will silently drop it.
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateID  int
	LastLogIndex int // index of the candidate's last log entry
	LastLogTerm  int // term of the candidate's last log entry
}

type RequestVoteReply struct {
	Term        int // the voter's current term, so a stale candidate can update itself
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderID     int
	PrevLogIndex int        // index of the entry just before Entries
	PrevLogTerm  int        // term of that entry
	Entries      []LogEntry // empty for a heartbeat
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// Fast backup (section 5.3, end): when Success is false because the
	// follower's log doesn't match, these let the leader skip back a whole
	// term at a time instead of one entry per round trip.
	ConflictTerm  int // term of the follower's entry at PrevLogIndex, or -1 if it has none
	ConflictIndex int // first index the follower holds for ConflictTerm, or its log length
}
