package raft

// Milestone gates the test suite. Raise it as each part works:
//
//	0  nothing implemented yet; only the network tests run
//	1  leader election                     (TestElection*)
//	2  log replication and commit          (TestAgree*, TestBackup, TestRPCCount)
//	3  persistence and unreliable networks (TestPersist*, TestChurn*)
//
// `go test ./raft -run . -race` should pass completely before you raise it.
const Milestone = 0
