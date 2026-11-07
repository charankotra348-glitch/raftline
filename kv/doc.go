// Package kv is the replicated key-value store built on package raft.
//
// It is written in week 4 (see docs/plan.md). The planned API:
//
//	type Clerk struct{ ... }                       // one per client
//	func MakeClerk(servers []int, t Transport) *Clerk
//	func (ck *Clerk) Get(key string) string        // "" if the key is missing
//	func (ck *Clerk) Put(key, value string)
//	func (ck *Clerk) Append(key, arg string)
//
//	type Server struct{ ... }                      // one per Raft server
//	func StartServer(me, n int, t Transport, p *raft.Persister) *Server
//
// Every operation goes through the Raft log; docs/design.md explains how
// retries are made exactly-once.
package kv
