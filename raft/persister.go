package raft

import "sync"

// Persister stands in for a disk. Raft saves its persistent state here and
// reads it back on restart. Tests "crash" a server by keeping its Persister
// and starting a new Raft on a copy of it, so only what was saved survives.
type Persister struct {
	mu    sync.Mutex
	state []byte
}

func NewPersister() *Persister { return &Persister{} }

// Save replaces the stored state. Treat it as one atomic disk write.
func (p *Persister) Save(state []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state = append([]byte(nil), state...)
}

// Read returns a copy of the stored state (nil if nothing was saved).
func (p *Persister) Read() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]byte(nil), p.state...)
}

// Size is the number of bytes stored, for tests that bound log growth.
func (p *Persister) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.state)
}

// Copy returns an independent Persister holding the same state.
func (p *Persister) Copy() *Persister {
	return &Persister{state: p.Read()}
}
