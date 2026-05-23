package main

import (
	"sync"
	"sync/atomic"
)

// sv2WorkerRegistry tracks active SV2 connections and supports filtered
// snapshots for status pages.
type sv2WorkerRegistry struct {
	mu    sync.Mutex
	conns map[*sv2Conn]struct{}
}

func newSV2WorkerRegistry() *sv2WorkerRegistry {
	return &sv2WorkerRegistry{conns: make(map[*sv2Conn]struct{})}
}

func (r *sv2WorkerRegistry) add(c *sv2Conn) {
	if r == nil || c == nil {
		return
	}
	r.mu.Lock()
	r.conns[c] = struct{}{}
	r.mu.Unlock()
}

func (r *sv2WorkerRegistry) remove(c *sv2Conn) {
	if r == nil || c == nil {
		return
	}
	r.mu.Lock()
	delete(r.conns, c)
	r.mu.Unlock()
}

func (r *sv2WorkerRegistry) snapshot() []*sv2Conn {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	out := make([]*sv2Conn, 0, len(r.conns))
	for c := range r.conns {
		out = append(out, c)
	}
	r.mu.Unlock()
	return out
}

func (r *sv2WorkerRegistry) getConnectionsByHash(hash string) []*sv2Conn {
	if r == nil || hash == "" {
		return nil
	}
	all := r.snapshot()
	out := make([]*sv2Conn, 0, len(all))
	for _, c := range all {
		if c == nil {
			continue
		}
		if c.workerHash() == hash {
			out = append(out, c)
		}
	}
	return out
}

func (r *sv2WorkerRegistry) getConnectionsByWalletHash(walletHash string) []*sv2Conn {
	if r == nil || walletHash == "" {
		return nil
	}
	all := r.snapshot()
	out := make([]*sv2Conn, 0, len(all))
	for _, c := range all {
		if c == nil {
			continue
		}
		if c.walletHash() == walletHash {
			out = append(out, c)
		}
	}
	return out
}

func (r *sv2WorkerRegistry) connectionBySeq(seq uint64) *sv2Conn {
	if r == nil || seq == 0 {
		return nil
	}
	all := r.snapshot()
	for _, c := range all {
		if c == nil {
			continue
		}
		if atomic.LoadUint64(&c.connectionSeq) == seq {
			return c
		}
	}
	return nil
}