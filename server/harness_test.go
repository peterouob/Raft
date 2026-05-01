package server

import (
	"fmt"
	"testing"
	"time"
)

// Harness manages a cluster of Raft servers for testing.
type Harness struct {
	t       *testing.T
	n       int
	servers []*Server
	ready   []chan any
	alive   []bool
}

func NewHarness(t *testing.T, n int) *Harness {
	t.Helper()
	h := &Harness{
		t:       t,
		n:       n,
		servers: make([]*Server, n),
		ready:   make([]chan any, n),
		alive:   make([]bool, n),
	}

	peerIds := make([]int64, n)
	for i := range n {
		peerIds[i] = int64(i)
	}

	// Create and start all servers.
	for i := range n {
		peers := make([]int64, 0, n-1)
		for j := range n {
			if j != i {
				peers = append(peers, int64(j))
			}
		}
		h.ready[i] = make(chan any)
		h.servers[i] = NewServer(int64(i), peers, h.ready[i])
		// Port 0 lets the OS pick a free port.
		h.servers[i].Serve(0)
		h.alive[i] = true
	}

	// Connect every server to all its peers.
	for i := range n {
		for j := range n {
			if i != j {
				addr := h.servers[j].GetListenAddr()
				if err := h.servers[i].ConnectPeer(int64(j), addr); err != nil {
					t.Fatalf("ConnectPeer(%d->%d): %v", i, j, err)
				}
			}
		}
	}

	// Signal all servers that they are ready to start election timers.
	for i := range n {
		close(h.ready[i])
	}

	return h
}

func (h *Harness) Shutdown() {
	for i := range h.n {
		if h.alive[i] {
			h.servers[i].cm.Stop()
		}
	}
}

// CheckSingleLeader polls until exactly one leader is elected or times out.
// Returns the leader id and term.
func (h *Harness) CheckSingleLeader() (int64, int64) {
	h.t.Helper()
	for range 8 {
		leaderId := int64(-1)
		leaderTerm := int64(-1)
		for i := range h.n {
			if !h.alive[i] {
				continue
			}
			id, term, isLeader := h.servers[i].cm.Report()
			if isLeader {
				if leaderId >= 0 {
					h.t.Fatalf("both %d and %d think they are leader", leaderId, id)
				}
				leaderId = id
				leaderTerm = term
			}
		}
		if leaderId >= 0 {
			return leaderId, leaderTerm
		}
		time.Sleep(150 * time.Millisecond)
	}
	h.t.Fatal("no leader elected after timeout")
	return -1, -1
}

// CheckNoLeader asserts that no server believes it is leader right now.
func (h *Harness) CheckNoLeader() {
	h.t.Helper()
	for i := range h.n {
		if !h.alive[i] {
			continue
		}
		_, _, isLeader := h.servers[i].cm.Report()
		if isLeader {
			h.t.Fatalf("server %d unexpectedly thinks it is leader", i)
		}
	}
}

// CrashPeer stops a server and disconnects all its connections.
func (h *Harness) CrashPeer(id int) {
	h.t.Helper()
	h.t.Logf("crash peer %d", id)
	h.servers[id].cm.Stop()

	// Disconnect this peer from all others.
	for j := range h.n {
		if j != id && h.alive[j] {
			h.servers[j].DisconnectPeer(int64(id))
		}
	}
	h.alive[id] = false
}

// RestartPeer starts a previously crashed server and reconnects it.
func (h *Harness) RestartPeer(id int) {
	h.t.Helper()
	if h.alive[id] {
		h.t.Fatalf("RestartPeer(%d): server is not crashed", id)
	}
	h.t.Logf("restart peer %d", id)

	peers := make([]int64, 0, h.n-1)
	for j := range h.n {
		if j != id {
			peers = append(peers, int64(j))
		}
	}

	ready := make(chan any)
	h.ready[id] = ready
	h.servers[id] = NewServer(int64(id), peers, ready)
	h.servers[id].Serve(0)
	h.alive[id] = true

	// Re-connect to and from all live peers.
	for j := range h.n {
		if j != id && h.alive[j] {
			addr := h.servers[j].GetListenAddr()
			if err := h.servers[id].ConnectPeer(int64(j), addr); err != nil {
				h.t.Logf("warn: ConnectPeer(%d->%d): %v", id, j, err)
			}
			newAddr := h.servers[id].GetListenAddr()
			if err := h.servers[j].ConnectPeer(int64(id), newAddr); err != nil {
				h.t.Logf("warn: ConnectPeer(%d->%d): %v", j, id, err)
			}
		}
	}
	close(ready)
	fmt.Printf("[harness] restarted peer %d\n", id)
}
