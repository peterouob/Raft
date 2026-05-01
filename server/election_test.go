package server

import (
	"testing"
	"time"

	"github.com/peterouob/Raft/protobuf"
)

// --- Integration tests using Harness ---

// TestInitialElection verifies that a single leader is elected after startup.
func TestInitialElection(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	h.CheckSingleLeader()
}

// TestLeaderReelection crashes the leader and verifies a new one is elected.
func TestLeaderReelection(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	originalLeaderId, originalTerm := h.CheckSingleLeader()
	t.Logf("original leader: id=%d term=%d", originalLeaderId, originalTerm)

	h.CrashPeer(int(originalLeaderId))
	time.Sleep(350 * time.Millisecond)

	newLeaderId, newTerm := h.CheckSingleLeader()
	t.Logf("new leader: id=%d term=%d", newLeaderId, newTerm)

	if newLeaderId == originalLeaderId {
		t.Errorf("new leader (%d) is same as crashed leader", newLeaderId)
	}
	if newTerm <= originalTerm {
		t.Errorf("new term %d should be > original term %d", newTerm, originalTerm)
	}
}

// TestNoQuorumNoLeader verifies no leader when majority is down.
func TestNoQuorumNoLeader(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderId, _ := h.CheckSingleLeader()

	// Crash two of the three nodes (majority).
	h.CrashPeer(int(leaderId))
	other := (int(leaderId) + 1) % 3
	h.CrashPeer(other)

	time.Sleep(450 * time.Millisecond)
	h.CheckNoLeader()
}

// TestRestoreQuorum crashes one peer then restores it; cluster should stay healthy.
func TestRestoreQuorum(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderId, _ := h.CheckSingleLeader()

	// Crash a follower.
	follower := (int(leaderId) + 1) % 3
	h.CrashPeer(follower)
	time.Sleep(200 * time.Millisecond)

	// Leader should still hold.
	h.CheckSingleLeader()

	// Restart the follower.
	h.RestartPeer(follower)
	time.Sleep(300 * time.Millisecond)

	// Cluster should still have exactly one leader.
	h.CheckSingleLeader()
}

// --- Unit tests for ConsensusModule methods ---

// makeTestCM creates a ConsensusModule wired to a mock server with no real peers.
func makeTestCM(id int64) (*ConsensusModule, chan any) {
	ready := make(chan any)
	s := NewServer(id, nil, ready)
	// Don't call Serve — we don't need a real listener for unit tests.
	s.cm = NewConsensusModule(id, nil, s, ready)
	return s.cm, ready
}

// TestRequestVote_GrantsVote ensures a follower grants a vote for a higher-term candidate.
func TestRequestVote_GrantsVote(t *testing.T) {
	cm, ready := makeTestCM(0)
	close(ready)

	args := &protobuf.RequestVoteArgs{
		Term:        1,
		CandidateId: 1,
	}
	var reply protobuf.RequestVoteArgsResponse
	if err := cm.RequestVote(args, &reply); err != nil {
		t.Fatalf("RequestVote error: %v", err)
	}
	if !reply.VoteGranted {
		t.Error("expected vote to be granted for higher term")
	}
	if reply.Term != 1 {
		t.Errorf("expected term 1, got %d", reply.Term)
	}
}

// TestRequestVote_DeniesStaleCandidate ensures a vote is denied for a stale term.
func TestRequestVote_DeniesStaleCandidate(t *testing.T) {
	cm, ready := makeTestCM(0)
	close(ready)

	// First call: advance the CM to term 5.
	cm.RequestVote(&protobuf.RequestVoteArgs{Term: 5, CandidateId: 1}, &protobuf.RequestVoteArgsResponse{})

	// Second call: candidate with older term.
	args := &protobuf.RequestVoteArgs{Term: 3, CandidateId: 2}
	var reply protobuf.RequestVoteArgsResponse
	cm.RequestVote(args, &reply)

	if reply.VoteGranted {
		t.Error("expected vote to be denied for stale term")
	}
}

// TestAppendEntries_HeartbeatSuccess verifies a follower accepts a valid heartbeat.
func TestAppendEntries_HeartbeatSuccess(t *testing.T) {
	cm, ready := makeTestCM(0)
	close(ready)

	// Advance term first.
	cm.RequestVote(&protobuf.RequestVoteArgs{Term: 2, CandidateId: 1}, &protobuf.RequestVoteArgsResponse{})

	args := &protobuf.AppendEntriesArgs{Term: 2, LeaderId: 1}
	var reply protobuf.AppendEntriesReply
	if err := cm.AppendEntries(args, &reply); err != nil {
		t.Fatalf("AppendEntries error: %v", err)
	}
	if !reply.Success {
		t.Error("expected heartbeat to succeed")
	}
}

// TestAppendEntries_RejectsStaleTerm ensures a stale-term heartbeat is rejected.
func TestAppendEntries_RejectsStaleTerm(t *testing.T) {
	cm, ready := makeTestCM(0)
	close(ready)

	cm.RequestVote(&protobuf.RequestVoteArgs{Term: 5, CandidateId: 1}, &protobuf.RequestVoteArgsResponse{})

	args := &protobuf.AppendEntriesArgs{Term: 2, LeaderId: 1}
	var reply protobuf.AppendEntriesReply
	cm.AppendEntries(args, &reply)

	if reply.Success {
		t.Error("expected stale heartbeat to be rejected")
	}
}

// TestFollowerBecomesCandidate verifies that a follower starts an election after timeout.
func TestFollowerBecomesCandidate(t *testing.T) {
	cm, ready := makeTestCM(0)
	// Start the election timer by signaling ready.
	go cm.runElectionTimer()
	close(ready)

	// Wait long enough for election timeout (max 300ms) + margin.
	time.Sleep(500 * time.Millisecond)

	_, term, _ := cm.Report()
	if term == 0 {
		t.Error("expected term to have advanced after election timeout")
	}
}
