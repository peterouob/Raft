package server

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/peterouob/Raft/protobuf"
)

type CMState int64

const (
	Follower CMState = iota + 1
	Leader
	Candidate
	Dead
)

func (s CMState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Leader:
		return "Leader"
	case Candidate:
		return "Candidate"
	case Dead:
		return "Dead"
	default:
		panic("unknow state")
	}
}

type LogEntry struct {
	Term    int64
	Command any
}

type ConsensusModule struct {
	mu sync.Mutex

	id int64

	peerIds []int64

	server *Server

	currentTerm int64
	votedFor    int64
	log         []LogEntry

	state              CMState
	electionResetEvent time.Time

	done chan struct{}
}

func NewConsensusModule(id int64, peerIds []int64, server *Server, ready <-chan any) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.server = server
	cm.state = Follower
	cm.votedFor = -1
	cm.done = make(chan struct{})
	go func() {
		<-ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()

	return cm
}

func (cm *ConsensusModule) Report() (id, term int64, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.state == Leader
}

func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.state = Dead
}

func (cm *ConsensusModule) dlog(format string, args ...any) {
	format = fmt.Sprintf("[%d]", cm.id) + format
	log.Printf(format, args...)
}

func (cm *ConsensusModule) electionTimeout() time.Duration {
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	}

	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.electionTimeout()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.mu.Unlock()
	cm.dlog("election timer started (%v), term=%d", timeoutDuration, termStarted)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {

		cm.mu.Lock()
		if cm.state != Candidate && cm.state != Follower {
			cm.dlog("in election timer state=%s, bailing out", cm.state)
			cm.mu.Unlock()
			return
		}

		if termStarted != cm.currentTerm {
			cm.dlog("in election timer term changed from %d to %d, bailing out", termStarted, cm.currentTerm)
			cm.mu.Unlock()
			return
		}

		if elapsed := time.Since(cm.electionResetEvent); elapsed >= timeoutDuration {
			cm.dlog("election timer elapsed (%v), starting new election", elapsed)
			cm.startElection()
			cm.mu.Unlock()
			return
		}

		cm.mu.Unlock()

		select {
		case <-ticker.C:
		case <-cm.done:
			return
		}
	}
}

// startElection use mutex to protect then make sure only on election at a time
func (cm *ConsensusModule) startElection() {

	// you will vote for yourself
	cm.state = Candidate
	cm.currentTerm += 1
	savedCurrentTerm := cm.currentTerm
	cm.votedFor = cm.id
	cm.electionResetEvent = time.Now()

	cm.dlog("becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, cm.log)
	votesReceived := 1

	for _, peerId := range cm.peerIds {
		go func(id int64) {

			args := &protobuf.RequestVoteArgs{
				Term:        savedCurrentTerm,
				CandidateId: cm.id,
			}

			cm.dlog("sending RequestVote to %d: %+v", id, args)

			if reply, err := cm.server.CallRequestVote(id, args); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				cm.dlog("Received the reply %v", reply)

				// make sure only have one leader at time
				if cm.state != Candidate {
					cm.dlog("while waiting for reply, state = %v", cm.state)
					return
				}

				// reply.term > cm.currentTerm 相當於回覆期限比別人長就直接先變成 follower
				if reply.Term > cm.currentTerm {
					cm.dlog("term out of date in RequestVoteReply")
					cm.becomeFollower(reply.Term)
					return
				} else if reply.Term == cm.currentTerm {
					if reply.VoteGranted {
						votesReceived += 1
						// mini vote needs to be leader
						if votesReceived*2 > len(cm.peerIds)+1 {
							cm.dlog("wins election with %d votes", votesReceived)
							cm.startLeader()
							return
						}
					}
				}
			}
		}(peerId)
	}

	go cm.runElectionTimer()
}

func (cm *ConsensusModule) RequestVote(args *protobuf.RequestVoteArgs, reply *protobuf.RequestVoteArgsResponse) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.state == Dead {
		return nil
	}

	if args.Term > cm.currentTerm {
		cm.becomeFollower(args.Term)
	}

	if cm.currentTerm == args.Term && (cm.votedFor == -1 || cm.votedFor == args.CandidateId) {
		reply.VoteGranted = true
		cm.votedFor = args.CandidateId
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}

	reply.Term = cm.currentTerm
	return nil
}

func (cm *ConsensusModule) AppendEntries(args *protobuf.AppendEntriesArgs, reply *protobuf.AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.state == Dead {
		cm.dlog("in AppendEntries, but state is Dead, bailing out")
		return nil
	}

	cm.dlog("AppendEntries: %+v", args)

	if args.Term > cm.currentTerm {
		cm.dlog("term out of date in AppendEntries")
		cm.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == cm.currentTerm {
		if cm.state != Follower {
			cm.becomeFollower(args.Term)
		}
		cm.electionResetEvent = time.Now()
		reply.Success = true
	}

	reply.Term = cm.currentTerm
	cm.dlog("AppendEntries reply: %+v", reply)

	return nil
}

func (cm *ConsensusModule) becomeFollower(term int64) {
	cm.dlog("becomes Follower with term=%d; log=%v", term, cm.log)
	cm.state = Follower
	cm.votedFor = -1
	cm.currentTerm = term
	cm.electionResetEvent = time.Now()

	go cm.runElectionTimer()
}

func (cm *ConsensusModule) startLeader() {
	cm.state = Leader
	cm.dlog("becomes Leader; term=%d, log=%v", cm.currentTerm, cm.log)
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {

			cm.mu.Lock()
			if cm.state != Leader {
				cm.dlog("in leader state=%s, bailing out", cm.state)
				cm.mu.Unlock()
				return
			}
			cm.mu.Unlock()

			cm.leaderSendHeartbeats()

			select {
			case <-ticker.C:
				log.Println("goroutines alive:", runtime.NumGoroutine())
			case <-cm.done:
				return
			}
		}
	}()
}

// leaderSendHeartbeats ping the other node
func (cm *ConsensusModule) leaderSendHeartbeats() {
	cm.mu.Lock()
	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()

	for _, peerId := range cm.peerIds {

		args := &protobuf.AppendEntriesArgs{
			Term:     savedCurrentTerm,
			LeaderId: cm.id,
		}

		go func(id int64) {
			cm.dlog("sending AppendEntries to %v: ni=%d, args=%+v", peerId, 0, args)

			if reply, err := cm.server.CallAppendEntries(id, args); err == nil {
				cm.mu.Lock()
				if reply.Term > cm.currentTerm {
					cm.dlog("term out of date in AppendEntriesReply")
					cm.becomeFollower(reply.Term)
					cm.mu.Unlock()
					return
				}
				cm.mu.Unlock()
			}
		}(peerId)
	}
}
