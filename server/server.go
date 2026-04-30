package server

import (
	"context"
	"fmt"
	"net"
	"raft/protobuf"
	"sync"
	"time"

	"google.golang.org/grpc"
)

type Server struct {
	mu sync.Mutex

	id    int64
	peers []int64

	cm       *ConsensusModule
	rpcProxy *RPCProxy

	rpcServer *protobuf.RaftServiceServer
	listener  net.Listener

	//peerClientConn key is for the server id
	peerClientConn map[int64]*grpc.ClientConn
	peerClients    map[int64]protobuf.RaftServiceClient

	ready <-chan any
	quit  chan any
	wg    sync.WaitGroup
}

func NewServer(serverId int64, peerIds []int64, ready <-chan any) *Server {
	s := new(Server)
	s.id = serverId
	s.peers = peerIds
	s.peerClientConn = make(map[int64]*grpc.ClientConn)
	s.ready = ready
	s.quit = make(chan any)
	return s
}

type RPCProxy struct {
	cm *ConsensusModule
}

func (s *Server) CallAppendEntries(peerId int64, args AppendEntriesArgs) (*AppendEntriesReply, error) {
	s.mu.Lock()
	peer := s.peerClients[peerId]
	s.mu.Unlock()

	if peer == nil {
		return nil, fmt.Errorf("peer %d not found", peerId)
	}

	pbArgs := &protobuf.AppendEntriesArgs{
		Term:         args.term,
		LeaderId:     args.leaderId,
		PrevLogIndex: args.prevLogIndex,
		PrevLogTerm:  args.prevLogTerm,
		Entries:      args.entries,
		LeaderCommit: args.leaderCommit,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pbReply, err := peer.AppendEntries(ctx, pbArgs)
	if err != nil {
		return nil, err
	}

	reply := &AppendEntriesReply{
		term:    pbReply.Term,
		success: pbReply.Success,
	}

	return reply, nil
}

func (s *Server) CallRequestVote(peerId int64, args RequestVoteArgs) (*RequestVoteArgsReply, error) {
	s.mu.Lock()
	peer := s.peerClients[peerId]
	s.mu.Unlock()

	if peer == nil {
		return nil, fmt.Errorf("peer %d not found", peerId)
	}

	pbArgs := &protobuf.RequestVoteArgs{
		Term:         args.term,
		CandidateId:  args.candidateId,
		LastLogIndex: args.lastLogIndex,
		LastLogTerm:  args.lastLogTerm,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pbReply, err := peer.RequestVote(ctx, pbArgs)
	if err != nil {
		return nil, err
	}

	reply := &RequestVoteArgsReply{
		term:        pbReply.Term,
		voteGranted: pbReply.VoteGranted,
	}

	return reply, nil
}
