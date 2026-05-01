package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/peterouob/Raft/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Server struct {
	mu sync.Mutex

	id    int64
	peers []int64

	cm *ConsensusModule

	gRpcServer *grpc.Server
	listener   net.Listener

	//peerClientConn key is for the server id
	peerClientConn map[int64]*grpc.ClientConn
	peerClients    map[int64]protobuf.RaftServiceClient

	ready <-chan any
	quit  chan any
	wg    sync.WaitGroup

	protobuf.UnimplementedRaftServiceServer
}

func NewServer(serverId int64, peerIds []int64, ready <-chan any) *Server {
	s := new(Server)
	s.id = serverId
	s.peers = peerIds
	s.peerClientConn = make(map[int64]*grpc.ClientConn)
	s.peerClients = make(map[int64]protobuf.RaftServiceClient)
	s.ready = ready
	s.quit = make(chan any)
	return s
}

func (s *Server) AppendEntries(_ context.Context, args *protobuf.AppendEntriesArgs) (*protobuf.AppendEntriesReply, error) {
	var reply protobuf.AppendEntriesReply
	if err := s.cm.AppendEntries(args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil

}

func (s *Server) RequestVote(_ context.Context, args *protobuf.RequestVoteArgs) (*protobuf.RequestVoteArgsResponse, error) {
	var reply protobuf.RequestVoteArgsResponse
	if err := s.cm.RequestVote(args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

func (s *Server) Serve() {
	var err error

	s.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s.mu.Lock()
	if s.cm == nil {
		s.cm = NewConsensusModule(s.id, s.peers, s, s.ready)
	}
	s.mu.Unlock()

	s.gRpcServer = grpc.NewServer()
	protobuf.RegisterRaftServiceServer(s.gRpcServer, s)

	s.wg.Go(func() {
		if err := s.gRpcServer.Serve(s.listener); err != nil {
			s.gRpcServer.Stop()
		}
	})
}

func (s *Server) ConnectPeer(peerId int64, addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.peerClients[peerId]; ok {
		return nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	s.peerClientConn[peerId] = conn
	s.peerClients[peerId] = protobuf.NewRaftServiceClient(conn)
	return nil
}

func (s *Server) DisconnectPeer(peerId int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if conn, ok := s.peerClientConn[peerId]; ok {
		conn.Close()
		delete(s.peerClientConn, peerId)
		delete(s.peerClients, peerId)
	}
}

func (s *Server) DIsconnectAllPeers() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, conn := range s.peerClientConn {
		conn.Close()
	}
	for peerId, _ := range s.peerClientConn {
		delete(s.peerClientConn, peerId)
	}

	for peerId, _ := range s.peerClients {
		delete(s.peerClients, peerId)
	}
}

func (s *Server) Shutdown() {
	s.DIsconnectAllPeers()
	s.gRpcServer.Stop()
	close(s.quit)
	close(s.cm.done)
}

func (s *Server) GetListenAddr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener.Addr().String()
}

func (s *Server) CallAppendEntries(peerId int64, args *protobuf.AppendEntriesArgs) (*protobuf.AppendEntriesReply, error) {
	s.mu.Lock()
	peer := s.peerClients[peerId]
	s.mu.Unlock()

	if peer == nil {
		return nil, fmt.Errorf("peer %d not found", peerId)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pbReply, err := peer.AppendEntries(ctx, args)
	if err != nil {
		return nil, err
	}

	return pbReply, nil
}

func (s *Server) CallRequestVote(peerId int64, args *protobuf.RequestVoteArgs) (*protobuf.RequestVoteArgsResponse, error) {
	s.mu.Lock()
	peer := s.peerClients[peerId]
	s.mu.Unlock()

	if peer == nil {
		return nil, fmt.Errorf("peer %d not found", peerId)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pbReply, err := peer.RequestVote(ctx, args)
	if err != nil {
		return nil, err
	}

	return pbReply, nil
}
