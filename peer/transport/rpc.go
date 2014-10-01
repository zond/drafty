package transport

import (
	"github.com/zond/drafty/log"
	"github.com/zond/drafty/peer"
	"github.com/zond/drafty/peer/messages"
	"github.com/zond/drafty/peer/ring"
)

type DebugRPCServer struct {
	Peer *peer.Peer
}

func (self *DebugRPCServer) Dump(a struct{}, b *struct{}) (err error) {
	log.Debugf(self.Peer.Dump())
	return
}

type RPCServer struct {
	Peer *peer.Peer
}

func (self *RPCServer) Ring(a struct{}, result *ring.Ring) (err error) {
	if err = self.Peer.WhileRunning(func() (err error) {
		r := self.Peer.Ring()
		*result = *r
		return
	}); err != nil {
		return
	}
	return
}

func (self *RPCServer) Stop(a struct{}, b *struct{}) (err error) {
	return self.Peer.Stop()
}

func (self *RPCServer) Continue(r *ring.Ring, b *struct{}) (err error) {
	return self.Peer.Continue(r)
}

func (self *RPCServer) accept(req *messages.JoinRequest) {
	if err := self.Peer.AddPeer(&ring.Peer{
		Name:             req.RaftJoinCommand.Name,
		ConnectionString: req.RaftJoinCommand.ConnectionString,
		Pos:              req.Pos,
	}); err != nil {
		log.Errorf("Unable to accept %v into ring: %v", req.RaftJoinCommand.Name, err)
		return
	}
	log.Infof("%v accepted %v into ring", self.Peer.Name(), req.RaftJoinCommand.Name)
}

func (self *RPCServer) Join(req *messages.JoinRequest, result *messages.JoinResponse) (err error) {
	forwarded, err := self.Peer.LeaderForward("Peer.Join", req, result)
	if forwarded || err != nil {
		return
	}
	if _, err = self.Peer.RaftDo(req.RaftJoinCommand); err != nil {
		log.Warnf("Unable to add new peer %v onto raft: %v", req.RaftJoinCommand.Name, err)
		return
	}
	log.Infof("%v accepted %v onto raft", self.Peer.Name(), req.RaftJoinCommand.Name)
	go self.accept(req)
	*result = messages.JoinResponse{
		Name: self.Peer.Name(),
	}
	return
}
