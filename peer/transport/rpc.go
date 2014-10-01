package transport

import (
	"github.com/goraft/raft"
	"github.com/zond/drafty/log"
	"github.com/zond/drafty/peer/ring"
)

type Debuggable interface {
	Dump() (string, error)
}

type DebugRPCServer struct {
	Debuggable Debuggable
}

func (self *DebugRPCServer) Dump(a struct{}, b *struct{}) (err error) {
	log.Debugf(self.Debuggable.Dump())
	return
}

type Controllable interface {
	Ring() (*ring.Ring, error)
	Stop() error
	Continue(*ring.Ring) error
	AddPeer(*ring.Peer) error
	Name() string
	LeaderForward(string, interface{}, interface{}) (bool, error)
	RaftDo(raft.Command) (interface{}, error)
}

type RPCServer struct {
	Controllable Controllable
}

func (self *RPCServer) Ring(a struct{}, result *ring.Ring) (err error) {
	r, err := self.Controllable.Ring()
	if err != nil {
		return
	}
	*result = *r
	return
}

func (self *RPCServer) Stop(a struct{}, b *struct{}) (err error) {
	return self.Controllable.Stop()
}

func (self *RPCServer) Continue(r *ring.Ring, b *struct{}) (err error) {
	return self.Controllable.Continue(r)
}

type JoinRequest struct {
	RaftJoinCommand *raft.DefaultJoinCommand
	Pos             []byte
}

type JoinResponse struct {
	Name string
}

func (self *RPCServer) accept(req *JoinRequest) {
	if err := self.Controllable.AddPeer(&ring.Peer{
		Name:             req.RaftJoinCommand.Name,
		ConnectionString: req.RaftJoinCommand.ConnectionString,
		Pos:              req.Pos,
	}); err != nil {
		log.Errorf("Unable to accept %v into ring: %v", req.RaftJoinCommand.Name, err)
		return
	}
	log.Infof("%v accepted %v into ring", self.Controllable.Name(), req.RaftJoinCommand.Name)
}

func (self *RPCServer) Join(req *JoinRequest, result *JoinResponse) (err error) {
	forwarded, err := self.Controllable.LeaderForward("Peer.Join", req, result)
	if forwarded || err != nil {
		return
	}
	if _, err = self.Controllable.RaftDo(req.RaftJoinCommand); err != nil {
		log.Warnf("Unable to add new peer %v onto raft: %v", req.RaftJoinCommand.Name, err)
		return
	}
	log.Infof("%v accepted %v onto raft", self.Controllable.Name(), req.RaftJoinCommand.Name)
	go self.accept(req)
	*result = JoinResponse{
		Name: self.Controllable.Name(),
	}
	return
}
