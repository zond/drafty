package node

import (
	"fmt"

	"github.com/goraft/raft"
	"github.com/zond/drafty/log"
	"github.com/zond/drafty/node/ring"
	"github.com/zond/drafty/switchboard"
)

type DebugRPCServer struct {
	node *Node
}

func (self *DebugRPCServer) Dump(a struct{}, b *struct{}) (err error) {
	log.Debugf(self.node.storage.PPStrings())
	return
}

type RPCServer struct {
	node *Node
}

func (self *RPCServer) GetRing(a struct{}, result *ring.Ring) (err error) {
	if err = self.node.WhileRunning(func() (err error) {
		r := self.node.ring.Clone()
		*result = *r
		return
	}); err != nil {
		return
	}
	return
}

func (self *RPCServer) Stop(a struct{}, b *struct{}) (err error) {
	return self.node.Stop()
}

func (self *RPCServer) Continue(r *ring.Ring, b *struct{}) (err error) {
	return self.node.Continue(r)
}

type JoinRequest struct {
	RaftJoinCommand *raft.DefaultJoinCommand
	Pos             []byte
}

type JoinResponse struct {
	Name string
}

type Kicker interface {
	Kick(string) error
}

type multiError []error

func (self multiError) Error() string {
	return fmt.Sprintf("%+v", self)
}

func (self *RPCServer) accept(req *JoinRequest) {
	if err := self.node.AddPeer(&ring.Peer{
		Name:             req.RaftJoinCommand.Name,
		ConnectionString: req.RaftJoinCommand.ConnectionString,
		Pos:              req.Pos,
	}); err != nil {
		log.Errorf("Unable to accept %v into ring: %v", req.RaftJoinCommand.Name, err)
		return
	}
	log.Infof("%v accepted %v into ring", self.node.raft.Name(), req.RaftJoinCommand.Name)
}

func (self *RPCServer) Join(req *JoinRequest, result *JoinResponse) (err error) {
	if self.node.raft.Name() != self.node.raft.Leader() {
		return switchboard.Switch.Call(self.node.raft.Peers()[self.node.raft.Leader()].ConnectionString, "Node.Join", req, result)
	}
	if _, err = self.node.raft.Do(req.RaftJoinCommand); err != nil {
		log.Warnf("Unable to add new node %v onto raft: %v", req.RaftJoinCommand.Name, err)
		return
	}
	log.Infof("%v accepted %v onto raft", self.node.raft.Name(), req.RaftJoinCommand.Name)
	go self.accept(req)
	*result = JoinResponse{
		Name: self.node.raft.Name(),
	}
	return
}
