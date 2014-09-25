package node

import (
	"github.com/zond/drafty/log"
	"github.com/zond/drafty/node/commands"
	"github.com/zond/drafty/node/ring"
	"github.com/zond/drafty/switchboard"
)

type RPCServer struct {
	node *Node
}

func (self *RPCServer) AddPeer(peer *ring.Peer, resp *ring.Peer) (err error) {
	if self.node.raft.Name() != self.node.raft.Leader() {
		return switchboard.Switch.Call(self.node.raft.Peers()[self.node.raft.Leader()].ConnectionString, "Node.AddPeer", peer, resp)
	}
	if err = self.node.WhileStopped(func() (err error) {
		if _, err = self.node.raft.Do(&commands.AddPeerCommand{
			Peer: peer,
		}); err != nil {
			log.Warnf("Unable to add new node %v: %v", peer, err)
			return
		}
		log.Infof("%v accepted %v", self.node, peer)
		return
	}); err != nil {
		return
	}
	r := self.node.AsPeer()
	*resp = *r
	return
}

func (self *RPCServer) GetRing(a *struct{}, result *ring.Ring) (err error) {
	if err = self.node.WhileRunning(func() (err error) {
		r := self.node.ring.Clone()
		*result = *r
		return
	}); err != nil {
		return
	}
	return
}
