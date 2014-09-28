package node

import (
	"github.com/zond/drafty/log"
	"github.com/zond/drafty/node/ring"
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
