package node

import (
	"github.com/zond/drafty/peer"
	peerTransactor "github.com/zond/drafty/peer/transactor"
	peerTransport "github.com/zond/drafty/peer/transport"
	raftTransport "github.com/zond/drafty/raft/transport"
	storageTransport "github.com/zond/drafty/storage/transport"
	"github.com/zond/drafty/switchboard"
	"github.com/zond/drafty/transactor"
	transactorTransport "github.com/zond/drafty/transactor/transport"
)

type Node struct {
	peer   *peer.Peer
	server *switchboard.Server
}

func New(addr string, dir string) (result *Node, err error) {
	result = &Node{
		server: switchboard.NewServer(addr),
	}
	if result.peer, err = peer.New(addr, dir); err != nil {
		return
	}
	return
}

func (self *Node) Start(join string) (err error) {
	self.server.Serve("Raft", &raftTransport.RPCServer{
		Raft: self.peer.Raft(),
	})
	self.server.Serve("Synchronizable", &storageTransport.RPCServer{
		Storage: self.peer.Storage(),
	})
	self.server.Serve("Peer", &peerTransport.RPCServer{
		Peer: self.peer,
	})
	self.server.Serve("TX", &transactorTransport.RPCServer{
		Transactor: transactor.New(peerTransactor.New(self.peer)),
	})
	self.server.Serve("Debug", &peerTransport.DebugRPCServer{
		Peer: self.peer,
	})
	if err = self.peer.Start(join); err != nil {
		return
	}
	return
}

func (self *Node) Stop() (err error) {
	return self.peer.Stop()
}
