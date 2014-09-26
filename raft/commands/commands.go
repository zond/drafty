package commands

import (
	"github.com/goraft/raft"
	"github.com/zond/drafty/node/ring"
)

type Stopper interface {
	Stop() error
}

type Continuer interface {
	Continue() error
}

type PeerAdder interface {
	AddPeer(*ring.Peer) error
}

type PeerRemover interface {
	RemovePeer(string) error
}

type StopCommand struct {
}

func (self *StopCommand) CommandName() string {
	return "node/commands/StopCommand"
}

func (self *StopCommand) Apply(c raft.Context) (result interface{}, err error) {
	return nil, c.Server().Context().(Stopper).Stop()
}

type ContCommand struct {
}

func (self *ContCommand) CommandName() string {
	return "node/commands/ContCommand"
}

func (self *ContCommand) Apply(c raft.Context) (result interface{}, err error) {
	return nil, c.Server().Context().(Continuer).Continue()
}

type AddPeerCommand struct {
	Peer *ring.Peer
}

func (self *AddPeerCommand) CommandName() string {
	return "node/commands/AddPeerCommand"
}

func (self *AddPeerCommand) Apply(c raft.Context) (result interface{}, err error) {
	return nil, c.Server().Context().(PeerAdder).AddPeer(self.Peer)
}

type RemovePeerCommand struct {
	Name string
}

func (self *RemovePeerCommand) CommandName() string {
	return "node/commands/RemovePeerCommand"
}

func (self *RemovePeerCommand) Apply(c raft.Context) (result interface{}, err error) {
	return nil, c.Server().Context().(PeerRemover).RemovePeer(self.Name)
}
