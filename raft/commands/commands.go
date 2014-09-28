package commands

import (
	"github.com/goraft/raft"
	"github.com/zond/drafty/node/ring"
	"github.com/zond/drafty/switchboard"
)

type Stopper interface {
	Stop() error
}

type Continuer interface {
	Continue(*ring.Ring) error
}

type StopCommand struct {
}

func (self *StopCommand) CommandName() string {
	return "node/commands/StopCommand"
}

func (self *StopCommand) Apply(c raft.Context) (result interface{}, err error) {
	if c.Server().Leader() == c.Server().Name() {
		if err = c.Server().Context().(Stopper).Stop(); err != nil {
			return
		}
		for _, peer := range c.Server().Peers() {
			if err = switchboard.Switch.Call(peer.ConnectionString, "Node.Stop", struct{}{}, nil); err != nil {
				return
			}
		}
	}
	return
}

type ContCommand struct {
	Ring *ring.Ring
}

func (self *ContCommand) CommandName() string {
	return "node/commands/ContCommand"
}

func (self *ContCommand) Apply(c raft.Context) (result interface{}, err error) {
	if c.Server().Leader() == c.Server().Name() {
		if err = c.Server().Context().(Continuer).Continue(self.Ring); err != nil {
			return
		}
		for _, peer := range c.Server().Peers() {
			if err = switchboard.Switch.Call(peer.ConnectionString, "Node.Continue", self.Ring, nil); err != nil {
				return
			}
		}
	}
	return
}
