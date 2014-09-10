package commands

import "github.com/goraft/raft"

type StopContinuer interface {
	Stop() error
	Continue() error
}

type StopCommand struct {
}

func (self *StopCommand) CommandName() string {
	return "consensual:stop"
}

func (self *StopCommand) Apply(c raft.Context) (result interface{}, err error) {
	return nil, c.Server().Context().(StopContinuer).Stop()
}

type ContCommand struct {
}

func (self *ContCommand) CommandName() string {
	return "consensual:cont"
}

func (self *ContCommand) Apply(c raft.Context) (result interface{}, err error) {
	return nil, c.Server().Context().(StopContinuer).Continue()
}
