package client

import (
	"time"

	"github.com/zond/drafty/common"
	"github.com/zond/drafty/node/ring"
	"github.com/zond/drafty/switchboard"
)

const (
	rpcTimeout        = time.Second * 10
	rpcInitialBackoff = time.Millisecond * 10
)

type Client struct {
	ring *ring.Ring
}

func New(host string) (result *Client, err error) {
	result = &Client{
		ring: ring.New(),
	}
	if err = switchboard.Switch.Call(host, "Node.GetRing", nil, result.ring); err != nil {
		return
	}
	return
}

func (self *Client) refresh() (err error) {
	for {
		peer := self.ring.Rand()
		if err = switchboard.Switch.Call(peer.ConnectionString, "Node.GetRing", nil, self.ring); err == nil {
			break
		}
		self.ring.RemovePeer(peer.Name)
	}
	return
}

func (self *Client) callSuccessorOf(id []byte, service string, input interface{}, output interface{}) (err error) {
	deadline := time.Now().Add(rpcTimeout)
	backoff := rpcInitialBackoff
	for {
		successor := self.ring.Successors(id, 0)[0]
		if err = switchboard.Switch.Call(successor.ConnectionString, service, input, output); err == nil {
			break
		}
		if err = self.refresh(); err != nil {
			return
		}
		time.Sleep(backoff)
		if time.Now().After(deadline) {
			break
		}
		backoff *= 2
	}
	return
}

func (self *Client) Transact(func(*common.TX) error) (err error) {
	tx := &common.TX{
		Id: ring.RandomPos(4),
	}
	if err = self.callSuccessorOf(tx.Id, "TX.Initialize", tx, nil); err != nil {
		return
	}
	return
}
