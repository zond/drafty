package client

import (
	"github.com/zond/drafty/peer/ring"
	"github.com/zond/drafty/switchboard"
	"github.com/zond/drafty/transactor/messages"
	"time"
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
	if err = switchboard.Switch.Call(host, "Peer.Ring", struct{}{}, result.ring); err != nil {
		return
	}
	return
}

func (self *Client) refresh() (err error) {
	for {
		peer := self.ring.Rand()
		if err = switchboard.Switch.Call(peer.ConnectionString, "Peer.Ring", struct{}{}, self.ring); err == nil {
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

func (self *Client) Transact(f func(*TX) error) (err error) {
	tx := &TX{
		TX: &messages.TX{
			Id: ring.RandomPos(4),
		},
		client:     self,
		buffer:     map[string][]byte{},
		uwByKey:    map[string][][]byte{},
		wroteByKey: map[string]int64{},
	}
	defer func() {
		if e := recover(); e != nil {
			tx.Abort()
		}
	}()
	if err = f(tx); err != nil {
		tx.Abort()
		return
	}
	if err = tx.Commit(); err != nil {
		return
	}
	return
}
