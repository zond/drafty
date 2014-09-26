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

type TX struct {
	*common.TX
	client     *Client
	buffer     map[string][]byte
	uwByKey    map[string][][]byte
	wroteByKey map[string]int64
}

func (self *TX) Get(key []byte) (result []byte, err error) {
	skey := string(key)
	result, found := self.buffer[skey]
	if found {
		return
	}
	resp := &common.TXGetResp{}
	if err = self.client.callSuccessorOf(key, "TX.Get", &common.TXGetReq{TX: self.TX, Key: key}, resp); err != nil {
		return
	}
	self.buffer[skey] = resp.Value
	self.wroteByKey[skey] = resp.Wrote
	self.uwByKey[skey] = resp.UW
	result = resp.Value
	return
}

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

func (self *Client) Transact(f func(*TX) error) (err error) {
	tx := &TX{
		TX: &common.TX{
			Id: ring.RandomPos(4),
		},
		client:     self,
		buffer:     map[string]string{},
		uwByKey:    map[string][][]byte{},
		wroteByKey: map[string]int64{},
	}
	if err = f(tx); err != nil {
		return
	}
	return
}
