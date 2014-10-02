package client

import (
	"log"
	"sync"

	"github.com/zond/drafty/common"
	"github.com/zond/drafty/peer/ring"
	"github.com/zond/drafty/transactor/messages"
	"github.com/zond/drafty/treap"
)

type TX struct {
	*messages.TX
	ring   *ring.Ring
	buffer *treap.Treap
	meta   map[string]*messages.NodeContext
	lock   sync.Mutex
}

func newTX(r *ring.Ring) (result *TX) {
	result = &TX{
		TX: &messages.TX{
			Id: ring.RandomPos(4),
		},
		ring:   r.Clone(),
		buffer: &treap.Treap{},
		meta:   map[string]*messages.NodeContext{},
	}
	return
}

func (self *TX) Commit() (err error) {
	self.lock.Lock()
	defer self.lock.Unlock()
	if err = self.preWriteAndValidate(); err != nil {
		return
	}
	return
}

func (self *TX) Abort() {
	self.lock.Lock()
	defer self.lock.Unlock()
}

func (self *TX) write(key []byte, value []byte) {
	self.buffer.Put(key, value)
	nodeContext := self.getNodeContext(key)
	skey := string(key)
	v, found := nodeContext.Values[skey]
	if found {
		v.Context.RW = true
		return
	}
	v = messages.NewValue()
	v.Data = value
	nodeContext.Values[skey] = v
	nodeContext.ValueContexts[skey] = v.Context
}

func (self *TX) Put(key []byte, value []byte) {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.write(key, value)
	return
}

func (self *TX) getNodeContext(key []byte) (result *messages.NodeContext) {
	successor := self.ring.Successors(key, 1)[0]
	result, found := self.meta[successor.Name]
	if found {
		return
	}
	result = messages.NewNodeContext()
	self.meta[successor.Name] = result
	return
}

func (self *TX) cache(key []byte, value *messages.Value) {
	self.buffer.Put(key, value.Data)
	nodeContext := self.getNodeContext(key)
	skey := string(key)
	nodeContext.Values[skey] = value
	nodeContext.ValueContexts[skey] = value.Context
}

func (self *TX) Get(key []byte) (result []byte, err error) {
	self.lock.Lock()
	defer self.lock.Unlock()
	result, found := self.buffer.Get(key)
	if found {
		return
	}
	owner := self.ring.Successors(key, 1)[0]
	value := &messages.Value{}
	if err = owner.Call("TX.Get", &messages.GetRequest{
		TX:  self.TX.Id,
		Key: key,
	}, value); err != nil {
		return
	}
	self.cache(key, value)
	result = value.Data
	return
}

func (self *TX) preWriteAndValidate() (err error) {
	p := &common.Parallelizer{}
	for _owner, _nodeContext := range self.meta {
		nodeContext := _nodeContext
		owner := _owner
		p.Start(func() (err error) {
			peer, found := self.ring.ByName(owner)
			if !found {
				log.Fatalf("Unable to find peer %v, mentioned in meta, in the ring?", peer)
			}
			if err = peer.Call("TX.PrewriteAndValidate", &messages.PrewriteAndValidateRequest{
				TX:            self.TX.Id,
				ValueContexts: nodeMeta.ValueContexts,
			}, nil); err != nil {
				return
			}
			return
		})
	}
	if err = p.Wait(); err != nil {
		return
	}
	return
}
