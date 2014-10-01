package client

import (
	"encoding/hex"
	"log"
	"sync"

	"github.com/zond/drafty/common"
	"github.com/zond/drafty/peer/ring"
	"github.com/zond/drafty/transactor/messages"
)

type TX struct {
	*messages.TX
	ring       *ring.Ring
	buffer     map[string]*messages.Value
	metaByNode map[string]*messages.NodeMeta
	lock       sync.Mutex
}

func newTX(r *ring.Ring) (result *TX) {
	result = &TX{
		TX: &messages.TX{
			Id: ring.RandomPos(4),
		},
		ring:       r.Clone(),
		buffer:     map[string]*messages.Value{},
		metaByNode: map[string]*messages.NodeMeta{},
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

func (self *TX) getNodeMeta(key []byte) (result *messages.NodeMeta) {
	owner := self.ring.Successors(key, 1)[0]
	result, found := self.metaByNode[owner.Name]
	if !found {
		result = &messages.NodeMeta{
			Values: map[string]*messages.Value{},
		}
		self.metaByNode[owner.Name] = result
	}
	return
}

func (self *TX) setValue(key []byte, value *messages.Value) {
	nodeMeta := self.getNodeMeta(key)
	skey := string(key)
	if oldValue, found := self.buffer[skey]; found {
		oldValue.Data = value.Data
	} else {
		self.buffer[skey] = value
		if _, found = nodeMeta.Values[skey]; found {
			log.Fatalf("Found %v in nodeMeta, but not in buffer?!?!", hex.EncodeToString(key))
		}
		nodeMeta.Values[skey] = value
	}
}

func (self *TX) getValue(key []byte) (result *messages.Value) {
	nodeMeta := self.getNodeMeta(key)
	skey := string(key)
	result, found := self.buffer[skey]
	if !found {
		result = &messages.Value{}
		self.buffer[skey] = result
		if _, found = nodeMeta.Values[skey]; found {
			log.Fatalf("Found %v in nodeMeta, but not in buffer?!?!", hex.EncodeToString(key))
		}
		nodeMeta.Values[skey] = result
	}
	return
}

func (self *TX) Put(key []byte, value []byte) {
	self.lock.Lock()
	defer self.lock.Unlock()
	v := self.getValue(key)
	v.Data = value
	return
}

func (self *TX) Get(key []byte) (result []byte, err error) {
	self.lock.Lock()
	defer self.lock.Unlock()
	skey := string(key)
	v, found := self.buffer[skey]
	if found {
		result = v.Data
		return
	}
	owner := self.ring.Successors(key, 1)[0]
	value := &messages.Value{}
	if err = owner.Call("TX.Get", &messages.GetRequest{TX: self.TX, Key: key}, value); err != nil {
		return
	}
	self.setValue(key, value)
	result = value.Data
	return
}

func (self *TX) preWriteAndValidate() (err error) {
	p := &common.Parallelizer{}
	for _owner, _nodeMeta := range self.metaByNode {
		nodeMeta := _nodeMeta
		owner := _owner
		p.Start(func() (err error) {
			peer, found := self.ring.ByName(owner)
			if !found {
				log.Fatalf("Unable to find peer %v, mentioned in metaByNode, in the ring?", peer)
			}
			if err = peer.Call("TX.PrewriteAndValidate", nodeMeta, nil); err != nil {
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
