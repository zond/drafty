package client

import (
	"encoding/hex"
	"log"
	"sync"

	"github.com/zond/drafty/common"
	"github.com/zond/drafty/peer/ring"
	"github.com/zond/drafty/transactor/messages"
)

type buffer struct {
	data     map[string]*messages.Value
	nodeMeta map[string]*messages.NodeMeta
}

type TX struct {
	*messages.TX
	ring   *ring.Ring
	writes *buffer
	reads  *buffer
	lock   sync.Mutex
}

func newTX(r *ring.Ring) (result *TX) {
	result = &TX{
		TX: &messages.TX{
			Id: ring.RandomPos(4),
		},
		ring: r.Clone(),
		reads: &buffer{
			data:     map[string]*messages.Value{},
			nodeMeta: map[string]*messages.NodeMeta{},
		},
		writes: &buffer{
			data:     map[string]*messages.Value{},
			nodeMeta: map[string]*messages.NodeMeta{},
		},
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

func (self *TX) getNodeMeta(b *buffer, key []byte) (result *messages.NodeMeta) {
	owner := self.ring.Successors(key, 1)[0]
	result, found := b.nodeMeta[owner.Name]
	if !found {
		result = &messages.NodeMeta{
			Values: map[string]*messages.Value{},
			Meta:   map[string]*messages.ValueMeta{},
		}
		b.nodeMeta[owner.Name] = result
	}
	return
}

func (self *TX) cache(key []byte, value *messages.Value) {
	nodeMeta := self.getNodeMeta(self.reads, key)
	skey := string(key)
	if oldValue, found := self.reads.data[skey]; found {
		oldValue.Data = value.Data
	} else {
		self.reads.data[skey] = value
		if _, found = nodeMeta.Values[skey]; found {
			log.Fatalf("Found %v in nodeMeta, but not in buffer?!?!", hex.EncodeToString(key))
		}
		nodeMeta.Values[skey] = value
		nodeMeta.Meta[skey] = value.Meta
	}
}

func (self *TX) lookup(key string) (result []byte, found bool) {
	v, found := self.writes.data[key]
	if found {
		result = v.Data
		return
	}
	v, found = self.reads.data[key]
	if found {
		result = v.Data
		return
	}
	return
}

func (self *TX) write(key []byte, data []byte) {
	nodeMeta := self.getNodeMeta(self.writes, key)
	skey := string(key)
	value, found := self.writes.data[skey]
	if !found {
		value = &messages.Value{
			Meta: &messages.ValueMeta{},
		}
		self.writes.data[skey] = value
		if _, found = nodeMeta.Values[skey]; found {
			log.Fatalf("Found %v in nodeMeta, but not in buffer?!?!", hex.EncodeToString(key))
		}
		nodeMeta.Values[skey] = value
		nodeMeta.Meta[skey] = value.Meta
	}
	value.Data = data
}

func (self *TX) Put(key []byte, value []byte) {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.write(key, value)
	return
}

func (self *TX) Get(key []byte) (result []byte, err error) {
	self.lock.Lock()
	defer self.lock.Unlock()
	skey := string(key)
	result, found := self.lookup(skey)
	if found {
		return
	}
	owner := self.ring.Successors(key, 1)[0]
	value := &messages.Value{}
	if err = owner.Call("TX.Get", &messages.GetRequest{TX: self.TX.Id, Key: key}, value); err != nil {
		return
	}
	self.cache(key, value)
	result = value.Data
	return
}

func (self *TX) preWriteAndValidate() (err error) {
	p := &common.Parallelizer{}
	for _owner, _nodeMeta := range self.reads.nodeMeta {
		nodeMeta := _nodeMeta
		owner := _owner
		p.Start(func() (err error) {
			peer, found := self.ring.ByName(owner)
			if !found {
				log.Fatalf("Unable to find peer %v, mentioned in metaByNode, in the ring?", peer)
			}
			if err = peer.Call("TX.PrewriteAndValidate", nodeMeta.Meta, nil); err != nil {
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
