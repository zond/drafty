package node

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/boltdb/bolt"
	"github.com/goraft/raft"
	"github.com/zond/drafty/common"
	"github.com/zond/drafty/log"
	"github.com/zond/drafty/node/ring"
	"github.com/zond/drafty/raft/commands"
	raftTransport "github.com/zond/drafty/raft/transport"
	"github.com/zond/drafty/storage"
	storageTransport "github.com/zond/drafty/storage/transport"
	"github.com/zond/drafty/switchboard"
	"github.com/zond/drafty/transactor"
	transactorTransport "github.com/zond/drafty/transactor/transport"
)

const (
	maintenanceChunkSize = 16
)

func init() {
	rand.Seed(time.Now().UnixNano())
	raft.RegisterCommand(&commands.StopCommand{})
	raft.RegisterCommand(&commands.ContCommand{})
	raft.RegisterCommand(&commands.AddPeerCommand{})
	raft.RegisterCommand(&commands.RemovePeerCommand{})
}

var posKey = []byte("pos")
var metadataBucketKey = []byte("metadata")

type Node struct {
	pos            []byte
	server         *switchboard.Server
	dir            string
	raft           raft.Server
	ring           *ring.Ring
	transactor     *transactor.Transactor
	stopped        int32
	stops          uint64
	stopLock       sync.Mutex
	stopCond       *sync.Cond
	procsDoneGroup sync.WaitGroup
	storage        *storage.DB
	metadata       *bolt.DB
}

func New(addr string, dir string) (result *Node, err error) {
	result = &Node{
		server: switchboard.NewServer(addr),
		dir:    dir,
		ring:   ring.New(),
	}
	result.stopCond = sync.NewCond(&result.stopLock)
	return
}

type MultiError []error

func (self MultiError) Error() string {
	return fmt.Sprint([]error(self))
}

type nodeState struct {
	Ring    *ring.Ring
	Stopped int32
}

func (self *Node) Save() (b []byte, err error) {
	log.Debugf("Compacting log")
	buf := &bytes.Buffer{}
	if err = gob.NewEncoder(buf).Encode(nodeState{
		Ring:    self.ring,
		Stopped: atomic.LoadInt32(&self.stopped),
	}); err != nil {
		return
	}
	return
}

func (self *Node) Recovery(b []byte) (err error) {
	log.Debugf("Recovering from log")
	state := &nodeState{}
	if err = gob.NewDecoder(bytes.NewBuffer(b)).Decode(state); err != nil {
		return
	}
	self.ring = state.Ring
	atomic.StoreInt32(&self.stopped, state.Stopped)
	return
}

func (self *Node) Addr() string {
	return self.server.Addr()
}

func (self *Node) Stop() (err error) {
	self.stopLock.Lock()
	defer self.stopLock.Unlock()
	atomic.StoreInt32(&self.stopped, 1)
	atomic.AddUint64(&self.stops, 1)
	self.procsDoneGroup.Wait()
	log.Debugf("%v stopped", self)
	return
}

func (self *Node) Continue() (err error) {
	atomic.StoreInt32(&self.stopped, 0)
	self.stopCond.Broadcast()
	go self.synchronize()
	//go self.clean()
	log.Debugf("%v continued", self)
	return
}

func (self *Node) WhileStopped(f func() error) (err error) {
	if _, err = self.raft.Do(&commands.StopCommand{}); err != nil {
		log.Warnf("Unable to issue stop command before running %v: %v", f, err)
		return
	}
	defer func() {
		if _, err = self.raft.Do(&commands.ContCommand{}); err != nil {
			log.Fatalf("Unable to issue continue command after running %v: %v", f, err)
		}
		if err = self.raft.TakeSnapshot(); err != nil {
			log.Warnf("Unable to issue continue command after running %v: %v", f, err)
		}
	}()
	if err = f(); err != nil {
		return
	}
	return
}

func (self *Node) WhileRunning(f func() error) error {
	self.stopLock.Lock()
	for atomic.LoadInt32(&self.stopped) == 1 {
		self.stopCond.Wait()
	}
	self.stopLock.Unlock()
	self.procsDoneGroup.Add(1)
	defer self.procsDoneGroup.Done()
	return f()
}

func (self *Node) AddPeer(peer *ring.Peer) (err error) {
	self.ring.AddPeer(peer)
	return
}

func (self *Node) RemovePeer(name string) (err error) {
	self.ring.RemovePeer(name)
	return
}

func (self *Node) syncOnceWith(i int) (acted bool, err error) {
	if err = self.WhileRunning(func() (err error) {
		r := storage.Range{
			FromInc: self.ring.Predecessors(self.pos, 1)[0].Pos,
			ToExc:   self.pos,
		}
		peer := self.ring.Successors(self.pos, common.NBackups)[i]
		ops, err := self.storage.Sync(storageTransport.RPCTransport(peer.ConnectionString), r, maintenanceChunkSize)
		if err != nil {
			return
		}
		acted = ops > 0
		log.Debugf("%v synced %v values within %v with %v\n", self, ops, r, peer)
		return
	}); err != nil {
		return
	}
	return
}

func (self *Node) offer(data [][2][]byte) (err error) {
	for _, kv := range data {
		key := kv[0]
		value := storage.Value(kv[1])
		vRTS := value.ReadTimestamp()
		vWTS := value.WriteTimestamp()
		peer := self.ring.Successors(key, 1)[0]
		var current []byte
		if err = switchboard.Switch.Call(peer.ConnectionString, "Synchronizable.Get", key, &current); err != nil {
			return
		}
		curRTS := int64(-1)
		curWTS := int64(-1)
		if current != nil {
			curRTS = storage.Value(current).ReadTimestamp()
			curWTS = storage.Value(current).WriteTimestamp()
		}
		if curRTS < vRTS && curWTS > vWTS {
			log.Fatalf("%v has greater read timestamp but less write timestamp than %v", value, current)
		}
		if vRTS > curRTS || vWTS > curWTS {
			if err = switchboard.Switch.Call(peer.ConnectionString, "Synchronizable.Put", kv, nil); err != nil {
				return
			}
		}
		if err = self.storage.Delete(key); err != nil {
			return
		}
	}
	return
}

func (self *Node) cleanOnce() (acted bool, err error) {
	if err = self.WhileRunning(func() (err error) {
		toOffer := [][2][]byte{}
		var preds ring.Peers
		if err = self.storage.View(func(bucket *bolt.Bucket) (err error) {
			preds = self.ring.Predecessors(self.pos, common.NBackups+1)
			cursor := bucket.Cursor()
			for k, v := cursor.Seek(self.pos); len(toOffer) < maintenanceChunkSize && k != nil && (bytes.Compare(self.pos, k) <= 0 || bytes.Compare(preds[0].Pos, k) > 0); k, v = cursor.Next() {
				toOffer = append(toOffer, [2][]byte{k, v})
			}
			for k, v := cursor.First(); len(toOffer) < maintenanceChunkSize && k != nil && (bytes.Compare(self.pos, k) <= 0 || bytes.Compare(preds[0].Pos, k) > 0); k, v = cursor.Next() {
				toOffer = append(toOffer, [2][]byte{k, v})
			}
			return
		}); err != nil {
			return
		}
		if err = self.offer(toOffer); err != nil {
			return
		}
		log.Debugf("%v cleaned %v values outside %v\n", self, len(toOffer), storage.Range{preds[0].Pos, self.pos})
		acted = len(toOffer) > 0
		return
	}); err != nil {
		return
	}
	return
}

func (self *Node) clean() {
	stops := atomic.LoadUint64(&self.stops)
	var acted bool
	var err error
	for acted, err = self.cleanOnce(); err == nil && acted && atomic.LoadUint64(&self.stops) == stops; acted, err = self.cleanOnce() {
	}
	if err != nil {
		log.Warnf("While trying to clean: %v", err)
	}
}

func (self *Node) synchronize() {
	stops := atomic.LoadUint64(&self.stops)
	for i := 0; i < common.NBackups; i++ {
		var acted bool
		var err error
		for acted, err = self.syncOnceWith(i); err == nil && acted && atomic.LoadUint64(&self.stops) == stops; acted, err = self.syncOnceWith(i) {
		}
		if err != nil {
			log.Warnf("While trying to sync with backup %v: %v", i, err)
		}
	}
}

func (self *Node) String() string {
	return self.AsPeer().String()
}

func (self *Node) AsPeer() (result *ring.Peer) {
	return &ring.Peer{
		Name:             self.raft.Name(),
		Pos:              self.pos,
		ConnectionString: self.server.Addr(),
	}
}

func (self *Node) selectPos() (err error) {
	if err = self.metadata.Update(func(tx *bolt.Tx) (err error) {
		bucket, err := tx.CreateBucketIfNotExists(metadataBucketKey)
		if err != nil {
			return
		}
		self.pos = bucket.Get(posKey)
		if self.pos == nil {
			self.pos = ring.RandomPos(1)
			if err = bucket.Put(posKey, self.pos); err != nil {
				return
			}
		}
		return
	}); err != nil {
		return
	}
	return
}

func (self *Node) setupRaft() (err error) {
	logdir := filepath.Join(self.dir, "raft.log")
	if err = os.RemoveAll(logdir); err != nil {
		return
	}
	if err = os.MkdirAll(logdir, 0700); err != nil {
		return
	}
	rpcTransport := &raftTransport.RPCTransport{
		Stopper: self,
	}
	if self.raft, err = raft.NewServer(fmt.Sprintf("%08x", rand.Int63()), logdir, rpcTransport, self, self, self.server.Addr()); err != nil {
		return
	}
	rpcTransport.Raft = self.raft
	return
}

func (self *Node) setupPersistence() (err error) {
	if err = os.MkdirAll(self.dir, 0700); err != nil {
		return
	}
	if self.storage, err = storage.New(filepath.Join(self.dir, "storage.db")); err != nil {
		return
	}
	self.transactor = transactor.New(self.storage)
	if self.metadata, err = bolt.Open(filepath.Join(self.dir, "metadata.db"), 0700, nil); err != nil {
		return
	}
	return
}

func (self *Node) startServing() (err error) {
	self.server.Serve("Raft", &raftTransport.RPCServer{
		Raft: self.raft,
	})
	self.server.Serve("Synchronizable", &storageTransport.RPCServer{
		Storage: self.storage,
	})
	self.server.Serve("Node", &RPCServer{
		node: self,
	})
	self.server.Serve("TX", &transactorTransport.RPCServer{
		Transactor: self.transactor,
	})
	self.server.Serve("Debug", &DebugRPCServer{
		node: self,
	})
	if err = self.raft.Start(); err != nil {
		return
	}
	return
}

func (self *Node) lead() (err error) {
	if _, err = self.raft.Do(&raft.DefaultJoinCommand{
		Name:             self.raft.Name(),
		ConnectionString: self.server.Addr(),
	}); err != nil {
		return
	}
	if _, err = self.raft.Do(&commands.AddPeerCommand{
		Peer: self.AsPeer(),
	}); err != nil {
		return
	}
	log.Infof("%v is cluster leader", self)
	return
}

func (self *Node) join(leader string) (err error) {
	joinResp := &raftTransport.JoinResponse{}
	if err = switchboard.Switch.Call(leader, "Raft.Join", &raft.DefaultJoinCommand{
		Name:             self.raft.Name(),
		ConnectionString: self.server.Addr(),
	}, joinResp); err != nil {
		return
	}
	master := &ring.Peer{}
	if err = switchboard.Switch.Call(leader, "Node.AddPeer", self.AsPeer(), master); err != nil {
		return
	}
	log.Infof("%v joined %v", self, master)
	return
}

func (self *Node) Start(join string) (err error) {
	if self.raft != nil {
		err = fmt.Errorf("Node is already started")
		return
	}
	if err = self.setupPersistence(); err != nil {
		return
	}
	if err = self.selectPos(); err != nil {
		return
	}
	if err = self.setupRaft(); err != nil {
		return
	}
	if err = self.startServing(); err != nil {
		return
	}
	if join == "" {
		if err = self.lead(); err != nil {
			return
		}
	} else {
		if err = self.join(join); err != nil {
			return
		}
	}
	return
}
