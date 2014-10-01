package node

import (
	"bytes"
	"encoding/gob"
	"encoding/hex"
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
	nodeTransport "github.com/zond/drafty/node/transport"
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
}

var posKey = []byte("pos")
var metadataBucketKey = []byte("metadata")

type Node struct {
	pos        []byte
	server     *switchboard.Server
	dir        string
	raft       raft.Server
	ring       *ring.Ring
	transactor *transactor.Transactor
	stops      uint64
	stopped    int32
	stopLock   sync.RWMutex
	maintLock  sync.Mutex
	storage    *storage.DB
	metadata   *bolt.DB
}

func New(addr string, dir string) (result *Node, err error) {
	result = &Node{
		server: switchboard.NewServer(addr),
		dir:    dir,
		ring:   ring.New(),
	}
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
	atomic.StoreInt32(&self.stopped, 1)
	atomic.AddUint64(&self.stops, 1)
	log.Debugf("%v stopped", self)
	return
}

func (self *Node) Continue(r *ring.Ring) (err error) {
	self.ring = r
	if atomic.LoadInt32(&self.stopped) == 1 {
		self.stopLock.Unlock()
		atomic.StoreInt32(&self.stopped, 0)
	}
	go self.cleanAndSync()
	log.Debugf("%v continued with ring %v", self, self.ring)
	return
}

func (self *Node) Dump() (result string, err error) {
	if err = self.WhileRunning(func() (err error) {
		result = self.storage.PPStrings()
		return
	}); err != nil {
		return
	}
	return
}

func (self *Node) WhileRunning(f func() error) error {
	self.stopLock.RLock()
	defer self.stopLock.RUnlock()
	return f()
}

func (self *Node) updateRing(gen func(*ring.Ring) *ring.Ring) (err error) {
	p := &common.Parallelizer{}
	self.ring.Each(func(peer *ring.Peer) {
		p.Start(func() error { return peer.Call("Node.Stop", struct{}{}, nil) })
	})
	if err = p.Wait(); err != nil {
		return
	}
	newRing := gen(self.ring)
	p = &common.Parallelizer{}
	newRing.Each(func(peer *ring.Peer) {
		p.Start(func() error { return peer.Call("Node.Continue", newRing, nil) })
	})
	if err = p.Wait(); err != nil {
		return
	}
	return
}

func (self *Node) LeaderForward(method string, input interface{}, output interface{}) (forwarded bool, err error) {
	if self.raft.Name() == self.raft.Leader() {
		return
	}
	forwarded = true
	err = switchboard.Switch.Call(self.raft.Peers()[self.raft.Leader()].ConnectionString, method, input, output)
	return
}

func (self *Node) Name() string {
	return self.raft.Name()
}

func (self *Node) RaftDo(cmd raft.Command) (result interface{}, err error) {
	return self.raft.Do(cmd)
}

func (self *Node) Ring() (result *ring.Ring, err error) {
	if err = self.WhileRunning(func() (err error) {
		result = self.ring
		return
	}); err != nil {
		return
	}
	return
}

func (self *Node) AddPeer(peer *ring.Peer) (err error) {
	return self.updateRing(func(r *ring.Ring) (result *ring.Ring) {
		result = self.ring.Clone()
		result.AddPeer(peer)
		return
	})
}

func (self *Node) RemovePeer(name string) (err error) {
	return self.updateRing(func(r *ring.Ring) (result *ring.Ring) {
		result = self.ring.Clone()
		result.RemovePeer(name)
		return
	})
}

func (self *Node) offer(data [][2][]byte) (err error) {
	for _, kv := range data {
		key := kv[0]
		value := storage.Value(kv[1])
		vRTS := value.ReadTimestamp()
		for _, peer := range self.ring.Successors(key, common.NBackups+1) {
			var current []byte
			if err = peer.Call("Synchronizable.Get", key, &current); err != nil {
				return
			}
			curRTS := int64(-1)
			if current != nil {
				curRTS = storage.Value(current).ReadTimestamp()
			}
			if vRTS > curRTS {
				if err = peer.Call("Synchronizable.Put", &storageTransport.PutRequest{
					Logs:  fmt.Sprintf("%v offering to %v", hex.EncodeToString(self.pos), hex.EncodeToString(peer.Pos)),
					Key:   key,
					Value: value,
				}, nil); err != nil {
					return
				}
			}
		}
		if err = self.storage.Delete(key); err != nil {
			return
		}
	}
	return
}

func (self *Node) cleanOnce(stops uint64) (acted bool, err error) {
	if err = self.WhileRunning(func() (err error) {
		if atomic.LoadUint64(&self.stops) != stops {
			return
		}
		toOffer := [][2][]byte{}
		var preds ring.Peers
		preds = self.ring.Predecessors(self.pos, common.NBackups+1)
		ranges := storage.Ranges{}
		for i := 0; i < len(preds)-1; i++ {
			ranges = append(ranges, storage.Range{
				FromInc: preds[i+1].Pos,
				ToExc:   preds[i].Pos,
			})
		}
		ranges = append(ranges, storage.Range{
			FromInc: preds[0].Pos,
			ToExc:   self.pos,
		})
		log.Tracef("%v cleaning outside of %v", hex.EncodeToString(self.pos), ranges)
		if err = self.storage.View(func(bucket *bolt.Bucket) (err error) {
			cursor := bucket.Cursor()
			for k, v := cursor.Seek(self.pos); len(toOffer) < maintenanceChunkSize && k != nil && !ranges.Within(k); k, v = cursor.Next() {
				log.Tracef("%v cleaning: %v => %v does NOT belong here\n", hex.EncodeToString(self.pos), hex.EncodeToString(k), v)
				toOffer = append(toOffer, [2][]byte{
					append([]byte{}, k...),
					append([]byte{}, v...),
				})
			}
			for k, v := cursor.First(); bytes.Compare(k, self.pos) < 0 && len(toOffer) < maintenanceChunkSize && k != nil && !ranges.Within(k); k, v = cursor.Next() {
				log.Tracef("%v cleaning: %v => %v does NOT belong here\n", hex.EncodeToString(self.pos), hex.EncodeToString(k), v)
				toOffer = append(toOffer, [2][]byte{
					append([]byte{}, k...),
					append([]byte{}, v...),
				})
			}
			return
		}); err != nil {
			return
		}
		if err = self.offer(toOffer); err != nil {
			return
		}
		log.Debugf("%v cleaned %v values outside %v\n", self, len(toOffer), ranges)
		acted = len(toOffer) > 0
		return
	}); err != nil {
		return
	}
	return
}

func (self *Node) cleanAndSync() {
	self.maintLock.Lock()
	defer self.maintLock.Unlock()
	stops := atomic.LoadUint64(&self.stops)
	var acted bool
	var err error
	for acted, err = self.cleanOnce(stops); err == nil && acted && atomic.LoadUint64(&self.stops) == stops; acted, err = self.cleanOnce(stops) {
	}
	if err != nil {
		log.Warnf("While trying to clean: %v", err)
	}
	anyAction := true
	for anyAction && atomic.LoadUint64(&self.stops) == stops {
		anyAction = false
		for i := 0; i < common.NBackups; i++ {
			for acted, err = self.syncOnceWith(i, stops); err == nil && acted && atomic.LoadUint64(&self.stops) == stops; acted, err = self.syncOnceWith(i, stops) {
				anyAction = true
			}
			if err != nil {
				log.Warnf("While trying to sync with backup %v: %v", i, err)
			}
		}
	}
}

func (self *Node) syncOnceWith(i int, stops uint64) (acted bool, err error) {
	if err = self.WhileRunning(func() (err error) {
		if atomic.LoadUint64(&self.stops) != stops {
			return
		}
		r := storage.Range{
			FromInc: self.ring.Predecessors(self.pos, 1)[0].Pos,
			ToExc:   self.pos,
		}
		peer := self.ring.Successors(self.pos, common.NBackups)[i]
		ops, err := self.storage.Sync(storageTransport.RPCTransport(peer.ConnectionString), r, maintenanceChunkSize, fmt.Sprintf("%v syncing with %v", hex.EncodeToString(self.pos), hex.EncodeToString(peer.Pos)))
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
		PeerRemover: self,
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
	self.server.Serve("Node", &nodeTransport.RPCServer{
		Controllable: self,
	})
	self.transactor = transactor.New(&transactorBackend{node: self})
	self.server.Serve("TX", &transactorTransport.RPCServer{
		Transactor: self.transactor,
	})
	self.server.Serve("Debug", &nodeTransport.DebugRPCServer{
		Debuggable: self,
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
	self.ring.AddPeer(self.AsPeer())
	log.Infof("%v is cluster leader", self)
	return
}

func (self *Node) join(leader string) (err error) {
	joinResp := &nodeTransport.JoinResponse{}
	if err = switchboard.Switch.Call(leader, "Node.Join", &nodeTransport.JoinRequest{
		RaftJoinCommand: &raft.DefaultJoinCommand{
			Name:             self.raft.Name(),
			ConnectionString: self.server.Addr(),
		},
		Pos: self.pos,
	}, joinResp); err != nil {
		return
	}
	log.Infof("%v joined %v", self.raft.Name(), joinResp.Name)
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
