package peer

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
	"github.com/zond/drafty/peer/messages"
	"github.com/zond/drafty/peer/ring"
	raftTransport "github.com/zond/drafty/raft/transport"
	"github.com/zond/drafty/storage"
	storageMessages "github.com/zond/drafty/storage/messages"
	storageTransport "github.com/zond/drafty/storage/transport"
	"github.com/zond/drafty/switchboard"
)

const (
	maintenanceChunkSize = 16
	NBackups             = 2
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var posKey = []byte("pos")
var metadataBucketKey = []byte("metadata")

type Peer struct {
	pos       []byte
	dir       string
	addr      string
	raft      raft.Server
	ring      *ring.Ring
	stops     uint64
	stopped   int32
	stopLock  sync.RWMutex
	maintLock sync.Mutex
	storage   *storage.DB
	metadata  *bolt.DB
}

func New(addr string, dir string) (result *Peer, err error) {
	result = &Peer{
		dir:  dir,
		ring: ring.New(),
		addr: addr,
	}
	if err = result.setupPersistence(); err != nil {
		return
	}
	if err = result.selectPos(); err != nil {
		return
	}
	if err = result.setupRaft(); err != nil {
		return
	}
	return
}

func (self *Peer) setupPersistence() (err error) {
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

func (self *Peer) selectPos() (err error) {
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

func (self *Peer) setupRaft() (err error) {
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
	if self.raft, err = raft.NewServer(fmt.Sprintf("%08x", rand.Int63()), logdir, rpcTransport, self, self, self.addr); err != nil {
		return
	}
	rpcTransport.Raft = self.raft
	return
}

type peerState struct {
	Ring    *ring.Ring
	Stopped int32
}

func (self *Peer) Save() (b []byte, err error) {
	log.Debugf("Compacting log")
	buf := &bytes.Buffer{}
	if err = gob.NewEncoder(buf).Encode(peerState{
		Ring:    self.ring,
		Stopped: atomic.LoadInt32(&self.stopped),
	}); err != nil {
		return
	}
	return
}

func (self *Peer) Storage() *storage.DB {
	return self.storage
}

func (self *Peer) Raft() raft.Server {
	return self.raft
}

func (self *Peer) Recovery(b []byte) (err error) {
	log.Debugf("Recovering from log")
	state := &peerState{}
	if err = gob.NewDecoder(bytes.NewBuffer(b)).Decode(state); err != nil {
		return
	}
	self.ring = state.Ring
	atomic.StoreInt32(&self.stopped, state.Stopped)
	return
}

func (self *Peer) Stop() (err error) {
	self.stopLock.Lock()
	atomic.StoreInt32(&self.stopped, 1)
	atomic.AddUint64(&self.stops, 1)
	log.Debugf("%v stopped", self)
	return
}

func (self *Peer) Continue(r *ring.Ring) (err error) {
	self.ring = r
	if atomic.LoadInt32(&self.stopped) == 1 {
		self.stopLock.Unlock()
		atomic.StoreInt32(&self.stopped, 0)
	}
	go self.cleanAndSync()
	log.Debugf("%v continued with ring %v", self, self.ring)
	return
}

func (self *Peer) Dump() (result string, err error) {
	if err = self.WhileRunning(func() (err error) {
		result = self.storage.PPStrings()
		return
	}); err != nil {
		return
	}
	return
}

func (self *Peer) WhileRunning(f func() error) error {
	self.stopLock.RLock()
	defer self.stopLock.RUnlock()
	return f()
}

func (self *Peer) updateRing(gen func(*ring.Ring) *ring.Ring) (err error) {
	p := &common.Parallelizer{}
	self.ring.Each(func(peer *ring.Peer) {
		p.Start(func() error { return peer.Call("Peer.Stop", struct{}{}, nil) })
	})
	if err = p.Wait(); err != nil {
		return
	}
	newRing := gen(self.ring)
	p = &common.Parallelizer{}
	newRing.Each(func(peer *ring.Peer) {
		p.Start(func() error { return peer.Call("Peer.Continue", newRing, nil) })
	})
	if err = p.Wait(); err != nil {
		return
	}
	return
}

func (self *Peer) LeaderForward(method string, input interface{}, output interface{}) (forwarded bool, err error) {
	if self.raft.Name() == self.raft.Leader() {
		return
	}
	forwarded = true
	err = switchboard.Switch.Call(self.raft.Peers()[self.raft.Leader()].ConnectionString, method, input, output)
	return
}

func (self *Peer) Name() string {
	return self.raft.Name()
}

func (self *Peer) RaftDo(cmd raft.Command) (result interface{}, err error) {
	return self.raft.Do(cmd)
}

func (self *Peer) Ring() *ring.Ring {
	return self.ring.Clone()
}

func (self *Peer) Pos() (result []byte) {
	result = make([]byte, len(self.pos))
	copy(result, self.pos)
	return
}

func (self *Peer) AddPeer(peer *ring.Peer) (err error) {
	return self.updateRing(func(r *ring.Ring) (result *ring.Ring) {
		result = self.ring.Clone()
		result.AddPeer(peer)
		return
	})
}

func (self *Peer) RemovePeer(name string) (err error) {
	return self.updateRing(func(r *ring.Ring) (result *ring.Ring) {
		result = self.ring.Clone()
		result.RemovePeer(name)
		return
	})
}

func (self *Peer) offer(data [][2][]byte) (err error) {
	for _, kv := range data {
		key := kv[0]
		value := storage.Value(kv[1])
		vRTS := value.ReadTimestamp()
		for _, peer := range self.ring.Successors(key, NBackups+1) {
			var current []byte
			if err = peer.Call("Synchronizable.Get", key, &current); err != nil {
				return
			}
			curRTS := int64(-1)
			if current != nil {
				curRTS = storage.Value(current).ReadTimestamp()
			}
			if vRTS > curRTS {
				if err = peer.Call("Synchronizable.Put", &storageMessages.PutRequest{
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

func (self *Peer) cleanOnce(stops uint64) (acted bool, err error) {
	if err = self.WhileRunning(func() (err error) {
		if atomic.LoadUint64(&self.stops) != stops {
			return
		}
		toOffer := [][2][]byte{}
		var preds ring.Peers
		preds = self.ring.Predecessors(self.pos, NBackups+1)
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

func (self *Peer) cleanAndSync() {
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
		for i := 0; i < NBackups; i++ {
			for acted, err = self.syncOnceWith(i, stops); err == nil && acted && atomic.LoadUint64(&self.stops) == stops; acted, err = self.syncOnceWith(i, stops) {
				anyAction = true
			}
			if err != nil {
				log.Warnf("While trying to sync with backup %v: %v", i, err)
			}
		}
	}
}

func (self *Peer) syncOnceWith(i int, stops uint64) (acted bool, err error) {
	if err = self.WhileRunning(func() (err error) {
		if atomic.LoadUint64(&self.stops) != stops {
			return
		}
		r := storage.Range{
			FromInc: self.ring.Predecessors(self.pos, 1)[0].Pos,
			ToExc:   self.pos,
		}
		peer := self.ring.Successors(self.pos, NBackups)[i]
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

func (self *Peer) String() string {
	return self.AsPeer().String()
}

func (self *Peer) AsPeer() (result *ring.Peer) {
	return &ring.Peer{
		Name:             self.raft.Name(),
		Pos:              self.pos,
		ConnectionString: self.addr,
	}
}

func (self *Peer) lead() (err error) {
	if _, err = self.raft.Do(&raft.DefaultJoinCommand{
		Name:             self.raft.Name(),
		ConnectionString: self.addr,
	}); err != nil {
		return
	}
	self.ring.AddPeer(self.AsPeer())
	log.Infof("%v is cluster leader", self)
	return
}

func (self *Peer) join(leader string) (err error) {
	joinResp := &messages.JoinResponse{}
	if err = switchboard.Switch.Call(leader, "Peer.Join", &messages.JoinRequest{
		RaftJoinCommand: &raft.DefaultJoinCommand{
			Name:             self.raft.Name(),
			ConnectionString: self.addr,
		},
		Pos: self.pos,
	}, joinResp); err != nil {
		return
	}
	log.Infof("%v joined %v", self.raft.Name(), joinResp.Name)
	return
}

func (self *Peer) Start(join string) (err error) {
	if err = self.raft.Start(); err != nil {
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
