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

	"github.com/goraft/raft"
	"github.com/zond/drafty/log"
	"github.com/zond/drafty/node/commands"
	"github.com/zond/drafty/node/ring"
	raftTransport "github.com/zond/drafty/raft/transport"
	"github.com/zond/drafty/storage"
	storageTransport "github.com/zond/drafty/storage/transport"
	"github.com/zond/drafty/switchboard"
)

var metadataBucketKey = []byte("metadata")

func init() {
	rand.Seed(time.Now().UnixNano())
	raft.RegisterCommand(&commands.StopCommand{})
	raft.RegisterCommand(&commands.ContCommand{})
	raft.RegisterCommand(&commands.AddPeerCommand{})
	raft.RegisterCommand(&commands.RemovePeerCommand{})
}

type Node struct {
	pos            []byte
	server         *switchboard.Server
	dir            string
	raft           raft.Server
	ring           *ring.Ring
	stopped        int32
	stops          uint64
	stopLock       sync.Mutex
	stopCond       *sync.Cond
	procsDoneGroup sync.WaitGroup
	storage        storage.DB
}

func New(addr string, dir string) (result *Node, err error) {
	result = &Node{
		server: switchboard.NewServer(addr),
		dir:    dir,
		ring:   ring.New(),
	}
	result.stopCond = sync.NewCond(&result.stopLock)
	if err = os.MkdirAll(result.dir, 0700); err != nil {
		return
	}
	if result.storage, err = storage.New(filepath.Join(result.dir, "storage.db")); err != nil {
		return
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
	log.Debugf("%v continues", self)
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

func (self *Node) successor() (result *ring.Peer) {
	return self.ring.Successors(self.pos, 1)[0]
}

func (self *Node) responsibility() (result storage.Range) {
	result.FromInc = self.pos
	result.ToExc = []byte(self.successor().Pos)
	return
}

func (self *Node) sync(peer *ring.Peer, r storage.Range) (acted bool, err error) {
	if err = self.WhileRunning(func() (err error) {
		ops, err := self.storage.Sync(storageTransport.RPCTransport(peer.ConnectionString), r, 1)
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

func (self *Node) backups() (result ring.Peers) {
	return self.ring.Successors(self.pos, 3)
}

func (self *Node) syncBackups() {
	r := self.responsibility()
	stops := atomic.LoadUint64(&self.stops)
	for _, backup := range self.backups() {
		for acted, err := self.sync(backup, r); err == nil && acted && atomic.LoadUint64(&self.stops) == stops; acted, err = self.sync(backup, r) {
		}
	}
}

func (self *Node) synchronize() {
	self.syncBackups()
}

func (self *Node) randomPos() (result []byte) {
	i := rand.Int63()
	result = []byte{
		byte(i >> 7),
		byte(i >> 6),
		byte(i >> 5),
		byte(i >> 4),
		byte(i >> 3),
		byte(i >> 2),
		byte(i >> 1),
		byte(i >> 0),
	}
	return
}

func (self *Node) AsPeer() (result *ring.Peer) {
	return &ring.Peer{
		Name:             self.raft.Name(),
		Pos:              self.pos,
		ConnectionString: self.server.Addr(),
	}
}

func (self *Node) Start(join string) (err error) {
	if self.raft != nil {
		err = fmt.Errorf("Node is already started")
		return
	}
	self.pos = self.randomPos()
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
	self.server.Serve("Raft", &raftTransport.RPCServer{
		Raft: self.raft,
	})
	self.server.Serve("Synchronizable", &storageTransport.RPCServer{
		Storage: self.storage,
	})
	self.server.Serve("Node", &RPCServer{
		node: self,
	})
	if err = self.raft.Start(); err != nil {
		return
	}
	if join == "" {
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
	} else {
		joinResp := &raftTransport.JoinResponse{}
		if err = switchboard.Switch.Call(join, "Raft.Join", &raft.DefaultJoinCommand{
			Name:             self.raft.Name(),
			ConnectionString: self.server.Addr(),
		}, joinResp); err != nil {
			return
		}
		master := &ring.Peer{}
		if err = switchboard.Switch.Call(join, "Node.AddPeer", self.AsPeer(), master); err != nil {
			return
		}
		log.Infof("%v joined %v", self, master)
		return
	}
	return
}
