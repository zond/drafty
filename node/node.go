package node

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goraft/raft"
	"github.com/zond/drafty/log"
	"github.com/zond/drafty/node/commands"
	"github.com/zond/drafty/raft/transport"
	"github.com/zond/drafty/storage"
	"github.com/zond/drafty/switchboard"
)

var metadataBucketKey = []byte("metadata")
var nameKey = []byte("name")

func init() {
	rand.Seed(time.Now().UnixNano())
	raft.RegisterCommand(&commands.StopCommand{})
	raft.RegisterCommand(&commands.ContCommand{})
}

type Node struct {
	server         *switchboard.Server
	dir            string
	raft           raft.Server
	stopped        int32
	stops          uint64
	stoppedGroup   sync.WaitGroup
	procsDoneGroup sync.WaitGroup
	storage        storage.DB
}

func New(addr string, dir string) (result *Node, err error) {
	result = &Node{
		server: switchboard.NewServer(addr),
		dir:    dir,
	}
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

func (self *Node) Save() (b []byte, err error) {
	log.Debugf("Compacting log")
	return
}

func (self *Node) Recovery(b []byte) (err error) {
	log.Debugf("Recovering from log")
	return
}

func (self *Node) Stop() (err error) {
	self.stoppedGroup.Add(1)
	atomic.AddUint64(&self.stops, 1)
	self.procsDoneGroup.Wait()
	return
}

func (self *Node) Continue() (err error) {
	self.stoppedGroup.Done()
	go self.synchronize()
	return
}

type ring []*raft.Peer

func (self ring) Len() int {
	return len(self)
}

func (self ring) Less(i, j int) bool {
	return self[i].Name < self[j].Name
}

func (self ring) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

func (self ring) successors(name string, n int) (result ring) {
	result = make(ring, 0, n)
	for i := sort.Search(len(self), func(p int) bool { return self[p].Name > name }); len(result) < n; i++ {
		result = append(result, self[i%len(self)])
	}
	return
}

func (self *Node) ring() (result ring) {
	peers := self.raft.Peers()
	result = make(ring, 0, 1+len(peers))
	result = append(result, &raft.Peer{
		Name:             self.raft.Name(),
		ConnectionString: self.server.Addr(),
	})
	for _, peer := range peers {
		result = append(result, peer)
	}
	sort.Sort(result)
	return
}

func (self *Node) successor() (result *raft.Peer) {
	return self.ring().successors(self.raft.Name(), 1)[0]
}

func (self *Node) responsibility() (result storage.Range) {
	result.FromInc = []byte(self.raft.Name())
	result.ToExc = []byte(self.successor().Name)
	return
}

func (self *Node) sync(peer *raft.Peer, r storage.Range) (acted bool, err error) {
	log.Debugf("%v syncing %v with %v\n", self.name(), r, hex.EncodeToString([]byte(peer.Name)))
	return
}

func (self *Node) backups() (result ring) {
	return self.ring().successors(self.raft.Name(), 3)
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

func (self *Node) randomName() (result string) {
	i := rand.Int63()
	b, err := json.Marshal(string([]byte{
		byte(i >> 7),
		byte(i >> 6),
		byte(i >> 5),
		byte(i >> 4),
		byte(i >> 3),
		byte(i >> 2),
		byte(i >> 1),
		byte(i >> 0),
	}))
	if err != nil {
		return
	}
	if err = json.Unmarshal(b, &result); err != nil {
		return
	}
	return
}

func (self *Node) name() string {
	return hex.EncodeToString([]byte(self.raft.Name()))
}

func (self *Node) Start(join string) (err error) {
	if self.raft != nil {
		err = fmt.Errorf("Node is already started")
		return
	}
	name := self.randomName()
	logdir := filepath.Join(self.dir, "raft.log")
	if err = os.RemoveAll(logdir); err != nil {
		return
	}
	if err = os.MkdirAll(logdir, 0700); err != nil {
		return
	}
	rpcTransport := &transport.RPCTransport{
		Stopper: self,
	}
	if self.raft, err = raft.NewServer(name, logdir, rpcTransport, self, self, self.server.Addr()); err != nil {
		return
	}
	rpcTransport.Raft = self.raft
	self.server.Serve("Raft", &transport.RPC{
		Stopper: self,
		Raft:    self.raft,
	})
	self.server.Serve("Synchronizable", &storage.RPC{
		Storage: self.storage,
	})
	if err = self.raft.Start(); err != nil {
		return
	}
	if join == "" {
		cmd := &raft.DefaultJoinCommand{
			Name:             self.raft.Name(),
			ConnectionString: self.server.Addr(),
		}
		if _, err = self.raft.Do(cmd); err != nil {
			return
		}
		log.Infof("%v is cluster leader", self.name())
	} else {
		resp := &transport.JoinResponse{}
		if err = switchboard.Switch.Call(join, "Raft.Join", &raft.DefaultJoinCommand{
			Name:             self.raft.Name(),
			ConnectionString: self.server.Addr(),
		}, resp); err != nil {
			return
		}
		log.Infof("%v joined %#v", self.name(), hex.EncodeToString([]byte(resp.Name)))
		return
	}
	return
}
