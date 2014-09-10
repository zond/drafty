package node

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/boltdb/bolt"
	"github.com/goraft/raft"
	"github.com/zond/drafty/log"
	"github.com/zond/drafty/raft/transport"
	"github.com/zond/drafty/switchboard"
)

var metadataBucketKey = []byte("metadata")
var nameKey = []byte("name")

func init() {
	rand.Seed(time.Now().UnixNano())
}

type Node struct {
	server *switchboard.Server
	dir    string
	db     *bolt.DB
	raft   raft.Server
	name   string
}

func New(addr string, dir string) (result *Node, err error) {
	result = &Node{
		server: switchboard.NewServer(addr),
		dir:    dir,
	}
	if err = os.MkdirAll(result.dir, 0700); err != nil {
		return
	}
	if result.db, err = bolt.Open(filepath.Join(result.dir, "node.db"), 0600, nil); err != nil {
		return
	}
	if err = result.db.Update(func(tx *bolt.Tx) (err error) {
		metaBucket, err := tx.CreateBucketIfNotExists(metadataBucketKey)
		if err != nil {
			return
		}
		result.name = string(metaBucket.Get(nameKey))
		if result.name == "" {
			result.name = fmt.Sprintf("%07x", rand.Int())[0:7]
			if err = metaBucket.Put(nameKey, []byte(result.name)); err != nil {
				return
			}
		}
		return
	}); err != nil {
		return
	}
	return
}

func (self *Node) Start(join string) (err error) {
	if self.raft != nil {
		err = fmt.Errorf("Node is already started")
		return
	}
	logdir := filepath.Join(self.dir, "raft.log")
	if err = os.MkdirAll(logdir, 0700); err != nil {
		return
	}
	if self.raft, err = raft.NewServer(self.name, logdir, transport.RPCTransport{}, nil, nil, self.server.Addr()); err != nil {
		return
	}
	self.server.Serve("Raft", &transport.RPC{
		Raft: self.raft,
	})
	if err = self.raft.Start(); err != nil {
		return
	}
	if join == "" {
		if self.raft.IsLogEmpty() {
			if _, err = self.raft.Do(&raft.DefaultJoinCommand{
				Name:             self.raft.Name(),
				ConnectionString: self.server.Addr(),
			}); err != nil {
				return
			}
			log.Debugf("%v is cluster leader", self.raft.Name())
		} else {
			log.Debugf("%v recovered log", self.raft.Name())
		}
	} else {
		if !self.raft.IsLogEmpty() {
			err = fmt.Errorf("Cannot join with an existing log")
			return
		}
		resp := &transport.JoinResponse{}
		if err = switchboard.Switch.Call(join, "Raft.Join", &raft.DefaultJoinCommand{
			Name:             self.name,
			ConnectionString: self.server.Addr(),
		}, resp); err != nil {
			return
		}
		log.Debugf("%v joined %v", self.raft.Name(), resp.Name)
		return
	}
	return
}
