package consensual

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/goraft/raft"
	"github.com/zond/drafty/consensual/commands"
	"github.com/zond/drafty/log"
	"github.com/zond/drafty/raft/transport"
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
	server *switchboard.Server
	dir    string
	raft   raft.Server
	name   string
}

func New(name string, addr string, dir string) (result *Node, err error) {
	result = &Node{
		server: switchboard.NewServer(addr),
		name:   name,
		dir:    dir,
	}
	if err = os.MkdirAll(result.dir, 0700); err != nil {
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
	log.Infof("STOP, HAMMERTIME!\tWe have %v peers.", len(self.raft.Peers()))
	return
}

func (self *Node) Continue() (err error) {
	log.Infof("CONTINUE!\tWe have %v peers.", len(self.raft.Peers()))
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

func (self *Node) Start(join string) (err error) {
	if self.raft != nil {
		err = fmt.Errorf("Node is already started")
		return
	}
	logdir := filepath.Join(self.dir, "raft.log")
	if err = os.MkdirAll(logdir, 0700); err != nil {
		return
	}
	rpcTransport := &transport.RPCTransport{
		Stopper: self,
	}
	if self.raft, err = raft.NewServer(self.name, logdir, rpcTransport, self, self, self.server.Addr()); err != nil {
		return
	}
	rpcTransport.Raft = self.raft
	self.server.Serve("Raft", &transport.RPC{
		Stopper: self,
		Raft:    self.raft,
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
			log.Infof("%v is cluster leader", self.raft.Name())
		} else {
			log.Infof("%v recovered log", self.raft.Name())
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
		log.Infof("%v joined %v", self.raft.Name(), resp.Name)
		return
	}
	return
}
