package switchboard

import (
	"net"
	"net/rpc"
	"sync"

	"github.com/zond/drafty/log"
)

var Switch = newSwitchboard()

type Server struct {
	addr      string
	rpcServer *rpc.Server
}

func NewServer(addr string) *Server {
	return &Server{
		addr: addr,
	}
}

func (self *Server) Addr() string {
	return self.addr
}

func (self *Server) start() (err error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", self.addr)
	if err != nil {
		return
	}
	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return
	}
	self.rpcServer = rpc.NewServer()
	go self.rpcServer.Accept(listener)
	log.Infof("Listening to %#v", self.addr)
	return
}

func (self *Server) Serve(name string, service interface{}) (err error) {
	if self.rpcServer == nil {
		if err = self.start(); err != nil {
			return
		}
	}
	if err = self.rpcServer.RegisterName(name, service); err != nil {
		return
	}
	return
}

type Switchboard struct {
	lock    *sync.RWMutex
	clients map[string]*rpc.Client
}

func newSwitchboard() *Switchboard {
	return &Switchboard{
		lock:    new(sync.RWMutex),
		clients: make(map[string]*rpc.Client),
	}
}

func (self *Switchboard) client(addr string) (client *rpc.Client, err error) {
	self.lock.RLock()
	client, ok := self.clients[addr]
	self.lock.RUnlock()
	if !ok {
		if client, err = rpc.Dial("tcp", addr); err != nil {
			return
		}
		self.lock.Lock()
		self.clients[addr] = client
		self.lock.Unlock()
	}
	return
}

func (self *Switchboard) Call(addr, service string, args, reply interface{}) (err error) {
	client, err := self.client(addr)
	if err != nil {
		return
	}
	for err = client.Call(service, args, reply); err != nil && err.Error() == "connection is shut down"; err = client.Call(service, args, reply) {
		self.lock.Lock()
		delete(self.clients, addr)
		self.lock.Unlock()
		if client, err = self.client(addr); err != nil {
			return
		}
	}
	return
}

func (self *Switchboard) Close(addr string) error {
	client, err := self.client(addr)
	if err != nil {
		return err
	}

	return client.Close()
}
