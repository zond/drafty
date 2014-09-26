package rpc

import (
	"github.com/zond/drafty/storage"
	"github.com/zond/drafty/switchboard"
)

type RPCServer struct {
	Storage *storage.DB
}

func (self *RPCServer) Hash(a struct{}, result *[]byte) (err error) {
	r, err := self.Storage.Hash()
	if err != nil {
		return
	}
	*result = r
	return
}

func (self *RPCServer) Put(kv [2][]byte, a *struct{}) (err error) {
	return self.Storage.Put(kv[0], kv[1])
}

func (self *RPCServer) Get(key []byte, result *[]byte) (err error) {
	r, err := self.Storage.Get(key)
	if err != nil {
		return
	}
	*result = r
	return
}

type HashesRequest struct {
	Prefix []byte
	Level  uint
}

func (self *RPCServer) Hashes(req HashesRequest, result *[256][]byte) (err error) {
	r, err := self.Storage.Hashes(req.Prefix, req.Level)
	if err != nil {
		return
	}
	*result = r
	return
}

type RPCTransport string

func (self RPCTransport) Hash() (result []byte, err error) {
	err = switchboard.Switch.Call(string(self), "Synchronizable.Hash", struct{}{}, &result)
	return
}

func (self RPCTransport) Put(key []byte, value storage.Value) (err error) {
	err = switchboard.Switch.Call(string(self), "Synchronizable.Put", [2][]byte{key, value}, nil)
	return
}

func (self RPCTransport) Get(key []byte) (result storage.Value, err error) {
	err = switchboard.Switch.Call(string(self), "Synchronizable.Get", key, &result)
	return
}

func (self RPCTransport) Hashes(prefix []byte, level uint) (result [256][]byte, err error) {
	req := HashesRequest{
		Prefix: prefix,
		Level:  level,
	}
	err = switchboard.Switch.Call(string(self), "Synchronizable.Hashes", req, &result)
	return
}
