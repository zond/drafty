package rpc

import (
	"github.com/zond/drafty/storage"
	"github.com/zond/drafty/storage/messages"
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

func (self *RPCServer) Put(req *messages.PutRequest, a *struct{}) (err error) {
	return self.Storage.Put(req.Key, req.Value, req.Logs)
}

func (self *RPCServer) Get(key []byte, result *[]byte) (err error) {
	r, err := self.Storage.Get(key)
	if err != nil {
		return
	}
	*result = r
	return
}

func (self *RPCServer) Hashes(req messages.HashesRequest, result *[256][]byte) (err error) {
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

func (self RPCTransport) Put(key []byte, value storage.Value, logs string) (err error) {
	err = switchboard.Switch.Call(string(self), "Synchronizable.Put", &messages.PutRequest{
		Key:   key,
		Value: value,
		Logs:  logs,
	}, nil)
	return
}

func (self RPCTransport) Get(key []byte) (result storage.Value, err error) {
	err = switchboard.Switch.Call(string(self), "Synchronizable.Get", key, &result)
	return
}

func (self RPCTransport) Hashes(prefix []byte, level uint) (result [256][]byte, err error) {
	req := messages.HashesRequest{
		Prefix: prefix,
		Level:  level,
	}
	err = switchboard.Switch.Call(string(self), "Synchronizable.Hashes", req, &result)
	return
}
