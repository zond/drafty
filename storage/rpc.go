package storage

import "github.com/zond/drafty/switchboard"

type RPC struct {
	Storage DB
}

func (self *RPC) Hash(a struct{}, result *[]byte) (err error) {
	r, err := self.Storage.Hash()
	if err != nil {
		return
	}
	*result = r
	return
}

func (self *RPC) Put(kv [2][]byte, a *struct{}) (err error) {
	return self.Storage.Put(kv[0], kv[1])
}

func (self *RPC) Get(key []byte, result *[]byte) (err error) {
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

func (self *RPC) Hashes(req HashesRequest, result *[256][]byte) (err error) {
	r, err := self.Storage.Hashes(req.Prefix, req.Level)
	if err != nil {
		return
	}
	*result = r
	return
}

type RPCTransport struct {
	ConnectionString string
}

func (self *RPCTransport) Hash() (result []byte, err error) {
	err = switchboard.Switch.Call(self.ConnectionString, "Synchronizable.Hash", struct{}{}, &result)
	return
}

func (self *RPCTransport) Put(key []byte, value []byte) (err error) {
	err = switchboard.Switch.Call(self.ConnectionString, "Synchronizable.Put", [2][]byte{key, value}, nil)
	return
}

func (self *RPCTransport) Get(key []byte) (result []byte, err error) {
	err = switchboard.Switch.Call(self.ConnectionString, "Synchronizable.Get", key, &result)
	return
}

func (self *RPCTransport) Hashes(prefix []byte, level uint) (result [256][]byte, err error) {
	req := HashesRequest{
		Prefix: prefix,
		Level:  level,
	}
	err = switchboard.Switch.Call(self.ConnectionString, "Synchronizable.Hashes", req, &result)
	return
}
