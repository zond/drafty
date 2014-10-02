package transport

import (
	"github.com/zond/drafty/transactor"
	"github.com/zond/drafty/transactor/messages"
)

type RPCServer struct {
	Transactor *transactor.Transactor
}

func (self *RPCServer) Get(req *messages.GetRequest, resp *messages.Value) (err error) {
	r, err := self.Transactor.Get(req.TX, req.Key)
	if err != nil {
		return
	}
	*resp = *r
	return
}

func (self *RPCServer) PrewriteAndValidate(meta map[string]*messages.ValueMeta, resp *struct{}) (err error) {
	if err = self.Transactor.PrewriteAndValidate(meta); err != nil {
		return
	}
	return
}
