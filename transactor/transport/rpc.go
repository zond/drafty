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

func (self *RPCServer) PrewriteAndValidate(req *messages.PrewriteAndValidateRequest, resp *struct{}) (err error) {
	if err = self.Transactor.PrewriteAndValidate(req.TX, req.ValueContexts); err != nil {
		return
	}
	return
}
