package transport

import (
	"github.com/zond/drafty/common"
	"github.com/zond/drafty/transactor"
)

type RPCServer struct {
	Transactor *transactor.Transactor
}

func (self *RPCServer) Get(req *common.TXGetReq, resp *common.TXGetResp) (err error) {
	r, err := self.Transactor.Get(req.TX, req.Key)
	if err != nil {
		return
	}
	*resp = *r
	return
}
