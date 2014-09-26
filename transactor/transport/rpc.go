package transport

import (
	"github.com/zond/drafty/common"
	"github.com/zond/drafty/transactor"
)

type RPCServer struct {
	Transactor *transactor.Transactor
}

func (self *RPCServer) Initialize(tx *common.TX, a *struct{}) (err error) {
	self.Transactor.UpdateTX(tx)
	return
}
