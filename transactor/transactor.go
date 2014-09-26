package transactor

import (
	"github.com/zond/drafty/common"
	"github.com/zond/drafty/node"
)

type Transactor struct {
	node    *node.Node
	txById  map[string][]*common.TX
	urByKey map[string][]*common.TX
	uwByKey map[string][]*common.TX
}

func New(node *node.Node) (result *Transactor) {
	return &Transactor{
		node:   node,
		txById: map[string]*common.TX{},
	}
}

func (self *Transactor) Get(tx *common.TX, key []byte) (result *common.TXGetResp, err error) {
	// load data from storage
	value, err := self.node.storage.Get(key)
	if err != nil {
		return
	}
	// ensure tx exists here
	txsid := string(tx.Id)
	oldTx, found := self.txById[txsid]
	if found {
		tx = oldTx
	} else {
		self.txById[txsid] = tx
	}
	skey := string(key)
	// place soft read lock
	urByKey[skey] = append(urById[skey], tx)
	// create response with value and last write timestamp
	result = &common.TXGetResp{
		Value: value.Bytes(),
		Wrote: value.WriteTimestamp(),
	}
	// append ids of all soft write locks to result
	for _, uw := range uwByKey[skey] {
		result.UW = append(result.UW, tx.ID)
	}
	return
}
