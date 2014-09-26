package transactor

import (
	"github.com/zond/drafty/common"
	"github.com/zond/drafty/storage"
)

type Transactor struct {
	storage storage.DB
	txById  map[string]*common.TX
	urByKey map[string][]*common.TX
	uwByKey map[string][]*common.TX
}

func New(storage storage.DB) (result *Transactor) {
	return &Transactor{
		storage: storage,
		txById:  map[string]*common.TX{},
		urByKey: map[string][]*common.TX{},
		uwByKey: map[string][]*common.TX{},
	}
}

func (self *Transactor) Get(tx *common.TX, key []byte) (result *common.TXGetResp, err error) {
	// load data from storage
	value, err := self.storage.Get(key)
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
	self.urByKey[skey] = append(self.urByKey[skey], tx)
	// create response with value and last write timestamp
	result = &common.TXGetResp{
		Value: value.Bytes(),
		Wrote: value.WriteTimestamp(),
	}
	// append ids of all soft write locks to result
	for _, uw := range self.uwByKey[skey] {
		result.UW = append(result.UW, uw.Id)
	}
	return
}
