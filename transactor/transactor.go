package transactor

import (
	"github.com/zond/drafty/storage"
	"github.com/zond/drafty/transactor/messages"
)

type Backend interface {
	Get([]byte) (storage.Value, error)
}

type Transactor struct {
	backend Backend
	txById  map[string]*messages.TX
	urByKey map[string]map[string]*messages.TX
	uwByKey map[string]map[string]*messages.TX
}

func New(backend Backend) (result *Transactor) {
	return &Transactor{
		backend: backend,
		txById:  map[string]*messages.TX{},
		urByKey: map[string]map[string]*messages.TX{},
		uwByKey: map[string]map[string]*messages.TX{},
	}
}

func (self *Transactor) Get(tx *messages.TX, key []byte) (result *messages.Value, err error) {
	// load data from storage
	value, err := self.backend.Get(key)
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
	// ensure soft read lock
	skey := string(key)
	oldURs, found := self.urByKey[skey]
	if !found {
		oldURs = map[string]*messages.TX{}
		self.urByKey[skey] = oldURs
	}
	oldURs[txsid] = tx
	// create response with value and last write timestamp
	result = &messages.Value{
		Data:           value.Bytes(),
		WriteTimestamp: value.WriteTimestamp(),
	}
	// append ids of all soft write locks to result
	for _, uw := range self.uwByKey[skey] {
		result.UW = append(result.UW, uw.Id)
	}
	return
}
