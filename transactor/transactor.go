package transactor

import (
	"bytes"

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

func (self *Transactor) getTX(id string) (result *messages.TX) {
	result, found := self.txById[id]
	if !found {
		result = &messages.TX{}
		self.txById[id] = result
	}
	return
}

func (self *Transactor) getUxByKey(m map[string]map[string]*messages.TX, key string) (result map[string]*messages.TX) {
	result, found := m[key]
	if !found {
		result = map[string]*messages.TX{}
		m[key] = result
	}
	return
}

func (self *Transactor) addUx(src map[string]map[string]*messages.TX, dst map[string]struct{}, skey string, avoidId []byte) {
	for _, tx := range src[skey] {
		if bytes.Compare(tx.Id, avoidId) != 0 {
			dst[string(tx.Id)] = struct{}{}
		}
	}
}

func (self *Transactor) Get(txid []byte, key []byte) (result *messages.Value, err error) {
	txsid := string(txid)
	tx := self.getTX(txsid)
	value, err := self.backend.Get(key)
	if err != nil {
		return
	}
	skey := string(key)
	self.getUxByKey(self.urByKey, skey)[txsid] = tx
	result = &messages.Value{
		Data: value.Bytes(),
		Context: &messages.ValueContext{
			WriteTimestamp: value.WriteTimestamp(),
		},
	}
	self.addUx(self.uwByKey, result.Context.UW, skey, txid)
	return
}

func (self *Transactor) PrewriteAndValidate(txid []byte, valueContexts map[string]*messages.ValueContext) (err error) {
	txsid := string(txid)
	tx := self.getTX(txsid)
	for skey, valueContext := range valueContexts {
		key := []byte(skey)
		var value storage.Value
		if value, err = self.backend.Get(key); err != nil {
			return
		}
		valueContext.ReadTimestamp = value.ReadTimestamp()
		self.getUxByKey(self.uwByKey, skey)[txsid] = tx
		self.addUx(self.urByKey, valueContext.UR, skey, txid)
		self.addUx(self.uwByKey, valueContext.UW, skey, txid)
	}
	return
}
