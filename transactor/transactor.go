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

func (self *Transactor) getUWByKey(key string) (result map[string]*messages.TX) {
	return self.getUxByKey(self.urByKey, key)
}

func (self *Transactor) getURByKey(key string) (result map[string]*messages.TX) {
	return self.getUxByKey(self.uwByKey, key)
}

func (self *Transactor) Get(txid []byte, key []byte) (result *messages.Value, err error) {
	txsid := string(txid)
	tx := self.getTX(txsid)
	value, err := self.backend.Get(key)
	if err != nil {
		return
	}
	skey := string(key)
	self.getURByKey(skey)[txsid] = tx
	result = &messages.Value{
		Data: value.Bytes(),
		Meta: &messages.ValueMeta{
			WriteTimestamp: value.WriteTimestamp(),
			RO:             true,
		},
	}
	for _, uw := range self.uwByKey[skey] {
		result.Meta.UW = append(result.Meta.UW, uw.Id)
	}
	return
}

func (self *Transactor) PrewriteAndValidate(txid []byte, valueMeta map[string]*messages.ValueMeta) (err error) {
	txsid := string(txid)
	tx := self.getTX(txsid)
	skey := string(key)
	self.getUWByKey(skey)[txsid] = tx
	return
}
