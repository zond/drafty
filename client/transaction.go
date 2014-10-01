package client

import "github.com/zond/drafty/transactor/messages"

type TX struct {
	*messages.TX
	client     *Client
	buffer     map[string][]byte
	uwByKey    map[string][][]byte
	wroteByKey map[string]int64
}

func (self *TX) Commit() (err error) {
	if err = self.preWriteAndValidate(); err != nil {
		return
	}
	return
}

func (self *TX) Abort() {
}

func (self *TX) Put(key []byte, value []byte) {
	skey := string(key)
	self.buffer[skey] = value
	return
}

func (self *TX) Get(key []byte) (result []byte, err error) {
	skey := string(key)
	result, found := self.buffer[skey]
	if found {
		return
	}
	resp := &messages.TXGetResp{}
	if err = self.client.callSuccessorOf(key, "TX.Get", &messages.TXGetReq{TX: self.TX, Key: key}, resp); err != nil {
		return
	}
	self.buffer[skey] = resp.Value
	self.wroteByKey[skey] = resp.Wrote
	self.uwByKey[skey] = resp.UW
	result = resp.Value
	return
}

func (self *TX) preWriteAndValidate() (err error) {
	return
}
