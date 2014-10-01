package node

import (
	"encoding/hex"
	"fmt"

	"github.com/zond/drafty/common"
	"github.com/zond/drafty/storage"
)

type transactorBackend struct {
	node *Node
}

func (self *transactorBackend) assertResponsibility(key []byte) (err error) {
	successors := self.node.ring.Successors(key, common.NBackups+1)
	if !successors.ContainsPos(self.node.pos) {
		err = fmt.Errorf("%v is not responsible for %v", self, hex.EncodeToString(key))
		return
	}
	return
}

func (self *transactorBackend) Get(key []byte) (result storage.Value, err error) {
	if err = self.node.WhileRunning(func() (err error) {
		if err = self.assertResponsibility(key); err != nil {
			return
		}
		if result, err = self.node.storage.Get(key); err != nil {
			return
		}
		return
	}); err != nil {
		return
	}
	return
}
