package peer

import (
	"encoding/hex"
	"fmt"

	"github.com/zond/drafty/peer"
	"github.com/zond/drafty/storage"
)

func New(peer *peer.Peer) (result *TransactorBackend) {
	return &TransactorBackend{
		peer: peer,
	}
}

type TransactorBackend struct {
	peer *peer.Peer
}

func (self *TransactorBackend) assertResponsibility(key []byte) (err error) {
	successors := self.peer.Ring().Successors(key, peer.NBackups+1)
	if !successors.ContainsPos(self.peer.Pos()) {
		err = fmt.Errorf("%v is not responsible for %v", self, hex.EncodeToString(key))
		return
	}
	return
}

func (self *TransactorBackend) Get(key []byte) (result storage.Value, err error) {
	if err = self.peer.WhileRunning(func() (err error) {
		if err = self.assertResponsibility(key); err != nil {
			return
		}
		if result, err = self.peer.Storage().Get(key); err != nil {
			return
		}
		return
	}); err != nil {
		return
	}
	return
}
