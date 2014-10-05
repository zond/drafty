package peer

import (
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

func (self *TransactorBackend) Get(key []byte) (result storage.Value, err error) {
	if err = self.peer.WhileRunning(func() (err error) {
		if err = self.peer.AssertResponsibility(key); err != nil {
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
