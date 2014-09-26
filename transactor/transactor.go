package transactor

import (
	"github.com/zond/drafty/common"
	"github.com/zond/drafty/node"
)

type Transactor struct {
	node   *node.Node
	txById map[string]*common.TX
}

func New(node *node.Node) (result *Transactor) {
	return &Transactor{
		node:   node,
		txById: map[string]*common.TX{},
	}
}

func UpdateTX(tx *common.TX) {
	self.txById[string(tx.Id)] = tx
}
