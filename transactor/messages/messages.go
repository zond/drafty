package messages

import "time"

type TXState int

const (
	TXRunning TXState = iota
)

type GetRequest struct {
	TX  *TX
	Key []byte
}

type TX struct {
	Id    []byte
	Lower *time.Time
	Upper *time.Time
	State TXState
}

type Value struct {
	Data           []byte
	UW             [][]byte
	WriteTimestamp int64
	ReadTimestamp  int64
}

type NodeMeta struct {
	Values map[string]*Value
}
