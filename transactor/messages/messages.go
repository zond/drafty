package messages

import "time"

type TXState int

const (
	TXRunning TXState = iota
)

type TXGetReq struct {
	TX  *TX
	Key []byte
}

type TXGetResp struct {
	Value []byte
	Wrote int64
	UW    [][]byte
}

type TX struct {
	Id    []byte
	Lower *time.Time
	Upper *time.Time
	State TXState
}
