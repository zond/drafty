package common

import "time"

const (
	NBackups = 2
)

type TXState int

const (
	TXRunning TXState = iota
)

type TimeBound struct {
}

type TX struct {
	Id    []byte
	Lower time.Time
	Upper time.Time
	State TXState
}
