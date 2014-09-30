package common

import (
	"strings"
	"sync/atomic"
	"time"
)

const (
	NBackups = 2
)

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

type MultiError []error

func (self MultiError) Error() string {
	s := make([]string, len(self))
	for index, err := range self {
		s[index] = err.Error()
	}
	return strings.Join(s, ", ")
}

type Parallelizer struct {
	count int64
	c     chan error
}

func (self *Parallelizer) Start(f func() error) {
	if self.c == nil {
		self.c = make(chan error)
	}
	atomic.AddInt64(&self.count, 1)
	go func() {
		self.c <- f()
	}()
}

func (self *Parallelizer) Wait() (err error) {
	merr := MultiError{}
	for count := atomic.LoadInt64(&self.count); count > 0; count = atomic.AddInt64(&self.count, -1) {
		if e := <-self.c; e != nil {
			merr = append(merr, e)
		}
	}
	if len(merr) > 0 {
		err = merr
		return
	}
	return
}
