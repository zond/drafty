package ranje

import (
	"bytes"
	"encoding/hex"
	"fmt"
)

type Ranges []Range

func (self Ranges) Within(k []byte) bool {
	for _, r := range self {
		if r.Within(k) {
			return true
		}
	}
	return false
}

type Range struct {
	FromInc []byte
	ToExc   []byte
}

func (self Range) Empty() bool {
	return bytes.Compare(self.FromInc, self.ToExc) == 0
}

func (self Range) Reversed() bool {
	return bytes.Compare(self.FromInc, self.ToExc) > 0
}

func (self Range) Within(k []byte) bool {
	cmp := bytes.Compare(self.FromInc, self.ToExc)
	if cmp == 0 {
		return true
	}
	if cmp > 0 {
		return (self.FromInc == nil || bytes.Compare(self.FromInc, k) < 1) || (self.ToExc == nil || bytes.Compare(self.ToExc, k) > 0)
	}
	return (self.FromInc == nil || bytes.Compare(self.FromInc, k) < 1) && (self.ToExc == nil || bytes.Compare(self.ToExc, k) > 0)
}

func (self Range) PrefixWithin(k []byte) bool {
	from := self.FromInc
	if len(from) > len(k) {
		from = self.FromInc[:len(k)]
	}
	to := self.ToExc
	if len(to) > len(k) {
		to = self.ToExc[:len(k)]
	}
	cmp := bytes.Compare(from, to)
	if cmp == 0 {
		return true
	}
	if cmp > 0 {
		return (from == nil || bytes.Compare(from, k) < 1) || (to == nil || bytes.Compare(to, k) > -1)
	}
	return (from == nil || bytes.Compare(from, k) < 1) && (to == nil || bytes.Compare(to, k) > -1)
}

func (self Range) String() string {
	return fmt.Sprintf("[%v,%v)", hex.EncodeToString(self.FromInc), hex.EncodeToString(self.ToExc))
}
