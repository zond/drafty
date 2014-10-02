package treap

import (
	"bytes"
	"fmt"
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type TreapIterator func(key []byte, value []byte)

type Treap struct {
	Size int
	Root *node
}

func (self *Treap) Put(key []byte, value []byte) (old []byte, existed bool) {
	self.Root, existed, old = self.Root.insert(newNode(key, value))
	if !existed {
		self.Size++
	}
	return
}

func (self *Treap) Get(key []byte) (value []byte, existed bool) {
	return self.Root.get(key)
}

func (self *Treap) Describe() string {
	buffer := bytes.NewBufferString(fmt.Sprintf("<Treap Size:%v>\n", self.Size))
	self.Root.describe(0, buffer)
	return string(buffer.Bytes())
}

func (self *Treap) Del(key []byte) (old []byte, existed bool) {
	self.Root, existed, old = self.Root.del(key)
	if existed {
		self.Size--
	}
	return
}

func (self *Treap) Up(from, below []byte, f TreapIterator) {
	self.Root.up(from, below, f)
}

func (self *Treap) Down(from, above []byte, f TreapIterator) {
	self.Root.down(from, above, f)
}

func (self *Treap) String() string {
	return fmt.Sprint(self.ToMap())
}

func (self *Treap) ToMap() map[string][]byte {
	rval := make(map[string][]byte)
	self.Up(nil, nil, func(key []byte, value []byte) {
		rval[string(key)] = value
	})
	return rval
}

func merge(left, right *node) (result *node) {
	if left == nil {
		result = right
	} else if right == nil {
		result = left
	} else if left.Weight < right.Weight {
		result, left.Right = left, merge(left.Right, right)
	} else {
		result, right.Left = right, merge(right.Left, left)
	}
	return
}

type node struct {
	Weight int32
	Left   *node
	Right  *node
	Key    []byte
	Value  []byte
}

func newNode(key []byte, value []byte) (rval *node) {
	rval = &node{
		Weight: rand.Int31(),
		Key:    key,
		Value:  value,
	}
	return
}

func (self *node) describe(indent int, buffer *bytes.Buffer) {
	if self != nil {
		self.Left.describe(indent+1, buffer)
		indentation := &bytes.Buffer{}
		for i := 0; i < indent; i++ {
			fmt.Fprint(indentation, " ")
		}
		fmt.Fprintf(buffer, "%v%v [%v] => %v\n", string(indentation.Bytes()), self.Key, self.Weight, self.Value)
		self.Right.describe(indent+1, buffer)
	}
}
func (self *node) get(key []byte) (value []byte, existed bool) {
	if self != nil {
		switch bytes.Compare(key, self.Key) {
		case -1:
			return self.Left.get(key)
		case 1:
			return self.Right.get(key)
		default:
			value, existed = self.Value, true
		}

	}
	return
}
func (self *node) up(from, below []byte, f TreapIterator) {
	if self != nil {
		from_cmp := -1
		if from != nil {
			from_cmp = bytes.Compare(from, self.Key)
		}
		below_cmp := 1
		if below != nil {
			below_cmp = bytes.Compare(below, self.Key)
		}

		if from_cmp < 0 {
			self.Left.up(from, below, f)
		}
		if from_cmp < 1 && below_cmp > 0 {
			f(self.Key, self.Value)
		}
		if below_cmp > 0 {
			self.Right.up(from, below, f)
		}
	}
}
func (self *node) down(from, above []byte, f TreapIterator) {
	if self != nil {
		from_cmp := -1
		if from != nil {
			from_cmp = bytes.Compare(from, self.Key)
		}
		above_cmp := 1
		if above != nil {
			above_cmp = bytes.Compare(above, self.Key)
		}

		if from_cmp > 0 {
			self.Right.down(from, above, f)
		}
		if from_cmp > -1 && above_cmp < 0 {
			f(self.Key, self.Value)
		}
		if above_cmp < 0 {
			self.Left.down(from, above, f)
		}
	}
}
func (self *node) rotateLeft() (result *node) {
	result, self.Left, self.Left.Right = self.Left, self.Left.Right, self
	return
}
func (self *node) rotateRight() (result *node) {
	result, self.Right, self.Right.Left = self.Right, self.Right.Left, self
	return
}
func (self *node) del(key []byte) (result *node, existed bool, old []byte) {
	if self != nil {
		result = self
		switch bytes.Compare(key, self.Key) {
		case -1:
			self.Left, existed, old = self.Left.del(key)
		case 1:
			self.Right, existed, old = self.Right.del(key)
		default:
			result, existed, old = merge(self.Left, self.Right), true, self.Value
		}
	}
	return
}
func (self *node) insert(n *node) (result *node, existed bool, old []byte) {
	result = n
	if self != nil {
		result = self
		switch bytes.Compare(n.Key, self.Key) {
		case -1:
			self.Left, existed, old = self.Left.insert(n)
			if self.Left.Weight < self.Weight {
				result = self.rotateLeft()
			}
		case 1:
			self.Right, existed, old = self.Right.insert(n)
			if self.Right.Weight < self.Weight {
				result = self.rotateRight()
			}
		default:
			existed, old, self.Value = true, self.Value, n.Value
		}
	}
	return
}
