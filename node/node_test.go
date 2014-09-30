package node

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/zond/drafty/common"
	"github.com/zond/drafty/log"
	"github.com/zond/drafty/node/ring"
)

var nextPort = 9797

func init() {
	testfiles, err := filepath.Glob("test-*")
	if err != nil {
		panic(err)
	}
	for _, testfile := range testfiles {
		if err := os.RemoveAll(testfile); err != nil {
			panic(err)
		}
	}
}

func withNode(t *testing.T, f func(*Node)) {
	dirname := fmt.Sprintf("test-%v", rand.Int63())
	node, err := New(fmt.Sprintf("127.0.0.1:%v", nextPort), dirname)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer func() {
		if err := node.Stop(); err != nil {
			t.Errorf("%v", err)
		}
	}()
	nextPort += 1
	f(node)
}

type output struct {
	format string
	args   []interface{}
}

type failer struct {
	errors []output
	fatals []output
}

func (self *failer) Errorf(f string, a ...interface{}) {
	self.errors = append(self.errors, output{f, a})
}

func (self *failer) Fatalf(f string, a ...interface{}) {
	self.fatals = append(self.fatals, output{f, a})
}

func (self *failer) fail(t *testing.T) {
	for _, err := range self.errors {
		t.Errorf(err.format, err.args...)
	}
	for _, fat := range self.fatals {
		t.Fatalf(fat.format, fat.args...)
	}
}

func (self *failer) empty() bool {
	return len(self.errors) == 0 && len(self.fatals) == 0
}

func (self *failer) clear() {
	self.errors = nil
	self.fatals = nil
}

func assertWithin(t *testing.T, d time.Duration, f func(*failer)) {
	deadline := time.Now().Add(d)
	backoff := time.Millisecond * 20
	failer := &failer{}
	for time.Now().Before(deadline) {
		failer.clear()
		f(failer)
		if failer.empty() {
			break
		}
		time.Sleep(backoff)
		backoff = (backoff * 3 / 2)
	}
	if time.Now().After(deadline) {
		failer.fail(t)
	}
}

func assertRing(t *testing.T, n *Node, r *ring.Ring) {
	assertWithin(t, time.Second*2, func(f *failer) {
		n.WhileRunning(func() (err error) {
			if !n.ring.Equal(r) {
				f.Fatalf("Wrong ring, wanted %v to have %v but it has %v", n, r, n.ring)
			}
			return
		})
	})
}

type nodes []*Node

func (self nodes) Len() int {
	return len(self)
}

func (self nodes) Less(i, j int) bool {
	return bytes.Compare(self[i].pos, self[j].pos) < 0
}

func (self nodes) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

func withCluster(t *testing.T, f func(*ring.Ring, []*Node), prep func(*Node, int)) {
	withNode(t, func(n1 *Node) {
		withNode(t, func(n2 *Node) {
			withNode(t, func(n3 *Node) {
				withNode(t, func(n4 *Node) {
					n1.Start("")
					if prep != nil {
						prep(n1, 0)
					}
					n2.Start(n1.Addr())
					if prep != nil {
						prep(n2, 1)
					}
					n3.Start(n1.Addr())
					if prep != nil {
						prep(n3, 2)
					}
					n4.Start(n1.Addr())
					if prep != nil {
						prep(n4, 3)
					}
					r := ring.New()
					r.AddPeer(n1.AsPeer())
					r.AddPeer(n2.AsPeer())
					r.AddPeer(n3.AsPeer())
					r.AddPeer(n4.AsPeer())
					n := nodes{
						n1, n2, n3, n4,
					}
					sort.Sort(n)
					f(r, n)
				})
			})
		})
	})
}

func TestJoining(t *testing.T) {
	log.Level = log.Warn
	withCluster(t, func(r *ring.Ring, n []*Node) {
		assertRing(t, n[0], r)
		assertRing(t, n[1], r)
		assertRing(t, n[2], r)
		assertRing(t, n[3], r)
	}, nil)
}

func TestSyncAndClean(t *testing.T) {
	log.Level = log.Warn
	val := make([]byte, 18)
	val[17] = 1
	keys := [][][]byte{}
	for n := 0; n < 3; n++ {
		nodeKeys := [][]byte{}
		for i := 0; i < 100; i++ {
			nodeKeys = append(nodeKeys, ring.RandomPos(2))
		}
		keys = append(keys, nodeKeys)
	}
	withCluster(t, func(r *ring.Ring, n []*Node) {
		assertWithin(t, time.Second*5, func(f *failer) {
			for _, keyset := range keys {
				for _, key := range keyset {
					successors := r.Successors(key, common.NBackups+1)
					for _, node := range n {
						v, err := node.storage.Get(key)
						if err != nil {
							f.Fatalf("Unable to load %v from %v: %v", key, node, err)
						}
						if successors.ContainsPos(node.pos) {
							if bytes.Compare(val, v) != 0 {
								f.Errorf("Wrong value for %v %v in %v: %v", r, hex.EncodeToString(key), hex.EncodeToString(node.pos), v)
							}
						} else {
							if v != nil {
								f.Errorf("Wrong value for %v %v in %v: %v", r, hex.EncodeToString(key), hex.EncodeToString(node.pos), v)
							}
						}
					}
				}
			}
		})
	}, func(n *Node, i int) {
		if i < 3 {
			for _, key := range keys[i] {
				if err := n.storage.Put(key, val, fmt.Sprintf("test setup of %v", hex.EncodeToString(n.pos))); err != nil {
					t.Fatalf("%v", err)
				}
			}
		}
	})
}
