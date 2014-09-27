package node

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/zond/drafty/log"
	"github.com/zond/drafty/node/ring"
)

var nextPort = 9797

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
		if err := os.RemoveAll(dirname); err != nil {
			t.Fatalf("%v", err)
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
	assertWithin(t, time.Second*10, func(f *failer) {
		n.WhileRunning(func() (err error) {
			if !n.ring.Equal(r) {
				f.Fatalf("Wrong ring, wanted %v to have %v but it has %v", n, r, n.ring)
			}
			return
		})
	})
}

func withCluster(t *testing.T, f func(*ring.Ring, []*Node), preps ...func(*Node)) {
	withNode(t, func(n1 *Node) {
		withNode(t, func(n2 *Node) {
			withNode(t, func(n3 *Node) {
				withNode(t, func(n4 *Node) {
					n1.Start("")
					if len(preps) > 0 {
						preps[0](n1)
					}
					n2.Start(n1.Addr())
					if len(preps) > 1 {
						preps[1](n2)
					}
					n3.Start(n1.Addr())
					if len(preps) > 2 {
						preps[2](n3)
					}
					n4.Start(n1.Addr())
					if len(preps) > 4 {
						preps[3](n4)
					}
					r := ring.New()
					r.AddPeer(n1.AsPeer())
					r.AddPeer(n2.AsPeer())
					r.AddPeer(n3.AsPeer())
					r.AddPeer(n4.AsPeer())
					f(r, []*Node{
						n1, n2, n3, n4,
					})
				})
			})
		})
	})
}

func TestJoining(t *testing.T) {
	log.Level = 10
	withCluster(t, func(r *ring.Ring, n []*Node) {
		assertRing(t, n[0], r)
		assertRing(t, n[1], r)
		assertRing(t, n[2], r)
		assertRing(t, n[3], r)
	})
}

func TestSync(t *testing.T) {
	log.Level = 10
	withCluster(t, func(r *ring.Ring, n []*Node) {
		fmt.Println(n[0].storage.PP(), n[1].storage.PP(), n[2].storage.PP(), n[3].storage.PP())
	}, func(n1 *Node) {
		n1.storage.PutString("a", "a")
	})
}
