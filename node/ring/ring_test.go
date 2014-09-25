package ring

import (
	"reflect"
	"testing"
)

func TestSuccessors(t *testing.T) {
	r := New()
	p0 := &Peer{
		Name: "a",
		Pos:  []byte{0},
	}
	r.AddPeer(p0)
	p1 := &Peer{
		Name: "b",
		Pos:  []byte{1},
	}
	r.AddPeer(p1)
	p2 := &Peer{
		Name: "c",
		Pos:  []byte{2},
	}
	r.AddPeer(p2)
	p3 := &Peer{
		Name: "d",
		Pos:  []byte{3},
	}
	r.AddPeer(p3)
	if !reflect.DeepEqual(r.Successors([]byte{1}, 3), Peers{
		p2, p3, p0,
	}) {
		t.Errorf("Not right")
	}
	if !reflect.DeepEqual(r.Successors([]byte{0}, 3), Peers{
		p1, p2, p3,
	}) {
		t.Errorf("Not right")
	}
	if !reflect.DeepEqual(r.Predecessors([]byte{1}, 3), Peers{
		p0, p3, p2,
	}) {
		t.Errorf("Not right")
	}
	if !reflect.DeepEqual(r.Predecessors([]byte{3}, 3), Peers{
		p2, p1, p0,
	}) {
		t.Errorf("Not right")
	}
}
