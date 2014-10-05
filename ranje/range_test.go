package ranje

import "testing"

func TestRange(t *testing.T) {
	r := Range{
		FromInc: []byte{1, 2, 3},
		ToExc:   []byte{2, 3, 4},
	}
	if r.Within([]byte{1, 2, 2}) {
		t.Errorf("noo")
	}
	if r.Within([]byte{1, 2, 2, 9, 9, 9}) {
		t.Errorf("noo")
	}
	if r.Within([]byte{2, 3, 4}) {
		t.Errorf("noo")
	}
	if r.Within([]byte{2, 3, 4, 0}) {
		t.Errorf("noo")
	}
	if !r.Within([]byte{1, 2, 3}) {
		t.Errorf("noo")
	}
	if !r.Within([]byte{1, 2, 3, 4}) {
		t.Errorf("noo")
	}
	if !r.Within([]byte{2, 3, 3, 9, 9}) {
		t.Errorf("noo")
	}
}
