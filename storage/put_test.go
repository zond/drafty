package storage

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"testing"
)

func withDB(t *testing.T, f func(DB)) {
	fname := fmt.Sprintf("test-%v.db", rand.Int())
	db, err := New(fname)
	if err != nil {
		t.Fatalf("%v", err)
	}
	f(db)
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("%v", err)
		}
		if err := os.Remove(fname); err != nil {
			t.Fatalf("%v", err)
		}
	}()

}

func TestTopHash(t *testing.T) {
	keys := [][]byte{}
	values := [][]byte{}
	for i := 0; i < 100; i++ {
		key := make([]byte, binary.MaxVarintLen64)
		keys = append(keys, key[:binary.PutVarint(key, rand.Int63())])
		value := make([]byte, binary.MaxVarintLen64)
		values = append(values, value[:binary.PutVarint(value, rand.Int63())])
	}
	withDB(t, func(db1 DB) {
		withDB(t, func(db2 DB) {
			for _, i := range rand.Perm(len(keys)) {
				if err := db1.Put(keys[i], values[i]); err != nil {
					t.Fatalf("%v", err)
				}
			}
			for _, i := range rand.Perm(len(keys)) {
				if err := db2.Put(keys[i], values[i]); err != nil {
					t.Fatalf("%v", err)
				}
			}
			if eq, err := db2.Equal(db2); err != nil {
				t.Fatalf("%v", err)
			} else if !eq {
				t.Errorf("Not equal")
			}
		})
	})
}
