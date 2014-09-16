package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().Unix())
}

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

func TestLevels(t *testing.T) {
	for i := 0; i < 100; i++ {
		key := make([]byte, 1+rand.Int()%32)
		for index, _ := range key {
			key[index] = byte(rand.Int())
		}
		for lvl := 1; lvl < 24; lvl++ {
			start := levelStart(uint(lvl), key)
			end := levelEnd(uint(lvl), key)
			if bytes.Compare(start, key) > 0 {
				t.Errorf("Start level of %v for %v is %v, but that is greater than the key", lvl, fmtBytes(key), fmtBytes(start))
			}
			if (len(end) != 1 || end[0] != 0) && bytes.Compare(end, key) < 1 {
				t.Errorf("End of level %v for %v is %v, but that is not greater than the key", lvl, fmtBytes(key), fmtBytes(end))
			}
		}
	}
}

func TestTopHash(t *testing.T) {
	keys := [][]byte{}
	values := [][]byte{}
	toDelete := [][]byte{}
	for i := 0; i < 100; i++ {
		key := make([]byte, binary.MaxVarintLen64)
		key = key[:binary.PutVarint(key, rand.Int63())]
		keys = append(keys, key)
		if rand.Int()%2 == 0 {
			toDelete = append(toDelete, key)
		}
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
			for _, i := range rand.Perm(len(toDelete)) {
				if err := db1.Delete(toDelete[i]); err != nil {
					t.Fatalf("%v", err)
				}
			}
			for _, i := range rand.Perm(len(keys)) {
				if err := db2.Put(keys[i], values[i]); err != nil {
					t.Fatalf("%v", err)
				}
			}
			for _, i := range rand.Perm(len(toDelete)) {
				if err := db2.Delete(toDelete[i]); err != nil {
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
