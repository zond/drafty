package storage

import (
	"encoding/binary"
	"fmt"
	"math/big"
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

func TestLevels(t *testing.T) {
	for i := 0; i < 100; i++ {
		key := make([]byte, 1+rand.Int()%32)
		for index, _ := range key {
			key[index] = byte(rand.Int())
		}
		keyBn := big.NewInt(0).SetBytes(key)
		for lvl := 1; lvl < 24; lvl++ {
			start := levelStart(lvl, key)
			startBn := big.NewInt(0).SetBytes(start)
			end := levelEnd(lvl, key)
			endBn := big.NewInt(0).SetBytes(end)
			if startBn.Cmp(keyBn) > 0 {
				t.Errorf("Start level of %v for %v is %v, but that is greater than the key", lvl, fmtBytes(key), fmtBytes(start))
			}
			if endBn.Cmp(keyBn) < 1 {
				t.Errorf("End of level %v for %v is %v, but that is not greater than the key", lvl, fmtBytes(key), fmtBytes(end))
			}
			if big.NewInt(0).Sub(endBn, startBn).Cmp(big.NewInt(0).Lsh(big.NewInt(1), uint(lvl*4))) != 0 {
				t.Errorf("End of level %v for %v is %v, and start of level %v is %v, but they are not at the right distance", lvl, fmtBytes(key), fmtBytes(end), lvl, fmtBytes(start))
			}
		}
	}
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
