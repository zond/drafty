package storage

import (
	"math/big"
	"math/rand"
	"testing"
)

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
