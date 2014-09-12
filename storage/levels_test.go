package storage

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

func fmtBytes(b []byte) string {
	buf := &bytes.Buffer{}
	for _, b := range b {
		fmt.Fprintf(buf, "%08b", b)
	}
	return buf.String()
}

func TestLevels(t *testing.T) {
	for i := 0; i < 10; i++ {
		key := make([]byte, 1+rand.Int()%10)
		for index, _ := range key {
			key[index] = byte(rand.Int())
		}
		fmt.Printf("Key       %v\n", fmtBytes(key))
		for lvl := 1; lvl < 10; lvl++ {
			fmt.Printf("%03v start %v\n", lvl, fmtBytes(levelStart(lvl, key)))
			fmt.Printf("%03v end   %v\n", lvl, fmtBytes(levelEnd(lvl, key)))
		}
	}
}
