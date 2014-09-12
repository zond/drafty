package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"

	"github.com/zond/drafty/consensual"
)

func main() {
	addr := flag.String("addr", "localhost:9797", "Where to listen for connections")
	cwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	dir := flag.String("dir", filepath.Join(cwd, "drafty"), "Where to store data")
	join := flag.String("join", "", "A cluster to join")

	flag.Parse()

	n, err := consensual.New(fmt.Sprintf("%08x", rand.Int63()), *addr, *dir)
	if err != nil {
		panic(err)
	}
	if err := n.Start(*join); err != nil {
		panic(err)
	}
	c := make(chan struct{})
	<-c
}
