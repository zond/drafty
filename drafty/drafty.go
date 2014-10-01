package main

import (
	"flag"
	"os"
	"path/filepath"

	"github.com/zond/drafty/peer"
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

	n, err := peer.New(*addr, *dir)
	if err != nil {
		panic(err)
	}
	if err := n.Start(*join); err != nil {
		panic(err)
	}
	c := make(chan struct{})
	<-c
}
