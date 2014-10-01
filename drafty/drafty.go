package main

import (
	"flag"
	"github.com/zond/drafty/node"
	"os"
	"path/filepath"
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

	n, err := node.New(*addr, *dir)
	if err != nil {
		panic(err)
	}
	if err := n.Start(*join); err != nil {
		panic(err)
	}
	c := make(chan struct{})
	<-c
}
