package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/zond/drafty/node/ring"
	"github.com/zond/drafty/switchboard"
)

const (
	syncPut   = "Sync.Put"
	syncGet   = "Sync.Get"
	debugDump = "Debug.Dump"
)

func durToByte(d time.Duration) (result []byte) {
	result = []byte{
		byte(d >> 7),
		byte(d >> 6),
		byte(d >> 5),
		byte(d >> 4),
		byte(d >> 3),
		byte(d >> 2),
		byte(d >> 1),
		byte(d),
	}
	return
}

var commands = map[string]func() error{
	syncPut: func() (err error) {
		if err = switchboard.Switch.Call(*host, "Synchronizable.Put", [2][]byte{
			[]byte(flag.Args()[0]),
			append(append(append(durToByte(*readAge), durToByte(*writeAge)...), 0), []byte(flag.Args()[1])...),
		}, nil); err != nil {
			return
		}
		return
	},
	syncGet: func() (err error) {
		var res []byte
		if err = switchboard.Switch.Call(*host, "Synchronizable.Get", []byte(flag.Args()[0]), &res); err != nil {
			return
		}
		fmt.Println(string(res))
		return
	},
	debugDump: func() (err error) {
		r := &ring.Ring{}
		if err = switchboard.Switch.Call(*host, "Node.GetRing", struct{}{}, r); err != nil {
			return
		}
		r.Each(func(p *ring.Peer) {
			if err := switchboard.Switch.Call(p.ConnectionString, "Debug.Dump", struct{}{}, nil); err != nil {
				panic(err)
			}
		})
		return
	},
}

var host = flag.String("host", "localhost:9797", "Where to connect")
var cmd = flag.String("cmd", "", fmt.Sprintf("What to do: %+v", commands))
var readAge = flag.Duration("readAge", 0, "Age of read timestamp when applicable")
var writeAge = flag.Duration("writeAge", 0, "Age of write timestamp when applicable")

func main() {
	flag.Parse()

	if *cmd == "" {
		flag.Usage()
		os.Exit(1)
	}

	if f, found := commands[*cmd]; found {
		if err := f(); err != nil {
			fmt.Println(err)
		}
	} else {
		flag.Usage()
		os.Exit(2)
	}
}
