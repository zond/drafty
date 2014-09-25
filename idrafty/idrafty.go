package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/zond/drafty/switchboard"
)

const (
	syncPut = "sync.put"
	syncGet = "sync.get"
	syncDel = "sync.del"
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

var commands = map[string]func(){
	syncPut: func() {
		if err := switchboard.Switch.Call(*host, "Synchronizable.Put", [2][]byte{
			[]byte(flag.Args()[0]),
			append(append(append(durToByte(*readAge), durToByte(*writeAge)...), 0), []byte(flag.Args()[1])...),
		}, nil); err != nil {
			fmt.Println(err)
		}
	},
	syncGet: func() {
		var res []byte
		if err := switchboard.Switch.Call(*host, "Synchronizable.Get", []byte(flag.Args()[0]), &res); err != nil {
			fmt.Println(err)
		} else {
			fmt.Println(string(res))
		}
	},
}

var host = flag.String("host", "localhost:9797", "Where to connect")
var cmd = flag.String("cmd", "", fmt.Sprintf("What to do: %v", commands))
var readAge = flag.Duration("readAge", 0, "Age of read timestamp when applicable")
var writeAge = flag.Duration("writeAge", 0, "Age of write timestamp when applicable")

func main() {
	flag.Parse()

	if *cmd == "" {
		flag.Usage()
		os.Exit(1)
	}

	if f, found := commands[*cmd]; found {
		f()
	} else {
		flag.Usage()
		os.Exit(2)
	}
}
