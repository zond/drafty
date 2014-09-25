package client

import (
	"github.com/zond/drafty/node/ring"
)

type Client struct {
	ring *ring.Ring
}

func New(host string) (result *Client, err error) {
	return
}
