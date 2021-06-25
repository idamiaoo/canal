package canal

import (
	"net"
	"sync"
)

type Client struct {
	opt     *Options
	netConn net.Conn
	pool    sync.Pool
}

func NewClient(opt *Options) *Client {
	opt.init()
	return &Client{
		opt: opt,
	}
}

func (c *Client) initConn() {

}
