package canal

import (
	"time"
)

type Options struct {
	Address      string // host:port address.
	Username     string
	Password     string
	Destination  int
	ClientID     string
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func (opt *Options) init() {
	if opt.Address == "" {
		opt.Address = "localhost:6379"
	}

	if opt.DialTimeout == 0 {
		opt.DialTimeout = 5 * time.Second
	}
}
