package canal

import (
	"time"
)

type Options struct {
	// host:port address.
	Addr         string
	Username     string
	Password     string
	Destination  int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func (opt *Options) init() {
	if opt.Addr == "" {
		opt.Addr = "localhost:6379"
	}

	if opt.DialTimeout == 0 {
		opt.DialTimeout = 5 * time.Second
	}
}
