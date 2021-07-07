package canal

import (
	"time"
)

type clientOptions struct {
	clientID             int
	username             string
	password             string
	dialTimeout          time.Duration
	readTimeout          time.Duration
	writeTimeout         time.Duration
	lazyParseEntry       bool
	rollbackOnConnect    bool
	rollbackOnDisConnect bool
}

func defaultClientOptions() clientOptions {
	return clientOptions{
		dialTimeout: 5 * time.Second,
	}
}

// ClientOption .
type ClientOption interface {
	apply(*clientOptions)
}

type funcClientOption struct {
	f func(*clientOptions)
}

func (fco *funcClientOption) apply(co *clientOptions) {
	fco.f(co)
}

func newFuncDialOption(f func(*clientOptions)) *funcClientOption {
	return &funcClientOption{
		f: f,
	}
}

func EnableLazyParseEntry() ClientOption {
	return newFuncDialOption(func(o *clientOptions) {
		o.lazyParseEntry = true
	})
}
