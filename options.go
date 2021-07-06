package canal

import (
	"time"
)

type clientOptions struct {
	destination          string
	clientID             int
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
