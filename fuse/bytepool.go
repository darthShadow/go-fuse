// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fuse

import (
	"sync"
)

/**
Some additional reading:
    * https://blog.cloudflare.com/recycling-memory-buffers-in-go/
    * https://blog.questionable.services/article/using-buffer-pools-with-go/
*/

// bytePool implements a leaky pool of []byte in the form of a bounded
// channel.
type bytePool struct {
	channel chan []byte
	pool    sync.Pool
}

func newBytePool(size int, allocator func() interface{}) bytePool {
	return bytePool{
		channel: make(chan []byte, size),
		pool:    sync.Pool{New: allocator},
	}
}

// Get gets a []byte from the bytePool, or creates a new one if none are
// available in the pool.
func (bp *bytePool) Get() (b []byte) {
	select {
	case b = <-bp.channel:
		// reuse existing buffer
	default:
		// create new buffer
		b = bp.pool.Get().([]byte)
	}
	return
}

// Put returns the given Buffer to the bytePool.
func (bp *bytePool) Put(b []byte) {
	b = b[:cap(b)]
	select {
	case bp.channel <- b:
		// buffer went back into the channel
	default:
		// buffer didn't go back into the channel, put it back into the pool
		bp.pool.Put(b)
	}
}

// NumPooled returns the number of items currently pooled.
func (bp *bytePool) NumPooled() int {
	return len(bp.channel)
}
