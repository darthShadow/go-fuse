// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package fuse keeps read-buffer pooling as a mutex-guarded LIFO slice so the
// package, not the runtime, sets the retained-buffer ceiling.
// A kernel-cache-driven scanner workload can burst into many large read
// buffers, and a sync.Pool-only design cannot cap that idle footprint because
// the runtime decides when pool-local state is cleared.
// A channel-only pool was also ruled out because this fork needs reclamation
// without adding a goroutine, timer, or other out-of-band machinery.
// The retained set therefore lives behind one lock, reuses the hottest buffer
// first, and shrinks entirely from Get and Put.
// That preserves demand-fill behavior while keeping the retained memory budget
// deterministic.
package fuse

import (
	"sync"
	"time"
)

const (
	// Each retained buffer is MaxWrite + maxInputSize plus a logicalBlockSize alignment
	// pad; the 128-buffer ceiling bounds retained memory near 128 * MaxWrite (~16 MiB
	// at go-fuse's 128 KiB default). This is a hard budget, not a soft target: excess
	// idle buffers are dropped instead of being retained past this count.
	readPoolMaxRetainedBuffers = 128
	// Reclaim drops at most one retained buffer per interval, so a full 128-buffer
	// post-burst excess drains in about 256 seconds, or 4.3 minutes, while the
	// pool is still active. The slow decay avoids oscillation during bursts while
	// still releasing the excess within a few minutes once demand subsides.
	bytePoolReclaimInterval = 2 * time.Second
)

// bytePool retains fixed-size []byte buffers up to a bounded, decaying target.
// bytePool must be initialized via newBytePool or newBytePoolWithClock; direct struct literals are unsupported.
type bytePool struct {
	mu        sync.Mutex
	buffers   [][]byte
	allocator func() interface{}
	// now is injected for tests via newBytePoolWithClock; newBytePool sets time.Now.
	now            func() time.Time
	maxRetained    int
	targetRetained int
	inUse          int
	lastReclaim    time.Time
}

func newBytePool(size int, allocator func() interface{}) bytePool {
	return newBytePoolWithClock(size, allocator, time.Now)
}

func newBytePoolWithClock(size int, allocator func() interface{}, now func() time.Time) bytePool {
	if size < 0 {
		size = 0
	}
	if size > readPoolMaxRetainedBuffers {
		size = readPoolMaxRetainedBuffers
	}
	if now == nil {
		now = time.Now
	}

	return bytePool{
		buffers:     make([][]byte, 0, size),
		allocator:   allocator,
		now:         now,
		maxRetained: size,
		lastReclaim: now(),
	}
}

// Get gets a []byte from the bytePool, or creates a new one if none are
// available in the pool.
func (bp *bytePool) Get() (b []byte) {
	bp.mu.Lock()
	bp.reclaimLocked(bp.now())

	bp.inUse++
	if bp.inUse > bp.targetRetained {
		bp.targetRetained = bp.inUse
		if bp.targetRetained > bp.maxRetained {
			bp.targetRetained = bp.maxRetained
		}
	}

	n := len(bp.buffers)
	if n > 0 {
		b = bp.buffers[n-1]
		bp.buffers[n-1] = nil
		bp.buffers = bp.buffers[:n-1]
	}
	bp.mu.Unlock()

	if b == nil {
		b = bp.allocator().([]byte)
	}
	return b
}

// Put returns the given Buffer to the bytePool.
func (bp *bytePool) Put(b []byte) {
	b = b[:cap(b)]

	bp.mu.Lock()
	if bp.inUse > 0 {
		bp.inUse--
	}
	bp.reclaimLocked(bp.now())

	// targetRetained <= maxRetained by construction; first conjunct is sufficient.
	if len(bp.buffers) < bp.targetRetained {
		bp.buffers = append(bp.buffers, b)
	}
	bp.mu.Unlock()
}

// NumPooled returns the number of items currently pooled.
func (bp *bytePool) NumPooled() int {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	return len(bp.buffers)
}

// reclaimLocked applies fast attack and slow decay: Get raises
// targetRetained to demand capped at maxRetained, but decay trims one step per interval.
func (bp *bytePool) reclaimLocked(now time.Time) {
	if now.Before(bp.lastReclaim) {
		bp.lastReclaim = now
		return
	}
	if now.Sub(bp.lastReclaim) < bytePoolReclaimInterval {
		return
	}

	bp.lastReclaim = now
	if bp.targetRetained > bp.inUse {
		bp.targetRetained--
	}
	if len(bp.buffers) > bp.targetRetained {
		bp.dropOneLocked()
	}
}

// dropOneLocked nils and removes the last retained buffer for GC; caller must hold bp.mu.
func (bp *bytePool) dropOneLocked() {
	n := len(bp.buffers)
	if n == 0 {
		return
	}
	bp.buffers[n-1] = nil
	bp.buffers = bp.buffers[:n-1]
}
