// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package fuse keeps request read-buffer pooling in explicit bounded LIFO
// shards so the package, not the runtime, sets the retained-buffer ceiling.
// Each active fuseFD owns one shard, and the shard caps sum to one server-level
// retained-buffer budget. Hot request paths use only the fd's shard lock, while
// each serving Server runs one background reclaimer goroutine that decays idle
// shard targets and trims retained buffers.
package fuse

import (
	"sync"
	"time"
)

const (
	// Each retained buffer is MaxWrite + maxInputSize plus a logicalBlockSize
	// alignment pad. The 128-buffer ceiling bounds retained memory near
	// 128 * MaxWrite at the default write size.
	readPoolMaxRetainedBuffers = 128

	// Package-internal cadence for read-buffer reclamation. One elapsed
	// interval decays each shard's targetRetained by one toward inUse.
	bytePoolReclaimInterval = 2 * time.Second
)

// bytePool manages fixed-size []byte buffer shards.
// bytePool must be initialized via init, newBytePool, or newBytePoolWithClock;
// direct struct literals are unsupported.
type bytePool struct {
	// New supports package tests that construct Servers with the old
	// sync.Pool-style allocator shape. Production initialization uses init.
	New func() interface{}

	allocator func() []byte
	// now is injected for tests via newBytePoolWithClock; newBytePool sets time.Now.
	now         func() time.Time
	maxRetained int
	shards      []*bytePoolShard

	reclaimerMu   sync.Mutex
	reclaimerStop chan struct{}
	reclaimerDone chan struct{}
}

// bytePoolShard retains fixed-size []byte buffers up to its static shard cap.
type bytePoolShard struct {
	mu        sync.Mutex
	buffers   [][]byte
	allocator func() []byte

	maxRetained    int
	targetRetained int
	inUse          int
	lastReclaim    time.Time
	closing        bool
}

func newBytePool(size int, allocator func() []byte) *bytePool {
	return newBytePoolWithClock(size, allocator, time.Now)
}

func newBytePoolWithClock(size int, allocator func() []byte, now func() time.Time) *bytePool {
	bp := &bytePool{}
	bp.init(size, allocator, now)
	return bp
}

func (bp *bytePool) init(size int, allocator func() []byte, now func() time.Time) {
	if size < 0 {
		size = 0
	}
	if size > readPoolMaxRetainedBuffers {
		size = readPoolMaxRetainedBuffers
	}
	if now == nil {
		now = time.Now
	}

	bp.New = nil
	bp.allocator = allocator
	bp.now = now
	bp.maxRetained = size
	bp.setShardCount(1)
}

func (bp *bytePool) initialized() bool {
	return bp.allocator != nil && len(bp.shards) > 0
}

// setShardCount sets the setup-time shard count and recomputes static caps.
func (bp *bytePool) setShardCount(n int) {
	if n < 1 {
		n = 1
	}
	now := bp.now()
	for len(bp.shards) < n {
		bp.shards = append(bp.shards, &bytePoolShard{
			allocator:   bp.allocator,
			lastReclaim: now,
		})
	}
	for _, shard := range bp.shards[n:] {
		shard.setMaxRetained(0, now)
	}
	bp.shards = bp.shards[:n]

	base := bp.maxRetained / n
	extra := bp.maxRetained % n
	for i, shard := range bp.shards {
		maxRetained := base
		if i < extra {
			maxRetained++
		}
		shard.setMaxRetained(maxRetained, now)
	}
}

func (bp *bytePool) shard(i int) *bytePoolShard {
	return bp.shards[i]
}

// bindFDs assigns one shard per fd. Call after all fuseFDs exist and before
// Serve starts; it is not safe to call concurrently with Get/Put.
func (bp *bytePool) bindFDs(fds []*fuseFD) {
	bp.setShardCount(len(fds))
	for i, fd := range fds {
		fd.readPool = bp.shard(i)
	}
}

// NumPooled returns the number of items currently pooled across all shards.
func (bp *bytePool) NumPooled() int {
	total := 0
	for _, shard := range bp.shards {
		total += shard.NumPooled()
	}
	return total
}

func (bp *bytePool) reclaim(now time.Time) {
	for _, shard := range bp.shards {
		shard.reclaim(now)
	}
}

func (bp *bytePool) drain() {
	for _, shard := range bp.shards {
		shard.drain()
	}
}

func (bp *bytePool) closeAndDrain() {
	for _, shard := range bp.shards {
		shard.closeAndDrain()
	}
}

func (bp *bytePool) startReclaimer() {
	bp.reclaimerMu.Lock()
	if bp.reclaimerStop != nil || bp.reclaimerDone != nil {
		bp.reclaimerMu.Unlock()
		return
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	bp.reclaimerStop = stop
	bp.reclaimerDone = done
	bp.reclaimerMu.Unlock()

	go func() {
		ticker := time.NewTicker(bytePoolReclaimInterval)
		defer ticker.Stop()
		defer close(done)
		for {
			select {
			case now := <-ticker.C:
				bp.reclaim(now)
			case <-stop:
				return
			}
		}
	}()
}

func (bp *bytePool) stopReclaimer() {
	bp.reclaimerMu.Lock()
	stop := bp.reclaimerStop
	done := bp.reclaimerDone
	if done == nil {
		bp.reclaimerMu.Unlock()
		return
	}
	if stop != nil {
		bp.reclaimerStop = nil
		close(stop)
	}
	bp.reclaimerMu.Unlock()

	<-done

	bp.reclaimerMu.Lock()
	// Only nil reclaimerDone if unchanged; a concurrent startReclaimer may have installed a new one.
	if bp.reclaimerDone == done {
		bp.reclaimerDone = nil
	}
	bp.reclaimerMu.Unlock()
}

// Get gets a []byte from the shard, or creates a new one if none are available.
func (s *bytePoolShard) Get() (b []byte) {
	s.mu.Lock()

	s.inUse++
	if s.targetRetained < s.inUse {
		s.targetRetained = s.inUse
		if s.targetRetained > s.maxRetained {
			s.targetRetained = s.maxRetained
		}
	}

	n := len(s.buffers)
	if n > 0 {
		b = s.buffers[n-1]
		s.buffers[n-1] = nil
		s.buffers = s.buffers[:n-1]
	}
	s.mu.Unlock()

	if b == nil {
		b = s.allocator()
	}
	return b
}

// Put returns the given buffer to the shard.
func (s *bytePoolShard) Put(b []byte) {
	b = b[:cap(b)]

	s.mu.Lock()
	if s.inUse > 0 {
		s.inUse--
	}
	if !s.closing && len(s.buffers) < s.targetRetained {
		s.buffers = append(s.buffers, b)
	}
	s.mu.Unlock()
}

// NumPooled returns the number of items currently pooled.
func (s *bytePoolShard) NumPooled() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.buffers)
}

func (s *bytePoolShard) reclaim(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if now.Before(s.lastReclaim) {
		s.lastReclaim = now
		return
	}
	elapsed := now.Sub(s.lastReclaim)
	intervals := int(elapsed / bytePoolReclaimInterval)
	if intervals == 0 {
		return
	}
	if s.targetRetained > s.inUse {
		decay := intervals
		if maxDecay := s.targetRetained - s.inUse; decay > maxDecay {
			decay = maxDecay
		}
		s.targetRetained -= decay
	}
	s.lastReclaim = s.lastReclaim.Add(time.Duration(intervals) * bytePoolReclaimInterval)
	if len(s.buffers) > s.targetRetained {
		s.trimLocked(s.targetRetained)
	}
}

func (s *bytePoolShard) drain() {
	s.mu.Lock()
	s.trimLocked(0)
	// Reset target to in-flight count so a Put racing drain can still retain live request buffers.
	s.targetRetained = s.inUse
	if s.targetRetained > s.maxRetained {
		s.targetRetained = s.maxRetained
	}
	s.mu.Unlock()
}

func (s *bytePoolShard) closeAndDrain() {
	s.mu.Lock()
	s.closing = true
	s.targetRetained = 0
	s.trimLocked(0)
	s.mu.Unlock()
}

func (s *bytePoolShard) setMaxRetained(max int, now time.Time) {
	if max < 0 {
		max = 0
	}

	s.mu.Lock()
	s.maxRetained = max
	if s.targetRetained > s.maxRetained {
		s.targetRetained = s.maxRetained
	}
	if len(s.buffers) > s.targetRetained {
		s.trimLocked(s.targetRetained)
	}
	s.resizeBuffersLocked(s.maxRetained)
	s.lastReclaim = now
	s.mu.Unlock()
}

// trimLocked nils and removes retained buffers above n; caller must hold s.mu.
func (s *bytePoolShard) trimLocked(n int) {
	for len(s.buffers) > n {
		last := len(s.buffers) - 1
		s.buffers[last] = nil
		s.buffers = s.buffers[:last]
	}
}

// resizeBuffersLocked preallocates shard storage to the assigned cap; caller must hold s.mu.
func (s *bytePoolShard) resizeBuffersLocked(n int) {
	if cap(s.buffers) == n {
		return
	}
	resized := make([][]byte, len(s.buffers), n)
	copy(resized, s.buffers)
	s.buffers = resized
}
