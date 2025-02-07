package fs

import (
	"sync/atomic"
	"testing"
)

func TestNodeMapInit(t *testing.T) {
	m := &shardedMap[uint64, *inode]{}
	m.Init()

	// Verify all shards are initialized
	for i := 0; i < mapShards; i++ {
		if m.shards[i] == nil {
			t.Errorf("shard %d was not initialized", i)
		}
		if m.shards[i].pool == nil {
			t.Errorf("shard %d pool was not initialized", i)
		}
	}
}

func TestNodeMapShardOperations(t *testing.T) {
	pool := &mapPool[uint64, *inode]{
		defaultSize: defaultMapSize,
		maxSize:     maxMapSize,
	}
	shard := &mapShard[uint64, *inode]{pool: pool}

	// Test lazy initialization
	if shard.entries != nil {
		t.Error("entries should be nil before first Set")
	}

	// Test first Set creates map
	node := &inode{}
	shard.Set(1, node)
	if shard.entries == nil {
		t.Error("entries should be initialized after Set")
	}

	// Verify count tracking
	if count := atomic.LoadInt32(&shard.count); count != 1 {
		t.Errorf("count = %d, want 1", count)
	}

	// Test overwrite behavior
	node2 := &inode{}
	shard.Set(1, node2)
	if got, _ := shard.Get(1); got != node2 {
		t.Error("Set did not overwrite existing entry")
	}
	if count := atomic.LoadInt32(&shard.count); count != 1 {
		t.Errorf("count after overwrite = %d, want 1", count)
	}

	// Test Delete with count verification
	shard.Delete(1)
	if count := atomic.LoadInt32(&shard.count); count != 0 {
		t.Errorf("count after delete = %d, want 0", count)
	}
}

func TestNodeMapShardConcurrent(t *testing.T) {
	pool := &mapPool[uint64, *inode]{
		defaultSize: defaultMapSize,
		maxSize:     maxMapSize,
	}
	shard := &mapShard[uint64, *inode]{pool: pool}
	done := make(chan bool)
	const ops = 1000

	// Multiple writers to same key
	go func() {
		for i := 0; i < ops; i++ {
			shard.Set(1, &inode{})
		}
		done <- true
	}()

	go func() {
		for i := 0; i < ops; i++ {
			shard.Set(1, &inode{})
		}
		done <- true
	}()

	// Concurrent reader
	go func() {
		for i := 0; i < ops; i++ {
			shard.Get(1)
		}
		done <- true
	}()

	// Concurrent deleter
	go func() {
		for i := 0; i < ops; i++ {
			shard.Delete(1)
		}
		done <- true
	}()

	// Wait for all operations
	for i := 0; i < 4; i++ {
		<-done
	}

	// Verify shard is in consistent state
	count := atomic.LoadInt32(&shard.count)
	if count < 0 {
		t.Errorf("count became negative: %d", count)
	}
	high := atomic.LoadInt32(&shard.countHigh)
	if count > high {
		t.Errorf("count (%d) exceeded high water mark (%d)", count, high)
	}
}

func TestNodeMapShardCompactionThresholds(t *testing.T) {
	pool := &mapPool[uint64, *inode]{
		defaultSize: defaultMapSize,
		maxSize:     maxMapSize,
	}
	shard := &mapShard[uint64, *inode]{pool: pool}

	// Fill beyond maxMapSize
	for i := uint64(0); i < maxMapSize+100; i++ {
		shard.Set(i, &inode{})
	}

	// Record size before deletion
	originalSize := len(shard.entries)

	// Force compact
	shard.Compact()

	// Verify map wasn't compacted if above maxMapSize
	if len(shard.entries) != originalSize {
		t.Error("map was compacted despite being above maxMapSize")
	}

	// Now delete everything
	for i := uint64(0); i < maxMapSize+100; i++ {
		shard.Delete(i)
	}

	// Verify complete cleanup
	if shard.entries != nil {
		t.Error("entries not nil after deleting everything")
	}
	if count := atomic.LoadInt32(&shard.count); count != 0 {
		t.Errorf("count = %d after complete deletion", count)
	}
	if high := atomic.LoadInt32(&shard.countHigh); high != 0 {
		t.Errorf("high water mark = %d after complete deletion", high)
	}
}

func TestNodeMapEdgeCases(t *testing.T) {
	m := &shardedMap[uint64, *inode]{}
	m.Init()

	// Test setting nil node
	m.Set(1, nil)
	if got, _ := m.Get(1); got != nil {
		t.Error("Set(nil) should not store anything")
	}

	// Test deleting non-existent key
	m.Delete(999)

	// Test high water mark growth
	node := &inode{}
	for i := uint64(0); i < 10; i++ {
		m.Set(i, node)
	}

	shard := m.getMapShard(0)
	high := atomic.LoadInt32(&shard.countHigh)

	// Add more to same shard
	for i := uint64(mapShards); i < mapShards+10; i++ {
		m.Set(i, node)
	}

	newHigh := atomic.LoadInt32(&shard.countHigh)
	if newHigh <= high {
		t.Error("high water mark did not increase with more entries")
	}

	// Test multiple deletes of same key
	m.Delete(1)
	m.Delete(1) // Should not make count go negative

	if count := atomic.LoadInt32(&shard.count); count < 0 {
		t.Errorf("multiple deletes caused negative count: %d", count)
	}
}

func TestConcurrentOperations(t *testing.T) {
	m := &shardedMap[uint64, *inode]{}
	m.Init()
	done := make(chan bool)

	// Concurrent writers
	go func() {
		for i := uint64(0); i < 1000; i++ {
			m.Set(i, &inode{})
		}
		done <- true
	}()

	// Concurrent readers
	go func() {
		for i := uint64(0); i < 1000; i++ {
			_, _ = m.Get(i)
		}
		done <- true
	}()

	// Concurrent deleters
	go func() {
		for i := uint64(0); i < 1000; i += 2 {
			m.Delete(i)
		}
		done <- true
	}()

	// Wait for all operations to complete
	for i := 0; i < 3; i++ {
		<-done
	}
}

func TestMapResizing(t *testing.T) {
	m := &shardedMap[uint64, *inode]{}
	m.Init()

	// Test growth beyond defaultMapSize
	for i := uint64(0); i < defaultMapSize*2; i++ {
		m.Set(i, &inode{})
	}

	// Verify all entries are still accessible
	for i := uint64(0); i < defaultMapSize*2; i++ {
		if got, _ := m.Get(i); got == nil {
			t.Errorf("after resize, Get(%d) = nil, want non-nil", i)
		}
	}
}

func TestHighWaterMark(t *testing.T) {
	m := &shardedMap[uint64, *inode]{}
	m.Init()
	shard := m.getMapShard(0)

	valueCount := mapShrinkFactor * 100

	// Fill to establish high water mark
	for i := uint64(0); i < uint64(valueCount); i++ {
		shard.Set(i, &inode{})
	}

	initialHigh := atomic.LoadInt32(&shard.countHigh)

	// Delete some entries but stay above 1/4
	for i := uint64(0); i < uint64(valueCount)/2; i++ {
		shard.Delete(i)
	}

	// High water mark should not have changed
	if atomic.LoadInt32(&shard.countHigh) != initialHigh {
		t.Error("high water mark changed when count > 1/4")
	}

	// Now delete below 1/4
	for i := uint64(valueCount / 2); i < uint64((mapShrinkFactor-1)*valueCount/mapShrinkFactor)+1; i++ {
		shard.Delete(i)
	}

	// High water mark should have been adjusted
	if atomic.LoadInt32(&shard.countHigh) >= initialHigh {
		t.Error("high water mark not adjusted when count < 1/4")
	}
}

func TestPoolMemoryLeak(t *testing.T) {
	m := &shardedMap[uint64, *inode]{}
	m.Init()

	// Create and delete many maps to stress the pool
	for i := 0; i < 100; i++ {
		// Fill a shard
		for j := uint64(0); j < 100; j++ {
			m.Set(j, &inode{})
		}

		// Delete everything to return map to pool
		for j := uint64(0); j < 100; j++ {
			m.Delete(j)
		}

		// Verify shard is empty
		shard := m.getMapShard(0)
		if shard.entries != nil {
			t.Error("shard not properly cleared after compaction")
		}
	}
}

func TestNodeMapOperations(t *testing.T) {
	m := &shardedMap[uint64, *inode]{}
	m.Init()

	// Test Set and Get
	node := &inode{}
	m.Set(1, node)
	if got, _ := m.Get(1); got != node {
		t.Errorf("Get(1) = %v, want %v", got, node)
	}

	// Test non-existent key
	if got, _ := m.Get(2); got != nil {
		t.Errorf("Get(2) = %v, want nil", got)
	}

	// Test Delete
	m.Delete(1)
	if got, _ := m.Get(1); got != nil {
		t.Errorf("after Delete(1), Get(1) = %v, want nil", got)
	}
}

func TestNodeMapCompaction(t *testing.T) {
	m := &shardedMap[uint64, *inode]{}
	m.Init()

	// Add some nodes
	for i := uint64(0); i < 100; i++ {
		m.Set(i, &inode{})
	}

	// Delete most nodes to trigger compaction
	for i := uint64(0); i < 75; i++ {
		m.Delete(i)
	}

	// Verify remaining nodes are still accessible
	for i := uint64(75); i < 100; i++ {
		if got, _ := m.Get(i); got == nil {
			t.Errorf("after compaction, Get(%d) = nil, want non-nil", i)
		}
	}
}

func TestNodeMapSharding(t *testing.T) {
	m := &shardedMap[uint64, *inode]{}
	m.Init()

	// Test that different IDs go to different shards
	id1 := uint64(1)
	id2 := uint64(2)

	shard1 := m.getMapShardKey(id1)
	shard2 := m.getMapShardKey(id2)

	if shard1 == shard2 {
		t.Errorf("IDs %d and %d mapped to same shard %d", id1, id2, shard1)
	}

	// Verify shard calculation
	if shard1 != id1&(mapShards-1) {
		t.Errorf("incorrect shard calculation for id %d", id1)
	}
}
