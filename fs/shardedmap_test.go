package fs

import (
	"testing"
	"time"
)

type inode struct{}

func shardCount[K comparable, V any](shard *mapShard[K, V]) int32 {
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	return shard.count
}

func shardCountHigh[K comparable, V any](shard *mapShard[K, V]) int32 {
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	return shard.countHigh
}

func TestNodeMapInit(t *testing.T) {
	m := &shardedMap[uint64, *inode]{}
	m.Init()

	// Verify all shards are initialized
	for i := 0; i < mapShards; i++ {
		if m.shards[i] == nil {
			t.Errorf("shard %d was not initialized", i)
		}
	}
}

func TestNodeMapShardOperations(t *testing.T) {
	shard := &mapShard[uint64, *inode]{}

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
	if count := shardCount(shard); count != 1 {
		t.Errorf("count = %d, want 1", count)
	}

	// Test overwrite behavior
	node2 := &inode{}
	shard.Set(1, node2)
	if got, _ := shard.Get(1); got != node2 {
		t.Error("Set did not overwrite existing entry")
	}
	if count := shardCount(shard); count != 1 {
		t.Errorf("count after overwrite = %d, want 1", count)
	}

	// Test Delete with count verification
	candidate := shard.Delete(1)
	if !candidate {
		t.Error("Delete returned candidate = false, want true")
	}
	if count := shardCount(shard); count != 0 {
		t.Errorf("count after delete = %d, want 0", count)
	}
	if shard.entries == nil {
		t.Error("entries should remain initialized before compaction")
	}
}

func TestNodeMapShardConcurrent(t *testing.T) {
	shard := &mapShard[uint64, *inode]{}
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
	count := shardCount(shard)
	if count < 0 {
		t.Errorf("count became negative: %d", count)
	}
	high := shardCountHigh(shard)
	if count > high {
		t.Errorf("count (%d) exceeded high water mark (%d)", count, high)
	}
}

func TestNodeMapLoadOrStore(t *testing.T) {
	t.Run("miss stores value", func(t *testing.T) {
		m := &shardedMap[uint64, *inode]{}
		m.Init()

		node1 := &inode{}
		actual, loaded := m.LoadOrStore(1, node1)
		if actual != node1 {
			t.Errorf("LoadOrStore miss actual = %v, want %v", actual, node1)
		}
		if loaded {
			t.Error("LoadOrStore miss loaded = true, want false")
		}
		if got, ok := m.Get(1); !ok || got != node1 {
			t.Errorf("Get(1) after LoadOrStore miss = (%v, %v), want (%v, true)", got, ok, node1)
		}
		if count := m.Count(); count != 1 {
			t.Errorf("Count after LoadOrStore miss = %d, want 1", count)
		}
	})

	t.Run("hit returns existing", func(t *testing.T) {
		m := &shardedMap[uint64, *inode]{}
		m.Init()

		node1 := &inode{}
		node2 := &inode{}
		m.Set(1, node1)
		actual, loaded := m.LoadOrStore(1, node2)
		if actual != node1 {
			t.Errorf("LoadOrStore hit actual = %v, want %v", actual, node1)
		}
		if !loaded {
			t.Error("LoadOrStore hit loaded = false, want true")
		}
		if got, ok := m.Get(1); !ok || got != node1 {
			t.Errorf("Get(1) after LoadOrStore hit = (%v, %v), want (%v, true)", got, ok, node1)
		}
		if count := m.Count(); count != 1 {
			t.Errorf("Count after LoadOrStore hit = %d, want 1", count)
		}
	})
}

func TestNodeMapEdgeCases(t *testing.T) {
	m := &shardedMap[uint64, *inode]{}
	m.Init()

	// Test setting nil node stores a nil entry
	m.Set(1, nil)
	if got, ok := m.Get(1); !ok || got != nil {
		t.Errorf("Set(nil) = (%v, %v), want stored nil", got, ok)
	}
	if count := m.Count(); count != 1 {
		t.Errorf("count after Set(nil) = %d, want 1", count)
	}

	// Test deleting non-existent key
	m.Delete(999)

	// Test high water mark growth
	node := &inode{}
	for i := uint64(0); i < 10; i++ {
		m.Set(i, node)
	}

	shard := m.getMapShard(0)
	high := shardCountHigh(shard)

	// Add more to same shard
	for i := uint64(mapShards); i < mapShards+10; i++ {
		m.Set(i, node)
	}

	newHigh := shardCountHigh(shard)
	if newHigh <= high {
		t.Error("high water mark did not increase with more entries")
	}

	// Test multiple deletes of same key
	m.Delete(1)
	m.Delete(1) // Should not make count go negative

	if count := shardCount(shard); count < 0 {
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

func TestNodeMapDeleteIf(t *testing.T) {
	t.Run("missing key", func(t *testing.T) {
		m := &shardedMap[uint64, *inode]{}
		m.Init()

		deleted, candidate := m.DeleteIf(1, func(*inode) bool { return true })
		if deleted {
			t.Error("DeleteIf missing deleted = true, want false")
		}
		if candidate {
			t.Error("DeleteIf missing candidate = true, want false")
		}
		if count := m.Count(); count != 0 {
			t.Errorf("Count after DeleteIf missing = %d, want 0", count)
		}
	})

	t.Run("predicate rejects", func(t *testing.T) {
		m := &shardedMap[uint64, *inode]{}
		m.Init()

		node := &inode{}
		m.Set(1, node)
		deleted, candidate := m.DeleteIf(1, func(*inode) bool { return false })
		if deleted {
			t.Error("DeleteIf rejected deleted = true, want false")
		}
		if candidate {
			t.Error("DeleteIf rejected candidate = true, want false")
		}
		if got, ok := m.Get(1); !ok || got != node {
			t.Errorf("Get(1) after DeleteIf rejected = (%v, %v), want (%v, true)", got, ok, node)
		}
		if count := m.Count(); count != 1 {
			t.Errorf("Count after DeleteIf rejected = %d, want 1", count)
		}
	})

	t.Run("predicate accepts last entry", func(t *testing.T) {
		m := &shardedMap[uint64, *inode]{}
		m.Init()

		node := &inode{}
		m.Set(1, node)
		deleted, candidate := m.DeleteIf(1, func(got *inode) bool { return got == node })
		if !deleted {
			t.Error("DeleteIf accepted deleted = false, want true")
		}
		if !candidate {
			t.Error("DeleteIf accepted candidate = false, want true")
		}
		if got, ok := m.Get(1); ok || got != nil {
			t.Errorf("Get(1) after DeleteIf accepted = (%v, %v), want (nil, false)", got, ok)
		}
		if count := m.Count(); count != 0 {
			t.Errorf("Count after DeleteIf accepted = %d, want 0", count)
		}
		shard := m.getMapShard(m.getMapShardKey(1))
		if shard.entries == nil {
			t.Error("entries should remain initialized before compaction")
		}
	})
}

func TestNodeMapCompactCandidates(t *testing.T) {
	now := time.Unix(1000, 0)

	newTargetShard := func() (*shardedMap[uint64, *inode], *mapShard[uint64, *inode]) {
		m := &shardedMap[uint64, *inode]{}
		m.Init()
		return m, m.getMapShard(0)
	}

	t.Run("empty shard cleared", func(t *testing.T) {
		m, shard := newTargetShard()
		node := &inode{}
		shard.Set(0, node)
		candidate := shard.Delete(0)
		if !candidate {
			t.Error("Delete returned candidate = false, want true")
		}

		m.compactCandidates(now)

		if shard.entries != nil {
			t.Error("entries after empty shard compaction = non-nil, want nil")
		}
		if count := shardCount(shard); count != 0 {
			t.Errorf("count after empty shard compaction = %d, want 0", count)
		}
		if high := shardCountHigh(shard); high != 0 {
			t.Errorf("countHigh after empty shard compaction = %d, want 0", high)
		}
		if shard.compactCandidate {
			t.Error("compactCandidate after empty shard compaction = true, want false")
		}
		if !shard.lastCompact.Equal(now) {
			t.Errorf("lastCompact after empty shard compaction = %v, want %v", shard.lastCompact, now)
		}
	})

	t.Run("eligible sparse shard copied", func(t *testing.T) {
		m, shard := newTargetShard()
		node := &inode{}
		for i := uint64(0); i < 32; i++ {
			m.Set(i*mapShards, node)
		}
		for i := uint64(0); i < 29; i++ {
			m.Delete(i * mapShards)
		}
		shard.lastCompact = now.Add(-mapCompactMinInterval)
		oldID := mapPoolIdentity(shard.entries)

		m.compactCandidates(now)

		if newID := mapPoolIdentity(shard.entries); newID == oldID {
			t.Errorf("map identity after eligible compaction = %d, want different from %d", newID, oldID)
		}
		if count := shardCount(shard); count != 3 {
			t.Errorf("count after eligible compaction = %d, want 3", count)
		}
		if high := shardCountHigh(shard); high != 3 {
			t.Errorf("countHigh after eligible compaction = %d, want 3", high)
		}
		if shard.compactCandidate {
			t.Error("compactCandidate after eligible compaction = true, want false")
		}
		if !shard.lastCompact.Equal(now) {
			t.Errorf("lastCompact after eligible compaction = %v, want %v", shard.lastCompact, now)
		}
		for i := uint64(29); i < 32; i++ {
			key := i * mapShards
			if got, ok := m.Get(key); !ok || got != node {
				t.Errorf("Get(%d) after eligible compaction = (%v, %v), want (%v, true)", key, got, ok, node)
			}
		}
	})

	t.Run("rate-limited candidate skipped", func(t *testing.T) {
		m, shard := newTargetShard()
		node := &inode{}
		for i := uint64(0); i < 32; i++ {
			m.Set(i*mapShards, node)
		}
		for i := uint64(0); i < 29; i++ {
			m.Delete(i * mapShards)
		}
		shard.lastCompact = now
		oldID := mapPoolIdentity(shard.entries)
		oldHigh := shardCountHigh(shard)

		m.compactCandidates(now)

		if newID := mapPoolIdentity(shard.entries); newID != oldID {
			t.Errorf("map identity after rate-limited compaction = %d, want %d", newID, oldID)
		}
		if high := shardCountHigh(shard); high != oldHigh {
			t.Errorf("countHigh after rate-limited compaction = %d, want %d", high, oldHigh)
		}
		if !shard.compactCandidate {
			t.Error("compactCandidate after rate-limited compaction = false, want true")
		}
		if !shard.lastCompact.Equal(now) {
			t.Errorf("lastCompact after rate-limited compaction = %v, want %v", shard.lastCompact, now)
		}
	})
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
	m.Compact()

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
