package fs

import (
	"sync"
	"time"

	"github.com/dolthub/maphash"
)

const (
	mapShards       = 32               // Must be power of 2
	defaultMapSize  = 32               // Initial size for new maps
	maxMapPower     = 20               // Maximum power of 2 for pooled maps
	maxMapSize      = 1 << maxMapPower // Maximum size for pooled maps
	mapShrinkFactor = 8                // Shrink factor for map compaction
)

type inode struct{}

// mapShard provides a sharded generic map implementation
type mapShard[K comparable, V any] struct {
	mu        sync.RWMutex
	pool      *mapPool[K, V]
	entries   map[K]V
	count     int32 // Counter for total nodes
	countHigh int32 // Counter for high water mark
}

func (s *mapShard[K, V]) Get(id K) (V, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.entries == nil {
		var zero V
		return zero, false
	}

	val, ok := s.entries[id]
	return val, ok
}

func (s *mapShard[K, V]) Set(id K, val V) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.entries == nil {
		s.entries = s.pool.Get(defaultMapSize)
	}

	if _, exists := s.entries[id]; exists {
		s.count--
	}

	s.entries[id] = val
	s.count++

	if s.count > s.countHigh {
		s.countHigh = s.count
	}
}

func (s *mapShard[K, V]) Delete(id K) {
	s.mu.Lock()

	if s.entries == nil {
		s.mu.Unlock()
		return
	}

	if _, exists := s.entries[id]; !exists {
		s.mu.Unlock()
		return
	}

	delete(s.entries, id)
	s.count--

	s.mu.Unlock()

	s.Compact()
}

func (s *mapShard[K, V]) Compact() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.count == 0 {
		if s.entries != nil {
			s.pool.Put(s.entries)
			s.entries = nil
		}

		s.count = 0
		s.countHigh = 0
		return
	}

	if s.count > maxMapSize {
		return
	}

	if s.count*mapShrinkFactor >= s.countHigh {
		return
	}

	newMap := s.pool.Get(uint32(s.count) * 2)
	for id, val := range s.entries {
		newMap[id] = val
	}

	s.pool.Put(s.entries)
	s.entries = newMap

	s.count = int32(len(newMap))
	s.countHigh = int32(len(newMap))
}

func (s *mapShard[K, V]) Count() int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.count
}

// shardedMap provides a sharded generic map
type shardedMap[K comparable, V any] struct {
	hasher      maphash.Hasher[K]
	shards      [mapShards]*mapShard[K, V]
	lastCompact time.Time
}

func (m *shardedMap[K, V]) Init() {
	pool := &mapPool[K, V]{
		defaultSize: defaultMapSize,
		maxSize:     maxMapSize,
	}
	for i := range m.shards {
		m.shards[i] = &mapShard[K, V]{
			pool: pool,
		}
	}
	m.hasher = maphash.NewHasher[K]()
}

func (m *shardedMap[K, V]) getMapShardKey(id K) uint64 {
	// If id is uint64, use it directly
	if u64, ok := any(id).(uint64); ok {
		return u64 & (mapShards - 1)
	}

	// Otherwise use maphash from runtime
	return m.hasher.Hash(id) & (mapShards - 1)
}

func (m *shardedMap[K, V]) getMapShard(key uint64) *mapShard[K, V] {
	return m.shards[key]
}

func (m *shardedMap[K, V]) Get(id K) (V, bool) {
	key := m.getMapShardKey(id)
	return m.getMapShard(key).Get(id)
}

func (m *shardedMap[K, V]) Set(id K, val V) {
	key := m.getMapShardKey(id)
	m.getMapShard(key).Set(id, val)
}

func (m *shardedMap[K, V]) Delete(id K) {
	key := m.getMapShardKey(id)
	m.getMapShard(key).Delete(id)
}

func (m *shardedMap[K, V]) Compact() {
	if time.Since(m.lastCompact) < 5*time.Minute {
		return
	}

	m.lastCompact = time.Now()

	var wg sync.WaitGroup
	shards := m.shards[:]
	wg.Add(len(shards))

	for _, shard := range shards {
		go func(s *mapShard[K, V]) {
			defer wg.Done()
			s.Compact()
		}(shard)
	}

	wg.Wait()
}

func (m *shardedMap[K, V]) Count() int32 {
	var wg sync.WaitGroup
	shards := m.shards[:]
	counts := make([]int32, len(shards))
	wg.Add(len(shards))

	for i, shard := range shards {
		go func(index int, s *mapShard[K, V]) {
			defer wg.Done()
			counts[index] = s.Count()
		}(i, shard)
	}

	wg.Wait()

	total := int32(0)
	for _, count := range counts {
		total += count
	}
	return total
}
