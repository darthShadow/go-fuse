package fs

import (
	"sync"
	"time"

	"github.com/dolthub/maphash"
)

const (
	mapShards             = 32               // Must be power of 2
	defaultMapSize        = 32               // Initial size for new maps
	maxMapPower           = 20               // Maximum power of 2 for pooled maps
	maxMapSize            = 1 << maxMapPower // Maximum size for pooled maps
	mapShrinkFactor       = 8                // Shrink factor for map compaction
	mapCompactMinInterval = 5 * time.Minute
)

// mapShard provides a sharded generic map implementation
type mapShard[K comparable, V any] struct {
	mu      sync.RWMutex
	entries map[K]V
	// count, countHigh, compactCandidate, and lastCompact are guarded by s.mu.
	count            int32
	countHigh        int32
	compactCandidate bool
	lastCompact      time.Time
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

func (s *mapShard[K, V]) GetAndDo(id K, fn func(V)) (V, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.getAndDoLocked(id, fn)
}

func (s *mapShard[K, V]) getAndDoLocked(id K, fn func(V)) (V, bool) {
	if s.entries == nil {
		var zero V
		return zero, false
	}

	val, ok := s.entries[id]
	if ok {
		fn(val)
	}
	return val, ok
}

func (s *mapShard[K, V]) Set(id K, val V) (candidate bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.entries == nil {
		s.entries = make(map[K]V, defaultMapSize)
	}

	_, exists := s.entries[id]
	s.entries[id] = val

	if !exists {
		s.count++
		if s.count > s.countHigh {
			s.countHigh = s.count
		}
	}

	return s.compactCandidate
}

func (s *mapShard[K, V]) LoadOrStore(id K, val V) (actual V, loaded bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.entries == nil {
		s.entries = make(map[K]V, defaultMapSize)
	}

	if actual, loaded = s.entries[id]; loaded {
		return actual, true
	}

	s.entries[id] = val
	s.count++
	if s.count > s.countHigh {
		s.countHigh = s.count
	}

	return val, false
}

func (s *mapShard[K, V]) Delete(id K) (candidate bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.entries == nil {
		return s.compactCandidate
	}

	if _, exists := s.entries[id]; !exists {
		return s.compactCandidate
	}

	delete(s.entries, id)
	s.count--
	if s.count == 0 || s.count*mapShrinkFactor < s.countHigh {
		s.compactCandidate = true
	}

	return s.compactCandidate
}

func (s *mapShard[K, V]) DeleteIf(id K, match func(V) bool) (deleted bool, candidate bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.entries == nil {
		return false, s.compactCandidate
	}

	val, exists := s.entries[id]
	if !exists {
		return false, s.compactCandidate
	}

	if !match(val) {
		return false, s.compactCandidate
	}

	delete(s.entries, id)
	s.count--
	if s.count == 0 || s.count*mapShrinkFactor < s.countHigh {
		s.compactCandidate = true
	}

	return true, s.compactCandidate
}

func (s *mapShard[K, V]) Count() int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.count
}

// shardedMap provides a sharded generic map
type shardedMap[K comparable, V any] struct {
	hasher maphash.Hasher[K]
	shards [mapShards]*mapShard[K, V]
}

func (m *shardedMap[K, V]) Init() {
	for i := range m.shards {
		m.shards[i] = &mapShard[K, V]{}
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

func (m *shardedMap[K, V]) GetAndDo(id K, fn func(V)) (V, bool) {
	key := m.getMapShardKey(id)
	return m.getMapShard(key).GetAndDo(id, fn)
}

func (m *shardedMap[K, V]) Set(id K, val V) (candidate bool) {
	key := m.getMapShardKey(id)
	return m.getMapShard(key).Set(id, val)
}

func (m *shardedMap[K, V]) LoadOrStore(id K, val V) (actual V, loaded bool) {
	key := m.getMapShardKey(id)
	return m.getMapShard(key).LoadOrStore(id, val)
}

func (m *shardedMap[K, V]) Delete(id K) (candidate bool) {
	key := m.getMapShardKey(id)
	return m.getMapShard(key).Delete(id)
}

func (m *shardedMap[K, V]) DeleteIf(id K, match func(V) bool) (deleted bool, candidate bool) {
	key := m.getMapShardKey(id)
	return m.getMapShard(key).DeleteIf(id, match)
}

func (m *shardedMap[K, V]) Range(fn func(K, V) bool) {
	type entry struct {
		key K
		val V
	}

	for _, shard := range m.shards {
		shard.mu.RLock()
		entries := make([]entry, 0, len(shard.entries))
		for key, val := range shard.entries {
			entries = append(entries, entry{key: key, val: val})
		}
		shard.mu.RUnlock()

		for _, ent := range entries {
			if !fn(ent.key, ent.val) {
				return
			}
		}
	}
}

func (m *shardedMap[K, V]) Compact() {
	m.compactCandidates(time.Now())
}

func (m *shardedMap[K, V]) compactCandidates(now time.Time) {
	for _, shard := range m.shards {
		shard.mu.Lock()
		count := shard.count
		if count == 0 {
			shard.entries = nil
			shard.count = 0
			shard.countHigh = 0
			shard.compactCandidate = false
			shard.lastCompact = now
			shard.mu.Unlock()
			continue
		}

		if !shard.compactCandidate ||
			count*mapShrinkFactor >= shard.countHigh ||
			now.Sub(shard.lastCompact) < mapCompactMinInterval {
			shard.mu.Unlock()
			continue
		}

		newMap := make(map[K]V, int(count)*2)
		for id, val := range shard.entries {
			newMap[id] = val
		}

		shard.entries = newMap
		count = int32(len(newMap))
		shard.count = count
		shard.countHigh = count
		shard.compactCandidate = false
		shard.lastCompact = now
		shard.mu.Unlock()
	}
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
