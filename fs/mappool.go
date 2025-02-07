package fs

import (
	"math/bits"
	"sync"
)

// Pool represents a generic map pool.
//
// Distinct pools may be used for distinct types of maps.
// Properly determined map types with their own pools may help reducing
// memory waste.
type mapPool[K comparable, V any] struct {
	defaultSize uint32
	maxSize     uint32

	pools [maxMapPower]sync.Pool // a list of singlePools
}

// Get returns new map with default size.
//
// The map may be returned to the pool via Put after the use
// in order to minimize GC overhead.
func (p *mapPool[K, V]) Get(length uint32) map[K]V {
	if length == 0 {
		length = uint32(p.defaultSize)
	}
	if length > p.maxSize {
		return make(map[K]V, length)
	}
	idx := nextLogBase2(uint32(length))
	if idx >= maxMapPower {
		return make(map[K]V, length)
	}
	if ptr := p.pools[idx].Get(); ptr != nil {
		return ptr.(map[K]V)
	}
	return make(map[K]V, 1<<idx)
}

// Put releases map obtained via Get to the pool.
//
// The map mustn't be accessed after returning to the pool.
func (p *mapPool[K, V]) Put(m map[K]V) {
	mapLength := len(m)
	if mapLength == 0 || mapLength > int(p.maxSize) {
		return
	}
	idx := prevLogBase2(uint32(mapLength))
	if idx >= maxMapPower {
		return
	}
	clear(m)
	p.pools[idx].Put(m)
}

// Log of base two, round up (for v > 0).
func nextLogBase2(v uint32) uint32 {
	return uint32(bits.Len32(v - 1))
}

// Log of base two, round down (for v > 0)
func prevLogBase2(num uint32) uint32 {
	next := nextLogBase2(num)
	if num == (1 << uint32(next)) {
		return next
	}
	return next - 1
}
