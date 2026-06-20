package fs

import (
	"math/bits"
	"reflect"
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

	// mapPool is temporary and slated for removal by A-bridge-concurrency.
	classes sync.Map // map identity to allocated size class
	pools   [maxMapPower]sync.Pool
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
		m := ptr.(map[K]V)
		p.recordClass(m, idx)
		return m
	}
	m := make(map[K]V, 1<<idx)
	p.recordClass(m, idx)
	return m
}

// Put releases map obtained via Get to the pool.
//
// The caller must be the sole owner of m, must Put each map at most once, and
// must not retain or access m after Put returns. The pool may hand m to another
// caller immediately.
func (p *mapPool[K, V]) Put(m map[K]V) {
	mapLength := len(m)
	if mapLength == 0 || mapLength > int(p.maxSize) {
		p.forgetClass(m)
		return
	}
	idx, ok := p.class(m)
	p.forgetClass(m)
	clear(m)
	if !ok {
		return
	}
	if idx >= maxMapPower {
		return
	}
	if p.shrunkBelowClass(mapLength, idx) {
		return
	}
	p.pools[idx].Put(m)
}

func mapPoolIdentity[K comparable, V any](m map[K]V) uintptr {
	if m == nil {
		return 0
	}
	return reflect.ValueOf(m).Pointer()
}

func (p *mapPool[K, V]) recordClass(m map[K]V, idx uint32) {
	if id := mapPoolIdentity(m); id != 0 {
		p.classes.Store(id, idx)
	}
}

func (p *mapPool[K, V]) class(m map[K]V) (uint32, bool) {
	id := mapPoolIdentity(m)
	if id == 0 {
		return 0, false
	}
	value, ok := p.classes.Load(id)
	if !ok {
		return 0, false
	}
	idx, ok := value.(uint32)
	return idx, ok
}

func (p *mapPool[K, V]) forgetClass(m map[K]V) {
	if id := mapPoolIdentity(m); id != 0 {
		p.classes.Delete(id)
	}
}

func (p *mapPool[K, V]) shrunkBelowClass(length int, idx uint32) bool {
	classSize := uint64(1) << idx
	return uint64(length)*uint64(mapShrinkFactor) < classSize
}

// Log of base two, round up (for v > 0).
func nextLogBase2(v uint32) uint32 {
	return uint32(bits.Len32(v - 1))
}
