package fs

import (
	"sync"
)

// simpleMap provides a basic concurrent map with periodic recreation
type simpleMap[K comparable, V any] struct {
	mu        sync.RWMutex
	entries   map[K]V
	count     int32 // Counter for total nodes
	countHigh int32 // Counter for high water mark
}

func (m *simpleMap[K, V]) Init() {
	m.entries = make(map[K]V, defaultMapSize)
}

func (m *simpleMap[K, V]) Get(id K) V {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.entries[id]
}

func (m *simpleMap[K, V]) Set(id K, val V) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.entries == nil {
		m.entries = make(map[K]V, defaultMapSize)
	}

	if _, exists := m.entries[id]; exists {
		m.count--
	}

	m.entries[id] = val
	m.count++

	if m.count > m.countHigh {
		m.countHigh = m.count
	}
}

func (m *simpleMap[K, V]) Delete(id K) {
	m.mu.Lock()

	if m.entries == nil {
		m.mu.Unlock()
		return
	}

	if _, exists := m.entries[id]; !exists {
		m.mu.Unlock()
		return
	}

	delete(m.entries, id)
	m.count--

	m.mu.Unlock()

	m.Compact()
}

func (m *simpleMap[K, V]) Compact() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.count == 0 {
		m.entries = make(map[K]V, defaultMapSize)
		m.count = 0
		m.countHigh = 0
		return
	}

	if m.count*100 >= m.countHigh {
		return
	}

	// Create new map with 2 * current size
	newMap := make(map[K]V, m.count*2)
	for k, v := range m.entries {
		newMap[k] = v
	}

	m.entries = newMap

	m.count = int32(len(newMap))
	m.countHigh = int32(len(newMap))
}
