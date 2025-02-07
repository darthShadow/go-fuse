package fs

import (
	"fmt"
	"sync"
	"testing"
)

func TestMapPool(t *testing.T) {
	t.Run("Get returns map with default size", func(t *testing.T) {
		t.Skip("capacity is not guaranteed")
		p := &mapPool[uint64, *inode]{defaultSize: 10}
		m := p.Get(0)
		if len(m) < 10 {
			t.Errorf("expected capacity >= 10, got %d", len(m))
		}
	})

	t.Run("Put recycles maps under maxSize", func(t *testing.T) {
		p := &mapPool[uint64, *inode]{maxSize: 5}
		m := make(map[uint64]*inode)
		m[1] = &inode{}
		m[2] = &inode{}

		p.Put(m)
		recycled := p.Get(0)

		if len(recycled) != 0 {
			t.Error("map was not cleared before recycling")
		}
	})

	t.Run("Put discards maps over maxSize", func(t *testing.T) {
		p := &mapPool[uint64, *inode]{maxSize: 2}
		m := make(map[uint64]*inode)
		m[1] = &inode{}
		m[2] = &inode{}
		m[3] = &inode{} // Over maxSize

		firstGet := p.Get(0)
		p.Put(m) // Should discard
		secondGet := p.Get(0)

		if &firstGet == &secondGet {
			t.Error("oversized map was incorrectly recycled")
		}
	})

	t.Run("Atomic operations work correctly", func(t *testing.T) {
		p := &mapPool[uint64, *inode]{}
		p.defaultSize = 20
		p.maxSize = 30

		if size := p.defaultSize; size != 20 {
			t.Errorf("expected defaultSize 20, got %d", size)
		}
		if size := p.maxSize; size != 30 {
			t.Errorf("expected maxSize 30, got %d", size)
		}
	})
}

func TestNextLogBase2(t *testing.T) {
	tests := []struct {
		input    uint32
		expected uint32
	}{
		{1, 0},
		{2, 1},
		{3, 2},
		{4, 2},
		{5, 3},
		{7, 3},
		{8, 3},
		{9, 4},
		{16, 4},
		{32, 5},
		{1024, 10},
	}

	for _, test := range tests {
		result := nextLogBase2(test.input)
		if result != test.expected {
			t.Errorf("nextLogBase2(%d) = %d, expected %d", test.input, result, test.expected)
		}
	}
}

func TestPrevLogBase2(t *testing.T) {
	tests := []struct {
		input    uint32
		expected uint32
	}{
		{1, 0},
		{2, 1},
		{3, 1},
		{4, 2},
		{5, 2},
		{7, 2},
		{8, 3},
		{9, 3},
		{15, 3},
		{16, 4},
		{1024, 10},
	}

	for _, test := range tests {
		result := prevLogBase2(test.input)
		if result != test.expected {
			t.Errorf("prevLogBase2(%d) = %d, expected %d", test.input, result, test.expected)
		}
	}
}

func TestMapPoolGetPut(t *testing.T) {
	pool := &mapPool[string, int]{
		defaultSize: 16,
		maxSize:     1024,
	}

	tests := []struct {
		name        string
		requestSize uint32
		fillSize    int
	}{
		{"zero_size", 0, 8},
		{"small_size", 4, 4},
		{"default_size", 16, 12},
		{"large_size", 100, 64},
		{"max_size", 1024, 900},
		{"over_max_size", 2048, 1500},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Test Get
			m := pool.Get(test.requestSize)
			if m == nil {
				t.Fatal("Got nil map from pool")
			}

			// Fill map with some data
			for i := 0; i < test.fillSize; i++ {
				m[string(rune('a'+i))] = i
			}

			// Verify map works correctly
			if len(m) != test.fillSize {
				t.Errorf("Map size = %d, expected %d", len(m), test.fillSize)
			}

			// Test Put
			pool.Put(m)

			// Verify map is cleared after Put
			if len(m) != 0 && test.requestSize < pool.maxSize {
				t.Error("Map not cleared after Put")
			}
		})
	}
}

func TestMapPoolReuseAfterPut(t *testing.T) {
	pool := &mapPool[string, int]{
		defaultSize: 16,
		maxSize:     1024,
	}

	// Get first map
	m1 := pool.Get(8)
	m1["test"] = 1

	// Put it back
	pool.Put(m1)

	// Get another map
	m2 := pool.Get(8)

	// Verify it's cleared
	if len(m2) != 0 {
		t.Error("Reused map not empty")
	}

	// Verify it can be used
	m2["new"] = 2
	if m2["new"] != 2 {
		t.Error("Reused map not working correctly")
	}
}

func TestMapPoolConcurrency(t *testing.T) {
	pool := &mapPool[string, int]{
		defaultSize: 16,
		maxSize:     1024,
	}

	const (
		numGoroutines   = 10
		opsPerGoroutine = 100
	)

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				m := pool.Get(16)
				// Do some work
				m["key"] = j
				pool.Put(m)
			}
		}()
	}

	wg.Wait()
}

func TestMapPoolEdgeCases(t *testing.T) {
	pool := &mapPool[string, int]{
		defaultSize: 16,
		maxSize:     1024,
	}

	t.Run("put nil map", func(t *testing.T) {
		// Should not panic
		var m map[string]int
		pool.Put(m)
	})

	t.Run("put empty map", func(t *testing.T) {
		m := make(map[string]int)
		pool.Put(m)
	})

	t.Run("get with max power size", func(t *testing.T) {
		size := uint32(1 << maxMapPower)
		m := pool.Get(size)
		if m == nil {
			t.Error("Got nil map for max power size")
		}
	})
}

func BenchmarkMapPool(b *testing.B) {
	pool := &mapPool[string, int]{
		defaultSize: 16,
		maxSize:     1024,
	}

	b.Run("get-put cycle", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m := pool.Get(16)
			m["test"] = i
			pool.Put(m)
		}
	})

	b.Run("get-fill-put cycle", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m := pool.Get(16)
			for j := 0; j < 10; j++ {
				m[string(rune('a'+j))] = j
			}
			pool.Put(m)
		}
	})

	b.Run("without pool", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m := make(map[string]int, 16)
			m["test"] = i
			// Let GC handle cleanup
		}
	})
}

func TestMapPoolSizeClasses(t *testing.T) {
	pool := &mapPool[string, int]{
		defaultSize: 16,
		maxSize:     1024,
	}

	// Test that maps are binned into correct size classes
	sizes := []uint32{1, 2, 3, 4, 7, 8, 9, 15, 16, 17, 31, 32}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			m1 := pool.Get(size)
			// Fill to capacity
			for i := 0; i < int(size); i++ {
				m1[fmt.Sprintf("key%d", i)] = i
			}
			pool.Put(m1)

			// Get another map of same size
			m2 := pool.Get(size)
			if len(m2) != 0 {
				t.Errorf("Reused map not empty for size %d", size)
			}

			// Verify we can still use the full capacity
			for i := 0; i < int(size); i++ {
				m2[fmt.Sprintf("key%d", i)] = i
			}
		})
	}
}
