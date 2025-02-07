package fs

import (
	"sync"
	"testing"

	"github.com/valyala/fastrand"
)

func BenchmarkNodeMapSet(b *testing.B) {
	m := &shardedMap[uint64, *inode]{}
	m.Init()
	node := &inode{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Set(uint64(i), node)
	}
}

func BenchmarkCompareImplementations(b *testing.B) {
	shardedMap := &shardedMap[uint64, *inode]{}
	shardedMap.Init()

	simpleMap := &simpleMap[uint64, *inode]{}
	simpleMap.Init()

	node := &inode{}

	comparisons := []struct {
		name string
		fn   func(b *testing.B)
	}{
		{"Set/Sharded", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				shardedMap.Set(uint64(i), node)
			}
		}},
		{"Set/Simple", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				simpleMap.Set(uint64(i), node)
			}
		}},
		{"Get/Sharded", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				shardedMap.Get(uint64(i % 1000))
			}
		}},
		{"Get/Simple", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				simpleMap.Get(uint64(i % 1000))
			}
		}},
		{"Mixed/Sharded", func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				i := uint64(0)
				for pb.Next() {
					switch i % 3 {
					case 0:
						shardedMap.Set(i, node)
					case 1:
						shardedMap.Get(i)
					case 2:
						shardedMap.Delete(i)
					}
					i++
				}
			})
		}},
		{"Mixed/Simple", func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				i := uint64(0)
				for pb.Next() {
					switch i % 3 {
					case 0:
						simpleMap.Set(i, node)
					case 1:
						simpleMap.Get(i)
					case 2:
						simpleMap.Delete(i)
					}
					i++
				}
			})
		}},
		{"CompactionStress/Sharded", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for j := uint64(0); j < 1000; j++ {
					shardedMap.Set(j, node)
				}
				for j := uint64(0); j < 900; j++ {
					shardedMap.Delete(j)
				}
				shardedMap.Compact()
			}
		}},
		{"CompactionStress/Simple", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for j := uint64(0); j < 1000; j++ {
					simpleMap.Set(j, node)
				}
				for j := uint64(0); j < 900; j++ {
					simpleMap.Delete(j)
				}
				simpleMap.Compact()
			}
		}},
	}

	for _, c := range comparisons {
		b.Run(c.name, c.fn)
	}
}

func BenchmarkNodeMapShardDistribution(b *testing.B) {
	m := &shardedMap[uint64, *inode]{}
	m.Init()
	node := &inode{}

	b.Run("Sequential", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Set(uint64(i), node)
		}
	})

	b.Run("PowerOf2", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Set(uint64(i<<5), node)
		}
	})

	b.Run("Scattered", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Set(uint64(i*mapShards), node)
		}
	})
}

func BenchmarkNodeMapAllocation(b *testing.B) {
	m := &shardedMap[uint64, *inode]{}
	m.Init()
	node := &inode{}

	b.Run("GrowthPattern", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			m.Set(uint64(i), node)
			if i%(defaultMapSize*2) == 0 {
				m.Compact() // Force occasional compaction
			}
		}
	})

	b.Run("ChurnPattern", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if i%2 == 0 {
				m.Set(uint64(i), node)
			} else {
				m.Delete(uint64(i - 1))
			}
		}
	})
}

func BenchmarkNodeMapCacheEffects(b *testing.B) {
	m := &shardedMap[uint64, *inode]{}
	m.Init()
	node := &inode{}

	// Fill map with entries
	for i := 0; i < 1000000; i++ {
		m.Set(uint64(i), node)
	}

	b.Run("LocalityFriendly", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Access pattern that stays within cache lines
			base := uint64((i / 8) * 8)
			for j := uint64(0); j < 8; j++ {
				m.Get(base + j)
			}
		}
	})

	b.Run("LocalityHostile", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Access pattern that jumps across memory
			m.Get(uint64(i * 251)) // Use prime number to avoid patterns
		}
	})
}

func BenchmarkNodeMapStress(b *testing.B) {
	m := &shardedMap[uint64, *inode]{}
	m.Init()
	node := &inode{}

	b.Run("HighContention", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			// All goroutines hammer same few keys
			for pb.Next() {
				key := uint64(fastrand.Uint32() % 100)
				switch fastrand.Uint32() % 3 {
				case 0:
					m.Set(key, node)
				case 1:
					m.Get(key)
				case 2:
					m.Delete(key)
				}
			}
		})
	})

	b.Run("BurstPattern", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			burst := make([]uint64, 100)
			for pb.Next() {
				// Generate burst of operations
				for i := range burst {
					burst[i] = uint64(fastrand.Uint32())
				}
				// Execute burst
				for _, key := range burst {
					m.Set(key, node)
				}
				for _, key := range burst {
					m.Get(key)
				}
				for _, key := range burst {
					m.Delete(key)
				}
			}
		})
	})
}

func BenchmarkNodeMapGet(b *testing.B) {
	m := &shardedMap[uint64, *inode]{}
	m.Init()
	node := &inode{}

	// Setup: Fill map with some entries
	for i := 0; i < 1000; i++ {
		m.Set(uint64(i), node)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Get(uint64(i % 1000))
	}
}

func BenchmarkNodeMapDelete(b *testing.B) {
	m := &shardedMap[uint64, *inode]{}
	m.Init()
	node := &inode{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Setup for each iteration
		m.Set(uint64(i), node)
		b.StartTimer()
		m.Delete(uint64(i))
		b.StopTimer()
	}
}

func BenchmarkNodeMapSetParallel(b *testing.B) {
	m := &shardedMap[uint64, *inode]{}
	m.Init()
	node := &inode{}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := uint64(0)
		for pb.Next() {
			m.Set(i, node)
			i++
		}
	})
}

func BenchmarkNodeMapGetParallel(b *testing.B) {
	m := &shardedMap[uint64, *inode]{}
	m.Init()
	node := &inode{}

	// Setup: Fill map with entries
	for i := 0; i < 1000; i++ {
		m.Set(uint64(i), node)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := uint64(0)
		for pb.Next() {
			m.Get(i % 1000)
			i++
		}
	})
}

func BenchmarkNodeMapMixed(b *testing.B) {
	m := &shardedMap[uint64, *inode]{}
	m.Init()
	node := &inode{}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := uint64(0)
		for pb.Next() {
			switch i % 3 {
			case 0:
				m.Set(i, node)
			case 1:
				m.Get(i)
			case 2:
				m.Delete(i)
			}
			i++
		}
	})
}

func BenchmarkNodeMapCompaction(b *testing.B) {
	m := &shardedMap[uint64, *inode]{}
	m.Init()
	node := &inode{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Setup
		b.StopTimer()
		for j := uint64(0); j < 1000; j++ {
			m.Set(j, node)
		}
		for j := uint64(0); j < 750; j++ {
			m.Delete(j)
		}
		b.StartTimer()

		// Benchmark the compaction
		m.Compact()
	}
}

func BenchmarkNodeMapShardOperations(b *testing.B) {
	pool := &mapPool[uint64, *inode]{
		defaultSize: defaultMapSize,
		maxSize:     maxMapSize,
	}
	shard := &mapShard[uint64, *inode]{pool: pool}
	node := &inode{}

	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			shard.Set(uint64(i), node)
		}
	})

	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			shard.Get(uint64(i % 1000))
		}
	})

	b.Run("Delete", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			shard.Delete(uint64(i))
		}
	})
}

func BenchmarkLargeScaleOperations(b *testing.B) {
	numNodes := 1_000_000 * b.N // (1xb.n) M nodes

	m := &shardedMap[uint64, *inode]{}
	m.Init()

	b.Run("ShardedNodeMap", func(b *testing.B) {
		node := &inode{}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Add nodes
			for j := 0; j < numNodes; j++ {
				m.Set(uint64(j), node)
			}

			// Delete all nodes and compact
			for j := 0; j < numNodes; j++ {
				m.Delete(uint64(j))
			}

			// Force compaction
			m.Compact()
		}
	})

	sM := &simpleMap[uint64, *inode]{}
	sM.Init()

	b.Run("SimpleNodeMap", func(b *testing.B) {
		node := &inode{}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Add nodes
			for j := 0; j < numNodes; j++ {
				sM.Set(uint64(j), node)
			}

			// Delete all nodes
			for j := 0; j < numNodes; j++ {
				sM.Delete(uint64(j))
			}

			// Force compaction
			sM.Compact()
		}
	})
}

func BenchmarkLargeScaleParallelOperations(b *testing.B) {
	const numWorkers = 8

	numNodes := 1_000_000 * b.N // (1xb.n) M nodes

	m := &shardedMap[uint64, *inode]{}
	m.Init()

	b.Run("ShardedNodeMap", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var wg sync.WaitGroup

			// Add workers
			for w := 0; w < numWorkers; w++ {
				wg.Add(1)
				go func(offset int) {
					defer wg.Done()
					node := &inode{}
					for j := 0; j < numNodes/numWorkers; j++ {
						id := uint64(offset*numNodes/numWorkers + j)
						m.Set(id, node)
					}
				}(w)
			}

			// Delete workers
			for w := 0; w < numWorkers; w++ {
				wg.Add(1)
				go func(offset int) {
					defer wg.Done()
					for j := 0; j < numNodes/numWorkers; j++ {
						id := uint64(offset*numNodes/numWorkers + j)
						m.Delete(id)
					}
				}(w)
			}

			wg.Wait()
		}
	})

	sM := &simpleMap[uint64, *inode]{}
	sM.Init()

	b.Run("SimpleNodeMap", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var wg sync.WaitGroup

			// Add workers
			for w := 0; w < numWorkers; w++ {
				wg.Add(1)
				go func(offset int) {
					defer wg.Done()
					node := &inode{}
					for j := 0; j < numNodes/numWorkers; j++ {
						id := uint64(offset*numNodes/numWorkers + j)
						sM.Set(id, node)
					}
				}(w)
			}

			// Delete workers
			for w := 0; w < numWorkers; w++ {
				wg.Add(1)
				go func(offset int) {
					defer wg.Done()
					for j := 0; j < numNodes/numWorkers; j++ {
						id := uint64(offset*numNodes/numWorkers + j)
						sM.Delete(id)
					}
				}(w)
			}

			wg.Wait()
		}
	})
}
