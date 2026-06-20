# Sharded bridge maps

`rawBridge.stableAttrs` and `rawBridge.kernelNodeIds` in `fs/bridge.go` are
`shardedMap` fields. They replace plain maps for the node lookup paths while
preserving the bridge's existing `rawBridge.mu` for global bridge state.

Related docs: [overview](README.md), [cloned FUSE file descriptors](numclonefds.md),
[sharded read-buffer pool](sharded-bytepool.md).

## Map Surface

`fs/shardedmap.go` exposes the helper surface used by `rawBridge`:

| Helper | Current behavior |
|---|---|
| `Init` | Creates the shard array and a shared `mapPool` for the map value type. |
| `Get` | Hashes the key, takes the shard `RLock`, and returns the value from that shard. |
| `Set` | Lazily allocates the shard backing map, updates count and high-water counters, and stores the value under the shard lock. |
| `Delete` | Removes the key under the shard lock, updates count, and compacts that shard. |
| `Compact` | Applies a map-level time gate, then fans out `mapShard.Compact` across all shards. |

`uint64` keys use the low shard bits directly. Other key types use
`github.com/dolthub/maphash`.

## Shards And Pooling

Each `mapShard` owns an `RWMutex`, a lazy `entries` map, a count, and a
high-water count. Empty shards allocate no backing map until the first `Set`.

`mapShard.Compact` returns empty backing maps to the pool, or rebuilds a shard
when the current count is far below the high-water count. `fs/mappool.go` owns
the pooled backing maps, rounds requested sizes to powers of two, clears maps
before returning them, and avoids pooling empty or oversized maps.

`shardedMap.Compact` has one `shardedMap.lastCompact` timestamp for the whole
map. If the map-level gate allows compaction, it runs per-shard compaction work
across the shard array. This is not an independent per-shard timestamp gate.

## Bridge Locking

`rawBridge` still has a global `b.mu`. The sharded maps reduce contention inside
the map storage, but the bridge is not fully lock-free. Existing bridge methods
continue to take `b.mu` where they coordinate node IDs, file handles, backing
IDs, or other bridge-wide state.

`NewNodeFS` initializes both maps with `stableAttrs.Init()` and
`kernelNodeIds.Init()`. The bridge accesses them through `_getStableNode`,
`_getNode`, `_setStableNode`, `_setNode`, `_removeStableNode`, and
`_removeNode`.

## Backing-ID Coordination

`rawBridge.addBackingID` requires `bridge.mu`. `Create` takes `b.mu` before
calling `addBackingID`, and `Open` holds `b.mu` while registering the file and
calling `addBackingID`.

`fs/bridge_backing_test.go` covers this coordination with
`TestCreateBackingIDDisableRace`, which runs concurrent creates against a
server that succeeds once and then returns an error from `RegisterBackingFd`.
The bridge serializes the backing-ID update and disables backing files after
the failing registration path.

