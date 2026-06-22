# Bridge concurrency and sharded node maps

`rawBridge` owns bridge-local node identity, file-handle bookkeeping, backing
registration state, and node-map reclamation. Request paths have no mixed-state
bridge mutex.

Related docs: [overview](README.md), [cloned FUSE file descriptors](numclonefds.md),
[sharded read-buffer pool](sharded-bytepool.md).

## Lock Model

| Lock | Protects | Rule |
|---|---|---|
| `Inode.mu` | Namespace and inode lifecycle fields: `lookupCount`, `persistent`, `children`, `parents`, and `changeCounter`. | Namespace mutation uses `lockNodes`/`unlockNodes`. The only namespace lock nesting is `Inode.mu -> mapShard.mu` for node-map publication, conditional delete, and retirement. |
| `mapShard.mu` | One `shardedMap` shard: `entries`, `count`, `countHigh`, `compactCandidate`, and `lastCompact`. | Held inside `shardedMap` helpers. Helper callbacks are bridge-local predicates or ref increments only. |
| `rawBridge.fileMu` | `rawBridge.files`, `rawBridge.freeFiles`, `fileEntry.nodeIndex`, and every `Inode.openFiles`. | Leaf handle-bookkeeping lock. Callers copy the `fileEntry` or `FileHandle`, release `fileMu`, then invoke filesystem callbacks or backing cleanup. |
| `Inode.backingMu` | One inode's `backingID`, `backingIDRefcount`, and `backingFd`. | Leaf passthrough lock. Register and unregister ioctls run while holding this lock. |
| `fileEntry.mu` | One directory handle's read state: overflow entry, last-read cache, offset, and waitgroup coordination. | Per-handle directory serialization; not part of node identity or namespace ordering. |

No Node or File request-operation callback runs while a node-map shard lock,
`rawBridge.fileMu`, or `Inode.backingMu` is held. Passthrough fd extraction and
backing register/unregister ioctls run under `Inode.backingMu`; they do not
hold namespace, file-table, or map locks.

## Node Identity Maps

`rawBridge` maintains three sharded maps:

| Map | Key | Value | Role |
|---|---|---|---|
| `stableAttrs` | `StableAttr` | `*Inode` | Canonical inode lookup for stable file identity and hard links. |
| `kernelNodeIds` | `uint64` node ID | `*Inode` | Live kernel node ID lookup for request handling. |
| `retiredKernelNodeIds` | `uint64` node ID | `*Inode` | Retired node ID lookup while bridge-local request refs drain. |

`NewNodeFS` initializes all three maps. `Init` starts the bridge compactor
lazily after the server is available.

Request paths resolve node IDs with `acquireNode`:

1. Hold the retired map shard `RLock`.
2. Try `retiredKernelNodeIds.getAndDoLocked(id, addRef)`.
3. While still holding the retired shard `RLock`, try `kernelNodeIds.GetAndDo(id, addRef)`.
4. Return a release closure that decrements `Inode.localRefs`.
5. When release drops `localRefs` to zero on a retired inode, schedule node-map compaction.

Final forget runs under the forgotten inode's `Inode.mu`:

1. Delete `stableAttrs[stableAttr]` with `DeleteIf(... got == n ...)`.
2. Insert `retiredKernelNodeIds[nodeID] = n`.
3. Delete `kernelNodeIds[nodeID]` with `DeleteIf(... got == n ...)`.
4. Set `n.retired` only when the live-map delete succeeds.
5. Roll back the retired entry if the live-map delete does not match the inode.
6. Schedule compaction when any map reports a compaction candidate.

Late requests from cloned FUSE file descriptors resolve safely because the
retired shard `RLock` gives `acquireNode` a closed transition window:

- If final forget already retired the node, `acquireNode` pins the retired entry before releasing the shard.
- If final forget has not retired the node, the retired shard `RLock` blocks the retire insert while `acquireNode` pins the live entry.
- Retired reaping uses `DeleteIf` and rechecks `localRefs == 0` under the retired shard lock, so a newly pinned retired entry is not removed.

## Hard-Link Canonicalization

`addNewChild` uses `stableAttrs.LoadOrStore` as the linearization point for
non-`O_EXCL` hard-link canonicalization. The parent and candidate child are
locked with `lockNodes` while `LoadOrStore` runs under the stable-attr shard
lock.

When `LoadOrStore` publishes the candidate, that inode becomes canonical for
the `StableAttr`. When it finds an existing inode, `addNewChild` releases the
namespace locks and retries with the existing inode. The selected inode receives
the lookup count update, parent entry, `kernelNodeIds` publication, and reply
fields while namespace locks are held. File-handle registration happens after
namespace locks are released, keeping `fileMu` outside the
`Inode.mu -> mapShard.mu` namespace chain.

`O_EXCL` creation stores the selected inode into `stableAttrs` under namespace
locks and preserves create-new semantics.

## Reclamation

`shardedMap` has 32 shards. `uint64` keys use low shard bits directly; other
key types use `github.com/dolthub/maphash`.

Each shard owns a lazy `entries` map plus `count`, `countHigh`,
`compactCandidate`, and `lastCompact` under `mapShard.mu`. `Set`,
`LoadOrStore`, `Delete`, and `DeleteIf` update membership under one shard lock.
Deletes mark compaction candidates; they do not rebuild maps inline on request
paths.

Bridge node maps do not use `mapPool`. Compaction allocates fresh Go maps and
lets old maps become garbage-collector input. Empty shard compaction sets
`entries = nil`.

`compactCandidates(now)` walks shards sequentially. A non-empty shard compacts
only when all conditions hold:

- `compactCandidate` is true.
- `count * mapShrinkFactor < countHigh`.
- `now.Sub(lastCompact) >= mapCompactMinInterval`.

Eligible shards copy live entries into a fresh map sized from the current
count, reset `countHigh`, clear `compactCandidate`, and store `lastCompact =
now`. `Compact()` is a wrapper around `compactCandidates(time.Now())`.

`rawBridge.compactNodeMaps` compacts `stableAttrs` and `kernelNodeIds`, scans
`retiredKernelNodeIds` for entries with `localRefs == 0`, conditionally deletes
those retired entries, then compacts the retired map.

## Backing Files

`disableBackingFiles` is an atomic, sticky bridge-wide latch. Unsupported
servers and registration errno store `true`; later opens skip passthrough
registration.

`addBackingID` checks the latch, checks server backing-fd support, checks
`FilePassthroughFder`, then takes `n.backingMu`. Inside the lock it rechecks the
latch, calls `PassthroughFd`, runs `RegisterBackingFd`, stores the backing ID on
success, sets passthrough open flags, and increments the per-inode refcount.

`releaseBackingIDRef` takes `n.backingMu`, consumes the handle's backing ref,
decrements the per-inode refcount, and runs `UnregisterBackingFd` while holding
the same lock when the refcount reaches zero. Different inodes use different
`backingMu` locks and can register or unregister independently.

## Compactor Lifetime

`NewNodeFS` creates the compactor channels and initializes all node maps. The
worker starts lazily in `rawBridge.Init` through `startNodeMapCompactor`.

`scheduleNodeMapCompaction` sends a non-blocking wake signal. The worker also
runs on a `mapCompactMinInterval` ticker and exits on `compactStop`.

`stopNodeMapCompactor` is idempotent. It closes `compactStop` once, closes
`compactDone` itself when the worker has not started, and waits for
`compactDone`. `startNodeMapCompactor` ignores calls after stop. `Mount` stops
the compactor when server creation or mount setup fails. `OnUnmount` stops it
after root `OnForget` handling.

## Verification

CI validates bridge concurrency with:

```shell
go test -race ./fs ./fuse -count=1
```

The local focused gate is:

```shell
go test -race ./fs -run 'Test.*(HardLink|Forget|Backing|Retired|Compaction|FileHandle)' -count=1
```
