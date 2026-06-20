# Sharded read-buffer pool

`Server.readPool` in `fuse/server.go` is a `bytePool` manager value. Each
active `fuseFD` holds one `*bytePoolShard` in `fuse/fusefd.go`, and request
read buffers return to the shard owned by the descriptor that read the request.

Related docs: [overview](README.md), [cloned FUSE file descriptors](numclonefds.md),
[sharded bridge maps](sharded-bridge-maps.md).

## Binding

`newFuseFD` calls `ensureReadPool` and initially points each descriptor at shard
`0`. This gives a valid shard while descriptors are being constructed.

After INIT and clone setup complete, `NewServer` calls
`ms.readPool.bindFDs(ms.fuseFDs)`. `bindFDs` runs before `Serve`, calls
`setShardCount(len(fds))`, and reassigns each descriptor to its final shard.
The shard count therefore reflects graceful clone degradation.

## Retained-Buffer Budget

`bytePool.setShardCount` splits one server-level retained-buffer ceiling across
the active descriptor count. The current ceiling is
`readPoolMaxRetainedBuffers`, capped inside `bytePool.init`.

If there are more active descriptors than retained-buffer slots, later shards
receive `maxRetained == 0`. These zero-cap shards still work: `Get` allocates
through the pool allocator when no retained buffer is available, and `Put`
drops the buffer because the shard target cannot retain it.

## Hot Path

`bytePoolShard.Get` takes the shard lock, increments `inUse`, and raises
`targetRetained` toward current demand, capped by `maxRetained`. It then pops a
retained buffer from the shard LIFO or allocates a new buffer after releasing
the lock.

`bytePoolShard.Put` restores the full slice capacity, decrements `inUse`, and
retains the buffer only when the retained count is below `targetRetained`.

`reserveRequestBytes`, `canReserveRequestBytes`, `putReadBuf`, and `putReq`
remain per-fd accounting in `fuseFD`. Buffer pooling does not make
`MaxInflightRequestBytes` a server-global budget.

## Request Return

`requestAlloc.clear` in `fuse/request.go` nils `bufferPoolInputBuf`.
`fuseFD.returnRequest` therefore saves `req.bufferPoolInputBuf`, nils the field,
calls `req.clear()`, and then returns the saved read buffer to the origin shard
while holding the descriptor request lock.

This order preserves the buffer ownership recorded by `requestAlloc.setInput`
when a read buffer is large enough to be attached directly to the request.

## Reclaimer And Teardown

`Serve` starts the read-pool reclaimer with `startReclaimer`. `Serve` and
`Unmount` both stop the reclaimer and drain retained buffers through
`stopAndDrainReadPool`, which calls `readPool.stopReclaimer()` and
`readPool.closeAndDrain()`. Late buffer returns after teardown are dropped, not
re-pooled; each shard's `closing` flag blocks retention in `Put`.

Reclaim is slow decay. For each elapsed `bytePoolReclaimInterval`,
`bytePoolShard.reclaim` lowers `targetRetained` by one step toward `inUse` and
trims retained buffers above the new target.

On non-Linux, `fuse/fusefd_other.go` sets `useSingleReader = true` because
multiple goroutines reading the FUSE device can hang during unmount. Clone
attempts also return `ENOSYS` there, so the sharded pool normally binds to the
primary descriptor only on non-Linux.
