# go-fuse Performance Fork

This fork keeps the `github.com/hanwen/go-fuse/v2` module shape while carrying
performance-oriented changes for the darthShadow/rclone `cmd/mount2` workload.
The target workload is high-concurrency metadata traffic over many small files.
Reads usually go through FUSE passthrough, so read throughput itself is not the
main fork focus. Changes that are generally useful stay small and upstreamable.

## Current Divergences

- Cloned FUSE fds: `MountOptions.NumCloneFDs` in `fuse/api.go` opens additional
  `/dev/fuse` fds and binds them to the same Linux mount session. `Server.fuseFDs`
  in `fuse/server.go` and `fuseFD` in `fuse/fusefd.go` keep one reader tree,
  request-byte budget, and write coordinator per fd. Replies use the fd that read
  the request; notifications and backing-fd ioctls use the primary fd. See
  `fuse/fusefd_linux.go` and `fuse/fusefd_other.go` for platform behavior.

- Sharded read-buffer pool: `Server.readPool` is a `bytePool` manager in
  `fuse/bytepool.go`. It assigns one bounded LIFO `bytePoolShard` to each
  `fuseFD`, splits one server-level retained-buffer budget across active fds, and
  uses per-shard locks on request hot paths. `Get` raises the shard target to
  current demand, while one server-local reclaimer goroutine applies slow decay
  and trims idle buffers. `fuse/server.go` stops the reclaimer before draining
  retained buffers during `Serve` and `Unmount` teardown.

- Bridge node maps: `rawBridge.stableAttrs` and `rawBridge.kernelNodeIds` in
  `fs/bridge.go` are `shardedMap` instances instead of plain Go maps.
  `fs/shardedmap.go` provides shard-local locks, map pooling, and rate-limited
  per-shard compaction; `fs/mappool.go` owns the pooled backing maps. The bridge
  accesses these maps through helper methods (Init/Get/Set/Delete/Compact).

- Xattr disable coverage and notify writes: `MountOptions.DisableXAttrs` in
  `fuse/api.go` covers get, list, set, and remove xattr opcodes in
  `fuse/opcode.go`. `fuseFD.writeMu` in `fuse/fusefd.go` is an `RWMutex`:
  notify writes take `RLock`, while close and backing-fd ioctls take the
  exclusive lock.

- Splice fallback: Linux splice in `fuse/splice_linux.go` handles fd-backed read
  results and falls back for slice-backed `ReadResult` values. This keeps ordinary
  byte-slice reads on the non-splice path while preserving splice for passthrough
  reads.

- Request buffer lifecycle: `requestAlloc.clear` in `fuse/request.go` resets the
  pooled request object and inline buffers. `fuseFD.returnRequest` in
  `fuse/fusefd.go` saves any gobbled read buffer before clearing the request, then
  returns it to the fd's shard.

## Caveats

- `MaxInflightRequestBytes` is enforced per fd. With cloned fds, the effective
  request-byte ceiling scales with the number of active fds.
- Read-buffer-pool teardown has a documented non-Linux single-reader limitation.
  See the comment on `stopAndDrainReadPool` in `fuse/server.go`.
