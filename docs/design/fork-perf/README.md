# go-fuse performance fork

This fork keeps the `github.com/hanwen/go-fuse/v2` module shape while carrying
performance-oriented changes for the darthShadow/rclone `cmd/mount2` workload.
The target workload is high-concurrency metadata traffic over many small files.
Reads normally go through FUSE passthrough, so bulk read throughput is not the
primary fork focus.

## Current Divergences

| Area | Detail |
|---|---|
| [Cloned FUSE file descriptors](numclonefds.md) | `MountOptions.NumCloneFDs` opens additional Linux FUSE descriptors for the same mount session. |
| [Sharded read-buffer pool](sharded-bytepool.md) | `Server.readPool` splits one retained-buffer budget across per-fd `bytePoolShard` instances. |
| [Sharded bridge maps](sharded-bridge-maps.md) | `rawBridge.stableAttrs` and `rawBridge.kernelNodeIds` use `shardedMap` instead of plain Go maps. |

## Splice Fallback

Linux splice support lives in `fuse/splice_linux.go`. `fuseFD.trySplice` uses
the splice path only for fd-backed `ReadResult` implementations that expose
seekable or stateful file data. Slice-backed results return `errRecoverSplice`,
so `fuseFD.write` falls back to `ReadResult.Bytes` and the normal write path.
Short fd reads rebuild the reply length and retry through `ReadResultPipe`.

## DisableXAttrs

`MountOptions.DisableXAttrs` in `fuse/api.go` disables the xattr operation
family. `fuse/opcode.go` returns `ENOSYS` for get and list through
`doGetXAttr`, for set through `doSetXAttr`, and for remove through
`doRemoveXAttr`.

## Inflight Ceiling

`MaxInflightRequestBytes` is enforced per active FUSE descriptor. When the
configured limit can hold one accounted request, the effective configured
ceiling is `(1 + active clones) * MaxInflightRequestBytes`. If the configured
limit is smaller than one request's accounting size, each active descriptor can
still admit one request.

