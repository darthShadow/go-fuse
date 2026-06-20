# Cloned FUSE file descriptors (`MountOptions.NumCloneFDs`)

`NumCloneFDs` opens that many extra `/dev/fuse` descriptors and binds them to the
mount session with `FUSE_DEV_IOC_CLONE` (Linux >= 4.2). Each cloned fd has its own
kernel queue and reader-goroutine tree, which removes the single-fd `read(2)`
contention that caps throughput on many-core machines. It defaults to 0 and is
ignored on non-Linux.

Per-fd state lives in the `fuseFD` type (`fusefd.go`, `fusefd_linux.go`,
`fusefd_other.go`); `Server` holds `fuseFDs []*fuseFD`. `fuseFDs[0]` is the original
mount fd; the rest are clones. Request replies are written back through the fd the
request was read from; notifications and `Register/UnregisterBackingFd` stay on
`fuseFDs[0]`. INIT runs on `fuseFDs[0]` before clones are created. A clone failure
logs and degrades to the already-open fds.

`MaxInflightRequestBytes` is enforced per fd, so the effective ceiling is
`(1+NumCloneFDs)x`; this is logged once at mount when a finite budget is set.

## Upstream origin

This implements upstream Gerrit change `hanwen/go-fuse` 1239029, which sits on two
upstream refactor commits:

- `6c5d127` -- encapsulate per-fd state in a `fuseFD` struct
- `8043edb` -- split `fuseFD` into its own file
- `266a7c1` -- the `NumCloneFDs` feature itself

The fork reproduces upstream's file layout and `fuseFD`/`fuseFDs` topology by hand
(the fork predated the refactor); it does not cherry-pick those commits.

## Divergences from upstream (preserve on any upstream merge)

These are intentional. When upstream's clone work lands or `266a7c1` changes, keep
the fork side of each:

| Area | Fork | Upstream | Why |
|---|---|---|---|
| `fuseFD.readPool` | `*bytePool` | `*sync.Pool` | fork's bounded decaying-LIFO read-buffer pool |
| `writeMu` | `sync.RWMutex` | `sync.Mutex` | parallel notify writes take `RLock`; close + passthrough take `Lock` |
| `returnRequest` | saves `bufferPoolInputBuf` before `req.clear()`, then returns it | reads it after `clear()` | fork's `requestAlloc.clear()` nils `bufferPoolInputBuf`; upstream order would leak the read buffer and undercount inflight bytes |
| latency stats | `latencies`/`recordStats`/`startTime` retained | removed in `03d7d38` | fork keeps `RecordLatencies`; `03d7d38` is NOT applied |
| clone seam | `var cloneFuseFDFn = cloneFuseFD` (`server.go`), called at the clone site | direct `cloneFuseFD` call | lets `TestNumCloneFDsGracefulDegrade` inject a clone failure without touching the verbatim `cloneFuseFD` |
| mount ceiling log | logs effective `(1+N)x` ceiling when finite | none | surfaces the per-fd budget multiplier |

Clean-merge points: `cloneFuseFD` and the `_FUSE_DEV_IOC_CLONE` constant
(`fusefd_linux.go`) are byte-verbatim from `266a7c1` and re-sync directly.

## Platform and testing

Linux-only; needs a kernel with `FUSE_DEV_IOC_CLONE` (>= 4.2). `TestNumCloneFDs`
exercises the success path (it skips when the kernel lacks clone support);
`TestNumCloneFDsGracefulDegrade` forces a clone failure and asserts the server still
serves with the surviving fd. Per-fd budget behavior is covered portably in
`bytepool_test.go`. The clone runtime, `-race`, and throughput benchmarks run only on
a Linux host.

## Re-sync checklist when upstream 1239029 changes

1. Diff the new upstream `fusefd*.go` / `server.go` clone code against this tree.
2. Re-take `cloneFuseFD` + `_FUSE_DEV_IOC_CLONE` verbatim if upstream changed them.
3. Keep every row of the divergence table above on the fork side.
4. Re-run the portable tests plus the Linux clone tests on a many-core box.
