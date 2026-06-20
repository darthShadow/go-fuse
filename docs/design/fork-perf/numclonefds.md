# Cloned FUSE file descriptors

`MountOptions.NumCloneFDs` in `fuse/api.go` requests additional Linux
`/dev/fuse` file descriptors for the same mount session. It defaults to `0`.
Each active descriptor has its own kernel queue, reader goroutine tree, and
request-byte accounting in `fuseFD`.

Related docs: [overview](README.md), [sharded read-buffer pool](sharded-bytepool.md),
[sharded bridge maps](sharded-bridge-maps.md).

## RawConn Shape

`fuseFD` in `fuse/fusefd.go` owns `file *os.File` and `conn syscall.RawConn`.
`newFuseFD` wraps the descriptor with `os.NewFile`, calls `SyscallConn`, and
returns `(*fuseFD, error)` because RawConn setup can fail. On that failure it
closes the file before returning the error.

Reads, writes, clone ioctls, and backing-fd ioctls run through
`fuseFD.withFD`, which calls `conn.Control`. The `*os.File` owns descriptor
lifetime; `RawConn.Control` holds a reference while the syscall runs.

## Setup Order

`NewServer` in `fuse/server.go` mounts the primary descriptor, creates
`fuseFDs[0]`, and sets `protocolServer.writev = ms.fuseFDs[0].writev`.
It then calls `handleInit` before any clone open attempt.

`handleInit` reads the INIT request from `fuseFDs[0]` with `singleReader`
temporarily enabled, handles that request through `handleRequest(primary, req)`,
sets splice support when the kernel supports it, and calls `fileSystem.Init`.

Only after INIT succeeds does `NewServer` run the clone loop:

| Step | Current behavior |
|---|---|
| Clone request | `cloneFuseFDFn(ms.fuseFDs[0])` opens and binds a new descriptor. |
| Linux implementation | `cloneFuseFD` in `fuse/fusefd_linux.go` opens `/dev/fuse` and applies `FUSE_DEV_IOC_CLONE` through the primary RawConn. |
| Non-Linux implementation | `cloneFuseFD` in `fuse/fusefd_other.go` returns `ENOSYS`; `NewServer` logs and continues without that clone. |
| RawConn setup | Each clone passes through `newFuseFD`; a setup failure is logged and the server continues with already active descriptors. |
| Final binding | `ms.readPool.bindFDs(ms.fuseFDs)` runs after clone setup, so buffer shards match the final active descriptor set. |

`cloneFuseFDFn` is a package-level seam in `fuse/server.go`; tests replace it
to exercise full and partial graceful degradation.

## Request Routing

`Serve` starts one event loop per active descriptor. Clones run in goroutines;
the primary loop runs on the calling goroutine.

`loop(fd)` reads from that descriptor and passes the same `fd` into
`handleRequest(fd, req)`. `handleRequest` writes replies by calling
`fd.write(&req.request)`, so each reply goes back through the descriptor that
read the request.

Notifications stay on the primary descriptor because `NewServer` assigns
`protocolServer.writev` from `ms.fuseFDs[0].writev`. Kernel passthrough backing
IDs are session-global, so `RegisterBackingFd` and `UnregisterBackingFd` in
`fuse/passthrough_linux.go` also target `ms.fuseFDs[0]`.

## Failure Contract

Clone failure is non-fatal. `NewServer` logs failed `FUSE_DEV_IOC_CLONE`
attempts, logs clone `newFuseFD` failures, and continues serving with the
primary descriptor plus any clones already opened.

`NumCloneFDs` is a request count, not a guarantee. The active descriptor count
is `1 + active clones`, where `active clones` excludes clone attempts that fail
or fail RawConn setup.

## Inflight Request Bytes

`reserveRequestBytes` and `inflightRequestBytes` are fields on each `fuseFD`.
The accounting limit is therefore per descriptor, not global to the server.

When one request's accounting size fits within `MaxInflightRequestBytes`, the
effective configured ceiling is:

```text
(1 + active clones) * MaxInflightRequestBytes
```

When the configured limit is smaller than one request's accounting size, the
per-fd rule still admits one request per active descriptor and blocks the next
reservation on that descriptor.

