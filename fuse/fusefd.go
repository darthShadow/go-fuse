// Copyright 2026 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fuse

import (
	"log"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

// fuseFD owns the per-FUSE-fd state: the fd itself, the read-side
// bookkeeping (reqReaders, inflightRequestBytes), the write
// serialization mutex, and the WaitGroup tracking reader goroutines on
// this fd. The pools are shared with the owning Server via pointers, and
// a back pointer to the Server gives the per-fd methods access to shared
// configuration and callbacks.
//
// fuseFDs[0] is the original /dev/fuse fd from mount(2); fuseFDs[1:] are
// FUSE_DEV_IOC_CLONE'd siblings (opt-in via MountOptions.NumCloneFDs).
// Each fd has an independent kernel queue and reader tree.
type fuseFD struct {
	server *Server

	// I/O with kernel and daemon.
	fd int

	// writeMu coordinates exclusive lifecycle operations on fd against
	// concurrent notify writes:
	//   - close takes Lock so that fd is not closed while a notify
	//     write or a passthrough ioctl is in flight.
	//   - registerBackingFd / unregisterBackingFd take Lock so their
	//     SYS_IOCTL on fd is excluded from a concurrent close and from
	//     the read-side used by notify writes.
	//   - writev (notify writes only) takes RLock so multiple notify
	//     writev's may proceed in parallel; each writev is one atomic
	//     /dev/fuse packet.
	// Regular handler replies (write) do not hold this lock; shutdown
	// safety for those depends on loops.Wait before close.
	writeMu sync.RWMutex

	reqMu                sync.Mutex
	reqReaders           int
	inflightRequestBytes int

	// loops tracks reader goroutines servicing fd.
	loops sync.WaitGroup

	// Shared pools owned by Server (per-fd pointers; accounting is
	// per-fd; each readPool shard is assigned by the Server).
	reqPool  *sync.Pool
	readPool *bytePoolShard
	buffers  *bufferPool

	// Accounting constants, set once at fuseFD construction.
	reqAllocBytes int
	readBufBytes  int
}

// newFuseFD returns a fuseFD bound to ms and ready to read from fd. All
// shared state (pools, buffer pool, accounting constants) is wired up
// here.
func (ms *Server) newFuseFD(fd int) *fuseFD {
	_, readBufBytes, reqAllocBytes := requestAccountingSizes(ms.opts.MaxWrite)
	return &fuseFD{
		server:  ms,
		fd:      fd,
		reqPool: &ms.reqPool,
		// Temporary: shard(0) is a placeholder; bindFDs rebinds each fd to its own shard before Serve.
		readPool:      ms.readPool.shard(0),
		buffers:       &ms.buffers,
		reqAllocBytes: reqAllocBytes,
		readBufBytes:  readBufBytes,
	}
}

// readRequest reads one request from the kernel. Returns nil, OK if
// there are too many concurrent readers or insufficient request-bytes
// budget on this fd.
func (r *fuseFD) readRequest() (req *requestAlloc, code Status) {
	ms := r.server
	r.reqMu.Lock()
	if r.reqReaders > ms.maxReaders || !r.reserveRequestBytes() {
		r.reqMu.Unlock()
		return nil, OK
	}
	r.reqReaders++
	r.reqMu.Unlock()

	req = r.reqPool.Get().(*requestAlloc)
	dest := r.readPool.Get()

	var n int
	err := handleEINTR(func() error {
		var err error
		n, err = syscall.Read(r.fd, dest)
		return err
	})
	if err != nil {
		r.reqMu.Lock()
		r.putReadBuf(dest)
		r.putReq(req)
		r.reqReaders--
		r.reqMu.Unlock()
		return nil, ToStatus(err)
	}

	if ms.latencies != nil {
		req.startTime = time.Now()
	}
	r.reqMu.Lock()
	defer r.reqMu.Unlock()
	gobbled := req.setInput(dest[:n])
	if len(req.inputBuf) < int(unsafe.Sizeof(InHeader{})) {
		log.Printf("Short read for input header: %v", req.inputBuf)
		r.putReadBuf(dest)
		r.putReq(req)
		r.reqReaders--
		return nil, EINVAL
	}
	opCode := ((*InHeader)(unsafe.Pointer(&req.inputBuf[0]))).Opcode
	/* These messages don't expect reply, so they cost nothing for
	   the kernel to send. Make sure we're not overwhelmed by not
	   spawning a new reader.
	*/
	needsBackPressure := (opCode == _OP_FORGET || opCode == _OP_BATCH_FORGET)

	if !gobbled {
		r.putReadBuf(dest)
	}
	r.reqReaders--
	if !ms.singleReader && r.reqReaders <= 0 && !needsBackPressure {
		r.loops.Add(1)
		go ms.loop(r)
	}

	return req, OK
}

func (r *fuseFD) reserveRequestBytes() bool {
	if !r.canReserveRequestBytes() {
		return false
	}
	r.inflightRequestBytes += r.requestBytes()
	return true
}

func (r *fuseFD) canReserveRequestBytes() bool {
	return r.inflightRequestBytes == 0 ||
		r.requestBytes() <= r.server.opts.MaxInflightRequestBytes-r.inflightRequestBytes
}

// canAcceptAnother wraps canReserveRequestBytes with reqMu, for callers
// that don't already hold the lock.
func (r *fuseFD) canAcceptAnother() bool {
	r.reqMu.Lock()
	defer r.reqMu.Unlock()
	return r.canReserveRequestBytes()
}

func (r *fuseFD) requestBytes() int {
	return r.reqAllocBytes + r.readBufBytes
}

func (r *fuseFD) putReadBuf(buf []byte) {
	r.readPool.Put(buf)
	r.inflightRequestBytes -= r.readBufBytes
}

func (r *fuseFD) putReq(req *requestAlloc) {
	r.reqPool.Put(req)
	r.inflightRequestBytes -= r.reqAllocBytes
}

// returnRequest returns a request to the pool of unused requests.
//
// Save-before-clear: the fork's requestAlloc.clear() nils
// bufferPoolInputBuf, so the gobbled read buffer pointer MUST be saved
// before clear() and freed after, or it leaks (verified constraint 3).
func (r *fuseFD) returnRequest(req *requestAlloc) {
	r.server.recordStats(&req.request)

	if req.bufferPoolOutputBuf != nil {
		r.buffers.FreeBuffer(req.bufferPoolOutputBuf)
		req.bufferPoolOutputBuf = nil
	}

	if req.interrupted {
		req.interrupted = false
		req.cancel = make(chan struct{}, 0)
	}
	p := req.bufferPoolInputBuf
	req.bufferPoolInputBuf = nil
	req.clear()

	r.reqMu.Lock()
	if p != nil {
		r.putReadBuf(p)
	}
	r.putReq(req)
	r.reqMu.Unlock()
}

// close closes the underlying FUSE fd. Serialized with writers via
// writeMu (Lock, exclusive).
func (r *fuseFD) close() error {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	return syscall.Close(r.fd)
}

// writev performs a notify writev to fd. RLock allows multiple notify
// writev's to proceed in parallel (each writev is atomic at the
// /dev/fuse boundary); close takes the exclusive Lock.
func (r *fuseFD) writev(iov [][]byte) (int, syscall.Errno) {
	r.writeMu.RLock()
	defer r.writeMu.RUnlock()
	n, err := writev(r.fd, iov)

	var errno syscall.Errno
	if err != nil {
		errno = err.(syscall.Errno)
		if errno == syscall.EINVAL {
			// Detail: the kernel returns EINVAL for unsupported
			// notify methods.
			errno = syscall.ENOSYS
		}
	}
	return n, errno
}
