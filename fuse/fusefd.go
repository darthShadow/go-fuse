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
// bookkeeping (reqReaders, inflightRequestBytes), the write serialization
// mutex, and the WaitGroup tracking reader goroutines on this fd. The
// pools are shared with the owning Server via pointers, and a back
// pointer to the Server gives the per-fd methods access to shared
// configuration and callbacks.
//
// Today a Server has a single fuseFD; this struct exists so that adding
// support for FUSE_DEV_IOC_CLONE'd fds is a matter of widening the field
// to a slice.
type fuseFD struct {
	server *Server

	// I/O with kernel and daemon.
	fd int

	// writeMu serializes close and notify writes on fd.
	writeMu sync.Mutex

	reqMu                sync.Mutex
	reqReaders           int
	inflightRequestBytes int

	// loops tracks reader goroutines servicing fd.
	loops sync.WaitGroup

	// Shared pools owned by Server.
	reqPool  *sync.Pool
	readPool *sync.Pool
	buffers  *bufferPool

	// Accounting constants, set once at server construction.
	reqAllocBytes int
	readBufBytes  int
}

// newFuseFD returns a fuseFD bound to ms and ready to read from fd.
// All shared state (pools, buffer pool, accounting constants) is wired
// up here.
func (ms *Server) newFuseFD(fd int) *fuseFD {
	_, readBufBytes, reqAllocBytes := requestAccountingSizes(ms.opts.MaxWrite)
	return &fuseFD{
		server:        ms,
		fd:            fd,
		reqPool:       &ms.reqPool,
		readPool:      &ms.readPool,
		buffers:       &ms.buffers,
		reqAllocBytes: reqAllocBytes,
		readBufBytes:  readBufBytes,
	}
}

// readRequest reads one request from the kernel. Returns nil, OK if
// there are too many concurrent readers or insufficient request-bytes
// budget.
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
	dest := r.readPool.Get().([]byte)

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
		go ms.loop()
	}

	return req, OK
}

// returnRequest returns a request to the pool of unused requests.
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
	req.clear()

	r.reqMu.Lock()
	if p := req.bufferPoolInputBuf; p != nil {
		req.bufferPoolInputBuf = nil
		r.putReadBuf(p)
	}
	r.putReq(req)
	r.reqMu.Unlock()
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

// close closes the underlying FUSE fd. Serialized with writers via writeMu.
func (r *fuseFD) close() error {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	return syscall.Close(r.fd)
}

func (r *fuseFD) writev(iov [][]byte) (int, syscall.Errno) {
	// Protect against concurrent close.
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
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
