package fuse

import (
	"io"
	"log"
	"math"
	"sync"
	"syscall"
	"testing"
	"time"
)

func newTestFuseFD(maxWrite int) (*Server, *fuseFD) {
	srv, fds := newTestFuseFDs(maxWrite, 1)
	return srv, fds[0]
}

func newTestFuseFDs(maxWrite int, fdCount int) (*Server, []*fuseFD) {
	readBufSize, readBufBytes, reqAllocBytes := requestAccountingSizes(maxWrite)
	srv := &Server{
		opts: &MountOptions{
			MaxWrite:                maxWrite,
			MaxInflightRequestBytes: math.MaxInt,
		},
	}
	srv.reqPool.New = func() interface{} {
		return &requestAlloc{
			request: request{
				cancel: make(chan struct{}),
			},
		}
	}
	srv.initReadPool(readBufSize, readBufBytes)

	fds := make([]*fuseFD, 0, fdCount)
	for i := 0; i < fdCount; i++ {
		fds = append(fds, &fuseFD{
			server:        srv,
			reqPool:       &srv.reqPool,
			readPool:      nil,
			buffers:       &srv.buffers,
			reqAllocBytes: reqAllocBytes,
			readBufBytes:  readBufBytes,
		})
	}
	srv.fuseFDs = fds
	srv.readPool.bindFDs(srv.fuseFDs)
	return srv, fds
}

func TestBytePoolBindFDsAssignsPerFDShards(t *testing.T) {
	const maxWrite = 4096

	srv, fds := newTestFuseFDs(maxWrite, 3)
	seen := make(map[*bytePoolShard]bool, len(fds))
	for i, fd := range fds {
		if fd.readPool == nil {
			t.Fatalf("fuseFD[%d] readPool shard is nil", i)
		}
		if got, want := fd.readPool, srv.readPool.shard(i); got != want {
			t.Fatalf("fuseFD[%d] readPool shard invalid: got %p want %p", i, got, want)
		}
		if seen[fd.readPool] {
			t.Fatalf("fuseFD[%d] reused a readPool shard; want distinct shards", i)
		}
		seen[fd.readPool] = true
	}
}

func TestBytePoolShardCapSumWithinServerBudget(t *testing.T) {
	const maxWrite = 4096
	const extraFDs = 7

	srv, fds := newTestFuseFDs(maxWrite, readPoolMaxRetainedBuffers+extraFDs)
	if got, want := len(srv.readPool.shards), len(fds); got != want {
		t.Fatalf("readPool shard count invalid: got %d want %d", got, want)
	}

	totalCap := 0
	for i, shard := range srv.readPool.shards {
		totalCap += shard.maxRetained
		if shard.maxRetained < 0 {
			t.Fatalf("readPool shard[%d] cap invalid: got %d want >= 0", i, shard.maxRetained)
		}
	}
	if totalCap > readPoolMaxRetainedBuffers {
		t.Fatalf("readPool shard cap sum invalid: got %d want <= %d", totalCap, readPoolMaxRetainedBuffers)
	}
}

func TestBytePoolReclaimerStartStopDrain(t *testing.T) {
	bufPool := newBytePool(2, func() []byte {
		return make([]byte, 10)
	})
	shard := bufPool.shard(0)
	b := shard.Get()
	shard.Put(b)
	if got, want := bufPool.NumPooled(), 1; got != want {
		t.Fatalf("retained buffers before stop/drain invalid: got %d want %d", got, want)
	}

	bufPool.startReclaimer()
	firstDone := waitBytePoolReclaimerDoneSignal(t, bufPool)
	bufPool.startReclaimer()
	secondDone := waitBytePoolReclaimerDoneSignal(t, bufPool)
	if firstDone != secondDone {
		t.Fatalf("startReclaimer installed a second done channel: got %p want %p", secondDone, firstDone)
	}

	bufPool.stopReclaimer()
	select {
	case <-firstDone:
	default:
		t.Fatal("reclaimerDone was not closed after stopReclaimer returned")
	}

	if got, want := bufPool.NumPooled(), 1; got != want {
		t.Fatalf("retained buffers after stop before drain invalid: got %d want %d", got, want)
	}

	bufPool.drain()
	bufPool.stopReclaimer()
	bufPool.drain()
	if got := bufPool.NumPooled(); got != 0 {
		t.Fatalf("retained buffers after repeated stop/drain invalid: got %d want %d", got, 0)
	}
}

func TestBytePoolUnmountStopsReclaimerAndDrains(t *testing.T) {
	const maxWrite = 4096

	srv, fd := newTestFuseFD(maxWrite)
	b := fd.readPool.Get()
	fd.readPool.Put(b)
	if got, want := srv.readPool.NumPooled(), 1; got != want {
		t.Fatalf("readPool retained size before unmount invalid: got %d want %d", got, want)
	}

	srv.readPool.startReclaimer()
	done := waitBytePoolReclaimerDoneSignal(t, &srv.readPool)
	if err := srv.Unmount(); err != nil {
		t.Fatalf("Unmount returned error: %v", err)
	}

	if got := srv.readPool.NumPooled(); got != 0 {
		t.Fatalf("readPool retained size after unmount invalid: got %d want %d", got, 0)
	}
	select {
	case <-done:
	default:
		t.Fatal("reclaimerDone was not closed after Unmount returned")
	}
}

func TestBytePoolServeExitStopsReclaimerAndDrains(t *testing.T) {
	const maxWrite = 4096

	srv, writeFD := newBytePoolServeExitTestServer(t, maxWrite)
	var closeWriteOnce sync.Once
	closeWrite := func() {
		closeWriteOnce.Do(func() {
			if err := syscall.Close(writeFD); err != nil {
				t.Errorf("close pipe write fd: %v", err)
			}
		})
	}

	serveDone := make(chan struct{})
	t.Cleanup(func() {
		closeWrite()
		select {
		case <-serveDone:
		case <-time.After(5 * time.Second):
			t.Error("Serve did not exit after closing test pipe")
		}
	})

	go func() {
		srv.Serve()
		close(serveDone)
	}()
	reclaimerDone := waitBytePoolReclaimerDoneSignal(t, &srv.readPool)

	b := srv.fuseFDs[0].readPool.Get()
	srv.fuseFDs[0].readPool.Put(b)
	if got := srv.readPool.NumPooled(); got == 0 {
		t.Fatal("readPool retained size before Serve exit invalid: got 0 want > 0")
	}

	closeWrite()
	select {
	case <-serveDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not exit after closing test pipe")
	}

	if got := srv.readPool.NumPooled(); got != 0 {
		t.Fatalf("readPool retained size after Serve exit invalid: got %d want %d", got, 0)
	}
	select {
	case <-reclaimerDone:
	default:
		t.Fatal("reclaimerDone was not closed after Serve returned")
	}
}

func newBytePoolServeExitTestServer(t *testing.T, maxWrite int) (*Server, int) {
	t.Helper()

	fs := NewDefaultRawFileSystem()
	opts := &MountOptions{
		MaxWrite: maxWrite,
		Logger:   log.New(io.Discard, "", 0),
	}
	opts.setDefaults(fs)

	readBufSize, readBufBytes, _ := requestAccountingSizes(opts.MaxWrite)
	srv := &Server{
		protocolServer: protocolServer{
			fileSystem:  fs,
			retrieveTab: make(map[uint64]*retrieveCacheRequest),
			opts:        opts,
		},
		opts:         opts,
		maxReaders:   1,
		singleReader: true,
		ready:        make(chan error, 1),
	}
	srv.reqPool.New = func() interface{} {
		return &requestAlloc{
			request: request{
				cancel: make(chan struct{}),
			},
		}
	}
	srv.initReadPool(readBufSize, readBufBytes)

	pipeFDs := []int{-1, -1}
	if err := syscall.Pipe(pipeFDs); err != nil {
		t.Fatalf("Pipe: %v", err)
	}
	fd, err := srv.newFuseFD(pipeFDs[0])
	if err != nil {
		syscall.Close(pipeFDs[0])
		syscall.Close(pipeFDs[1])
		t.Fatalf("newFuseFD: %v", err)
	}
	srv.fuseFDs = []*fuseFD{fd}
	srv.readPool.bindFDs(srv.fuseFDs)
	srv.protocolServer.writev = fd.writev
	fd.loops.Add(1)
	return srv, pipeFDs[1]
}

func waitBytePoolReclaimerDoneSignal(t *testing.T, bufPool *bytePool) <-chan struct{} {
	t.Helper()

	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		bufPool.reclaimerMu.Lock()
		done := bufPool.reclaimerDone
		bufPool.reclaimerMu.Unlock()
		if done != nil {
			return done
		}

		select {
		case <-deadline.C:
			t.Fatal("reclaimerDone was not set after reclaimer started")
		case <-ticker.C:
		}
	}
}

func TestBytePoolBindFDsAssignsZeroCapShardsPastRetainedBudget(t *testing.T) {
	const maxWrite = 4096
	const extraFDs = 3

	readBufSize, _, _ := requestAccountingSizes(maxWrite)
	srv, fds := newTestFuseFDs(maxWrite, readPoolMaxRetainedBuffers+extraFDs)
	if got, want := len(srv.readPool.shards), len(fds); got != want {
		t.Fatalf("readPool shard count invalid: got %d want %d", got, want)
	}

	for i, fd := range fds {
		if got, want := fd.readPool, srv.readPool.shard(i); got != want {
			t.Fatalf("fuseFD[%d] readPool shard invalid: got %p want %p", i, got, want)
		}

		first := fd.readPool.Get()
		if got, want := len(first), readBufSize; got != want {
			t.Fatalf("fuseFD[%d] read buffer length invalid: got %d want %d", i, got, want)
		}
		fd.readPool.Put(first[:1])

		if i < readPoolMaxRetainedBuffers {
			if got, want := fd.readPool.maxRetained, 1; got != want {
				t.Fatalf("fuseFD[%d] retained cap invalid: got %d want %d", i, got, want)
			}
			if got, want := fd.readPool.NumPooled(), 1; got != want {
				t.Fatalf("fuseFD[%d] retained buffers invalid: got %d want %d", i, got, want)
			}
			continue
		}

		if got, want := fd.readPool.maxRetained, 0; got != want {
			t.Fatalf("fuseFD[%d] retained cap invalid: got %d want %d", i, got, want)
		}
		if got, want := cap(fd.readPool.buffers), 0; got != want {
			t.Fatalf("fuseFD[%d] retained storage cap invalid: got %d want %d", i, got, want)
		}
		if got, want := fd.readPool.NumPooled(), 0; got != want {
			t.Fatalf("fuseFD[%d] retained buffers invalid: got %d want %d", i, got, want)
		}

		second := fd.readPool.Get()
		if got, want := len(second), readBufSize; got != want {
			t.Fatalf("fuseFD[%d] second read buffer length invalid: got %d want %d", i, got, want)
		}
		if &first[0] == &second[0] {
			t.Fatalf("fuseFD[%d] zero-cap shard reused a retained buffer; want fresh allocation", i)
		}
		fd.readPool.Put(second[:1])
		if got, want := fd.readPool.NumPooled(), 0; got != want {
			t.Fatalf("fuseFD[%d] retained buffers after second put invalid: got %d want %d", i, got, want)
		}
	}

	if got := srv.readPool.NumPooled(); got > readPoolMaxRetainedBuffers {
		t.Fatalf("server readPool retained too many buffers: got %d want <= %d", got, readPoolMaxRetainedBuffers)
	}
}

func TestFuseFDPutReadBufReturnsToOriginShard(t *testing.T) {
	const maxWrite = 4096

	srv, fds := newTestFuseFDs(maxWrite, 2)
	fd := fds[0]
	otherFD := fds[1]
	readBuf := fd.readPool.Get()
	fd.inflightRequestBytes = fd.readBufBytes

	fd.putReadBuf(readBuf[:2])

	if got := fd.inflightRequestBytes; got != 0 {
		t.Fatalf("inflightRequestBytes after putReadBuf invalid: got %d want %d", got, 0)
	}
	if got := srv.readPool.NumPooled(); got != 1 {
		t.Fatalf("readPool retained size after putReadBuf invalid: got %d want %d", got, 1)
	}
	if got := fd.readPool.NumPooled(); got != 1 {
		t.Fatalf("origin shard retained size after putReadBuf invalid: got %d want %d", got, 1)
	}
	if got := otherFD.readPool.NumPooled(); got != 0 {
		t.Fatalf("non-origin shard retained size after putReadBuf invalid: got %d want %d", got, 0)
	}
}

func TestReturnRequestReleasesGobbledReadBufferToOriginShard(t *testing.T) {
	const maxWrite = 4096

	srv, fds := newTestFuseFDs(maxWrite, 2)
	fd := fds[0]
	otherFD := fds[1]
	req := srv.reqPool.Get().(*requestAlloc)
	readBuf := fd.readPool.Get()
	fd.inflightRequestBytes = fd.requestBytes()

	gobbled := req.setInput(readBuf[:cap(req.smallInputBuf)])
	if !gobbled {
		t.Fatalf("setInput gobbled input: got %t want true", gobbled)
	}
	if got := srv.readPool.NumPooled(); got != 0 {
		t.Fatalf("readPool retained size before return invalid: got %d want %d", got, 0)
	}

	fd.returnRequest(req)

	if got := fd.inflightRequestBytes; got != 0 {
		t.Fatalf("inflightRequestBytes after return invalid: got %d want %d", got, 0)
	}
	if got := srv.readPool.NumPooled(); got != 1 {
		t.Fatalf("readPool retained size after gobbled request return invalid: got %d want %d", got, 1)
	}
	if got := fd.readPool.NumPooled(); got != 1 {
		t.Fatalf("origin shard retained size after gobbled request return invalid: got %d want %d", got, 1)
	}
	if got := otherFD.readPool.NumPooled(); got != 0 {
		t.Fatalf("non-origin shard retained size after gobbled request return invalid: got %d want %d", got, 0)
	}
	if req.bufferPoolInputBuf != nil {
		t.Fatalf("bufferPoolInputBuf after return invalid: got %v want <nil>", req.bufferPoolInputBuf)
	}
}

func TestFuseFDReserveRequestBytesAllowsSingleRequestBelowBudget(t *testing.T) {
	const maxWrite = 4096

	srv, fd := newTestFuseFD(maxWrite)
	srv.opts.MaxInflightRequestBytes = 1

	if ok := fd.reserveRequestBytes(); !ok {
		t.Fatal("first reservation failed below request size budget")
	}
	if ok := fd.reserveRequestBytes(); ok {
		t.Fatal("second reservation succeeded below request size budget")
	}
}

func TestFuseFDReserveRequestBytesBoundary(t *testing.T) {
	const maxWrite = 4096

	tests := []struct {
		name       string
		budget     func(requestBytes int) int
		wantSecond bool
	}{
		{
			name: "one byte below two requests",
			budget: func(requestBytes int) int {
				return requestBytes*2 - 1
			},
			wantSecond: false,
		},
		{
			name: "exactly two requests",
			budget: func(requestBytes int) int {
				return requestBytes * 2
			},
			wantSecond: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, fd := newTestFuseFD(maxWrite)
			requestBytes := fd.requestBytes()
			srv.opts.MaxInflightRequestBytes = tt.budget(requestBytes)

			if ok := fd.reserveRequestBytes(); !ok {
				t.Fatalf("first reservation failed with budget %d and request size %d", srv.opts.MaxInflightRequestBytes, requestBytes)
			}
			if got, want := fd.reserveRequestBytes(), tt.wantSecond; got != want {
				t.Fatalf("second reservation invalid: got %t want %t with budget %d and request size %d", got, want, srv.opts.MaxInflightRequestBytes, requestBytes)
			}
		})
	}
}

func TestFuseFDReserveRequestBytesIsPerFD(t *testing.T) {
	const maxWrite = 4096

	srv, fds := newTestFuseFDs(maxWrite, 3)
	primary := fds[0]
	requestBytes := primary.requestBytes()
	srv.opts.MaxInflightRequestBytes = requestBytes

	total := 0
	for i, fd := range srv.fuseFDs {
		if ok := fd.reserveRequestBytes(); !ok {
			t.Fatalf("reserveRequestBytes failed on fuseFD[%d]", i)
		}
		total += fd.inflightRequestBytes
	}
	if got, want := total, len(srv.fuseFDs)*requestBytes; got != want {
		t.Fatalf("total inflightRequestBytes invalid: got %d want %d", got, want)
	}
	if ok := srv.fuseFDs[0].reserveRequestBytes(); ok {
		t.Fatal("second reservation on one fd succeeded; want per-fd ceiling")
	}
}
