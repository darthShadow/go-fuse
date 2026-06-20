// Copyright 2026 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fuse

import (
	"bytes"
	"io"
	"log"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"
)

type blockingWriteFS struct {
	defaultRawFileSystem

	entered     chan uint64
	release     chan struct{}
	releaseOnce sync.Once
}

func newBlockingWriteFS() *blockingWriteFS {
	return &blockingWriteFS{
		entered: make(chan uint64, 32),
		release: make(chan struct{}),
	}
}

func (f *blockingWriteFS) unblock() {
	f.releaseOnce.Do(func() {
		close(f.release)
	})
}

func (f *blockingWriteFS) Lookup(cancel <-chan struct{}, header *InHeader, name string, out *EntryOut) (code Status) {
	if name != "file" {
		return ENOENT
	}

	out.NodeId = 2
	out.Attr = Attr{
		Ino:   2,
		Mode:  S_IFREG | 0644,
		Nlink: 1,
		Size:  1 << 20,
	}
	return OK
}

func (f *blockingWriteFS) GetAttr(cancel <-chan struct{}, input *GetAttrIn, out *AttrOut) (code Status) {
	out.Attr = Attr{
		Ino:   input.NodeId,
		Mode:  S_IFREG | 0644,
		Nlink: 1,
		Size:  1 << 20,
	}
	return OK
}

func (f *blockingWriteFS) Open(cancel <-chan struct{}, input *OpenIn, out *OpenOut) (status Status) {
	if input.NodeId != 2 {
		return ENOENT
	}

	out.OpenFlags = FOPEN_DIRECT_IO | FOPEN_PARALLEL_DIRECT_WRITES
	return OK
}

func (f *blockingWriteFS) Write(cancel <-chan struct{}, input *WriteIn, data []byte) (written uint32, code Status) {
	if len(data) < len(requestAlloc{}.smallInputBuf) {
		return 0, EINVAL
	}

	select {
	case f.entered <- input.Unique:
	default:
	}

	select {
	case <-f.release:
	case <-cancel:
		return 0, EINTR
	}

	return uint32(len(data)), OK
}

func TestMaxInflightRequestBytesLimitsLargeWritesAndKeepsReader(t *testing.T) {
	const (
		requestCount = 3
		maxWrite     = 4096
	)

	requestBytes := requestBytesForTest(maxWrite)
	for _, tc := range []struct {
		name              string
		maxInflight       int
		wantBeforeRelease int
	}{
		{
			name:              "below single request",
			maxInflight:       1,
			wantBeforeRelease: 1,
		},
		{
			name:              "exactly single request",
			maxInflight:       requestBytes,
			wantBeforeRelease: 1,
		},
		{
			name:              "one byte below two requests",
			maxInflight:       2*requestBytes - 1,
			wantBeforeRelease: 1,
		},
		{
			name:              "exactly two requests",
			maxInflight:       2 * requestBytes,
			wantBeforeRelease: 2,
		},
		{
			name:              "exactly all requests",
			maxInflight:       requestCount * requestBytes,
			wantBeforeRelease: requestCount,
		},
		{
			name:              "default unlimited",
			maxInflight:       0,
			wantBeforeRelease: requestCount,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testMaxInflightRequestBytesLargeWrites(t, maxWrite, requestCount, tc.maxInflight, tc.wantBeforeRelease)
		})
	}
}

func TestMaxInflightRequestBytesBudgetIsPerFuseFD(t *testing.T) {
	const maxWrite = 4096

	_, readBufBytes, reqAllocBytes := requestAccountingSizes(maxWrite)
	requestBytes := reqAllocBytes + readBufBytes
	srv := &Server{
		opts: &MountOptions{
			MaxInflightRequestBytes: requestBytes,
		},
	}
	primary := &fuseFD{
		server:        srv,
		reqAllocBytes: reqAllocBytes,
		readBufBytes:  readBufBytes,
	}
	clone := &fuseFD{
		server:        srv,
		reqAllocBytes: reqAllocBytes,
		readBufBytes:  readBufBytes,
	}
	srv.fuseFDs = []*fuseFD{primary, clone}

	if !primary.reserveRequestBytes() {
		t.Fatalf("primary.reserveRequestBytes() = false, want true for first request")
	}
	if primary.reserveRequestBytes() {
		t.Fatalf("primary.reserveRequestBytes() = true for second request, want false with per-fd limit %d", requestBytes)
	}
	if !clone.reserveRequestBytes() {
		t.Fatalf("clone.reserveRequestBytes() = false, want true because MaxInflightRequestBytes is per fd")
	}
	if got, want := primary.inflightRequestBytes+clone.inflightRequestBytes, 2*requestBytes; got != want {
		t.Fatalf("total inflight bytes across active fds = %d, want %d", got, want)
	}
}

func testMaxInflightRequestBytesLargeWrites(t *testing.T, maxWrite, requestCount, maxInflight, wantBeforeRelease int) {
	t.Helper()

	fs := newBlockingWriteFS()
	mnt := t.TempDir()
	opts := MountOptions{
		MaxWrite:                maxWrite,
		MaxInflightRequestBytes: maxInflight,
		Logger:                  log.New(io.Discard, "", 0),
	}

	srv, err := NewServer(fs, mnt, &opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		fs.unblock()
		if err := srv.Unmount(); err != nil {
			t.Fatalf("Unmount: %v", err)
		}
	})
	go srv.Serve()
	if err := srv.WaitMount(); err != nil {
		t.Fatal(err)
	}

	// Use one file descriptor per writer so the kernel does not serialize
	// direct writes through a single file handle before they reach FUSE.
	fds := make([]int, requestCount)
	for i := range fds {
		fd, err := syscall.Open(mnt+"/file", syscall.O_WRONLY, 0)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		fds[i] = fd
	}
	t.Cleanup(func() {
		// Release any blocked WRITE handlers before closing their file descriptors.
		fs.unblock()
		for _, fd := range fds {
			if err := syscall.Close(fd); err != nil {
				t.Fatalf("Close: %v", err)
			}
		}
	})

	payload := bytes.Repeat([]byte("x"), maxWrite)
	writeResults := make(chan error, requestCount)
	for i := 0; i < requestCount; i++ {
		fd := fds[i]
		offset := int64(i * maxWrite)
		go func() {
			n, err := syscall.Pwrite(fd, payload, offset)
			if err != nil {
				writeResults <- err
				return
			}
			if n != len(payload) {
				writeResults <- io.ErrShortWrite
				return
			}
			writeResults <- nil
		}()
	}

	seen := make(map[uint64]bool)
	waitWriteEnteredSet(t, fs.entered, wantBeforeRelease, seen)
	if wantBeforeRelease < requestCount {
		if got := receiveWriteEntered(fs.entered, 50*time.Millisecond); got != 0 {
			t.Fatalf("WRITE unique %d entered before release; max inflight request bytes = %d, want %d writes before release",
				got, maxInflight, wantBeforeRelease)
		}
	}

	fs.unblock()

	waitWriteEnteredSet(t, fs.entered, requestCount, seen)
	for i := 0; i < requestCount; i++ {
		if err := waitWriteResult(writeResults); err != nil {
			t.Fatalf("write %d failed: %v", i+1, err)
		}
	}

	waitForReader(t, srv)
}

func requestBytesForTest(maxWrite int) int {
	_, readBufBytes, reqAllocBytes := requestAccountingSizes(maxWrite)
	return reqAllocBytes + readBufBytes
}

func waitWriteEntered(t *testing.T, ch <-chan uint64) uint64 {
	t.Helper()
	select {
	case unique := <-ch:
		return unique
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for WRITE")
		return 0
	}
}

func waitWriteEnteredSet(t *testing.T, ch <-chan uint64, count int, got map[uint64]bool) {
	t.Helper()
	for len(got) < count {
		unique := waitWriteEntered(t, ch)
		if got[unique] {
			t.Fatalf("duplicate WRITE unique %d", unique)
		}
		got[unique] = true
	}
}

func receiveWriteEntered(ch <-chan uint64, timeout time.Duration) uint64 {
	select {
	case unique := <-ch:
		return unique
	case <-time.After(timeout):
		return 0
	}
}

func waitWriteResult(ch <-chan error) error {
	select {
	case err := <-ch:
		return err
	case <-time.After(time.Second):
		return syscall.ETIMEDOUT
	}
}

func waitForReader(t *testing.T, srv *Server) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var readers int
		for _, fd := range srv.fuseFDs {
			fd.reqMu.Lock()
			readers += fd.reqReaders
			fd.reqMu.Unlock()
		}
		if readers > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for a request reader")
}

func TestWaitForReaderObservesCloneFD(t *testing.T) {
	srv := &Server{
		fuseFDs: []*fuseFD{
			{},
			{reqReaders: 1},
		},
	}

	waitForReader(t, srv)
}

// minimalFS supports just enough operations to mount and stat the root.
type minimalFS struct{ defaultRawFileSystem }

func (*minimalFS) GetAttr(cancel <-chan struct{}, in *GetAttrIn, out *AttrOut) Status {
	out.Attr = Attr{Ino: 1, Mode: S_IFDIR | 0755, Nlink: 2}
	return OK
}

type pipeBackedFuseFD struct {
	fd     *fuseFD
	readFD int
}

func newPipeBackedFuseFD(t *testing.T, srv *Server) pipeBackedFuseFD {
	t.Helper()

	pipeFDs := []int{-1, -1}
	if err := syscall.Pipe(pipeFDs); err != nil {
		t.Fatalf("Pipe: %v", err)
	}
	if err := syscall.SetNonblock(pipeFDs[0], true); err != nil {
		syscall.Close(pipeFDs[0])
		syscall.Close(pipeFDs[1])
		t.Fatalf("SetNonblock: %v", err)
	}

	fd, err := srv.newFuseFD(pipeFDs[1])
	if err != nil {
		syscall.Close(pipeFDs[0])
		syscall.Close(pipeFDs[1])
		t.Fatalf("newFuseFD: %v", err)
	}
	pipeFDs[1] = -1
	t.Cleanup(func() {
		if err := fd.close(); err != nil {
			t.Errorf("close pipe-backed fuseFD: %v", err)
		}
		if err := syscall.Close(pipeFDs[0]); err != nil {
			t.Errorf("close pipe read fd: %v", err)
		}
	})

	return pipeBackedFuseFD{fd: fd, readFD: pipeFDs[0]}
}

func newHandleRequestTestServer(fs RawFileSystem) *Server {
	opts := &MountOptions{Logger: log.New(io.Discard, "", 0)}
	opts.setDefaults(fs)
	srv := &Server{
		opts: opts,
	}
	srv.protocolServer = protocolServer{
		fileSystem:  fs,
		retrieveTab: make(map[uint64]*retrieveCacheRequest),
		opts:        opts,
	}
	return srv
}

func newGetAttrRequest(unique uint64) *requestAlloc {
	getAttr := GetAttrIn{
		InHeader: InHeader{
			Length: uint32(unsafe.Sizeof(GetAttrIn{})),
			Opcode: _OP_GETATTR,
			Unique: unique,
			NodeId: FUSE_ROOT_ID,
		},
	}
	input := make([]byte, int(unsafe.Sizeof(getAttr)))
	copy(input, unsafe.Slice((*byte)(unsafe.Pointer(&getAttr)), len(input)))
	return &requestAlloc{
		request: request{
			cancel:   make(chan struct{}),
			inputBuf: input,
		},
	}
}

func readPipeBytes(t *testing.T, name string, fd int) []byte {
	t.Helper()

	buf := make([]byte, 4096)
	n, err := syscall.Read(fd, buf)
	if err == syscall.EAGAIN || err == syscall.EWOULDBLOCK {
		return nil
	}
	if err != nil {
		t.Fatalf("%s pipe read: %v", name, err)
	}
	return buf[:n]
}

func outHeaderFromBytes(t *testing.T, buf []byte) OutHeader {
	t.Helper()

	if len(buf) < int(sizeOfOutHeader) {
		t.Fatalf("reply length = %d, want at least %d", len(buf), int(sizeOfOutHeader))
	}
	var out OutHeader
	copy(unsafe.Slice((*byte)(unsafe.Pointer(&out)), int(sizeOfOutHeader)), buf[:int(sizeOfOutHeader)])
	return out
}

func TestHandleRequestWritesReplyToOriginatingFuseFD(t *testing.T) {
	const unique = 12345

	srv := newHandleRequestTestServer(&minimalFS{})
	primary := newPipeBackedFuseFD(t, srv)
	clone := newPipeBackedFuseFD(t, srv)
	srv.fuseFDs = []*fuseFD{primary.fd, clone.fd}
	srv.protocolServer.writev = primary.fd.writev

	if code := srv.handleRequest(clone.fd, newGetAttrRequest(unique)); !code.Ok() {
		t.Fatalf("handleRequest status = %v, want OK", code)
	}

	primaryReply := readPipeBytes(t, "primary", primary.readFD)
	cloneReply := readPipeBytes(t, "clone", clone.readFD)
	if len(cloneReply) == 0 {
		t.Fatalf("originating clone fd received no reply; primary fd received %d bytes", len(primaryReply))
	}
	if len(primaryReply) != 0 {
		t.Fatalf("primary fd received %d reply bytes, want none for request handled on clone fd", len(primaryReply))
	}
	if got, want := len(cloneReply), int(sizeOfOutHeader)+int(unsafe.Sizeof(AttrOut{})); got != want {
		t.Fatalf("clone reply length = %d, want %d", got, want)
	}
	out := outHeaderFromBytes(t, cloneReply)
	if got := out.Unique; got != unique {
		t.Fatalf("clone reply unique = %d, want %d", got, unique)
	}
	if got := out.Status; got != 0 {
		t.Fatalf("clone reply status = %d, want 0", got)
	}
}

func TestNumCloneFDs(t *testing.T) {
	const want = 3

	mnt := t.TempDir()
	srv, err := NewServer(&minimalFS{}, mnt, &MountOptions{
		NumCloneFDs: want - 1,
		Logger:      log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := srv.Unmount(); err != nil {
			t.Errorf("Unmount: %v", err)
		}
	})
	gotFDs := len(srv.fuseFDs)

	go srv.Serve()
	if err := srv.WaitMount(); err != nil {
		t.Fatal(err)
	}

	if gotFDs == 1 {
		if err := srv.Unmount(); err != nil {
			t.Fatalf("Unmount: %v", err)
		}
		t.Skipf("kernel lacks FUSE_DEV_IOC_CLONE support: len(fuseFDs) = %d, want %d", gotFDs, want)
	}
	if got := gotFDs; got != want {
		t.Errorf("len(fuseFDs) = %d, want %d", got, want)
	}
	for i, fd := range srv.fuseFDs {
		assertFuseFDLive(t, i, fd)
	}

	for i := 0; i < 64; i++ {
		var st syscall.Stat_t
		if err := syscall.Stat(mnt, &st); err != nil {
			t.Fatalf("stat: %v", err)
		}
	}

	if err := srv.Unmount(); err != nil {
		t.Fatalf("Unmount: %v", err)
	}
}

func TestNumCloneFDsGracefulDegrade(t *testing.T) {
	oldCloneFuseFDFn := cloneFuseFDFn
	cloneFuseFDFn = func(src *fuseFD) (int, error) {
		if got := src.server.kernelSettings.Minor; got == 0 {
			t.Fatalf("kernelSettings.Minor = %d during clone attempt, want INIT completed before cloning", got)
		}
		return -1, syscall.ENOSYS
	}
	t.Cleanup(func() {
		cloneFuseFDFn = oldCloneFuseFDFn
	})

	mnt := t.TempDir()
	srv, err := NewServer(&minimalFS{}, mnt, &MountOptions{
		NumCloneFDs: 2,
		Logger:      log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := srv.Unmount(); err != nil {
			t.Errorf("Unmount: %v", err)
		}
	})
	if got, want := len(srv.fuseFDs), 1; got != want {
		t.Fatalf("len(fuseFDs) = %d, want %d", got, want)
	}

	go srv.Serve()
	if err := srv.WaitMount(); err != nil {
		t.Fatal(err)
	}

	var st syscall.Stat_t
	if err := syscall.Stat(mnt, &st); err != nil {
		t.Fatalf("stat: %v", err)
	}

	if err := srv.Unmount(); err != nil {
		t.Fatalf("Unmount: %v", err)
	}
}

func TestNumCloneFDsPartialGracefulDegrade(t *testing.T) {
	oldCloneFuseFDFn := cloneFuseFDFn
	var cloneCalls int
	cloneFuseFDFn = func(src *fuseFD) (int, error) {
		cloneCalls++
		if got := src.server.kernelSettings.Minor; got == 0 {
			t.Fatalf("kernelSettings.Minor = %d during clone attempt %d, want INIT completed before cloning", got, cloneCalls)
		}
		if cloneCalls > 1 {
			return -1, syscall.ENOSYS
		}

		var cloned int
		var dupErr error
		if err := src.withFD(func(rawFD int) {
			cloned, dupErr = syscall.Dup(rawFD)
		}); err != nil {
			return -1, err
		}
		if dupErr != nil {
			return -1, dupErr
		}
		return cloned, nil
	}
	t.Cleanup(func() {
		cloneFuseFDFn = oldCloneFuseFDFn
	})

	mnt := t.TempDir()
	srv, err := NewServer(&minimalFS{}, mnt, &MountOptions{
		NumCloneFDs: 2,
		Logger:      log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := srv.Unmount(); err != nil {
			t.Errorf("Unmount: %v", err)
		}
	})
	if got, want := cloneCalls, 2; got != want {
		t.Fatalf("cloneFuseFDFn calls = %d, want %d", got, want)
	}
	if got, want := len(srv.fuseFDs), 2; got != want {
		t.Fatalf("len(fuseFDs) = %d, want %d", got, want)
	}
	for i, fd := range srv.fuseFDs {
		assertFuseFDLive(t, i, fd)
	}

	go srv.Serve()
	if err := srv.WaitMount(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 16; i++ {
		var st syscall.Stat_t
		if err := syscall.Stat(mnt, &st); err != nil {
			t.Fatalf("stat %d: %v", i+1, err)
		}
	}

	if err := srv.Unmount(); err != nil {
		t.Fatalf("Unmount: %v", err)
	}
}

func newFuseFDUnitTestServer() *Server {
	opts := &MountOptions{Logger: log.New(io.Discard, "", 0)}
	opts.setDefaults(&minimalFS{})
	srv := &Server{
		opts:       opts,
		maxReaders: 1,
	}
	srv.reqPool.New = func() interface{} {
		return &requestAlloc{
			request: request{
				cancel: make(chan struct{}),
			},
		}
	}
	srv.readPool.New = func() interface{} {
		return make([]byte, _FUSE_MIN_READ_BUFFER)
	}
	return srv
}

func TestFuseFDClosedRawConnReturnsError(t *testing.T) {
	srv := newFuseFDUnitTestServer()
	pipeFDs := []int{-1, -1}
	if err := syscall.Pipe(pipeFDs); err != nil {
		t.Fatalf("Pipe: %v", err)
	}
	t.Cleanup(func() {
		if pipeFDs[1] >= 0 {
			if err := syscall.Close(pipeFDs[1]); err != nil {
				t.Errorf("close pipe write fd: %v", err)
			}
		}
	})

	fd, err := srv.newFuseFD(pipeFDs[0])
	if err != nil {
		syscall.Close(pipeFDs[0])
		t.Fatalf("newFuseFD: %v", err)
	}
	pipeFDs[0] = -1
	if err := fd.close(); err != nil {
		t.Fatalf("fuseFD.close: %v", err)
	}

	called := false
	if err := fd.withFD(func(int) {
		called = true
	}); err == nil {
		t.Fatalf("withFD on closed fd returned nil error, want non-nil")
	}
	if called {
		t.Fatalf("withFD invoked callback for closed fd")
	}

	req, code := fd.readRequest()
	if req != nil {
		t.Fatalf("readRequest on closed fd returned request %p, want nil", req)
	}
	if code.Ok() {
		t.Fatalf("readRequest on closed fd status = %v, want non-OK", code)
	}
	fd.reqMu.Lock()
	readers := fd.reqReaders
	inflight := fd.inflightRequestBytes
	fd.reqMu.Unlock()
	if readers != 0 || inflight != 0 {
		t.Fatalf("closed-fd readRequest cleanup left reqReaders=%d inflightRequestBytes=%d, want 0/0", readers, inflight)
	}
}

func assertFuseFDLive(t *testing.T, index int, fd *fuseFD) {
	t.Helper()

	var called bool
	var errno syscall.Errno
	err := fd.withFD(func(rawFD int) {
		called = true
		_, _, errno = syscall.Syscall(syscall.SYS_FCNTL, uintptr(rawFD), uintptr(syscall.F_GETFD), 0)
	})
	if err != nil {
		t.Fatalf("fuseFDs[%d].withFD: %v", index, err)
	}
	if !called {
		t.Fatalf("fuseFDs[%d].withFD did not expose a RawConn fd", index)
	}
	if errno != 0 {
		t.Fatalf("fcntl(F_GETFD) on fuseFDs[%d]: %v", index, errno)
	}
}
