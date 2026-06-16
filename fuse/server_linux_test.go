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

	"github.com/hanwen/go-fuse/v2/internal/testutil"
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

// minimalFS supports just enough operations to mount and stat the root.
type minimalFS struct{ defaultRawFileSystem }

func (*minimalFS) GetAttr(cancel <-chan struct{}, in *GetAttrIn, out *AttrOut) Status {
	out.Attr = Attr{Ino: 1, Mode: S_IFDIR | 0755, Nlink: 2}
	return OK
}

// TestNumCloneFDs verifies that NumCloneFDs opens additional /dev/fuse
// fds bound to the same session, that the server still serves requests,
// and that mount/unmount complete cleanly.
func TestNumCloneFDs(t *testing.T) {
	const want = 3
	mnt := t.TempDir()
	srv, err := NewServer(&minimalFS{}, mnt, &MountOptions{
		NumCloneFDs: want - 1,
		Debug:       testutil.VerboseTest(),
	})
	if err != nil {
		t.Fatal(err)
	}
	gotFDs := len(srv.fuseFDs)

	go srv.Serve()
	if err := srv.WaitMount(); err != nil {
		t.Fatal(err)
	}

	if gotFDs < want {
		if err := srv.Unmount(); err != nil {
			t.Fatalf("Unmount: %v", err)
		}
		t.Skipf("kernel lacks FUSE_DEV_IOC_CLONE support: len(fuseFDs) = %d, want %d", gotFDs, want)
	}
	if got := gotFDs; got != want {
		t.Errorf("len(fuseFDs) = %d, want %d", got, want)
	}
	for i, fd := range srv.fuseFDs {
		if fd.fd <= 0 {
			t.Errorf("fuseFDs[%d].fd = %d, want > 0", i, fd.fd)
		}
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
	cloneFuseFDFn = func(int) (int, error) {
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
