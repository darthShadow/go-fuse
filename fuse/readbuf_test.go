// Copyright 2026 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fuse

import (
	"io"
	"log"
	"os"
	"runtime"
	"sync"
	"syscall"
	"testing"
)

func newReadBufTestFD(t *testing.T) *fuseFD {
	t.Helper()
	fd, err := syscall.Open(os.DevNull, syscall.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("open %s: %v", os.DevNull, err)
	}
	r, err := newFuseFDUnitTestServer().newFuseFD(fd)
	if err != nil {
		t.Fatalf("newFuseFD: %v", err)
	}
	t.Cleanup(func() {
		if err := r.close(); err != nil {
			t.Errorf("fuseFD.close: %v", err)
		}
	})
	return r
}

func TestReadBufFreeListSurvivesGC(t *testing.T) {
	r := newReadBufTestFD(t)
	buf := r.newReadBuf()
	r.reqMu.Lock()
	r.inflightRequestBytes += r.readBufBytes
	r.putReadBuf(buf)
	r.reqMu.Unlock()

	runtime.GC()
	runtime.GC()

	r.reqMu.Lock()
	got := r.getReadBufLocked()
	r.reqMu.Unlock()
	if len(got) == 0 {
		t.Fatal("getReadBufLocked after two GCs returned no buffer, want retained buffer")
	}
	if &got[0] != &buf[0] {
		t.Fatalf("getReadBufLocked after two GCs returned buffer %p, want %p", &got[0], &buf[0])
	}
}

func TestReadBufFreeListBounded(t *testing.T) {
	r := newReadBufTestFD(t)
	capacity := r.server.maxReaders + 1
	bufs := make([][]byte, capacity+1)
	for i := range bufs {
		bufs[i] = r.newReadBuf()
	}
	r.reqMu.Lock()
	r.inflightRequestBytes += len(bufs) * r.readBufBytes
	for _, buf := range bufs {
		r.putReadBuf(buf)
	}
	got := make([][]byte, capacity+1)
	for i := range got {
		got[i] = r.getReadBufLocked()
	}
	r.reqMu.Unlock()
	if got[capacity] != nil {
		t.Fatalf("pop %d returned a buffer, want nil after %d retained buffers", capacity+1, capacity)
	}
	for i := 0; i < capacity; i++ {
		want := bufs[capacity-1-i]
		if len(got[i]) == 0 {
			t.Fatalf("pop %d returned no buffer, want %p", i+1, &want[0])
		}
		if &got[i][0] != &want[0] {
			t.Errorf("pop %d returned buffer %p, want %p (LIFO)", i+1, &got[i][0], &want[0])
		}
	}
}

func newFuseFDUnitTestServer() *Server {
	opts := &MountOptions{Logger: log.New(io.Discard, "", 0)}
	opts.setDefaults(NewDefaultRawFileSystem())
	srv := &Server{
		opts:       opts,
		maxReaders: 1,
	}
	srv.reqPool.New = func() interface{} {
		return &requestAlloc{}
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

func BenchmarkReadBufCycle(b *testing.B) {
	for _, impl := range []string{"freeList", "syncPool"} {
		b.Run("impl="+impl, func(b *testing.B) {
			maxReaders := runtime.GOMAXPROCS(0)
			if maxReaders < minMaxReaders {
				maxReaders = minMaxReaders
			} else if maxReaders > maxMaxReaders {
				maxReaders = maxMaxReaders
			}
			readBufSize, readBufBytes, reqAllocBytes := requestAccountingSizes(128 * 1024)
			r := &fuseFD{
				server:        newFuseFDUnitTestServer(),
				readBufs:      make([][]byte, 0, maxReaders+1),
				readBufSize:   readBufSize,
				readBufBytes:  readBufBytes,
				reqAllocBytes: reqAllocBytes,
			}
			r.server.maxReaders = maxReaders
			pool := sync.Pool{New: func() any { return r.newReadBuf() }}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				if impl == "freeList" {
					for pb.Next() {
						r.reqMu.Lock()
						r.reserveRequestBytes()
						r.reqReaders++
						buf := r.getReadBufLocked()
						r.reqMu.Unlock()
						if buf == nil {
							buf = r.newReadBuf()
						}
						r.reqMu.Lock()
						r.putReadBuf(buf)
						r.inflightRequestBytes -= r.reqAllocBytes
						r.reqReaders--
						r.reqMu.Unlock()
					}
				} else {
					for pb.Next() {
						r.reqMu.Lock()
						r.reserveRequestBytes()
						r.reqReaders++
						r.reqMu.Unlock()
						buf := pool.Get().([]byte)
						r.reqMu.Lock()
						pool.Put(buf)
						r.inflightRequestBytes -= r.readBufBytes
						r.inflightRequestBytes -= r.reqAllocBytes
						r.reqReaders--
						r.reqMu.Unlock()
					}
				}
			})
			if r.reqReaders != 0 || r.inflightRequestBytes != 0 {
				b.Fatalf("reqReaders=%d inflightRequestBytes=%d, want 0/0", r.reqReaders, r.inflightRequestBytes)
			}
		})
	}
}
