// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fuse

import (
	"context"
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/hanwen/go-fuse/v2/internal/testutil"
)

func TestBufferPool(t *testing.T) {
	bp := bufferPool{}
	size := 1500
	buf1 := bp.AllocBuffer(uint32(size))
	if len(buf1) != size {
		t.Errorf("Expected buffer of %d bytes, got %d bytes", size, len(buf1))
	}
	bp.FreeBuffer(buf1)

	// tried testing to see if we get buf1 back if we ask again,
	// but it's not guaranteed and sometimes fails
}

// raceEnabled is set by race_test.go.
var raceEnabled bool

func TestBufferPoolCycleNoAlloc(t *testing.T) {
	if raceEnabled {
		t.Skip("sync.Pool drops a random share of Puts under the race detector")
	}
	var bp bufferPool
	for _, size := range []uint32{1, 4096, 32 << 10, 1 << 20} {
		bp.FreeBuffer(bp.AllocBuffer(size))
		n := testing.AllocsPerRun(100, func() { bp.FreeBuffer(bp.AllocBuffer(size)) })
		if n != 0 {
			t.Errorf("size %d: %v allocations per AllocBuffer/FreeBuffer cycle, want 0", size, n)
		}
	}
}

func TestBufferPoolSizeClasses(t *testing.T) {
	var bp bufferPool
	caps := map[int]bool{}
	for pages := 1; pages <= 64; pages++ {
		size := (pages-1)*pageSize + 1
		b := bp.AllocBuffer(uint32(size))
		if len(b) != size {
			t.Fatalf("AllocBuffer(%d): len %d", size, len(b))
		}
		c, lo := cap(b), pages*pageSize
		if c%pageSize != 0 || c < lo || 4*c > 5*lo {
			t.Errorf("AllocBuffer(%d): cap %d, want a page multiple in [%d, %d]", size, c, lo, lo*5/4)
		}
		caps[c] = true
		bp.FreeBuffer(b)
	}
	// 1-4 pages, then 4 classes per doubling up to 64 pages.
	if len(caps) != 20 {
		t.Errorf("%d distinct capacities for 1-64 pages, want 20", len(caps))
	}
}

func TestBufferPoolSizeClassBounds(t *testing.T) {
	maxPages := int((uint64(math.MaxUint32) + uint64(pageSize) - 1) / uint64(pageSize))
	prev := -1
	for pages := 1; pages <= maxPages; pages++ {
		idx, cp := sizeClass(pages)
		if idx < prev || idx > prev+1 || idx >= numBufferClasses {
			t.Fatalf("sizeClass(%d) = %d after %d, want %d or %d, below %d", pages, idx, prev, prev, prev+1, numBufferClasses)
		}
		if cp < pages || 4*cp > 5*pages {
			t.Fatalf("sizeClass(%d) = %d pages, want [%d, %d]", pages, cp, pages, pages*5/4)
		}
		if i, c := sizeClass(cp); i != idx || c != cp {
			t.Fatalf("sizeClass(%d) = (%d, %d), want (%d, %d)", cp, i, c, idx, cp)
		}
		prev = idx
	}
}

func TestBufferPoolForeignSlice(t *testing.T) {
	if raceEnabled {
		t.Skip("sync.Pool drops a random share of Puts under the race detector")
	}
	// sync.Pool may drop a Put on GC or when the goroutine moves to
	// another P. One P and no GC make the next Get see it.
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	defer debug.SetGCPercent(debug.SetGCPercent(-1))

	var bp bufferPool
	// 10 pages is the class a 9-page slice would wrongly join. If a
	// freed 10-page buffer does not come back, the check below
	// cannot fail, so refuse.
	own := bp.AllocBuffer(uint32(10 * pageSize))
	bp.FreeBuffer(own)
	if got := bp.AllocBuffer(uint32(10 * pageSize)); unsafe.SliceData(got) != unsafe.SliceData(own) {
		t.Fatal("the pool did not return a buffer just freed; cannot check foreign slices")
	}

	// 9 pages is not a size class, so it must not come back as a
	// 10-page buffer.
	foreign := make([]byte, 9*pageSize)
	bp.FreeBuffer(foreign)
	b := bp.AllocBuffer(uint32(len(foreign)))
	if unsafe.SliceData(b) == unsafe.SliceData(foreign) && cap(b) > cap(foreign) {
		t.Errorf("AllocBuffer returned a %d-byte view of a %d-byte foreign slice", cap(b), cap(foreign))
	}
}

func BenchmarkBufferPool(b *testing.B) {
	for _, size := range []int{4 << 10, 32 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			var bp bufferPool
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					buf := bp.AllocBuffer(uint32(size))
					buf[0] = 1
					bp.FreeBuffer(buf)
				}
			})
		})
	}
}

type readFS struct {
	defaultRawFileSystem

	// lostBuffers counts READs whose buffer is not the one
	// returnRequest gives back to the pool.
	lostBuffers atomic.Int32
}

func (fs *readFS) Open(ctx context.Context, input *OpenIn, out *OpenOut) (status Status) {
	if input.NodeId != 2 {
		return ENOENT
	}

	return OK
}

func (fs *readFS) Read(ctx context.Context, input *ReadIn, buf []byte) (ReadResult, Status) {
	if input.NodeId != 2 {
		return nil, ENOENT
	}

	// requestAlloc starts with its request.
	ra := (*requestAlloc)(unsafe.Pointer(ctx.(*requestContext).call.req))
	if unsafe.SliceData(ra.bufferPoolOutputBuf) != unsafe.SliceData(buf) {
		fs.lostBuffers.Add(1)
	}

	dest := buf[:input.Size]
	for i := range dest {
		dest[i] = 'x'
	}

	return ReadResultData(dest), OK
}

func (f *readFS) Lookup(ctx context.Context, header *InHeader, name string, out *EntryOut) (code Status) {
	if name != "file" {
		return ENOENT
	}

	*out = EntryOut{
		NodeId: 2,
		Attr: Attr{
			Mode: S_IFREG | 0666,
			Size: 1 << 20,
		},
		AttrValid: 1,
	}

	return OK
}

func TestBufferPoolRequestHandler(t *testing.T) {
	mnt := t.TempDir()
	opts := &MountOptions{
		Debug: testutil.VerboseTest(),
	}

	rfs := readFS{}
	srv, err := NewServer(&rfs, mnt, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Unmount() })
	go srv.Serve()
	if err := srv.WaitMount(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.ReadFile(mnt + "/file"); err != nil {
		t.Fatal(err)
	}

	if n := rfs.lostBuffers.Load(); n != 0 {
		t.Errorf("%d READs got a buffer that is not returned to the pool", n)
	}

	// TODO: test write as well?
}
