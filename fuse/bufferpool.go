// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fuse

import (
	"math/bits"
	"os"
	"sync"
	"unsafe"
)

// numBufferClasses covers every uint32 size at 4 KiB pages.
const numBufferClasses = 76

// bufferPool implements explicit memory management. It is used for
// minimizing the GC overhead of communicating with the kernel.
type bufferPool struct {
	// pools[i] holds *byte pointing at buffers of the i-th size
	// class. A pointer, unlike a slice, goes into a sync.Pool
	// without allocating.
	pools [numBufferClasses]sync.Pool
}

var pageSize = os.Getpagesize()

// sizeClass rounds a page count up to its size class: exact up to 4
// pages, then 4 classes per doubling (5, 6, 7, 8, 10, 12, 14, 16, 20,
// ...), so a buffer is less than 25% larger than requested.
func sizeClass(pages int) (idx, classPages int) {
	if pages <= 4 {
		return pages - 1, pages
	}
	shift := bits.Len(uint(pages-1)) - 3
	q := (pages-1)>>shift + 1
	return 4*shift + q - 1, q << shift
}

// AllocBuffer creates a buffer of at least the given size. After use,
// it should be deallocated with FreeBuffer().
func (p *bufferPool) AllocBuffer(size uint32) []byte {
	sz := max(int(size), pageSize)

	if sz%pageSize != 0 {
		sz += pageSize
	}
	idx, classPages := sizeClass(sz / pageSize)
	n := classPages * pageSize
	if b, ok := p.pools[idx].Get().(*byte); ok {
		return unsafe.Slice(b, n)[:size]
	}
	return make([]byte, size, n)
}

// FreeBuffer takes back a buffer if it was allocated through
// AllocBuffer.  It is not an error to call FreeBuffer() on a slice
// obtained elsewhere.
func (p *bufferPool) FreeBuffer(slice []byte) {
	if cap(slice)%pageSize != 0 || cap(slice) == 0 {
		return
	}
	pages := cap(slice) / pageSize
	// AllocBuffer hands out the full class size, so only a slice
	// whose capacity is exactly a class size can go back.
	idx, classPages := sizeClass(pages)
	if classPages != pages || idx >= len(p.pools) {
		return
	}
	p.pools[idx].Put(unsafe.SliceData(slice))
}
