// Copyright 2023 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fs

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
)

type dirCacheTestNode struct {
	Inode
	mu          sync.Mutex
	flags       uint32
	openCount   int
	cachedReads int
}

var _ = (NodeOpendirHandler)((*dirCacheTestNode)(nil))

type countingReaddirenter struct {
	FileReaddirenter
	*dirCacheTestNode
}

func (r *countingReaddirenter) Readdirent(ctx context.Context) (*fuse.DirEntry, syscall.Errno) {
	de, errno := r.FileReaddirenter.Readdirent(ctx)
	r.dirCacheTestNode.mu.Lock()
	defer r.dirCacheTestNode.mu.Unlock()
	if r.dirCacheTestNode.openCount > 1 {
		r.dirCacheTestNode.cachedReads++
	}
	return de, errno
}

func (n *dirCacheTestNode) OpendirHandle(ctx context.Context, flags uint32) (FileHandle, uint32, syscall.Errno) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.openCount++
	var entries []fuse.DirEntry

	for i := 0; i < 1024; i++ {
		entries = append(entries, fuse.DirEntry{
			Name: fmt.Sprintf("file%04d", i),
			Ino:  uint64(i + 42),
			Mode: fuse.S_IFREG,
		})
	}
	return &countingReaddirenter{&dirStreamAsFile{
		creator: func(context.Context) (DirStream, syscall.Errno) {
			return NewListDirStream(entries), 0
		},
	}, n}, n.flags, 0
}

// listRootTwice lists the root of a mount of root twice and checks that
// both listings are equal. It skips the test below protocol 7.28.
func listRootTwice(t *testing.T, root *dirCacheTestNode) {
	t.Helper()
	opts := Options{}
	opts.DisableReadDirPlus = true
	mnt, server := testMount(t, root, &opts)

	if !server.KernelSettings().SupportsVersion(7, 28) {
		t.Skip("need v7.28 for directory caching")
	}

	s, errno := NewLoopbackDirStream(mnt)
	if errno != 0 {
		t.Fatalf("NewLoopbackDirStream: %v", errno)
	}
	defer s.Close()

	want, errno := readDirStream(s)
	if errno != 0 {
		t.Fatalf("NewLoopbackDirStream: %v", errno)
	}
	s.Close()

	s, errno = NewLoopbackDirStream(mnt)
	if errno != 0 {
		t.Fatalf("NewLoopbackDirStream: %v", errno)
	}
	defer s.Close()
	got, errno := readDirStream(s)
	if errno != 0 {
		t.Fatalf("NewLoopbackDirStream: %v", errno)
	}
	s.Close()

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestDirCacheFlag(t *testing.T) {
	root := &dirCacheTestNode{flags: fuse.FOPEN_CACHE_DIR}
	listRootTwice(t, root)

	root.mu.Lock()
	defer root.mu.Unlock()
	if root.openCount != 2 || root.cachedReads != 0 {
		t.Errorf("got %d, %d want 2,0", root.openCount, root.cachedReads)
	}
}

// FOPEN_KEEP_CACHE without FOPEN_CACHE_DIR must reach the kernel as is, so
// the second listing is read through the file system and not from the cache.
func TestDirCacheKeepCacheOnly(t *testing.T) {
	root := &dirCacheTestNode{flags: fuse.FOPEN_KEEP_CACHE}
	listRootTwice(t, root)

	root.mu.Lock()
	defer root.mu.Unlock()
	if root.openCount != 2 || root.cachedReads == 0 {
		t.Errorf("got %d, %d want 2, >0", root.openCount, root.cachedReads)
	}
}
