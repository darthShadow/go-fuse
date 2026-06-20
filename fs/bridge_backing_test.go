// Copyright 2026 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fs

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
)

type createBackingIDRaceRoot struct {
	Inode

	nextIno atomic.Uint64
}

func (r *createBackingIDRaceRoot) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*Inode, FileHandle, uint32, syscall.Errno) {
	ino := r.nextIno.Add(1)
	child := r.NewInode(ctx, &createBackingIDRaceNode{}, StableAttr{
		Mode: syscall.S_IFREG,
		Ino:  ino,
	})
	return child, createBackingIDRaceFile{}, 0, 0
}

type createBackingIDRaceNode struct {
	Inode
}

type createBackingIDRaceFile struct{}

func (createBackingIDRaceFile) PassthroughFd() (int, bool) {
	return 0, true
}

type createBackingIDRaceServer struct {
	delay         time.Duration
	registerCalls atomic.Int32
}

func (s *createBackingIDRaceServer) DeleteNotify(parent uint64, child uint64, name string) fuse.Status {
	return fuse.ENOSYS
}

func (s *createBackingIDRaceServer) EntryNotify(parent uint64, name string) fuse.Status {
	return fuse.ENOSYS
}

func (s *createBackingIDRaceServer) InodeNotify(node uint64, off int64, length int64) fuse.Status {
	return fuse.ENOSYS
}

func (s *createBackingIDRaceServer) InodeRetrieveCache(node uint64, offset int64, dest []byte) (int, fuse.Status) {
	return 0, fuse.ENOSYS
}

func (s *createBackingIDRaceServer) InodeNotifyStoreCache(node uint64, offset int64, data []byte) fuse.Status {
	return fuse.ENOSYS
}

func (s *createBackingIDRaceServer) RegisterBackingFd(m *fuse.BackingMap) (int32, syscall.Errno) {
	call := s.registerCalls.Add(1)
	time.Sleep(s.delay)
	if call == 1 {
		return call, 0
	}
	return 0, syscall.EPERM
}

func (s *createBackingIDRaceServer) UnregisterBackingFd(id int32) syscall.Errno {
	return 0
}

func TestCreateBackingIDDisableRace(t *testing.T) {
	server := &createBackingIDRaceServer{
		delay: 10 * time.Millisecond,
	}
	root := &createBackingIDRaceRoot{}
	bridge := NewNodeFS(root, &Options{
		ServerCallbacks: server,
	}).(*rawBridge)

	const creates = 32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < creates; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start

			in := fuse.CreateIn{}
			in.NodeId = 1
			in.Flags = syscall.O_CREAT | syscall.O_RDWR
			in.Mode = 0600
			out := fuse.CreateOut{}
			if status := bridge.Create(nil, &in, fmt.Sprintf("file-%d", i), &out); !status.Ok() {
				t.Errorf("Create(file-%d) = %v, want OK", i, status)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if got := server.registerCalls.Load(); got < 2 {
		t.Fatalf("RegisterBackingFd call count invalid: got %d want >= %d", got, 2)
	}

	bridge.mu.Lock()
	disabled := bridge.disableBackingFiles
	bridge.mu.Unlock()
	if got, want := disabled, true; got != want {
		t.Fatalf("disableBackingFiles invalid: got %t want %t", got, want)
	}
}
