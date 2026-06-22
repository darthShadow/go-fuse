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
	delay                time.Duration
	allowMultipleSuccess bool
	trackInFlight        bool
	onUnregister         func(int32)

	registerCalls   atomic.Int32
	unregisterCalls atomic.Int32
	nextBackingID   atomic.Int32
	inFlight        atomic.Int32
	maxInFlight     atomic.Int32
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
	if s.trackInFlight {
		inFlight := s.inFlight.Add(1)
		s.recordMaxInFlight(inFlight)
		defer s.inFlight.Add(-1)
	}

	call := s.registerCalls.Add(1)
	time.Sleep(s.delay)
	if s.allowMultipleSuccess {
		return s.nextBackingID.Add(1), 0
	}
	if call == 1 {
		return call, 0
	}
	return 0, syscall.EPERM
}

func (s *createBackingIDRaceServer) recordMaxInFlight(inFlight int32) {
	for {
		max := s.maxInFlight.Load()
		if inFlight <= max {
			return
		}
		if s.maxInFlight.CompareAndSwap(max, inFlight) {
			return
		}
	}
}

func (s *createBackingIDRaceServer) UnregisterBackingFd(id int32) syscall.Errno {
	s.unregisterCalls.Add(1)
	if s.onUnregister != nil {
		s.onUnregister(id)
	}
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

	disabled := bridge.disableBackingFiles.Load()
	if got, want := disabled, true; got != want {
		t.Fatalf("disableBackingFiles invalid: got %t want %t", got, want)
	}
}

type bridgeBackingOpenNode struct {
	Inode
}

func (n *bridgeBackingOpenNode) Open(ctx context.Context, flags uint32) (FileHandle, uint32, syscall.Errno) {
	return createBackingIDRaceFile{}, 0, OK
}

func TestBridgeSameInodeOpenOpenBackingRegistration(t *testing.T) {
	server := &createBackingIDRaceServer{
		delay: 10 * time.Millisecond,
	}
	root := &bridgeBackingOpenNode{}
	bridge := NewNodeFS(root, &Options{
		ServerCallbacks: server,
	}).(*rawBridge)

	const opens = 64
	outs := make([]fuse.OpenOut, opens)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range outs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start

			out, status := openBridgeBackingRaceHandle(bridge, 1)
			if !status.Ok() {
				t.Errorf("Open(%d) status = %v, want OK", i, status)
				return
			}
			outs[i] = out
		}(i)
	}
	close(start)
	wg.Wait()

	if got := server.registerCalls.Load(); got != 1 {
		t.Fatalf("RegisterBackingFd calls = %d, want 1", got)
	}

	var backingID int32
	for i, out := range outs {
		if out.Fh == 0 {
			t.Fatalf("Open(%d) Fh = 0, want non-zero", i)
		}
		if out.BackingID == 0 {
			t.Fatalf("Open(%d) BackingID = 0, want non-zero", i)
		}
		if out.OpenFlags&fuse.FOPEN_PASSTHROUGH == 0 {
			t.Fatalf("Open(%d) OpenFlags = %#x, want FOPEN_PASSTHROUGH", i, out.OpenFlags)
		}
		if backingID == 0 {
			backingID = out.BackingID
		}
		if out.BackingID != backingID {
			t.Fatalf("Open(%d) BackingID = %d, want shared BackingID %d", i, out.BackingID, backingID)
		}
	}

	refcount, inodeBackingID := bridgeBackingState(bridge.root)
	if refcount != opens {
		t.Fatalf("backingIDRefcount before release = %d, want %d", refcount, opens)
	}
	if inodeBackingID != backingID {
		t.Fatalf("inode backingID before release = %d, want %d", inodeBackingID, backingID)
	}
	if got := server.unregisterCalls.Load(); got != 0 {
		t.Fatalf("UnregisterBackingFd calls before release = %d, want 0", got)
	}

	for i := 0; i < opens-1; i++ {
		releaseBridgeBackingRaceHandle(bridge, 1, outs[i].Fh)
	}
	if got := server.unregisterCalls.Load(); got != 0 {
		t.Fatalf("UnregisterBackingFd calls before last release = %d, want 0", got)
	}

	releaseBridgeBackingRaceHandle(bridge, 1, outs[opens-1].Fh)
	if got := server.unregisterCalls.Load(); got != 1 {
		t.Fatalf("UnregisterBackingFd calls after last release = %d, want 1", got)
	}
	refcount, inodeBackingID = bridgeBackingState(bridge.root)
	if refcount != 0 || inodeBackingID != 0 {
		t.Fatalf("backing state after release = (refcount=%d, id=%d), want (0, 0)", refcount, inodeBackingID)
	}
}

func TestBridgeSameInodeOpenReleaseBackingRace(t *testing.T) {
	server := &createBackingIDRaceServer{
		delay:                time.Millisecond,
		allowMultipleSuccess: true,
	}
	root := &bridgeBackingOpenNode{}
	bridge := NewNodeFS(root, &Options{
		ServerCallbacks: server,
	}).(*rawBridge)
	server.onUnregister = func(id int32) {
		if root.backingIDRefcount != 0 {
			t.Errorf("UnregisterBackingFd(%d) with backingIDRefcount = %d, want 0", id, root.backingIDRefcount)
		}
		if root.backingID != id {
			t.Errorf("UnregisterBackingFd(%d) with inode backingID = %d, want %d", id, root.backingID, id)
		}
	}

	const (
		waves = 32
		batch = 4
	)
	var previous []fuse.OpenOut
	for wave := 0; wave < waves; wave++ {
		next := make([]fuse.OpenOut, batch)
		var wg sync.WaitGroup
		start := make(chan struct{})

		for i := range next {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start

				out, status := openBridgeBackingRaceHandle(bridge, 1)
				if !status.Ok() {
					t.Errorf("wave %d Open(%d) status = %v, want OK", wave, i, status)
					return
				}
				if bridge.getFile(out.Fh) == nil {
					t.Errorf("wave %d getFile(%d) = nil, want live file entry", wave, out.Fh)
					return
				}
				next[i] = out
			}(i)
		}
		for _, out := range previous {
			wg.Add(1)
			go func(out fuse.OpenOut) {
				defer wg.Done()
				<-start
				releaseBridgeBackingRaceHandleRecover(t, bridge, 1, out.Fh)
			}(out)
		}

		close(start)
		waitBridgeBackingRaceGoroutines(t, &wg)
		for i, out := range next {
			if out.Fh == 0 {
				t.Fatalf("wave %d Open(%d) Fh = 0, want non-zero", wave, i)
			}
			if out.BackingID == 0 {
				t.Fatalf("wave %d Open(%d) BackingID = 0, want non-zero", wave, i)
			}
		}
		previous = next
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, out := range previous {
		wg.Add(1)
		go func(out fuse.OpenOut) {
			defer wg.Done()
			<-start
			releaseBridgeBackingRaceHandleRecover(t, bridge, 1, out.Fh)
		}(out)
	}
	close(start)
	waitBridgeBackingRaceGoroutines(t, &wg)

	refcount, backingID := bridgeBackingState(bridge.root)
	if refcount != 0 || backingID != 0 {
		t.Fatalf("backing state after concurrent Open/Release = (refcount=%d, id=%d), want (0, 0)", refcount, backingID)
	}
	assertBridgeBackingOpenFiles(t, bridge, 0)
	if got, want := server.unregisterCalls.Load(), server.registerCalls.Load(); got != want {
		t.Fatalf("UnregisterBackingFd calls = %d, want RegisterBackingFd calls %d", got, want)
	}

	out := mustOpenBridgeBackingRaceHandle(t, bridge, 1)
	if bridge.getFile(out.Fh) == nil {
		t.Fatalf("getFile(%d) after concurrent Open/Release = nil, want live file entry", out.Fh)
	}
	releaseBridgeBackingRaceHandle(bridge, 1, out.Fh)
	assertBridgeBackingOpenFiles(t, bridge, 0)
	if got, want := server.unregisterCalls.Load(), server.registerCalls.Load(); got != want {
		t.Fatalf("final UnregisterBackingFd calls = %d, want RegisterBackingFd calls %d", got, want)
	}
}

func TestBridgeDistinctInodeBackingRegistrationParallelism(t *testing.T) {
	server := &createBackingIDRaceServer{
		delay:                10 * time.Millisecond,
		allowMultipleSuccess: true,
		trackInFlight:        true,
	}
	bridge := NewNodeFS(&Inode{}, &Options{
		ServerCallbacks: server,
	}).(*rawBridge)

	const nodes = 32
	inodes := make([]*Inode, nodes)
	outs := make([]fuse.OpenOut, nodes)
	for i := range inodes {
		inodes[i] = newBridgeBackingOpenNode(t, bridge, fmt.Sprintf("file-%d", i), uint64(8001+i))
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, node := range inodes {
		wg.Add(1)
		go func(i int, node *Inode) {
			defer wg.Done()
			<-start

			out, status := openBridgeBackingRaceHandle(bridge, node.nodeId)
			if !status.Ok() {
				t.Errorf("Open(node=%d) status = %v, want OK", node.nodeId, status)
				return
			}
			outs[i] = out
		}(i, node)
	}
	close(start)
	wg.Wait()

	if got := server.maxInFlight.Load(); got <= 1 {
		t.Fatalf("max in-flight RegisterBackingFd calls = %d, want > 1", got)
	}
	if got := server.registerCalls.Load(); got != nodes {
		t.Fatalf("RegisterBackingFd calls = %d, want %d", got, nodes)
	}
	for i, out := range outs {
		if out.Fh == 0 {
			t.Fatalf("Open(%d) Fh = 0, want non-zero", i)
		}
		if out.BackingID == 0 {
			t.Fatalf("Open(%d) BackingID = 0, want non-zero", i)
		}
		releaseBridgeBackingRaceHandle(bridge, inodes[i].nodeId, out.Fh)
	}
	if got := server.unregisterCalls.Load(); got != nodes {
		t.Fatalf("UnregisterBackingFd calls = %d, want %d", got, nodes)
	}
}

func newBridgeBackingOpenNode(t *testing.T, bridge *rawBridge, name string, ino uint64) *Inode {
	t.Helper()

	node := bridge.root.NewInode(context.Background(), &bridgeBackingOpenNode{}, StableAttr{
		Mode: syscall.S_IFREG,
		Ino:  ino,
	})
	var out fuse.EntryOut
	selected, _ := bridge.addNewChild(bridge.root, name, node, nil, syscall.O_EXCL, &out)
	if selected != node {
		t.Fatalf("addNewChild(%s) selected %p, want node %p", name, selected, node)
	}
	return selected
}

func openBridgeBackingRaceHandle(bridge *rawBridge, nodeID uint64) (fuse.OpenOut, fuse.Status) {
	in := fuse.OpenIn{}
	in.NodeId = nodeID
	var out fuse.OpenOut
	status := bridge.Open(nil, &in, &out)
	return out, status
}

func mustOpenBridgeBackingRaceHandle(t *testing.T, bridge *rawBridge, nodeID uint64) fuse.OpenOut {
	t.Helper()

	out, status := openBridgeBackingRaceHandle(bridge, nodeID)
	if !status.Ok() {
		t.Fatalf("Open(%d) status = %v, want OK", nodeID, status)
	}
	if out.Fh == 0 {
		t.Fatalf("Open(%d) Fh = 0, want non-zero", nodeID)
	}
	if out.BackingID == 0 {
		t.Fatalf("Open(%d) BackingID = 0, want non-zero", nodeID)
	}
	return out
}

func releaseBridgeBackingRaceHandle(bridge *rawBridge, nodeID uint64, fh uint64) {
	in := fuse.ReleaseIn{
		Fh: fh,
	}
	in.NodeId = nodeID
	bridge.Release(nil, &in)
}

func releaseBridgeBackingRaceHandleRecover(t *testing.T, bridge *rawBridge, nodeID uint64, fh uint64) {
	t.Helper()

	defer func() {
		if v := recover(); v != nil {
			t.Errorf("Release(node=%d, fh=%d) panicked: %v", nodeID, fh, v)
		}
	}()
	releaseBridgeBackingRaceHandle(bridge, nodeID, fh)
}

func bridgeBackingState(node *Inode) (refcount int, backingID int32) {
	node.backingMu.Lock()
	defer node.backingMu.Unlock()
	return node.backingIDRefcount, node.backingID
}

func assertBridgeBackingOpenFiles(t *testing.T, bridge *rawBridge, want int) {
	t.Helper()

	bridge.fileMu.Lock()
	got := len(bridge.root.openFiles)
	bridge.fileMu.Unlock()
	if got != want {
		t.Fatalf("openFiles length = %d, want %d", got, want)
	}
}

func waitBridgeBackingRaceGoroutines(t *testing.T, wg *sync.WaitGroup) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent Open/Release goroutines did not finish within 5s")
	}
}
