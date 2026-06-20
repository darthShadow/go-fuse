// Copyright 2026 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fs

import (
	"context"
	"fmt"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
)

type bridgeFileLookupRaceRoot struct {
	Inode

	nextHandleID atomic.Uint64
}

func (r *bridgeFileLookupRaceRoot) Open(ctx context.Context, flags uint32) (FileHandle, uint32, syscall.Errno) {
	return &bridgeFileLookupRaceHandle{id: r.nextHandleID.Add(1)}, 0, OK
}

func (r *bridgeFileLookupRaceRoot) CopyFileRange(ctx context.Context, fhIn FileHandle, offIn uint64, out *Inode, fhOut FileHandle, offOut uint64, len uint64, flags uint64) (uint32, syscall.Errno) {
	if _, ok := fhIn.(*bridgeFileLookupRaceHandle); !ok {
		return 0, syscall.EBADF
	}
	if _, ok := fhOut.(*bridgeFileLookupRaceHandle); !ok {
		return 0, syscall.EBADF
	}
	return uint32(len), OK
}

type bridgeNodeLseekRaceRoot struct {
	bridgeFileLookupRaceRoot
}

func (r *bridgeNodeLseekRaceRoot) Lseek(ctx context.Context, f FileHandle, off uint64, whence uint32) (uint64, syscall.Errno) {
	if _, ok := f.(*bridgeFileLookupRaceHandle); !ok {
		return 0, syscall.EBADF
	}
	return off, OK
}

type bridgeFileLookupRaceHandle struct {
	id uint64
}

func (f *bridgeFileLookupRaceHandle) Lseek(ctx context.Context, off uint64, whence uint32) (uint64, syscall.Errno) {
	return off, OK
}

func TestBridgeFileHandleLookupRace(t *testing.T) {
	t.Run("node lseek", func(t *testing.T) {
		bridge := NewNodeFS(&bridgeNodeLseekRaceRoot{}, nil).(*rawBridge)
		runBridgeFileHandleLookupRace(t, bridge)
	})
	t.Run("file lseek", func(t *testing.T) {
		bridge := NewNodeFS(&bridgeFileLookupRaceRoot{}, nil).(*rawBridge)
		runBridgeFileHandleLookupRace(t, bridge)
	})
}

func runBridgeFileHandleLookupRace(t *testing.T, bridge *rawBridge) {
	t.Helper()

	// This test covers memory-safety under fh slot reuse. It does not assert
	// logical file identity; FUSE can race RELEASE with handle-based operations.
	t.Run("copy file range", func(t *testing.T) {
		assertBridgeRaceMemorySafe(t, "CopyFileRange", func() {
			runBridgeCopyFileRangeAfterHandleReuse(t, bridge)
		})
	})
	t.Run("lseek", func(t *testing.T) {
		assertBridgeRaceMemorySafe(t, "Lseek", func() {
			runBridgeLseekAfterHandleReuse(t, bridge)
		})
	})
}

func assertBridgeRaceMemorySafe(t *testing.T, op string, fn func()) {
	t.Helper()

	defer func() {
		if v := recover(); v != nil {
			t.Fatalf("%s panicked after fh slot reuse: %v", op, v)
		}
	}()

	fn()
}

func runBridgeCopyFileRangeAfterHandleReuse(t *testing.T, bridge *rawBridge) {
	t.Helper()

	activeHandles := []uint64{0, 0}
	defer releaseBridgeRaceHandles(bridge, activeHandles)

	activeHandles[0] = mustOpenBridgeRaceHandle(t, bridge)
	activeHandles[1] = mustOpenBridgeRaceHandle(t, bridge)

	nodeIn := mustGetBridgeRaceNode(t, bridge, 1)
	copyFileRange, ok := nodeIn.ops.(NodeCopyFileRanger)
	if !ok {
		t.Fatalf("node ops %T does not implement NodeCopyFileRanger", nodeIn.ops)
	}

	oldFH := activeHandles[0]
	activeHandles[0] = 0
	reusedFH, err := recycleBridgeRaceHandle(bridge, oldFH)
	if err != nil {
		t.Fatal(err)
	}
	activeHandles[0] = reusedFH

	fileIn := mustGetBridgeRaceFile(t, bridge, reusedFH)
	nodeOut := mustGetBridgeRaceNode(t, bridge, 1)
	fileOut := mustGetBridgeRaceFile(t, bridge, activeHandles[1])

	size, errno := copyFileRange.CopyFileRange(&fuse.Context{}, fileIn.file, 0, nodeOut, fileOut.file, 0, 1, 0)
	if errno != OK {
		t.Fatalf("CopyFileRange errno = %v, want OK", errno)
	}
	if size != 1 {
		t.Fatalf("CopyFileRange size = %d, want 1", size)
	}
}

func runBridgeLseekAfterHandleReuse(t *testing.T, bridge *rawBridge) {
	t.Helper()

	activeHandles := []uint64{mustOpenBridgeRaceHandle(t, bridge)}
	defer releaseBridgeRaceHandles(bridge, activeHandles)

	node := mustGetBridgeRaceNode(t, bridge, 1)
	nodeLseek, nodeLseekOK := node.ops.(NodeLseeker)

	oldFH := activeHandles[0]
	activeHandles[0] = 0
	reusedFH, err := recycleBridgeRaceHandle(bridge, oldFH)
	if err != nil {
		t.Fatal(err)
	}
	activeHandles[0] = reusedFH

	file := mustGetBridgeRaceFile(t, bridge, reusedFH)
	if nodeLseekOK {
		offset, errno := nodeLseek.Lseek(&fuse.Context{}, file.file, 1, 0)
		if errno != OK {
			t.Fatalf("node Lseek errno = %v, want OK", errno)
		}
		if offset != 1 {
			t.Fatalf("node Lseek offset = %d, want 1", offset)
		}
		return
	}

	fileLseek, ok := file.file.(FileLseeker)
	if !ok {
		t.Fatalf("file handle %T does not implement FileLseeker", file.file)
	}
	offset, errno := fileLseek.Lseek(&fuse.Context{}, 1, 0)
	if errno != OK {
		t.Fatalf("file Lseek errno = %v, want OK", errno)
	}
	if offset != 1 {
		t.Fatalf("file Lseek offset = %d, want 1", offset)
	}
}

func recycleBridgeRaceHandle(bridge *rawBridge, fh uint64) (uint64, error) {
	releaseBridgeRaceHandle(bridge, fh)

	reusedFH, err := openBridgeRaceHandle(bridge)
	if err != nil {
		return 0, fmt.Errorf("reopen released Fh %d: %w", fh, err)
	}
	if reusedFH != fh {
		releaseBridgeRaceHandle(bridge, reusedFH)
		return 0, fmt.Errorf("recycled Fh = %d, want same numeric Fh %d", reusedFH, fh)
	}
	return reusedFH, nil
}

func mustGetBridgeRaceNode(t *testing.T, bridge *rawBridge, nodeID uint64) *Inode {
	t.Helper()

	node := bridge.getNode(nodeID)
	if node == nil {
		t.Fatalf("getNode(%d) = nil", nodeID)
	}
	return node
}

func mustGetBridgeRaceFile(t *testing.T, bridge *rawBridge, fh uint64) *fileEntry {
	t.Helper()

	file := bridge.getFile(fh)
	if file == nil {
		t.Fatalf("getFile(%d) = nil after fh slot reuse", fh)
	}
	if file.file == nil {
		t.Fatalf("getFile(%d).file = nil after fh slot reuse", fh)
	}
	return file
}

func releaseBridgeRaceHandles(bridge *rawBridge, handles []uint64) {
	for i := len(handles) - 1; i >= 0; i-- {
		if handles[i] != 0 {
			releaseBridgeRaceHandle(bridge, handles[i])
		}
	}
}

func mustOpenBridgeRaceHandle(t *testing.T, bridge *rawBridge) uint64 {
	t.Helper()

	fh, err := openBridgeRaceHandle(bridge)
	if err != nil {
		t.Fatal(err)
	}
	return fh
}

func openBridgeRaceHandle(bridge *rawBridge) (uint64, error) {
	in := fuse.OpenIn{}
	in.NodeId = 1
	var out fuse.OpenOut
	if status := bridge.Open(nil, &in, &out); !status.Ok() {
		return 0, fmt.Errorf("Open status = %v, want OK", status)
	}
	if out.Fh == 0 {
		return 0, fmt.Errorf("Open Fh = 0, want non-zero")
	}
	return out.Fh, nil
}

func releaseBridgeRaceHandle(bridge *rawBridge, fh uint64) {
	in := fuse.ReleaseIn{
		Fh: fh,
	}
	in.NodeId = 1
	bridge.Release(nil, &in)
}
