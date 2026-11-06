// Copyright 2019 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fs

import (
	"context"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
)

func TestNewInodeConcurrentSameEmbedder(t *testing.T) {
	bridge := NewNodeFS(&Inode{}, nil).(*rawBridge)
	ops := &Inode{}
	before := bridge.ids.allocateNodeID()
	const callers = 32
	var nodes [callers]*Inode
	var nodeIDs [callers]uint64
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			nodes[i] = bridge.root.NewInode(context.Background(), ops, StableAttr{Mode: syscall.S_IFREG})
			nodeIDs[i] = nodes[i].nodeId
		}(i)
	}
	close(start)
	wg.Wait()
	after := bridge.ids.allocateNodeID()
	for i := 0; i < callers; i++ {
		if nodes[i] != ops.embed() {
			t.Errorf("caller %d returned %p, want %p", i, nodes[i], ops.embed())
		}
		if nodeIDs[i] != nodeIDs[0] {
			t.Errorf("caller %d saw nodeId %d, want %d", i, nodeIDs[i], nodeIDs[0])
		}
	}
	if got := after - before; got != 2 {
		t.Errorf("node ID allocation delta = %d, want 2", got)
	}
}

// TestTypeChange simulates inode number reuse that happens on real
// filesystems. For go-fuse, inode number reuse can look like a file changing
// to a directory or vice versa. Actually, the old inode does not exist anymore,
// we just have not received the FORGET yet.
func TestTypeChange(t *testing.T) {
	rootNode := testTypeChangeIno{}
	opts := Options{
		ExternalNodeID: true,
	}
	mnt, _ := testMount(t, &rootNode, &opts)

	for i := 0; i < 100; i++ {
		fi, err := os.Stat(mnt + "/file")
		if err != nil {
			t.Fatalf("Stat(file): %v", err)
		}

		syscall.Unlink(mnt + "/file")
		fi, err = os.Stat(mnt + "/dir")
		if err != nil {
			t.Fatalf("Stat(dir): %v", err)
		}
		if !fi.IsDir() {
			t.Fatal("should be a dir now")
		}
		syscall.Rmdir(mnt + "/dir")
		if fi, err := os.Stat(mnt + "/file"); err != nil {
			t.Fatalf("Stat after rmdir: %v", err)
		} else if fi.IsDir() {
			t.Fatal("should be a file now")
		}
	}
}

type testTypeChangeIno struct {
	Inode
}

// Lookup function for TestTypeChange:
// If name == "dir", returns a node of type dir,
// if name == "file" of type file,
// otherwise ENOENT.
func (fn *testTypeChangeIno) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*Inode, syscall.Errno) {
	var mode uint32
	switch name {
	case "file":
		mode = syscall.S_IFREG
	case "dir":
		mode = syscall.S_IFDIR
	default:
		return nil, syscall.ENOENT
	}
	stable := StableAttr{
		Mode: mode,
		Ino:  1234,
	}
	childFN := &testTypeChangeIno{}
	child := fn.NewInode(ctx, childFN, stable)
	return child, syscall.F_OK
}

// TestDeletedInodePath checks that Inode.Path returns ".deleted" if an Inode is
// disconnected from the hierarchy (=orphaned)
func TestDeletedInodePath(t *testing.T) {
	rootNode := testDeletedIno{}
	mnt, _ := testMount(t, &rootNode, &Options{Logger: log.New(os.Stderr, "", 0)})

	// Open a file handle so the kernel cannot FORGET the inode
	fd, err := os.Open(mnt + "/dir")
	if err != nil {
		t.Fatal(err)
	}
	defer fd.Close()

	// Delete it so the inode does not have a path anymore
	err = syscall.Rmdir(mnt + "/dir")
	if err != nil {
		t.Fatal(err)
	}
	atomic.StoreInt32(&rootNode.deleted, 1)

	// Our Getattr implementation `testDeletedIno.Getattr` should return
	// ENFILE when everything looks ok, EILSEQ otherwise.
	var st syscall.Stat_t
	err = syscall.Fstat(int(fd.Fd()), &st)
	if err != syscall.ENFILE {
		t.Error(err)
	}
}

type testDeletedIno struct {
	Inode

	deleted int32
}

func (n *testDeletedIno) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*Inode, syscall.Errno) {
	ino := n.Root().Operations().(*testDeletedIno)
	if atomic.LoadInt32(&ino.deleted) == 1 {
		return nil, syscall.ENOENT
	}
	if name != "dir" {
		return nil, syscall.ENOENT
	}
	childNode := &testDeletedIno{}
	stable := StableAttr{Mode: syscall.S_IFDIR, Ino: 999}
	child := n.NewInode(ctx, childNode, stable)
	return child, syscall.F_OK
}

var _ NodeOpendirHandler = (*testDeletedIno)(nil)

func (n *testDeletedIno) OpendirHandle(ctx context.Context, flags uint32) (fh FileHandle, fuseFlags uint32, errno syscall.Errno) {
	return &struct{}{}, 0, OK
}

func (n *testDeletedIno) Getattr(ctx context.Context, f FileHandle, out *fuse.AttrOut) syscall.Errno {
	prefix := ".go-fuse"
	p := n.Path(n.Root())
	if strings.HasPrefix(p, prefix) {
		// Return ENFILE when things look ok
		return syscall.ENFILE
	}
	// Otherwise EILSEQ
	return syscall.EILSEQ
}

// TestIno1 tests that inode number 1 is allowed.
//
// We used to panic like this because inode number 1 was special:
//
//	panic: using reserved ID 1 for inode number
func TestIno1(t *testing.T) {
	rootNode := testIno1{}
	opts := &Options{
		FirstAutomaticIno: 1,
	}
	mnt, _ := testMount(t, &rootNode, opts)

	var st syscall.Stat_t
	err := syscall.Stat(mnt+"/ino1", &st)
	if err != nil {
		t.Fatal(err)
	}
	if st.Ino != 1 {
		t.Errorf("wrong inode number: want=1 have=%d", st.Ino)
	}
}

type testIno1 struct {
	Inode
}

func (fn *testIno1) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*Inode, syscall.Errno) {
	if name != "ino1" {
		return nil, syscall.ENOENT
	}
	stable := StableAttr{
		Mode: syscall.S_IFREG,
		Ino:  1,
	}
	child := fn.NewInode(ctx, &testIno1{}, stable)
	return child, 0
}

type lookupCountRoot struct {
	Inode
	count uint32
}

func (r *lookupCountRoot) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*Inode, syscall.Errno) {
	if name == "TestNegativeLookupCache" {
		atomic.AddUint32(&r.count, 1)
	}
	return nil, syscall.ENOENT
}

func TestNegativeLookupCache(t *testing.T) {
	for count, negCache := range []bool{true, false} {
		r := lookupCountRoot{}
		opt := &Options{}
		if negCache {
			sec := time.Second
			opt.NegativeTimeout = &sec
		}

		mnt, _ := testMount(t, &r, opt)
		fn := mnt + "/TestNegativeLookupCache"
		for i := 0; i < 2; i++ {
			var st syscall.Stat_t
			if err := syscall.Lstat(fn, &st); err != syscall.ENOENT {
				t.Errorf("got %v, want ENOENT", err)
			}
		}
		want := uint32(count + 1)
		if got := atomic.LoadUint32(&r.count); got != want {
			t.Errorf("negCache=%v: got count %d, want %d", negCache, got, want)
		}
	}
}

// NewNodeFS should not crash with opts=nil
func TestNewNodeFSNilOpts(t *testing.T) {
	NewNodeFS(&Inode{}, nil)
}

type bridgeFileHandleLookupCopyRaceRoot struct {
	Inode

	bridge *rawBridge
	t      *testing.T
}

func (r *bridgeFileHandleLookupCopyRaceRoot) Open(ctx context.Context, flags uint32) (FileHandle, uint32, syscall.Errno) {
	return &bridgeFileHandleLookupCopyRaceHandle{
		bridge:       r.bridge,
		t:            r.t,
		lseekStarted: make(chan struct{}),
	}, 0, OK
}

type bridgeFileHandleLookupCopyRaceHandle struct {
	bridge *rawBridge
	t      *testing.T

	lseekOnce    sync.Once
	lseekStarted chan struct{}
}

func (h *bridgeFileHandleLookupCopyRaceHandle) Lseek(ctx context.Context, off uint64, whence uint32) (uint64, syscall.Errno) {
	h.lseekOnce.Do(func() {
		close(h.lseekStarted)
	})
	h.checkFileMuAvailable("Lseek")
	return off, OK
}

func (h *bridgeFileHandleLookupCopyRaceHandle) Release(ctx context.Context) syscall.Errno {
	h.checkFileMuAvailable("Release")
	return OK
}

func (h *bridgeFileHandleLookupCopyRaceHandle) checkFileMuAvailable(callback string) {
	h.t.Helper()

	locked := make(chan struct{})
	go func() {
		h.bridge.ids.filesMu.Lock()
		h.bridge.ids.filesMu.Unlock()
		close(locked)
	}()

	select {
	case <-locked:
	case <-time.After(time.Second):
		h.t.Errorf("%s callback could not acquire ids.filesMu within %s", callback, time.Second)
	}

	select {
	case <-time.After(time.Millisecond):
	}
}

func TestBridgeFileHandleLookupCopyRace(t *testing.T) {
	root := &bridgeFileHandleLookupCopyRaceRoot{
		t: t,
	}
	bridge := NewNodeFS(root, nil).(*rawBridge)
	root.bridge = bridge

	const handles = 64
	fhs := make([]uint64, handles)
	handleFiles := make([]*bridgeFileHandleLookupCopyRaceHandle, handles)
	for i := range fhs {
		fhs[i], handleFiles[i] = mustOpenBridgeFileHandleLookupCopyRaceHandle(t, bridge)
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, fh := range fhs {
		handleFile := handleFiles[i]

		wg.Add(1)
		go func(fh uint64) {
			defer wg.Done()
			<-start

			in := fuse.LseekIn{}
			in.NodeId = 1
			in.Fh = fh
			var out fuse.LseekOut
			if status := bridge.Lseek(context.Background(), &in, &out); !status.Ok() {
				t.Errorf("Lseek(%d) status = %v, want OK", fh, status)
			}
		}(fh)

		wg.Add(1)
		go func(fh uint64, handleFile *bridgeFileHandleLookupCopyRaceHandle) {
			defer wg.Done()
			<-start

			select {
			case <-handleFile.lseekStarted:
			case <-time.After(time.Second):
				t.Errorf("Lseek(%d) callback did not start within %s", fh, time.Second)
				return
			}

			releaseBridgeFileHandleLookupCopyRaceHandle(bridge, fh)
		}(fh, handleFile)
	}

	close(start)
	waitBridgeFileHandleLookupCopyRaceGoroutines(t, &wg)

	reopened := make([]uint64, handles)
	for i := range reopened {
		fh, _ := mustOpenBridgeFileHandleLookupCopyRaceHandle(t, bridge)
		if fh == 0 {
			t.Fatalf("reopened handle %d Fh = 0, want non-zero", i)
		}
		reopened[i] = fh
	}
	for _, fh := range reopened {
		releaseBridgeFileHandleLookupCopyRaceHandle(bridge, fh)
	}
}

func mustOpenBridgeFileHandleLookupCopyRaceHandle(t *testing.T, bridge *rawBridge) (uint64, *bridgeFileHandleLookupCopyRaceHandle) {
	t.Helper()

	in := fuse.OpenIn{}
	in.NodeId = 1
	var out fuse.OpenOut
	if status := bridge.Open(context.Background(), &in, &out); !status.Ok() {
		t.Fatalf("Open status = %v, want OK", status)
	}
	if out.Fh == 0 {
		t.Fatal("Open Fh = 0, want non-zero")
	}

	fe := bridgeFileEntry(t, bridge, 1, out.Fh)
	handleFile, ok := fe.file.(*bridgeFileHandleLookupCopyRaceHandle)
	if !ok {
		t.Fatalf("ids.file(%d).file = %T, want *bridgeFileHandleLookupCopyRaceHandle", out.Fh, fe.file)
	}
	return out.Fh, handleFile
}

func releaseBridgeFileHandleLookupCopyRaceHandle(bridge *rawBridge, fh uint64) {
	in := fuse.ReleaseIn{
		Fh: fh,
	}
	in.NodeId = 1
	bridge.Release(context.Background(), &in)
}

func waitBridgeFileHandleLookupCopyRaceGoroutines(t *testing.T, wg *sync.WaitGroup) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent Lseek/Release goroutines did not finish within 5s")
	}
}

func TestEntryNoFileSkipsFilesMu(t *testing.T) {
	bridge := NewNodeFS(&Inode{}, nil).(*rawBridge)
	wantEntry := bridge.ids.node(1)
	filesMu := &bridge.ids.filesMu
	filesMu.Lock()
	wantFile := bridge.ids.files[0]
	type result struct {
		entry *nodeEntry
		file  *fileEntry
	}
	done := make(chan result, 1)
	go func() {
		entry, file := bridge.entry(1, 0)
		done <- result{entry, file}
	}()
	var got result
	blocked := false
	select {
	case got = <-done:
	case <-time.After(time.Second):
		blocked = true
	}
	filesMu.Unlock()
	if blocked {
		<-done
		t.Fatal("entry(nodeid, 0) blocked on filesMu")
	}
	if got.entry != wantEntry {
		t.Errorf("entry(nodeid, 0) node = %p, want %p", got.entry, wantEntry)
	}
	if got.file != wantFile {
		t.Errorf("entry(nodeid, 0) file = %p, want files[0] %p", got.file, wantFile)
	}
}

type bridgeGetAttrNoOpenFileRoot struct {
	bridgeFileLookupRaceRoot
}

func (r *bridgeGetAttrNoOpenFileRoot) Getattr(ctx context.Context, file FileHandle, out *fuse.AttrOut) syscall.Errno {
	if file == nil {
		return OK
	}
	handle, ok := file.(*bridgeFileLookupRaceHandle)
	if !ok {
		return syscall.EBADF
	}
	out.Size = handle.id
	return OK
}

func TestGetAttrNoOpenFileSkipsFilesMu(t *testing.T) {
	bridge := NewNodeFS(&bridgeGetAttrNoOpenFileRoot{}, nil).(*rawBridge)
	in := fuse.GetAttrIn{}
	in.NodeId = 1
	type result struct {
		status fuse.Status
		out    fuse.AttrOut
	}
	done := make(chan result, 1)
	filesMu := &bridge.ids.filesMu
	filesMu.Lock()
	go func() {
		var got result
		got.status = bridge.GetAttr(context.Background(), &in, &got.out)
		done <- got
	}()
	var got result
	blocked := false
	select {
	case got = <-done:
	case <-time.After(time.Second):
		blocked = true
	}
	filesMu.Unlock()
	if blocked {
		<-done
		t.Fatal("GetAttr blocked on filesMu")
	}
	if got.status != fuse.OK {
		t.Fatalf("GetAttr without an open handle status = %v, want OK", got.status)
	}
	if got.out.Size != 0 {
		t.Fatalf("GetAttr without an open handle size = %d, want 0", got.out.Size)
	}

	fh := mustOpenBridgeRaceHandle(t, bridge)
	defer releaseBridgeRaceHandle(bridge, fh)
	file := mustGetBridgeRaceFile(t, bridge, fh).file.(*bridgeFileLookupRaceHandle)
	var out fuse.AttrOut
	if status := bridge.GetAttr(context.Background(), &in, &out); status != fuse.OK {
		t.Fatalf("GetAttr with an open handle status = %v, want OK", status)
	}
	if out.Size != file.id {
		t.Fatalf("GetAttr with an open handle size = %d, want handle id %d", out.Size, file.id)
	}
}
