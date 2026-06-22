// Copyright 2019 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fs

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/internal"
)

func errnoToStatus(errno syscall.Errno) fuse.Status {
	return fuse.Status(errno)
}

// fileEntry is the per-handle state stored in rawBridge.files. A live
// b.files[fh] slot stays non-nil until waiters, callbacks, and backing cleanup
// finish; only then can the handle be reused through rawBridge.freeFiles.
type fileEntry struct {
	file FileHandle

	// backingIDRef is true when this fileEntry owns one backing-ID ref on
	// its inode. The ref is per handle and protected by that inode's
	// backingMu.
	backingIDRef bool

	// index into Inode.openFiles
	nodeIndex int

	// Handle number which we communicate to the kernel.
	fh uint32

	// Protects directory fields.
	mu sync.Mutex

	// Directory
	hasOverflow   bool
	overflow      fuse.DirEntry
	overflowErrno syscall.Errno

	// Store the last read, in case readdir was interrupted.
	lastRead []fuse.DirEntry

	// dirOffset is the current location in the directory (see `telldir(3)`).
	// The value is equivalent to `d_off` (see `getdents(2)`) of the last
	// directory entry sent to the kernel so far.
	// If `dirOffset` and `fuse.DirEntryList.offset` disagree, then a
	// directory seek has taken place.
	dirOffset uint64

	// We try to associate a file for stat() calls, but the kernel
	// can issue a RELEASE and GETATTR in parallel. This waitgroup
	// avoids that the RELEASE will invalidate the file descriptor
	// before we finish processing GETATTR.
	wg sync.WaitGroup
}

// ServerCallbacks are calls into the kernel to manipulate the inode,
// entry and page cache.  They are stubbed so filesystems can be
// unittested without mounting them.
type ServerCallbacks interface {
	DeleteNotify(parent uint64, child uint64, name string) fuse.Status
	EntryNotify(parent uint64, name string) fuse.Status
	InodeNotify(node uint64, off int64, length int64) fuse.Status
	InodeRetrieveCache(node uint64, offset int64, dest []byte) (n int, st fuse.Status)
	InodeNotifyStoreCache(node uint64, offset int64, data []byte) fuse.Status
}

// TODO: fold serverBackingFdCallbacks into ServerCallbacks and bump API version
type serverBackingFdCallbacks interface {
	RegisterBackingFd(*fuse.BackingMap) (int32, syscall.Errno)
	UnregisterBackingFd(id int32) syscall.Errno
}

type rawBridge struct {
	options Options
	root    *Inode
	server  ServerCallbacks

	// fileMu protects files, freeFiles, and Inode.openFiles.
	fileMu sync.Mutex

	// stableAttrs is used to detect already-known nodes and hard links by
	// looking at:
	// 1) file type ......... StableAttr.Mode
	// 2) inode number ...... StableAttr.Ino
	// 3) generation number . StableAttr.Gen
	stableAttrs  shardedMap[StableAttr, *Inode]
	automaticIno atomic.Uint64

	// The *Node ID* is an arbitrary uint64 identifier chosen by the FUSE library.
	// It is used the identify *nodes* (files/directories/symlinks/...) in the
	// communication between the FUSE library and the Linux kernel.
	//
	// The kernelNodeIds map translates between the NodeID and the corresponding
	// go-fuse Inode object.
	//
	// A simple incrementing counter is used as the NodeID (see `nextNodeID`).
	kernelNodeIds shardedMap[uint64, *Inode]
	// retiredKernelNodeIds keeps retired node IDs reachable until local refs drain.
	retiredKernelNodeIds shardedMap[uint64, *Inode]
	// nextNodeID is the next free NodeID. Increment after copying the value.
	nextNodeId atomic.Uint64

	// files and freeFiles are protected by fileMu.
	files []*fileEntry

	// indices of files that are not allocated.
	freeFiles []uint32

	// disableBackingFiles is a sticky bridge-wide latch. Unsupported servers
	// or registration errno disable future backing-file registration attempts.
	disableBackingFiles atomic.Bool
	// Compactor goroutine lifecycle. The worker starts lazily in Init, and
	// stopNodeMapCompactor is idempotent.
	compactStopOnce sync.Once
	compactMu       sync.Mutex
	compactStarted  bool
	compactStopped  bool
	compactWake     chan struct{}
	compactStop     chan struct{}
	compactDone     chan struct{}
}

// newInode creates creates new inode pointing to ops.
func (b *rawBridge) newInodeUnlocked(ops InodeEmbedder, id StableAttr, persistent bool) *Inode {
	if id.Reserved() {
		log.Panicf("using reserved ID %d for inode number", id.Ino)
	}

	embedded := ops.embed()
	embedded.mu.Lock()
	defer embedded.mu.Unlock()

	// This ops already was populated. Just return it.
	if embedded.bridge != nil {
		return embedded
	}

	// Only the file type bits matter
	id.Mode = id.Mode & syscall.S_IFMT
	if id.Mode == 0 {
		id.Mode = fuse.S_IFREG
	}

	if id.Ino == 0 {
		// Find free inode number.
		for {
			id.Ino = b.automaticIno.Add(1) - 1
			_, ok := b.stableAttrs.Get(id)
			if !ok {
				break
			}
		}
	}

	initInode(embedded, ops, id, b, persistent, b.nextNodeId.Add(1)-1)
	return embedded
}

func (b *rawBridge) logf(format string, args ...interface{}) {
	if b.options.Logger != nil {
		b.options.Logger.Printf(format, args...)
	}
}

func (b *rawBridge) newInode(ctx context.Context, ops InodeEmbedder, id StableAttr, persistent bool) *Inode {
	ch := b.newInodeUnlocked(ops, id, persistent)
	if ch != ops.embed() {
		return ch
	}

	if oa, ok := ops.(NodeOnAdder); ok {
		oa.OnAdd(ctx)
	}
	return ch
}

// addNewChild inserts the child into the tree. Returns file handle if file != nil.
// Unless fileFlags has the syscall.O_EXCL bit set, child.stableAttr will be used
// to find an already-known node. If one is found, `child` is ignored and the
// already-known one is used. The node that was actually used is returned.
// Namespace updates hold Inode.mu and may nest shard-map operations; see
// docs/design/fork-perf/sharded-bridge-maps.md for the map-lock architecture.
func (b *rawBridge) addNewChild(parent *Inode, name string, child *Inode, file FileHandle, fileFlags uint32, out *fuse.EntryOut) (selected *Inode, fe *fileEntry) {
	if name == "." || name == ".." {
		log.Panicf("BUG: tried to add virtual entry %q to the actual tree", name)
	}

	// the same node can be looked up through 2 paths in parallel, eg.
	//
	//	    root
	//	    /  \
	//	  dir1 dir2
	//	    \  /
	//	    file
	//
	// dir1.Lookup("file") and dir2.Lookup("file") are executed
	// simultaneously.  The matching StableAttrs ensure that we return the
	// same node.
	orig := child
	id := child.stableAttr
	if id.Mode & ^(uint32(syscall.S_IFMT)) != 0 {
		log.Panicf("%#v", id)
	}
	for {
		lockNodes(parent, child)
		if fileFlags&syscall.O_EXCL != 0 {
			// must create a new node - don't look for existing nodes
			break
		}
		if child == orig {
			// LoadOrStore publishes the canonical hard-link inode under the
			// stableAttr shard lock while namespace locks are held.
			actual, loaded := b.stableAttrs.LoadOrStore(id, child)
			if !loaded || actual == child {
				break
			}
			unlockNodes(parent, child)
			child = actual
			continue
		}
		old, ok := b.stableAttrs.Get(id)
		if !ok {
			unlockNodes(parent, child)
			child = orig
			continue
		}
		if old == child {
			break
		}
		unlockNodes(parent, child)
		child = old
	}
	child.lookupCount++
	child.changeCounter++
	b.kernelNodeIds.Set(child.nodeId, child)
	if fileFlags&syscall.O_EXCL != 0 {
		// Any node that might be there is overwritten - it is obsolete now
		b.stableAttrs.Set(id, child)
	}
	parent.setEntry(name, child)
	out.NodeId = child.nodeId
	out.Generation = child.stableAttr.Gen
	out.Attr.Ino = child.stableAttr.Ino
	unlockNodes(parent, child)
	if file != nil {
		// Keep fileMu out of the Inode.mu -> mapShard.mu namespace lock chain.
		fe = b.registerFile(child, file, fileFlags)
	}
	return child, fe
}

func (b *rawBridge) setEntryOutTimeout(out *fuse.EntryOut) {
	b.setAttr(&out.Attr)
	if b.options.AttrTimeout != nil && out.AttrTimeout() == 0 {
		out.SetAttrTimeout(*b.options.AttrTimeout)
	}
	if b.options.EntryTimeout != nil && out.EntryTimeout() == 0 {
		out.SetEntryTimeout(*b.options.EntryTimeout)
	}
}

func (b *rawBridge) setAttr(out *fuse.Attr) {
	if !b.options.NullPermissions && out.Mode&07777 == 0 {
		out.Mode |= 0644
		if out.Mode&syscall.S_IFDIR != 0 {
			out.Mode |= 0111
		}
	}
	if b.options.UID != 0 && out.Uid == 0 {
		out.Uid = b.options.UID
	}
	if b.options.GID != 0 && out.Gid == 0 {
		out.Gid = b.options.GID
	}
	setBlocks(out)
}

func (b *rawBridge) setAttrTimeout(out *fuse.AttrOut) {
	if b.options.AttrTimeout != nil && out.Timeout() == 0 {
		out.SetTimeout(*b.options.AttrTimeout)
	}
}

// NewNodeFS creates a node based filesystem based on the
// InodeEmbedder instance for the root of the tree.
// If nil is given as opts, default settings are
// applied, which are 1 second entry and attribute timeout.
// The bridge owns a node-map compactor that starts lazily in Init and stops
// idempotently from OnUnmount.
func NewNodeFS(root InodeEmbedder, opts *Options) fuse.RawFileSystem {
	if opts == nil {
		oneSec := time.Second
		opts = &Options{
			EntryTimeout: &oneSec,
			AttrTimeout:  &oneSec,
		}
	}
	bridge := &rawBridge{
		server:               opts.ServerCallbacks,
		stableAttrs:          shardedMap[StableAttr, *Inode]{},
		kernelNodeIds:        shardedMap[uint64, *Inode]{},
		retiredKernelNodeIds: shardedMap[uint64, *Inode]{},
		compactWake:          make(chan struct{}, 1),
		compactStop:          make(chan struct{}),
		compactDone:          make(chan struct{}),
		options:              *opts,
	}
	bridge.nextNodeId.Store(2) // the root node has nodeid 1
	bridge.automaticIno.Store(opts.FirstAutomaticIno)

	if bridge.automaticIno.Load() == 0 {
		bridge.automaticIno.Store(1 << 63)
	}

	bridge.stableAttrs.Init()
	bridge.kernelNodeIds.Init()
	bridge.retiredKernelNodeIds.Init()

	stableAttr := StableAttr{
		Ino:  root.embed().StableAttr().Ino,
		Mode: fuse.S_IFDIR,
	}
	if opts.RootStableAttr != nil {
		stableAttr.Ino = opts.RootStableAttr.Ino
		stableAttr.Gen = opts.RootStableAttr.Gen
	}

	initInode(root.embed(), root,
		stableAttr,
		bridge,
		false,
		1,
	)
	bridge.root = root.embed()
	bridge.root.lookupCount = 1
	bridge._setNode(1, bridge.root)

	// Fh 0 means no file handle.
	bridge.files = []*fileEntry{{}}

	if opts.OnAdd != nil {
		opts.OnAdd(context.Background())
	} else if oa, ok := root.(NodeOnAdder); ok {
		oa.OnAdd(context.Background())
	}

	return bridge
}

func (b *rawBridge) String() string {
	return "rawBridge"
}

func (b *rawBridge) getNode(id uint64) *Inode {
	node, ok := b.kernelNodeIds.Get(id)
	if ok {
		return node
	}

	node, ok = b.retiredKernelNodeIds.Get(id)
	if !ok {
		return nil
	}
	return node
}

// acquireNode resolves a kernel node ID for request handling and pins the
// returned inode with a bridge-local ref. It checks retiredKernelNodeIds under
// that shard's RLock and uses getAndDoLocked(id, addRef) so a retired entry is
// pinned before the lookup releases the shard. While that RLock is held, a
// missing retired entry falls back to kernelNodeIds.GetAndDo, which pins live
// entries before final forget can move the ID into the retired table.
//
// The returned release closure is mandatory; request paths defer it. Release
// lets compactNodeMaps reap retiredKernelNodeIds after localRefs drains.
// removeRefInner's conditional retire rollback keeps the retired table from
// publishing an ID when the live table no longer points at the same inode.
// Request paths use acquireNode rather than getNode so late requests cannot
// race final forget and retired-entry reaping without a local ref. See
// docs/design/fork-perf/sharded-bridge-maps.md for map lifecycle details.
func (b *rawBridge) acquireNode(id uint64) (*Inode, func()) {
	addRef := func(n *Inode) {
		n.localRefs.Add(1)
	}

	retiredShard := b.retiredKernelNodeIds.getMapShard(b.retiredKernelNodeIds.getMapShardKey(id))
	retiredShard.mu.RLock()
	n, ok := retiredShard.getAndDoLocked(id, addRef)
	if !ok {
		n, ok = b.kernelNodeIds.GetAndDo(id, addRef)
	}
	retiredShard.mu.RUnlock()

	if !ok {
		return nil, func() {}
	}

	return n, func() {
		if n.localRefs.Add(-1) == 0 && n.retired.Load() {
			b.scheduleNodeMapCompaction()
		}
	}
}

func (b *rawBridge) _setNode(id uint64, node *Inode) {
	b.kernelNodeIds.Set(id, node)
}

// getFile returns the current file entry for fh under fileMu. The slot stays
// non-nil until release waiters, callbacks, and backing cleanup complete, and
// only then can b.freeFiles reuse the handle. Callers that invoke filesystem
// callbacks must copy fe.file into a local before the callback.
func (b *rawBridge) getFile(fh uint64) *fileEntry {
	b.fileMu.Lock()
	defer b.fileMu.Unlock()
	if fh >= uint64(len(b.files)) {
		return nil
	}
	fe := b.files[fh]
	return fe
}

func (b *rawBridge) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	parent, release := b.acquireNode(header.NodeId)
	defer release()
	ctx := &fuse.Context{Caller: header.Caller, Cancel: cancel}
	child, errno := b.lookup(ctx, parent, name, out)

	if errno != 0 {
		if errno == syscall.ENOENT && b.options.NegativeTimeout != nil && out.EntryTimeout() == 0 {
			out.SetEntryTimeout(*b.options.NegativeTimeout)
			errno = 0
		}
		return errnoToStatus(errno)
	}

	child, _ = b.addNewChild(parent, name, child, nil, 0, out)
	child.setEntryOut(out)
	b.setEntryOutTimeout(out)
	return fuse.OK
}

func (b *rawBridge) lookup(ctx *fuse.Context, parent *Inode, name string, out *fuse.EntryOut) (*Inode, syscall.Errno) {
	if lu, ok := parent.ops.(NodeLookuper); ok {
		return lu.Lookup(ctx, name, out)
	}

	child := parent.GetChild(name)
	if child == nil {
		return nil, syscall.ENOENT
	}

	if ga, ok := child.ops.(NodeGetattrer); ok {
		var a fuse.AttrOut
		errno := ga.Getattr(ctx, nil, &a)
		if errno == 0 {
			out.Attr = a.Attr
		}
	}

	return child, OK
}

func (b *rawBridge) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	parent, release := b.acquireNode(header.NodeId)
	defer release()
	var errno syscall.Errno
	if mops, ok := parent.ops.(NodeRmdirer); ok {
		errno = mops.Rmdir(&fuse.Context{Caller: header.Caller, Cancel: cancel}, name)
	}

	// TODO - this should not succeed silently.

	if errno == 0 {
		parent.RmChild(name)
	}
	return errnoToStatus(errno)
}

func (b *rawBridge) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	parent, release := b.acquireNode(header.NodeId)
	defer release()
	var errno syscall.Errno
	if mops, ok := parent.ops.(NodeUnlinker); ok {
		errno = mops.Unlink(&fuse.Context{Caller: header.Caller, Cancel: cancel}, name)
	}

	// TODO - this should not succeed silently.

	if errno == 0 {
		parent.RmChild(name)
	}
	return errnoToStatus(errno)
}

func (b *rawBridge) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) fuse.Status {
	parent, release := b.acquireNode(input.NodeId)
	defer release()

	ctx := &fuse.Context{Caller: input.Caller, Cancel: cancel}
	mops, ok := parent.ops.(NodeMkdirer)
	if !ok {
		return fuse.ENOTSUP
	}
	child, errno := mops.Mkdir(ctx, name, input.Mode, out)

	if errno != 0 {
		return errnoToStatus(errno)
	}

	if out.Attr.Mode&^07777 == 0 {
		out.Attr.Mode |= fuse.S_IFDIR
	}

	if out.Attr.Mode&^07777 != fuse.S_IFDIR {
		log.Panicf("Mkdir: mode must be S_IFDIR (%o), got %o", fuse.S_IFDIR, out.Attr.Mode)
	}

	child, _ = b.addNewChild(parent, name, child, nil, syscall.O_EXCL, out)
	child.setEntryOut(out)
	b.setEntryOutTimeout(out)
	return fuse.OK
}

func (b *rawBridge) Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) fuse.Status {
	parent, release := b.acquireNode(input.NodeId)
	defer release()

	mops, ok := parent.ops.(NodeMknoder)
	if !ok {
		return fuse.ENOTSUP
	}
	ctx := &fuse.Context{Caller: input.Caller, Cancel: cancel}
	child, errno := mops.Mknod(ctx, name, input.Mode, input.Rdev, out)
	if errno != 0 {
		return errnoToStatus(errno)
	}

	child, _ = b.addNewChild(parent, name, child, nil, syscall.O_EXCL, out)
	child.setEntryOut(out)
	b.setEntryOutTimeout(out)
	return fuse.OK
}

func (b *rawBridge) Create(cancel <-chan struct{}, input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	parent, release := b.acquireNode(input.NodeId)
	defer release()

	mops, ok := parent.ops.(NodeCreater)
	if !ok {
		return fuse.EROFS
	}
	ctx := &fuse.Context{Caller: input.Caller, Cancel: cancel}
	child, f, flags, errno := mops.Create(ctx, name, input.Flags, input.Mode, &out.EntryOut)

	if errno != 0 {
		return errnoToStatus(errno)
	}

	child, fe := b.addNewChild(parent, name, child, f, input.Flags|syscall.O_CREAT|syscall.O_EXCL, &out.EntryOut)
	if fe != nil {
		out.Fh = uint64(fe.fh)
	}
	out.OpenFlags = flags

	if fe != nil {
		b.addBackingID(child, fe, &out.OpenOut)
	}
	child.setEntryOut(&out.EntryOut)
	b.setEntryOutTimeout(&out.EntryOut)
	return fuse.OK
}

func (b *rawBridge) Forget(nodeid, nlookup uint64) {
	n, release := b.acquireNode(nodeid)
	defer release()
	hasLookups, _, _ := n.removeRef(nlookup, false)

	if !hasLookups {
		b.compactMemory()
	}
}

// compactMemory schedules reclamation of memory and retired node IDs left by
// forgotten nodes.
//
// Maps do not free all memory when elements get deleted
// ( https://github.com/golang/go/issues/20135 ).
// As a workaround, the sharded maps backing stableAttrs, kernelNodeIds, and
// retiredKernelNodeIds rebuild their per-shard tables when they have shrunk
// dramatically (see shardedmap.go for the shrink threshold and rate-limit).
// The worker starts lazily in Init, stops idempotently, and Mount stops it
// explicitly when server creation or mounting fails.
func (b *rawBridge) compactMemory() {
	b.scheduleNodeMapCompaction()
}

func (b *rawBridge) scheduleNodeMapCompaction() {
	select {
	case b.compactWake <- struct{}{}:
	default:
	}
}

func (b *rawBridge) compactNodeMaps(now time.Time) {
	b.stableAttrs.compactCandidates(now)
	b.kernelNodeIds.compactCandidates(now)
	b.retiredKernelNodeIds.Range(func(id uint64, n *Inode) bool {
		if n.localRefs.Load() == 0 {
			b.retiredKernelNodeIds.DeleteIf(id, func(got *Inode) bool {
				return got == n && got.localRefs.Load() == 0
			})
		}
		return true
	})
	b.retiredKernelNodeIds.compactCandidates(now)
}

// startNodeMapCompactor lazily starts the single bridge-owned compactor from
// Init.
func (b *rawBridge) startNodeMapCompactor() {
	b.compactMu.Lock()
	defer b.compactMu.Unlock()

	if b.compactStarted || b.compactStopped {
		return
	}
	b.compactStarted = true
	go b.runNodeMapCompactor()
}

// stopNodeMapCompactor idempotently stops the compactor. It also completes
// compactDone for bridges that stop before Init starts the worker.
func (b *rawBridge) stopNodeMapCompactor() {
	b.compactStopOnce.Do(func() {
		b.compactMu.Lock()
		defer b.compactMu.Unlock()

		b.compactStopped = true
		started := b.compactStarted
		close(b.compactStop)
		if !started {
			close(b.compactDone)
		}
	})
	<-b.compactDone
}

func (b *rawBridge) runNodeMapCompactor() {
	ticker := time.NewTicker(mapCompactMinInterval)
	defer ticker.Stop()
	defer close(b.compactDone)

	for {
		select {
		case <-b.compactWake:
			b.compactNodeMaps(time.Now())
		case now := <-ticker.C:
			b.compactNodeMaps(now)
		case <-b.compactStop:
			return
		}
	}
}

func (b *rawBridge) SetDebug(debug bool) {}

func (b *rawBridge) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	n, release := b.acquireNode(input.NodeId)
	defer release()
	fe := b.getFile(input.Fh())
	var f FileHandle
	if fe != nil {
		f = fe.file
	}
	if f == nil {
		var fallback *fileEntry
		// The linux kernel doesnt pass along the file
		// descriptor, so we have to fake it here.
		// See https://github.com/libfuse/libfuse/issues/62
		b.fileMu.Lock()
		if len(n.openFiles) > 0 {
			fallback = b.files[n.openFiles[0]]
			if fallback != nil {
				f = fallback.file
				fallback.wg.Add(1)
			}
		}
		b.fileMu.Unlock()
		if fallback != nil {
			defer fallback.wg.Done()
		}
	}
	ctx := &fuse.Context{Caller: input.Caller, Cancel: cancel}
	return errnoToStatus(b.getattr(ctx, n, f, out))
}

func (b *rawBridge) getattr(ctx context.Context, n *Inode, f FileHandle, out *fuse.AttrOut) syscall.Errno {
	var errno syscall.Errno

	if nodeOps, ok := n.ops.(NodeGetattrer); ok {
		errno = nodeOps.Getattr(ctx, f, out)
	} else if fileOps, ok := f.(FileGetattrer); ok {
		errno = fileOps.Getattr(ctx, out)
	} else {
		// We set Mode below, which is the minimum for success
	}

	if errno == 0 {
		if out.Ino != 0 && n.stableAttr.Ino > 1 && out.Ino != n.stableAttr.Ino {
			b.logf("warning: rawBridge.getattr: overriding ino %d with %d", out.Ino, n.stableAttr.Ino)
		}
		out.Ino = n.stableAttr.Ino
		out.Mode = (out.Attr.Mode & 07777) | n.stableAttr.Mode
		b.setAttr(&out.Attr)
		b.setAttrTimeout(out)
	}
	return errno
}

func (b *rawBridge) SetAttr(cancel <-chan struct{}, in *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {
	ctx := &fuse.Context{Caller: in.Caller, Cancel: cancel}

	fh, _ := in.GetFh()

	n, release := b.acquireNode(in.NodeId)
	defer release()
	fe := b.getFile(fh)
	file := fe.file

	var errno = syscall.ENOTSUP
	if fops, ok := n.ops.(NodeSetattrer); ok {
		errno = fops.Setattr(ctx, file, in, out)
	} else if fops, ok := file.(FileSetattrer); ok {
		errno = fops.Setattr(ctx, in, out)
	}

	out.Mode = n.stableAttr.Mode | (out.Mode & 07777)
	return errnoToStatus(errno)
}

func (b *rawBridge) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName string, newName string) fuse.Status {
	p1, release1 := b.acquireNode(input.NodeId)
	defer release1()
	p2, release2 := b.acquireNode(input.Newdir)
	defer release2()

	if mops, ok := p1.ops.(NodeRenamer); ok {
		errno := mops.Rename(&fuse.Context{Caller: input.Caller, Cancel: cancel}, oldName, p2.ops, newName, input.Flags)
		if errno == 0 {
			if input.Flags&RENAME_EXCHANGE != 0 {
				p1.ExchangeChild(oldName, p2, newName)
			} else {
				// MvChild cannot fail with overwrite=true.
				_ = p1.MvChild(oldName, p2, newName, true)
			}
		}
		return errnoToStatus(errno)
	}
	return fuse.ENOTSUP
}

func (b *rawBridge) Link(cancel <-chan struct{}, input *fuse.LinkIn, name string, out *fuse.EntryOut) fuse.Status {
	parent, release1 := b.acquireNode(input.NodeId)
	defer release1()
	target, release2 := b.acquireNode(input.Oldnodeid)
	defer release2()

	mops, ok := parent.ops.(NodeLinker)
	if !ok {
		return fuse.ENOTSUP
	}

	ctx := &fuse.Context{Caller: input.Caller, Cancel: cancel}
	child, errno := mops.Link(ctx, target.ops, name, out)
	if errno != 0 {
		return errnoToStatus(errno)
	}

	child, _ = b.addNewChild(parent, name, child, nil, 0, out)
	child.setEntryOut(out)
	b.setEntryOutTimeout(out)
	return fuse.OK
}

func (b *rawBridge) Symlink(cancel <-chan struct{}, header *fuse.InHeader, target string, name string, out *fuse.EntryOut) fuse.Status {
	parent, release := b.acquireNode(header.NodeId)
	defer release()

	mops, ok := parent.ops.(NodeSymlinker)
	if !ok {
		return fuse.ENOTSUP
	}
	ctx := &fuse.Context{Caller: header.Caller, Cancel: cancel}
	child, status := mops.Symlink(ctx, target, name, out)
	if status != 0 {
		return errnoToStatus(status)
	}

	child, _ = b.addNewChild(parent, name, child, nil, syscall.O_EXCL, out)
	child.setEntryOut(out)
	b.setEntryOutTimeout(out)
	return fuse.OK
}

func (b *rawBridge) Readlink(cancel <-chan struct{}, header *fuse.InHeader) (out []byte, status fuse.Status) {
	n, release := b.acquireNode(header.NodeId)
	defer release()

	linker, ok := n.ops.(NodeReadlinker)
	if !ok {
		return nil, fuse.ENOTSUP
	}
	ctx := &fuse.Context{Caller: header.Caller, Cancel: cancel}
	result, errno := linker.Readlink(ctx)
	if errno != 0 {
		return nil, errnoToStatus(errno)
	}

	return result, fuse.OK
}

func (b *rawBridge) Access(cancel <-chan struct{}, input *fuse.AccessIn) fuse.Status {
	n, release := b.acquireNode(input.NodeId)
	defer release()

	ctx := &fuse.Context{Caller: input.Caller, Cancel: cancel}
	if a, ok := n.ops.(NodeAccesser); ok {
		return errnoToStatus(a.Access(ctx, input.Mask))
	}

	// default: check attributes.
	caller := input.Caller

	var out fuse.AttrOut
	if s := b.getattr(ctx, n, nil, &out); s != 0 {
		return errnoToStatus(s)
	}

	if !internal.HasAccess(caller.Uid, caller.Gid, out.Uid, out.Gid, out.Mode, input.Mask) {
		return fuse.EACCES
	}
	return fuse.OK
}

// Extended attributes.

func (b *rawBridge) GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, data []byte) (uint32, fuse.Status) {
	n, release := b.acquireNode(header.NodeId)
	defer release()

	if xops, ok := n.ops.(NodeGetxattrer); ok {
		nb, errno := xops.Getxattr(&fuse.Context{Caller: header.Caller, Cancel: cancel}, attr, data)
		return nb, errnoToStatus(errno)
	}

	return 0, fuse.ENOATTR
}

func (b *rawBridge) ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (sz uint32, status fuse.Status) {
	n, release := b.acquireNode(header.NodeId)
	defer release()
	if xops, ok := n.ops.(NodeListxattrer); ok {
		sz, errno := xops.Listxattr(&fuse.Context{Caller: header.Caller, Cancel: cancel}, dest)
		return sz, errnoToStatus(errno)
	}
	return 0, fuse.OK
}

func (b *rawBridge) SetXAttr(cancel <-chan struct{}, input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	n, release := b.acquireNode(input.NodeId)
	defer release()
	if xops, ok := n.ops.(NodeSetxattrer); ok {
		return errnoToStatus(xops.Setxattr(&fuse.Context{Caller: input.Caller, Cancel: cancel}, attr, data, input.Flags))
	}
	return fuse.ENOATTR
}

func (b *rawBridge) RemoveXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string) fuse.Status {
	n, release := b.acquireNode(header.NodeId)
	defer release()
	if xops, ok := n.ops.(NodeRemovexattrer); ok {
		return errnoToStatus(xops.Removexattr(&fuse.Context{Caller: header.Caller, Cancel: cancel}, attr))
	}
	return fuse.ENOATTR
}

func (b *rawBridge) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	n, release := b.acquireNode(input.NodeId)
	defer release()

	op, ok := n.ops.(NodeOpener)
	if !ok {
		return fuse.ENOTSUP
	}
	f, flags, errno := op.Open(&fuse.Context{Caller: input.Caller, Cancel: cancel}, input.Flags)
	if errno != 0 {
		return errnoToStatus(errno)
	}
	out.OpenFlags = flags

	if f != nil {
		fe := b.registerFile(n, f, input.Flags)
		out.Fh = uint64(fe.fh)
		b.addBackingID(n, fe, out)
	}
	return fuse.OK
}

func (b *rawBridge) addBackingID(n *Inode, fe *fileEntry, out *fuse.OpenOut) {
	if b.disableBackingFiles.Load() {
		return
	}

	bc, ok := b.server.(serverBackingFdCallbacks)
	if !ok {
		// The bridge-wide latch is sticky: once this server lacks backing-fd
		// callbacks, later opens skip registration.
		b.disableBackingFiles.Store(true)
		return
	}
	pth, ok := fe.file.(FilePassthroughFder)
	if !ok {
		return
	}

	n.backingMu.Lock()
	defer n.backingMu.Unlock()
	if b.disableBackingFiles.Load() {
		return
	}
	fe.backingIDRef = b.addBackingIDLocked(n, pth, bc, out)
}

// must hold n.backingMu
func (b *rawBridge) addBackingIDLocked(n *Inode, pth FilePassthroughFder, bc serverBackingFdCallbacks, out *fuse.OpenOut) bool {
	if n.backingID == 0 {
		fd, ok := pth.PassthroughFd()
		if !ok {
			return false
		}
		m := fuse.BackingMap{
			Fd: int32(fd),
		}
		// RegisterBackingFd issues the kernel ioctl while n.backingMu is held.
		// The lock serializes this inode's backingID/refcount state; unrelated
		// inodes use their own backingMu and register concurrently.
		id, errno := bc.RegisterBackingFd(&m)
		if errno != 0 {
			// This happens if we're not root or CAP_PASSTHROUGH is missing.
			// Registration errno trips the sticky global latch.
			b.disableBackingFiles.Store(true)
			return false
		} else {
			n.backingID = id
		}
	}

	if n.backingID != 0 {
		out.BackingID = n.backingID
		out.OpenFlags |= fuse.FOPEN_PASSTHROUGH
		out.OpenFlags &= ^uint32(fuse.FOPEN_KEEP_CACHE)
		n.backingIDRefcount++
		return true
	}
	return false
}

func (b *rawBridge) releaseBackingIDRef(n *Inode, fe *fileEntry) {
	n.backingMu.Lock()
	defer n.backingMu.Unlock()
	if !fe.backingIDRef {
		return
	}
	fe.backingIDRef = false
	b.releaseBackingIDRefLocked(n)
}

// must hold n.backingMu
func (b *rawBridge) releaseBackingIDRefLocked(n *Inode) {
	if n.backingID == 0 {
		return
	}

	n.backingIDRefcount--
	if n.backingIDRefcount == 0 {
		// UnregisterBackingFd issues the kernel ioctl while n.backingMu is held
		// so this inode's ID/refcount cannot be reused until unregister
		// completes. Other inodes unregister under their own backingMu.
		errno := b.server.(serverBackingFdCallbacks).UnregisterBackingFd(n.backingID)
		if errno != 0 {
			b.logf("UnregisterBackingFd: %v", errno)
		}
		n.backingID = 0
		n.backingIDRefcount = 0
	} else if n.backingIDRefcount < 0 {
		log.Panic("backingIDRefcount underflow")
	}
}

// registerFile hands out a file handle. It takes b.fileMu and installs a
// non-nil b.files[fh] entry. The slot remains non-nil until release waiters,
// filesystem callbacks, and backing-ID cleanup finish; handle reuse happens
// only after freeFileHandle adds fh to b.freeFiles. Flags are the open flags
// (eg. syscall.O_EXCL).
func (b *rawBridge) registerFile(n *Inode, f FileHandle, flags uint32) *fileEntry {
	b.fileMu.Lock()
	defer b.fileMu.Unlock()
	fe := &fileEntry{}
	if len(b.freeFiles) > 0 {
		last := len(b.freeFiles) - 1
		fe.fh = b.freeFiles[last]
		b.freeFiles = b.freeFiles[:last]
		b.files[fe.fh] = fe
	} else {
		fe.fh = uint32(len(b.files))
		b.files = append(b.files, fe)
	}

	if _, ok := f.(FileReaddirenter); ok {
		fe.lastRead = make([]fuse.DirEntry, 0, 100)
	}
	fe.nodeIndex = len(n.openFiles)
	fe.file = f
	n.openFiles = append(n.openFiles, fe.fh)
	return fe
}

func (b *rawBridge) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	n, release := b.acquireNode(input.NodeId)
	defer release()
	fe := b.getFile(input.Fh)
	file := fe.file

	ctx := &fuse.Context{Caller: input.Caller, Cancel: cancel}
	if fops, ok := n.ops.(NodeReader); ok {
		res, errno := fops.Read(ctx, file, buf, int64(input.Offset))
		return res, errnoToStatus(errno)
	}
	if fr, ok := file.(FileReader); ok {
		res, errno := fr.Read(ctx, buf, int64(input.Offset))
		return res, errnoToStatus(errno)
	}

	return nil, fuse.ENOTSUP
}

func (b *rawBridge) GetLk(cancel <-chan struct{}, input *fuse.LkIn, out *fuse.LkOut) fuse.Status {
	n, release := b.acquireNode(input.NodeId)
	defer release()
	fe := b.getFile(input.Fh)
	file := fe.file

	ctx := &fuse.Context{Caller: input.Caller, Cancel: cancel}
	if lops, ok := n.ops.(NodeGetlker); ok {
		return errnoToStatus(lops.Getlk(ctx, file, input.Owner, &input.Lk, input.LkFlags, &out.Lk))
	}
	if gl, ok := file.(FileGetlker); ok {
		return errnoToStatus(gl.Getlk(ctx, input.Owner, &input.Lk, input.LkFlags, &out.Lk))
	}
	return fuse.ENOTSUP
}

func (b *rawBridge) SetLk(cancel <-chan struct{}, input *fuse.LkIn) fuse.Status {
	n, release := b.acquireNode(input.NodeId)
	defer release()
	fe := b.getFile(input.Fh)
	file := fe.file
	ctx := &fuse.Context{Caller: input.Caller, Cancel: cancel}
	if lops, ok := n.ops.(NodeSetlker); ok {
		return errnoToStatus(lops.Setlk(ctx, file, input.Owner, &input.Lk, input.LkFlags))
	}
	if sl, ok := file.(FileSetlker); ok {
		return errnoToStatus(sl.Setlk(ctx, input.Owner, &input.Lk, input.LkFlags))
	}
	return fuse.ENOTSUP
}
func (b *rawBridge) SetLkw(cancel <-chan struct{}, input *fuse.LkIn) fuse.Status {
	n, release := b.acquireNode(input.NodeId)
	defer release()
	fe := b.getFile(input.Fh)
	file := fe.file
	ctx := &fuse.Context{Caller: input.Caller, Cancel: cancel}
	if lops, ok := n.ops.(NodeSetlkwer); ok {
		return errnoToStatus(lops.Setlkw(ctx, file, input.Owner, &input.Lk, input.LkFlags))
	}
	if sl, ok := file.(FileSetlkwer); ok {
		return errnoToStatus(sl.Setlkw(ctx, input.Owner, &input.Lk, input.LkFlags))
	}
	return fuse.ENOTSUP
}

func (b *rawBridge) Release(cancel <-chan struct{}, input *fuse.ReleaseIn) {
	n, release := b.acquireNode(input.NodeId)
	if n == nil {
		return
	}
	defer release()

	n, f := b.releaseFileEntry(n, input.Fh)
	if f == nil {
		return
	}

	f.wg.Wait()

	file := f.file
	ctx := &fuse.Context{Caller: input.Caller, Cancel: cancel}
	if r, ok := n.ops.(NodeReleaser); ok {
		r.Release(ctx, file)
	} else if r, ok := file.(FileReleaser); ok {
		r.Release(ctx)
	}

	b.releaseBackingIDRef(n, f)
	b.freeFileHandle(input.Fh)
}

func (b *rawBridge) ReleaseDir(input *fuse.ReleaseIn) {
	n, release := b.acquireNode(input.NodeId)
	if n == nil {
		return
	}
	defer release()

	n, f := b.releaseFileEntry(n, input.Fh)
	if f == nil {
		return
	}
	f.wg.Wait()

	file := f.file
	if frd, ok := file.(FileReleasedirer); ok {
		frd.Releasedir(context.Background(), input.ReleaseFlags)
	}

	b.releaseBackingIDRef(n, f)
	b.freeFileHandle(input.Fh)
}

func (b *rawBridge) releaseFileEntry(n *Inode, fh uint64) (*Inode, *fileEntry) {
	if fh == 0 {
		return n, nil
	}
	b.fileMu.Lock()
	defer b.fileMu.Unlock()

	if fh >= uint64(len(b.files)) {
		return n, nil
	}
	entry := b.files[fh]
	if entry == nil || uint64(entry.fh) != fh || entry.nodeIndex < 0 {
		return n, nil
	}
	if len(n.openFiles) == 0 || entry.nodeIndex >= len(n.openFiles) || n.openFiles[entry.nodeIndex] != uint32(fh) {
		return n, nil
	}

	last := len(n.openFiles) - 1
	if last != entry.nodeIndex {
		n.openFiles[entry.nodeIndex] = n.openFiles[last]
		movedFH := n.openFiles[entry.nodeIndex]
		b.files[movedFH].nodeIndex = entry.nodeIndex
	}
	n.openFiles = n.openFiles[:last]
	entry.nodeIndex = -1
	return n, entry
}

func (b *rawBridge) freeFileHandle(fh uint64) {
	if fh == 0 {
		return
	}
	b.fileMu.Lock()
	b.freeFiles = append(b.freeFiles, uint32(fh))
	b.fileMu.Unlock()
}

func (b *rawBridge) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (written uint32, status fuse.Status) {
	n, release := b.acquireNode(input.NodeId)
	defer release()
	fe := b.getFile(input.Fh)
	file := fe.file

	ctx := &fuse.Context{Caller: input.Caller, Cancel: cancel}
	if wr, ok := n.ops.(NodeWriter); ok {
		w, errno := wr.Write(ctx, file, data, int64(input.Offset))
		return w, errnoToStatus(errno)
	}
	if fr, ok := file.(FileWriter); ok {
		w, errno := fr.Write(ctx, data, int64(input.Offset))
		return w, errnoToStatus(errno)
	}

	return 0, fuse.ENOTSUP
}

func (b *rawBridge) Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status {
	n, release := b.acquireNode(input.NodeId)
	defer release()
	fe := b.getFile(input.Fh)
	file := fe.file
	ctx := &fuse.Context{Caller: input.Caller, Cancel: cancel}
	if fl, ok := n.ops.(NodeFlusher); ok {
		return errnoToStatus(fl.Flush(ctx, file))
	}
	if fl, ok := file.(FileFlusher); ok {
		return errnoToStatus(fl.Flush(ctx))
	}
	return 0
}

func (b *rawBridge) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	n, release := b.acquireNode(input.NodeId)
	defer release()
	fe := b.getFile(input.Fh)
	file := fe.file
	ctx := &fuse.Context{Caller: input.Caller, Cancel: cancel}
	if fs, ok := n.ops.(NodeFsyncer); ok {
		return errnoToStatus(fs.Fsync(ctx, file, input.FsyncFlags))
	}
	if fs, ok := file.(FileFsyncer); ok {
		return errnoToStatus(fs.Fsync(ctx, input.FsyncFlags))
	}
	return fuse.ENOTSUP
}

func (b *rawBridge) Fallocate(cancel <-chan struct{}, input *fuse.FallocateIn) fuse.Status {
	n, release := b.acquireNode(input.NodeId)
	defer release()
	fe := b.getFile(input.Fh)
	file := fe.file
	ctx := &fuse.Context{Caller: input.Caller, Cancel: cancel}
	if a, ok := n.ops.(NodeAllocater); ok {
		return errnoToStatus(a.Allocate(ctx, file, input.Offset, input.Length, input.Mode))
	}
	if a, ok := file.(FileAllocater); ok {
		return errnoToStatus(a.Allocate(ctx, input.Offset, input.Length, input.Mode))
	}
	return fuse.ENOTSUP
}

func (b *rawBridge) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	n, release := b.acquireNode(input.NodeId)
	defer release()

	var fh FileHandle
	var fuseFlags uint32
	var errno syscall.Errno

	ctx := &fuse.Context{Caller: input.Caller, Cancel: cancel}

	nod, _ := n.ops.(NodeOpendirer)
	nrd, _ := n.ops.(NodeReaddirer)

	if odh, ok := n.ops.(NodeOpendirHandler); ok {
		fh, fuseFlags, errno = odh.OpendirHandle(ctx, input.Flags)

		if errno != 0 {
			return errnoToStatus(errno)
		}
	} else {
		if nod != nil {
			errno = nod.Opendir(ctx)
			if errno != 0 {
				return errnoToStatus(errno)
			}
		}

		var ctor func(context.Context) (DirStream, syscall.Errno)
		if nrd != nil {
			ctor = func(ctx context.Context) (DirStream, syscall.Errno) {
				return nrd.Readdir(ctx)
			}
		} else {
			ctor = func(ctx context.Context) (DirStream, syscall.Errno) {
				return n.childrenAsDirstream(), 0
			}
		}
		fh = &dirStreamAsFile{creator: ctor}
	}

	if fuseFlags&(fuse.FOPEN_CACHE_DIR|fuse.FOPEN_KEEP_CACHE) != 0 {
		fuseFlags |= fuse.FOPEN_CACHE_DIR | fuse.FOPEN_KEEP_CACHE
	}
	fe := b.registerFile(n, fh, 0)
	out.Fh = uint64(fe.fh)
	out.OpenFlags = fuseFlags
	return fuse.OK
}

func (n *Inode) childrenAsDirstream() DirStream {
	lst := n.childrenList()
	r := make([]fuse.DirEntry, 0, len(lst))
	for _, e := range lst {
		r = append(r, fuse.DirEntry{Mode: e.Inode.Mode(),
			Name: e.Name,
			Ino:  e.Inode.StableAttr().Ino})
	}
	return NewListDirStream(r)
}

func (b *rawBridge) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	return b.readDirMaybeLookup(cancel, input, out, true)
}

func (b *rawBridge) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	return b.readDirMaybeLookup(cancel, input, out, false)
}

func (b *rawBridge) readDirMaybeLookup(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList, lookup bool) fuse.Status {
	n, release := b.acquireNode(input.NodeId)
	defer release()
	fe := b.getFile(input.Fh)
	file := fe.file
	fileMu := &fe.mu

	direnter, ok := file.(FileReaddirenter)
	if !ok {
		return fuse.OK
	}
	getdent := direnter.Readdirent

	fileMu.Lock()
	defer fileMu.Unlock()

	ctx := &fuse.Context{Caller: input.Caller, Cancel: cancel}
	interruptedRead := false
	if input.Offset != fe.dirOffset {
		// If the last readdir(plus) was interrupted, the
		// kernel may consume just one entry from the readdir,
		// and redo it.
		for i, e := range fe.lastRead {
			if e.Off == input.Offset {
				interruptedRead = true
				todo := fe.lastRead[i+1:]
				todo = make([]fuse.DirEntry, len(todo))
				copy(todo, fe.lastRead[i+1:])
				getdent = func(context.Context) (*fuse.DirEntry, syscall.Errno) {
					if len(todo) > 0 {
						de := &todo[0]
						todo = todo[1:]
						return de, 0
					}
					return nil, 0
				}
				fe.dirOffset = input.Offset
				break
			}
		}
	}

	if input.Offset != fe.dirOffset {
		if sd, ok := file.(FileSeekdirer); ok {
			errno := sd.Seekdir(ctx, input.Offset)
			if errno != 0 {
				return errnoToStatus(errno)
			}
			fe.dirOffset = input.Offset
			fe.overflowErrno = 0
			fe.hasOverflow = false
		} else {
			return fuse.ENOTSUP
		}
	}

	defer func() {
		fe.dirOffset = out.Offset
	}()

	first := true
	fe.lastRead = fe.lastRead[:0]
	for {
		var de *fuse.DirEntry
		var errno syscall.Errno
		if fe.hasOverflow && !interruptedRead {
			fe.hasOverflow = false
			if fe.overflowErrno != 0 {
				return errnoToStatus(fe.overflowErrno)
			}
			de = &fe.overflow
		} else {
			de, errno = getdent(ctx)
			if errno != 0 {
				if first {
					return errnoToStatus(errno)
				} else {
					fe.hasOverflow = true
					fe.overflowErrno = errno
					return fuse.OK
				}
			}
		}

		if de == nil {
			break
		}

		first = false
		if de.Off == 0 {
			// This logic is dup from fuse.DirEntryList, but we need the offset here so it is part of lastRead
			de.Off = out.Offset + 1
		}
		if !lookup {
			if !out.AddDirEntry(*de) {
				fe.overflow = *de
				fe.hasOverflow = true
				return fuse.OK
			}

			fe.lastRead = append(fe.lastRead, *de)
			continue
		}

		entryOut := out.AddDirLookupEntry(*de)
		if entryOut == nil {
			fe.overflow = *de
			fe.hasOverflow = true
			return fuse.OK
		}
		fe.lastRead = append(fe.lastRead, *de)

		// Virtual entries "." and ".." should be part of the
		// directory listing, but not part of the filesystem tree.
		// The values in EntryOut are ignored by Linux
		// (see fuse_direntplus_link() in linux/fs/fuse/readdir.c), so leave
		// them at zero-value.
		if de.Name == "." || de.Name == ".." {
			continue
		}

		var child *Inode
		if fileLookupper, ok := file.(FileLookuper); ok {
			child, errno = fileLookupper.Lookup(ctx, de.Name, entryOut)
		} else {
			child, errno = b.lookup(ctx, n, de.Name, entryOut)
		}

		if errno != 0 {
			if b.options.NegativeTimeout != nil {
				entryOut.SetEntryTimeout(*b.options.NegativeTimeout)

				// TODO: maybe simply not produce the dirent here?
				// test?
			}
			// TODO: should break?
		} else {
			child, _ = b.addNewChild(n, de.Name, child, nil, 0, entryOut)
			child.setEntryOut(entryOut)
			b.setEntryOutTimeout(entryOut)
			if de.Mode&syscall.S_IFMT != child.stableAttr.Mode&syscall.S_IFMT {
				// The file type has changed behind our back. Use the new value.
				out.FixMode(child.stableAttr.Mode)
			}
			entryOut.Mode = child.stableAttr.Mode | (entryOut.Mode & 07777)
		}
	}

	return fuse.OK
}

func (b *rawBridge) FsyncDir(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	n, release := b.acquireNode(input.NodeId)
	defer release()
	fe := b.getFile(input.Fh)
	file := fe.file
	ctx := &fuse.Context{Caller: input.Caller, Cancel: cancel}
	if fsd, ok := file.(FileFsyncdirer); ok {
		return errnoToStatus(fsd.Fsyncdir(ctx, input.FsyncFlags))
	} else if fs, ok := n.ops.(NodeFsyncer); ok {
		return errnoToStatus(fs.Fsync(ctx, file, input.FsyncFlags))
	}

	return fuse.ENOTSUP
}

func (b *rawBridge) StatFs(cancel <-chan struct{}, input *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	n, release := b.acquireNode(input.NodeId)
	defer release()
	if sf, ok := n.ops.(NodeStatfser); ok {
		return errnoToStatus(sf.Statfs(&fuse.Context{Caller: input.Caller, Cancel: cancel}, out))
	}

	// leave zeroed out
	return fuse.OK
}

func (b *rawBridge) Init(s *fuse.Server) {
	b.server = s
	b.startNodeMapCompactor()
}

func (b *rawBridge) CopyFileRange(cancel <-chan struct{}, in *fuse.CopyFileRangeIn) (size uint32, status fuse.Status) {
	n1, release1 := b.acquireNode(in.NodeId)
	defer release1()
	cfr, ok := n1.ops.(NodeCopyFileRanger)
	if !ok {
		return 0, fuse.ENOTSUP
	}
	feIn := b.getFile(in.FhIn)
	fileIn := feIn.file

	n2, release2 := b.acquireNode(in.NodeIdOut)
	defer release2()
	feOut := b.getFile(in.FhOut)
	fileOut := feOut.file

	sz, errno := cfr.CopyFileRange(&fuse.Context{Caller: in.Caller, Cancel: cancel},
		fileIn, in.OffIn, n2, fileOut, in.OffOut, in.Len, in.Flags)
	return sz, errnoToStatus(errno)
}

func (b *rawBridge) Ioctl(cancel <-chan struct{}, in *fuse.IoctlIn, inbuf []byte, out *fuse.IoctlOut, outbuf []byte) (code fuse.Status) {
	n, release := b.acquireNode(in.NodeId)
	defer release()
	fe := b.getFile(in.Fh)
	file := fe.file
	if nio, ok := n.ops.(NodeIoctler); ok {
		ctx := &fuse.Context{Caller: in.Caller, Cancel: cancel}
		result, errno := nio.Ioctl(ctx, file, in.Cmd, in.Arg, inbuf, outbuf)
		out.Result = result
		return errnoToStatus(errno)
	}
	if fio, ok := file.(FileIoctler); ok {
		ctx := &fuse.Context{Caller: in.Caller, Cancel: cancel}
		result, errno := fio.Ioctl(ctx, in.Cmd, in.Arg, inbuf, outbuf)
		out.Result = result
		return errnoToStatus(errno)
	}
	return fuse.Status(syscall.ENOTTY)
}

func (b *rawBridge) Lseek(cancel <-chan struct{}, in *fuse.LseekIn, out *fuse.LseekOut) fuse.Status {
	n, release := b.acquireNode(in.NodeId)
	defer release()

	ctx := &fuse.Context{Caller: in.Caller, Cancel: cancel}

	ls, ok := n.ops.(NodeLseeker)
	fe := b.getFile(in.Fh)
	file := fe.file
	if ok {
		off, errno := ls.Lseek(ctx,
			file, in.Offset, in.Whence)
		out.Offset = off
		return errnoToStatus(errno)
	}
	if fs, ok := file.(FileLseeker); ok {
		off, errno := fs.Lseek(ctx, in.Offset, in.Whence)
		out.Offset = off
		return errnoToStatus(errno)
	}
	var attr fuse.AttrOut
	if s := b.getattr(ctx, n, nil, &attr); s != 0 {
		return errnoToStatus(s)
	}
	if in.Whence == _SEEK_DATA {
		if in.Offset >= attr.Size {
			return errnoToStatus(syscall.ENXIO)
		}
		out.Offset = in.Offset
		return fuse.OK
	}

	if in.Whence == _SEEK_HOLE {
		if in.Offset > attr.Size {
			return errnoToStatus(syscall.ENXIO)
		}
		out.Offset = attr.Size
		return fuse.OK
	}

	return fuse.ENOTSUP
}

func (b *rawBridge) OnUnmount() {
	if of, ok := b.root.ops.(NodeOnForgetter); ok {
		of.OnForget()
	}
	b.stopNodeMapCompactor()
}
