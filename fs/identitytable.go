// Copyright 2026 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fs

import (
	"log"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// nodeEntry is the per-nodeid state the kernel-facing tables need
// beyond the *Inode itself: which file handles are open on it, and (if
// passthrough is in use) its backing-fd state.
// Identity state is guarded by the shard that holds the nodeid.
//
// A nodeEntry lives for exactly as long as its nodeid is registered:
// mapIdentityTable.forget deletes it in the same step that drops the
// nodeid. There is nothing extra to clean up at that point because the
// FUSE protocol guarantees openFiles is already empty and the backing
// fields are already zero - a node with open handles or an active
// backing-fd registration keeps its lookup count (and therefore its
// entry) alive.
type nodeEntry struct {
	// inode is the *Inode currently registered under this nodeid.
	inode atomic.Pointer[Inode]

	// lookupCount is the kernel's outstanding lookup count for this
	// nodeid, guarded by its identity shard's mutex.
	lookupCount uint64

	// File handles open on this node, protected by mapIdentityTable.filesMu.
	openFiles []uint32
	// openCount mirrors len(openFiles); updates hold mapIdentityTable.filesMu.
	openCount atomic.Int32

	// backingMu protects the backing-fd state and each open file's
	// backingIDRef. It is never held with an identity shard or filesMu.
	// Inode locks precede backingMu; registration and unregistration run
	// under it without blocking backing operations on other nodes.
	backingMu         sync.Mutex
	backingID         int32
	backingIDRefcount int
}

const (
	identityShardBits = 7
	identityShards    = 1 << identityShardBits
)

type identityShardFields struct {
	mu              sync.Mutex
	stableAttrs     map[StableAttr]*Inode
	nodes           map[uint64]*nodeEntry
	stableAttrsHigh int
	nodesHigh       int
}

// identityShard pads its fields to a multiple of 128 bytes, so that
// two shard locks never share a cache line.
type identityShard struct {
	identityShardFields
	_ [(128 - unsafe.Sizeof(identityShardFields{})%128) % 128]byte
}

// mapIdentityTable owns every kernel-facing number: nodeids and file
// handles, and the mappings back to the entries they identify. It is
// held by value as rawBridge.ids (see bridge.go).
//
// Methods lock internally and return with no locks held. Identity maps
// use 128 shards. In default mode, nodeids use the lower half and stable
// keys the upper half. Under ExternalNodeID, both keys use the same shard
// except for the root. Each map has its own high-water mark per shard.
//
// Lock order is fileEntry.mu, then Inode.mu, then one of identity shards,
// filesMu, or nodeEntry.backingMu. These three inner groups never nest.
// Two shards are held only through lockPair, in ascending index order.
// No Node or File method runs under a table lock.
type mapIdentityTable struct {
	shards [identityShards]identityShard

	// externalNodeID mirrors Options.ExternalNodeID: under it, nodeid
	// == Ino
	externalNodeID bool

	// Atomic counters preserve the allocation sequence without a shard lock.
	automaticIno atomic.Uint64
	nextNodeId   atomic.Uint64
	// freePending is set by the shard rebuild that arms the delayed
	// FreeOSMemory, and cleared by that call.
	freePending atomic.Bool

	// filesMu protects files, freeFiles, nodeEntry.openFiles, and fileEntry.nodeIndex.
	filesMu sync.Mutex
	// files[fh] is nil if fh is free. files[0] is the immutable noFile placeholder.
	files []*fileEntry
	// indices of files that are not allocated.
	freeFiles []uint32
	// noFile is set once during initialization and read without filesMu.
	noFile *fileEntry
}

var (
	freeOSMemory      = debug.FreeOSMemory // test seam
	identityAfterFunc = time.AfterFunc     // test seam
)

const freeOSMemoryDelay = 10 * time.Second

// initIdentityTable prepares a zero-value mapIdentityTable for use.
// The pointer receiver ensures the embedding rawBridge - and its
// mutexes - is never copied.
func (t *mapIdentityTable) initIdentityTable(firstAutomaticIno uint64, externalNodeID bool) {
	if firstAutomaticIno == 0 {
		firstAutomaticIno = 1 << 63
	}
	t.automaticIno.Store(firstAutomaticIno)
	t.externalNodeID = externalNodeID
	for i := range t.shards {
		t.shards[i].stableAttrs = make(map[StableAttr]*Inode)
		t.shards[i].nodes = make(map[uint64]*nodeEntry)
	}
	// Fh 0 means no file handle.
	t.files = []*fileEntry{{}}
	t.noFile = t.files[0]
}

func shardIndex(x uint64, bits uint) int {
	return int((x * 0x9E3779B97F4A7C15) >> (64 - bits))
}

func (t *mapIdentityTable) stableShard(ino uint64) int {
	if t.externalNodeID {
		return shardIndex(ino, identityShardBits)
	}
	return identityShards/2 + shardIndex(ino, identityShardBits-1)
}

func (t *mapIdentityTable) nodeShard(nodeID uint64) int {
	if t.externalNodeID {
		return shardIndex(nodeID, identityShardBits)
	}
	return shardIndex(nodeID, identityShardBits-1)
}

func (t *mapIdentityTable) lockPair(ino, nodeID uint64) (a, n *identityShard) {
	i, j := t.stableShard(ino), t.nodeShard(nodeID)
	a, n = &t.shards[i], &t.shards[j]
	switch {
	case i == j:
		a.mu.Lock()
	case i < j:
		a.mu.Lock()
		n.mu.Lock()
	default:
		n.mu.Lock()
		a.mu.Lock()
	}
	return a, n
}

func unlockPair(a, n *identityShard) {
	if a != n {
		n.mu.Unlock()
	}
	a.mu.Unlock()
}

// dedupKey returns the key under which id is looked up/stored in
// stableAttrs.
func (t *mapIdentityTable) dedupKey(id StableAttr) StableAttr {
	if t.externalNodeID {
		return StableAttr{Ino: id.Ino}
	}
	return id
}

func (t *mapIdentityTable) registerRoot(root *Inode) {
	s := &t.shards[t.nodeShard(1)]
	s.mu.Lock()
	defer s.mu.Unlock()
	e := &nodeEntry{lookupCount: 1}
	e.inode.Store(root)
	s.nodes[1] = e
	t.nextNodeId.Store(2) // the root node has nodeid 1
}

func (t *mapIdentityTable) nextAutomaticIno() uint64 {
	return t.automaticIno.Add(1) - 1
}

func (t *mapIdentityTable) allocateNodeID() uint64 {
	return t.nextNodeId.Add(1) - 1
}

func (t *mapIdentityTable) findByAttr(id StableAttr) *Inode {
	key := t.dedupKey(id)
	s := &t.shards[t.stableShard(key.Ino)]
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stableAttrs[key]
}

// registerNew tries to register `child` under `id`. The following results are possible:
// * winner == child: normal case
// * winner == other node: racy lookup, retry call with the winner; no entry returned
//
// If `evicted` is returned, its bookkeeping must be updated must be under lock.
func (t *mapIdentityTable) registerNew(id StableAttr, child *Inode, exclusive bool) (winner *Inode, e *nodeEntry, evicted *Inode) {
	key := t.dedupKey(id)
	a, n := t.lockPair(key.Ino, child.nodeId)
	defer unlockPair(a, n)

	prev := a.stableAttrs[key]
	if !exclusive {
		if prev != nil && prev != child && prev.stableAttr == id {
			// Exact same identity found via a racing lookup (eg. two
			// dirs linking to the same file, both losing to the same
			// winner). We don't hold its lock, so we can't change it.
			// The caller discards e here; prev's nodeid may be in
			// a shard whose lock we do not hold.
			return prev, nil, nil
		}
	}

	e = n.nodes[child.nodeId]
	if e == nil {
		// Fresh node: create its entry. If child was already
		// registered (the old == child case above), its entry - and
		// any accumulated openFiles/backing state - is left alone.
		e = &nodeEntry{}
		e.inode.Store(child)
		n.nodes[child.nodeId] = e
		n.nodesHigh = max(n.nodesHigh, len(n.nodes))
	} else if old := e.inode.Load(); old != child {
		// we must have a different StableAttr now.
		evicted = old
		e.inode.Store(child)
	}
	// Any node that might be there is overwritten - it is obsolete now.
	if prev != child {
		a.stableAttrs[key] = child
		a.stableAttrsHigh = max(a.stableAttrsHigh, len(a.stableAttrs))
	}

	// child is kept as the definitive occupant of id/child.nodeId from
	// here on, and the caller holds child.mu for the duration of this
	// call - so crediting it now, atomically with the publish above, is
	// both safe and required (see the doc comment above).
	child.hasKernelRef = true
	child.changeCounter++
	e.lookupCount++

	return child, e, evicted
}

// decLookup applies a FORGET to nodeID, and reports whether that node ID
// lookup count reached zero.
//
// The caller must hold the Inode's mu: lookupCount and the Inode's
// hasKernelRef have to be updated together (registerNew credits them
// under that lock too), or a LOOKUP racing with a FORGET can leave the
// kernel holding a nodeid we already dropped.
func (t *mapIdentityTable) decLookup(nodeID uint64, nlookup uint64) (n *Inode, reachedZero bool) {
	s := &t.shards[t.nodeShard(nodeID)]
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.nodes[nodeID]
	if e == nil {
		log.Panicf("decLookup: unknown node %d", nodeID)
	}
	if nlookup > e.lookupCount {
		log.Panicf("n%d lookupCount underflow: lookupCount=%d, decrement=%d", nodeID, e.lookupCount, nlookup)
	}
	e.lookupCount -= nlookup
	return e.inode.Load(), e.lookupCount == 0
}

// node resolves a nodeid as sent by the kernel.
func (t *mapIdentityTable) node(nodeID uint64) *nodeEntry {
	s := &t.shards[t.nodeShard(nodeID)]
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nodes[nodeID]
}

// file returns the entry for fh, or the immutable placeholder for fh 0.
func (t *mapIdentityTable) file(fh uint64) *fileEntry {
	if fh == 0 {
		return t.noFile
	}
	t.filesMu.Lock()
	defer t.filesMu.Unlock()
	return t.files[fh]
}

// forget deletes only keys still owned by n; nodeid deletion is independent
// of stable-key ownership. Each map rebuilds after a 100x shrink to return
// retained map storage (https://github.com/golang/go/issues/20135).
func (t *mapIdentityTable) forget(n *Inode) {
	key := t.dedupKey(n.stableAttr)
	a, s := t.lockPair(key.Ino, n.nodeId)
	defer unlockPair(a, s)
	if a.stableAttrs[key] == n {
		delete(a.stableAttrs, key)
		if a.stableAttrsHigh > max(len(a.stableAttrs), 1)*100 {
			a.stableAttrs = rebuildMap(a.stableAttrs)
			a.stableAttrsHigh = len(a.stableAttrs)
			t.noteRebuild()
		}
	}
	// n occupies its nodeid without a kernel reference, so the
	// nodeid's lookup count is zero.
	if e, ok := s.nodes[n.nodeId]; ok && e.inode.Load() == n {
		delete(s.nodes, n.nodeId)
		if s.nodesHigh > max(len(s.nodes), 1)*100 {
			s.nodes = rebuildMap(s.nodes)
			s.nodesHigh = len(s.nodes)
			t.noteRebuild()
		}
	}
}

func rebuildMap[K comparable, V any](m map[K]V) map[K]V {
	r := make(map[K]V, len(m))
	for k, v := range m {
		r[k] = v
	}
	return r
}

// noteRebuild arms one delayed FreeOSMemory for a burst of shard
// rebuilds. It runs under shard locks; the timer callback runs
// on its own goroutine and takes no lock.
func (t *mapIdentityTable) noteRebuild() {
	if !t.freePending.CompareAndSwap(false, true) {
		return
	}
	// Capture the seam so a pending timer never calls a later test's fake.
	free := freeOSMemory
	identityAfterFunc(freeOSMemoryDelay, func() {
		t.freePending.Store(false) // a later rebuild can arm a new timer
		free()
	})
}

// registerFile hands out a fresh file handle number for f, open on the
// node behind e. flags are the open flags (eg. syscall.O_EXCL).
func (t *mapIdentityTable) registerFile(e *nodeEntry, f FileHandle, flags uint32) *fileEntry {
	t.filesMu.Lock()
	defer t.filesMu.Unlock()

	fe := &fileEntry{}
	if len(t.freeFiles) > 0 {
		last := len(t.freeFiles) - 1
		fe.fh = t.freeFiles[last]
		t.freeFiles = t.freeFiles[:last]
		t.files[fe.fh] = fe
	} else {
		fe.fh = uint32(len(t.files))
		t.files = append(t.files, fe)
	}

	if _, ok := f.(FileReaddirenter); ok {
		fe.lastRead = dirEntryPool.Get().([]fuse.DirEntry)
	}
	fe.nodeIndex = len(e.openFiles)
	fe.file = f
	fe.inode = e.inode.Load()
	e.openFiles = append(e.openFiles, fe.fh)
	e.openCount.Add(1)

	return fe
}

// firstOpenFile returns any currently-open FileHandle recorded on e,
// plus its entry. If non-nil, the entry needs wg.Done after use.
// Used to fake a file handle for kernel requests that don't supply one
// (eg. GETATTR without an explicit fh).
func (t *mapIdentityTable) firstOpenFile(e *nodeEntry) (FileHandle, *fileEntry) {
	if e.openCount.Load() == 0 {
		return nil, nil
	}
	t.filesMu.Lock()
	defer t.filesMu.Unlock()
	for _, fh := range e.openFiles {
		fe := t.files[fh]
		fe.wg.Add(1)
		return fe.file, fe
	}
	return nil, nil
}

// detachFile unlinks the file handle fh from its owning node's entry
// and returns the fileEntry. The fh number itself does not become reusable
// until recycleFile is called - the caller may still need to run release
// callbacks against the fileEntry first.
func (t *mapIdentityTable) detachFile(e *nodeEntry, fh uint64) *fileEntry {
	t.filesMu.Lock()
	defer t.filesMu.Unlock()
	var entry *fileEntry
	if fh > 0 {
		last := len(e.openFiles) - 1
		entry = t.files[fh]
		if last != entry.nodeIndex {
			e.openFiles[entry.nodeIndex] = e.openFiles[last]
			t.files[e.openFiles[entry.nodeIndex]].nodeIndex = entry.nodeIndex
		}
		e.openFiles = e.openFiles[:last]
		e.openCount.Add(-1)
	}
	return entry
}

// recycleFile marks fh as free for reuse by a future registerFile call.
func (t *mapIdentityTable) recycleFile(fh uint32) {
	t.filesMu.Lock()
	defer t.filesMu.Unlock()
	t.files[fh] = nil
	t.freeFiles = append(t.freeFiles, fh)
}
