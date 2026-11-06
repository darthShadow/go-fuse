// Copyright 2026 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fs

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// identityNodeCount returns the number of registered nodeids.
func identityNodeCount(t *mapIdentityTable) int {
	n := 0
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.Lock()
		n += len(s.nodes)
		s.mu.Unlock()
	}
	return n
}

func identityStableAttrCount(t *mapIdentityTable) int {
	n := 0
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.Lock()
		n += len(s.stableAttrs)
		s.mu.Unlock()
	}
	return n
}

func identityLookupCount(t *mapIdentityTable, nodeID uint64) (uint64, bool) {
	s := &t.shards[t.nodeShard(nodeID)]
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.nodes[nodeID]
	if e == nil {
		return 0, false
	}
	return e.lookupCount, true
}

func waitOrFail(t *testing.T, wg *sync.WaitGroup, d time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("workers did not finish within %v (deadlock?)", d)
	}
}

// Under ExternalNodeID, forget of an inode that no longer occupies its
// nodeid deletes nothing: the replacement keeps both keys.
func TestIdentityTableExternalForgetOfStaleInodeKeepsReplacement(t *testing.T) {
	root := &Inode{}
	b := NewNodeFS(root, &Options{ExternalNodeID: true}).(*rawBridge)
	ctx := context.Background()
	var out fuse.EntryOut

	a := root.NewInode(ctx, &Inode{}, StableAttr{Mode: syscall.S_IFREG, Ino: 100, Gen: 1})
	b.addNewNode(nil, "", a, nil, 0, &out)
	b.Forget(100, 1) // a reaches zero and is forgotten
	bb := root.NewInode(ctx, &Inode{}, StableAttr{Mode: syscall.S_IFREG, Ino: 100, Gen: 2})
	b.addNewNode(nil, "", bb, nil, 0, &out)

	a.mu.Lock()
	b.ids.forget(a) // as removeRefInner does for an inode without a kernel ref
	a.mu.Unlock()

	e := b.ids.node(100)
	if e == nil || e.inode.Load() != bb {
		t.Fatalf("nodes[100] = %v, want entry for replacement", e)
	}
	if got := b.ids.findByAttr(bb.stableAttr); got != bb {
		t.Fatalf("stableAttrs[{100}] = %p, want replacement %p", got, bb)
	}
}

// Concurrent generation replacement, resolution and FORGET under
// ExternalNodeID. At quiescence both keys of each Ino point to the
// same inode or are both absent, and every remaining count is zero.
func TestIdentityTableExternalGenerationChurn(t *testing.T) {
	root := &Inode{}
	b := NewNodeFS(root, &Options{ExternalNodeID: true}).(*rawBridge)
	ctx := context.Background()
	const inos, workers, iters = 4, 8, 2000

	// The checker reads both keys of each Ino under the one shard lock
	// they share and requires them to agree at every instant.
	stop := make(chan struct{})
	checked := make(chan struct{})
	go func() {
		defer close(checked)
		for {
			select {
			case <-stop:
				return
			default:
			}
			for ino := uint64(1000); ino < 1000+inos; ino++ {
				s := &b.ids.shards[b.ids.nodeShard(ino)]
				s.mu.Lock()
				var occ *Inode
				if e := s.nodes[ino]; e != nil {
					occ = e.inode.Load()
				}
				sa := s.stableAttrs[StableAttr{Ino: ino}]
				s.mu.Unlock()
				if occ != sa {
					t.Errorf("ino %d: nodes occupant %p, stableAttrs %p", ino, occ, sa)
					return
				}
			}
		}
	}()

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(seed, 1))
			for i := 0; i < iters; i++ {
				id := StableAttr{Mode: syscall.S_IFREG, Ino: 1000 + r.Uint64N(inos), Gen: 1 + r.Uint64N(3)}
				c := root.NewInode(ctx, &Inode{}, id)
				var out fuse.EntryOut
				b.addNewNode(nil, "", c, nil, 0, &out)
				if e := b.ids.node(out.NodeId); e == nil || e.inode.Load() == nil {
					t.Errorf("node(%d) after registration = nil", out.NodeId)
					return
				}
				b.Forget(out.NodeId, 1)
			}
		}(uint64(w))
	}
	waitOrFail(t, &wg, 60*time.Second)
	close(stop)
	<-checked

	for ino := uint64(1000); ino < 1000+inos; ino++ {
		e := b.ids.node(ino)
		sa := b.ids.findByAttr(StableAttr{Ino: ino})
		var occ *Inode
		if e != nil {
			occ = e.inode.Load()
		}
		if occ != sa {
			t.Errorf("ino %d: nodes occupant %p, stableAttrs %p", ino, occ, sa)
		}
		if c, ok := identityLookupCount(&b.ids, ino); ok && c != 0 {
			t.Errorf("ino %d: lookupCount %d after all FORGETs", ino, c)
		}
	}
}

// Default mode: hard-link candidates, LOOKUP racing final FORGET, many
// shards. Every credit is forgotten, so the table drains to the root.
func TestIdentityTableDefaultChurnDrains(t *testing.T) {
	root := &Inode{}
	b := NewNodeFS(root, &Options{}).(*rawBridge)
	ctx := context.Background()
	const inos, workers, iters = 256, 16, 4000

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(seed, 2))
			for i := 0; i < iters; i++ {
				id := StableAttr{Mode: syscall.S_IFREG, Ino: 5000 + r.Uint64N(inos), Gen: 1}
				c := root.NewInode(ctx, &Inode{}, id)
				var out fuse.EntryOut
				b.addNewNode(nil, "", c, nil, 0, &out)
				b.Forget(out.NodeId, 1)
			}
		}(uint64(w))
	}
	waitOrFail(t, &wg, 60*time.Second)

	if got := identityNodeCount(&b.ids); got != 1 {
		t.Errorf("nodes = %d after all FORGETs, want 1 (root)", got)
	}
	if got := identityStableAttrCount(&b.ids); got != 0 {
		t.Errorf("stableAttrs = %d after all FORGETs, want 0", got)
	}
}

func TestIdentityTableShardRebuildAndFree(t *testing.T) {
	var b *rawBridge
	var frees int
	var heldDuringFree []int
	var pendingDuringFree []bool
	var delays []time.Duration
	var callbacks []func()
	oldFree, oldAfter := freeOSMemory, identityAfterFunc
	freeOSMemory = func() {
		frees++
		pendingDuringFree = append(pendingDuringFree, b.ids.freePending.Load())
		for i := range b.ids.shards {
			if !b.ids.shards[i].mu.TryLock() {
				heldDuringFree = append(heldDuringFree, i)
				continue
			}
			b.ids.shards[i].mu.Unlock()
		}
	}
	identityAfterFunc = func(d time.Duration, f func()) *time.Timer {
		delays = append(delays, d)
		callbacks = append(callbacks, f)
		return nil
	}
	t.Cleanup(func() {
		freeOSMemory, identityAfterFunc = oldFree, oldAfter
	})
	b = NewNodeFS(&Inode{}, &Options{}).(*rawBridge)
	ctx := context.Background()
	fill := func(n int, base uint64) []*Inode {
		var r []*Inode
		for i := 0; i < n; i++ {
			c := b.root.NewInode(ctx, &Inode{}, StableAttr{Mode: syscall.S_IFREG, Ino: base + uint64(i)})
			var out fuse.EntryOut
			b.addNewNode(nil, "", c, nil, 0, &out)
			r = append(r, c)
		}
		return r
	}
	nodes := fill(20000, 10000)
	keepEntries := map[*Inode]*nodeEntry{}
	for _, n := range nodes[:3] {
		keepEntries[n] = b.ids.node(n.nodeId)
	}
	for _, n := range nodes[3:] {
		b.Forget(n.nodeId, 1)
	}
	for i := range b.ids.shards {
		s := &b.ids.shards[i]
		if s.nodesHigh > max(len(s.nodes), 1)*100 || s.stableAttrsHigh > max(len(s.stableAttrs), 1)*100 {
			t.Errorf("shard %d not rebuilt: nodes %d/%d stableAttrs %d/%d", i, len(s.nodes), s.nodesHigh, len(s.stableAttrs), s.stableAttrsHigh)
		}
	}
	for n, e := range keepEntries {
		if got := b.ids.node(n.nodeId); got != e {
			t.Errorf("survivor n%d entry changed by rebuild", n.nodeId)
		}
		if c, _ := identityLookupCount(&b.ids, n.nodeId); c != 1 {
			t.Errorf("survivor n%d lookupCount = %d, want 1", n.nodeId, c)
		}
	}
	if frees != 0 {
		t.Errorf("FreeOSMemory calls = %d before the timer fired, want 0", frees)
	}
	if len(heldDuringFree) > 0 {
		t.Errorf("shards %v locked during FreeOSMemory", heldDuringFree)
	}
	if len(callbacks) != 1 {
		t.Fatalf("identityAfterFunc calls = %d after one shrink, want 1", len(callbacks))
	}
	if delays[0] != 10*time.Second {
		t.Errorf("FreeOSMemory delay = %v, want 10s", delays[0])
	}
	if !b.ids.freePending.Load() {
		t.Errorf("freePending = false after a rebuild, want true")
	}

	for _, n := range fill(20000, 100000) {
		b.Forget(n.nodeId, 1)
	}
	if len(callbacks) != 1 {
		t.Errorf("identityAfterFunc calls = %d after a second shrink with a free pending, want 1", len(callbacks))
	}
	if frees != 0 {
		t.Errorf("FreeOSMemory calls = %d before the timer fired, want 0", frees)
	}

	callbacks[0]()
	if frees != 1 {
		t.Errorf("FreeOSMemory calls = %d after the timer fired, want 1", frees)
	}
	if len(heldDuringFree) > 0 {
		t.Errorf("shards %v locked during FreeOSMemory", heldDuringFree)
	}
	for _, pending := range pendingDuringFree {
		if pending {
			t.Errorf("freePending = true during FreeOSMemory, want false")
		}
	}
	if b.ids.freePending.Load() {
		t.Errorf("freePending = true after the timer fired, want false")
	}

	for _, n := range fill(20000, 200000) {
		b.Forget(n.nodeId, 1)
	}
	if len(callbacks) != 2 {
		t.Fatalf("identityAfterFunc calls = %d after a shrink that followed the free, want 2", len(callbacks))
	}
	callbacks[1]()
	if frees != 2 {
		t.Errorf("FreeOSMemory calls = %d after the second timer fired, want 2", frees)
	}
}

func TestIdentityTableDecLookupPanicReleasesShard(t *testing.T) {
	b := NewNodeFS(&Inode{}, &Options{}).(*rawBridge)
	c := b.root.NewInode(context.Background(), &Inode{}, StableAttr{Mode: syscall.S_IFREG, Ino: 7503})
	var out fuse.EntryOut
	b.addNewNode(nil, "", c, nil, 0, &out)

	for _, tc := range []struct {
		nodeID, nlookup uint64
		want            string
	}{
		{c.nodeId, 2, "lookupCount underflow"},
		{1 << 40, 1, "decLookup: unknown node"},
	} {
		var got any
		func() {
			defer func() { got = recover() }()
			b.ids.decLookup(tc.nodeID, tc.nlookup)
		}()
		if got == nil || !strings.Contains(fmt.Sprint(got), tc.want) {
			t.Errorf("decLookup(%d, %d) panic = %v, want %q", tc.nodeID, tc.nlookup, got, tc.want)
		}
		s := &b.ids.shards[b.ids.nodeShard(tc.nodeID)]
		if !s.mu.TryLock() {
			t.Fatalf("shard of %d locked after recovered panic", tc.nodeID)
		}
		s.mu.Unlock()
	}
	if cnt, _ := identityLookupCount(&b.ids, c.nodeId); cnt != 1 {
		t.Errorf("lookupCount = %d after failed decrement, want 1", cnt)
	}
}

func TestIdentityTableCounters(t *testing.T) {
	b := NewNodeFS(&Inode{}, &Options{FirstAutomaticIno: 10}).(*rawBridge)
	if got := b.ids.nextAutomaticIno(); got != 10 {
		t.Errorf("first automatic ino = %d, want 10", got)
	}
	if got := b.ids.allocateNodeID(); got != 2 {
		t.Errorf("first nodeid = %d, want 2", got)
	}
	const workers, per = 32, 200
	got := make([][]uint64, workers)
	var wg sync.WaitGroup
	for w := range got {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				got[w] = append(got[w], b.ids.allocateNodeID())
			}
		}(w)
	}
	wg.Wait()
	seen := map[uint64]bool{}
	for _, ids := range got {
		for _, id := range ids {
			if seen[id] || id < 3 || id >= 3+workers*per {
				t.Fatalf("nodeid %d duplicated or outside [3,%d)", id, 3+workers*per)
			}
			seen[id] = true
		}
	}
	d := NewNodeFS(&Inode{}, &Options{}).(*rawBridge)
	if got := d.ids.nextAutomaticIno(); got != 1<<63 {
		t.Errorf("default first automatic ino = %d, want 1<<63", got)
	}
}

func TestIdentityShardRouting(t *testing.T) {
	ext := NewNodeFS(&Inode{}, &Options{ExternalNodeID: true}).(*rawBridge)
	def := NewNodeFS(&Inode{}, &Options{}).(*rawBridge)
	half := identityShards / 2
	countsS := make([]int, identityShards)
	countsN := make([]int, identityShards)
	const n = 64 * 200
	for i := uint64(0); i < n; i++ {
		ino := 2 + i*64 // strided inode numbers
		if s, m := ext.ids.stableShard(ino), ext.ids.nodeShard(ino); s != m {
			t.Fatalf("ExternalNodeID: ino %d stable shard %d != nodeid shard %d", ino, s, m)
		}
		s, m := def.ids.stableShard(ino), def.ids.nodeShard(2+i)
		if s < half || m >= half {
			t.Fatalf("default: stable shard %d or nodeid shard %d in the wrong half", s, m)
		}
		countsS[s]++
		countsN[m]++
	}
	for i := 0; i < half; i++ {
		for _, c := range []int{countsN[i], countsS[half+i]} {
			if c == 0 || c > 2*n/half {
				t.Errorf("shard %d holds %d of %d keys; want 1..%d", i, c, n, 2*n/half)
			}
		}
	}
}

func TestIdentityShardSize(t *testing.T) {
	if got := unsafe.Sizeof(identityShard{}); got%128 != 0 {
		t.Errorf("identityShard size = %d, want a multiple of 128", got)
	}
}

// A shard rebuild inside forget races registration and resolution on
// the same shard. Every round of the churner crosses the 100x
// threshold of the target shard. Meanwhile registrars register fresh
// inodes on that shard and resolve them and the survivors. A completed
// registration must stay resolvable until its own FORGET, and survivors
// keep their entry and count.
func TestIdentityTableShardRebuildRacesRegistration(t *testing.T) {
	oldAfter := identityAfterFunc
	identityAfterFunc = func(time.Duration, func()) *time.Timer { return nil }
	t.Cleanup(func() { identityAfterFunc = oldAfter })

	for _, ext := range []bool{false, true} {
		t.Run(fmt.Sprintf("ExternalNodeID=%v", ext), func(t *testing.T) {
			b := NewNodeFS(&Inode{}, &Options{ExternalNodeID: ext}).(*rawBridge)
			ctx := context.Background()
			const survivors, registrars, perRegistrar, churn, rounds = 4, 4, 8, 1500, 30

			// Inos whose stable key (and, under ExternalNodeID, nodeid)
			// routes to one shard.
			target := b.ids.stableShard(2)
			var inos []uint64
			for ino := uint64(2); len(inos) < survivors+registrars*perRegistrar+churn; ino++ {
				if b.ids.stableShard(ino) == target {
					inos = append(inos, ino)
				}
			}
			register := func(ino uint64) *Inode {
				c := b.root.NewInode(ctx, &Inode{}, StableAttr{Mode: syscall.S_IFREG, Ino: ino})
				var out fuse.EntryOut
				b.addNewNode(nil, "", c, nil, 0, &out)
				return c
			}

			kept := make([]*Inode, survivors)
			keptEntries := make([]*nodeEntry, survivors)
			for i := range kept {
				kept[i] = register(inos[i])
				keptEntries[i] = b.ids.node(kept[i].nodeId)
			}
			fresh := inos[survivors : survivors+registrars*perRegistrar]
			churnInos := inos[survivors+registrars*perRegistrar:]

			done := make(chan struct{})
			var wg sync.WaitGroup
			for w := 0; w < registrars; w++ {
				wg.Add(1)
				go func(mine []uint64) {
					defer wg.Done()
					for i := 0; ; i++ {
						select {
						case <-done:
							return
						default:
						}
						c := register(mine[i%len(mine)])
						if e := b.ids.node(c.nodeId); e == nil || e.inode.Load() != c {
							t.Errorf("registered n%d not resolvable: entry %v", c.nodeId, e)
							return
						}
						if got := b.ids.findByAttr(c.stableAttr); got != c {
							t.Errorf("stableAttrs[%v] = %p after registration, want %p", c.stableAttr, got, c)
							return
						}
						for k, n := range kept {
							if e := b.ids.node(n.nodeId); e != keptEntries[k] || e.inode.Load() != n {
								t.Errorf("survivor n%d entry changed during rebuild", n.nodeId)
								return
							}
						}
						b.Forget(c.nodeId, 1)
					}
				}(fresh[w*perRegistrar : (w+1)*perRegistrar])
			}

			s := &b.ids.shards[target]
			for r := 0; r < rounds; r++ {
				nodes := make([]*Inode, len(churnInos))
				for i, ino := range churnInos {
					nodes[i] = register(ino)
				}
				for _, n := range nodes {
					b.Forget(n.nodeId, 1)
				}
				s.mu.Lock()
				rebuilt := s.stableAttrsHigh < churn && (!ext || s.nodesHigh < churn)
				s.mu.Unlock()
				if !rebuilt {
					t.Fatalf("round %d: shard %d not rebuilt; the test does not exercise the rebuild", r, target)
				}
			}
			close(done)
			waitOrFail(t, &wg, 60*time.Second)

			for k, n := range kept {
				if e := b.ids.node(n.nodeId); e != keptEntries[k] {
					t.Errorf("survivor n%d entry changed by rebuild", n.nodeId)
				}
				if c, _ := identityLookupCount(&b.ids, n.nodeId); c != 1 {
					t.Errorf("survivor n%d lookupCount = %d, want 1", n.nodeId, c)
				}
			}
			if got := identityNodeCount(&b.ids); got != 1+survivors {
				t.Errorf("nodes = %d after all FORGETs, want %d", got, 1+survivors)
			}
		})
	}
}

func identitySetStableAttr(t *mapIdentityTable, id StableAttr, n *Inode) {
	key := t.dedupKey(id)
	s := &t.shards[t.stableShard(key.Ino)]
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stableAttrs[key] = n
}

func TestIdentityTableExclDisplacedNodeForgotten(t *testing.T) {
	ctx := context.Background()
	root := &Inode{}
	b := NewNodeFS(root, &Options{}).(*rawBridge)
	id := StableAttr{Mode: syscall.S_IFREG, Ino: 42}
	var out fuse.EntryOut

	a := root.NewInode(ctx, &Inode{}, id)
	b.addNewChild(root, "a", a, nil, 0, &out)
	aID := a.nodeId
	a2 := root.NewInode(ctx, &Inode{}, id)
	b.addNewChild(root, "a2", a2, nil, syscall.O_EXCL, &out)
	b.Forget(aID, 1)

	if e := b.ids.node(aID); e != nil {
		t.Errorf("nodes[%d] present after FORGET to zero: inode=%p a=%p", aID, e.inode.Load(), a)
	}
	if got := b.ids.findByAttr(id); got != a2 {
		t.Errorf("stableAttrs[%v] = %p, want a2 %p", id, got, a2)
	}
}

func BenchmarkIdentityTable(b *testing.B) {
	for _, mode := range []struct {
		name     string
		external bool
	}{{"default", false}, {"external", true}} {
		b.Run("mode="+mode.name, func(b *testing.B) {
			for _, op := range []string{"entry", "getattr", "lookup-forget"} {
				b.Run("op="+op, func(b *testing.B) {
					br := NewNodeFS(&Inode{}, &Options{ExternalNodeID: mode.external}).(*rawBridge)
					ctx := context.Background()
					const n = 100000
					children := make([]*Inode, n)
					for i := range children {
						c := br.root.NewInode(ctx, &Inode{}, StableAttr{Mode: syscall.S_IFREG, Ino: uint64(i) + 2})
						var out fuse.EntryOut
						br.addNewNode(nil, "", c, nil, 0, &out)
						children[i] = c
					}

					var run func(*Inode)
					switch op {
					case "entry":
						run = func(c *Inode) {
							e, _ := br.entry(c.nodeId, 0)
							_ = e.inode.Load()
						}
					case "getattr":
						run = func(c *Inode) {
							in := fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: c.nodeId}}
							var out fuse.AttrOut
							br.GetAttr(ctx, &in, &out)
						}
					case "lookup-forget":
						run = func(c *Inode) {
							var out fuse.EntryOut
							br.addNewNode(nil, "", c, nil, 0, &out)
							c.removeRef(1, false)
						}
					}

					var offset atomic.Uint64
					b.ReportAllocs()
					b.ResetTimer()
					b.RunParallel(func(pb *testing.PB) {
						i := offset.Add(7919)
						for pb.Next() {
							run(children[i%n])
							i++
						}
					})
				})
			}
		})
	}
}
