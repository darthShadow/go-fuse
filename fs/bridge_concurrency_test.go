// Copyright 2026 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fs

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
)

type bridgeHardLinkLookupParent struct {
	Inode

	stable StableAttr
}

func (p *bridgeHardLinkLookupParent) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*Inode, syscall.Errno) {
	if name != "shared" {
		return nil, syscall.ENOENT
	}
	return p.NewInode(ctx, &bridgeHardLinkLookupChild{}, p.stable), OK
}

type bridgeHardLinkLookupChild struct {
	Inode
}

func TestBridgeHardLinkLookupForgetConvergence(t *testing.T) {
	t.Run("deletes current stable attr", func(t *testing.T) {
		runBridgeHardLinkLookupForgetConvergence(t, false)
	})
	t.Run("preserves replacement stable attr", func(t *testing.T) {
		runBridgeHardLinkLookupForgetConvergence(t, true)
	})
}

func runBridgeHardLinkLookupForgetConvergence(t *testing.T, replaceStableBeforeForget bool) {
	t.Helper()

	bridge := NewNodeFS(&Inode{}, nil).(*rawBridge)
	stable := StableAttr{Mode: syscall.S_IFREG, Ino: 7001, Gen: 1}
	parents := []*Inode{
		newBridgeHardLinkLookupParent(t, bridge, "parent-a", StableAttr{Mode: syscall.S_IFDIR, Ino: 7101}, stable),
		newBridgeHardLinkLookupParent(t, bridge, "parent-b", StableAttr{Mode: syscall.S_IFDIR, Ino: 7102}, stable),
	}

	const lookups = 64
	type lookupResult struct {
		node   *Inode
		nodeID uint64
		ok     bool
	}
	results := make([]lookupResult, lookups)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range results {
		parent := parents[i%len(parents)]
		wg.Add(1)
		go func(i int, parent *Inode) {
			defer wg.Done()
			<-start

			header := fuse.InHeader{NodeId: parent.nodeId}
			var out fuse.EntryOut
			if status := bridge.Lookup(nil, &header, "shared", &out); !status.Ok() {
				t.Errorf("Lookup(parent=%d, shared) status = %v, want OK", parent.nodeId, status)
				return
			}
			node := bridge.getNode(out.NodeId)
			if node == nil {
				t.Errorf("getNode(%d) after Lookup = nil, want canonical inode", out.NodeId)
				return
			}
			results[i] = lookupResult{node: node, nodeID: out.NodeId, ok: true}
		}(i, parent)
	}
	close(start)
	wg.Wait()

	var canonical *Inode
	successes := 0
	seen := map[*Inode]struct{}{}
	for i, result := range results {
		if !result.ok {
			continue
		}
		successes++
		if canonical == nil {
			canonical = result.node
		}
		if result.node != canonical {
			t.Errorf("Lookup result %d node = %p (n%d), want canonical %p (n%d)", i, result.node, result.nodeID, canonical, canonical.nodeId)
		}
		seen[result.node] = struct{}{}
	}
	if successes != lookups {
		t.Fatalf("successful lookups = %d, want %d", successes, lookups)
	}
	if len(seen) != 1 {
		t.Fatalf("canonical inode count = %d, want 1", len(seen))
	}

	if got, ok := bridge.stableAttrs.Get(stable); !ok || got != canonical {
		t.Fatalf("stableAttrs.Get(%v) = (%p, %t), want (%p, true)", stable, got, ok, canonical)
	}
	for _, parent := range parents {
		if got := parent.GetChild("shared"); got != canonical {
			t.Fatalf("parent n%d child = %p, want canonical %p", parent.nodeId, got, canonical)
		}
	}
	assertBridgeLookupCount(t, canonical, lookups)

	var replacement *Inode
	if replaceStableBeforeForget {
		replacement = bridge.root.NewInode(context.Background(), &bridgeHardLinkLookupChild{}, stable)
		bridge.stableAttrs.Set(stable, replacement)
	}

	bridge.Forget(canonical.nodeId, lookups)

	got, ok := bridge.stableAttrs.Get(stable)
	if replaceStableBeforeForget {
		if !ok || got != replacement {
			t.Fatalf("stableAttrs.Get(%v) after Forget = (%p, %t), want replacement %p", stable, got, ok, replacement)
		}
		return
	}
	if ok {
		t.Fatalf("stableAttrs.Get(%v) after Forget = (%p, true), want absent", stable, got)
	}
}

func newBridgeHardLinkLookupParent(t *testing.T, bridge *rawBridge, name string, parentStable StableAttr, childStable StableAttr) *Inode {
	t.Helper()

	parent := bridge.root.NewInode(context.Background(), &bridgeHardLinkLookupParent{stable: childStable}, parentStable)
	var out fuse.EntryOut
	selected, _ := bridge.addNewChild(bridge.root, name, parent, nil, syscall.O_EXCL, &out)
	if selected != parent {
		t.Fatalf("addNewChild(%s) selected %p, want parent %p", name, selected, parent)
	}
	return selected
}

func assertBridgeLookupCount(t *testing.T, node *Inode, want uint64) {
	t.Helper()

	node.mu.Lock()
	got := node.lookupCount
	node.mu.Unlock()
	if got != want {
		t.Fatalf("node n%d lookupCount = %d, want %d", node.nodeId, got, want)
	}
}

type bridgeConcurrencyNode struct {
	Inode
}

func TestBridgeForgetLookupMembershipCompaction(t *testing.T) {
	t.Run("compaction not run", func(t *testing.T) {
		bridge, node, stable, release := newForgottenBridgeConcurrencyNodeWithLocalRef(t, "no-compact", 7201)
		defer release()

		assertBridgeRetiredMembership(t, bridge, node, stable, true)
	})

	t.Run("compaction run while local ref live", func(t *testing.T) {
		bridge, node, stable, release := newForgottenBridgeConcurrencyNodeWithLocalRef(t, "compact-live", 7202)
		defer release()

		bridge.compactNodeMaps(time.Now().Add(mapCompactMinInterval))

		assertBridgeRetiredMembership(t, bridge, node, stable, true)
	})

	t.Run("compaction run after local refs drain", func(t *testing.T) {
		bridge, node, stable, release := newForgottenBridgeConcurrencyNodeWithLocalRef(t, "compact-drained", 7203)
		release()

		bridge.compactNodeMaps(time.Now().Add(mapCompactMinInterval))

		assertBridgeRetiredMembership(t, bridge, node, stable, false)
	})
}

func TestBridgeRetiredKernelNodeIDResolution(t *testing.T) {
	bridge := NewNodeFS(&Inode{}, nil).(*rawBridge)
	stable := StableAttr{Mode: syscall.S_IFREG, Ino: 7301, Gen: 1}
	node := newBridgeConcurrencyNode(t, bridge, "retired-resolution", stable)
	compactWhilePinnedAt := time.Now().Add(mapCompactMinInterval)
	compactWhileReleaseAt := compactWhilePinnedAt.Add(mapCompactMinInterval)
	compactWhileDrainAt := compactWhileReleaseAt.Add(mapCompactMinInterval)
	compactAfterDrainAt := compactWhileDrainAt.Add(mapCompactMinInterval)

	liveNode, releaseLive := bridge.acquireNode(node.nodeId)
	if liveNode != node {
		t.Fatalf("acquireNode(%d) before Forget = %p, want %p", node.nodeId, liveNode, node)
	}

	bridge.Forget(node.nodeId, 1)
	assertBridgeRetiredMembership(t, bridge, node, stable, true)
	if got := node.localRefs.Load(); got != 1 {
		t.Fatalf("localRefs after Forget = %d, want 1", got)
	}

	type acquireResult struct {
		node    *Inode
		release func()
	}

	acquireStart := make(chan struct{})
	acquired := make(chan acquireResult, 1)
	var acquireWG sync.WaitGroup
	acquireWG.Add(2)
	go func() {
		defer acquireWG.Done()
		<-acquireStart
		retiredNode, releaseRetired := bridge.acquireNode(node.nodeId)
		acquired <- acquireResult{node: retiredNode, release: releaseRetired}
	}()
	go func() {
		defer acquireWG.Done()
		<-acquireStart
		bridge.compactNodeMaps(compactWhilePinnedAt)
	}()
	close(acquireStart)
	retired := <-acquired
	acquireWG.Wait()

	if retired.node != node {
		t.Fatalf("acquireNode(%d) after Forget = %p, want retired node %p", node.nodeId, retired.node, node)
	}
	if got := node.localRefs.Load(); got != 2 {
		t.Fatalf("localRefs after retired acquire = %d, want 2", got)
	}
	assertBridgeRetiredMembership(t, bridge, node, stable, true)

	releaseLiveStart := make(chan struct{})
	var releaseLiveWG sync.WaitGroup
	releaseLiveWG.Add(2)
	go func() {
		defer releaseLiveWG.Done()
		<-releaseLiveStart
		releaseLive()
	}()
	go func() {
		defer releaseLiveWG.Done()
		<-releaseLiveStart
		bridge.compactNodeMaps(compactWhileReleaseAt)
	}()
	close(releaseLiveStart)
	releaseLiveWG.Wait()

	if got := node.localRefs.Load(); got != 1 {
		t.Fatalf("localRefs after first release = %d, want 1", got)
	}
	assertBridgeRetiredMembership(t, bridge, node, stable, true)

	releaseRetiredStart := make(chan struct{})
	releasedRetired := make(chan struct{})
	reaped := make(chan string, 1)
	var releaseRetiredWG sync.WaitGroup
	releaseRetiredWG.Add(2)
	go func() {
		defer releaseRetiredWG.Done()
		<-releaseRetiredStart
		retired.release()
		close(releasedRetired)
	}()
	go func() {
		defer releaseRetiredWG.Done()
		<-releaseRetiredStart
		for {
			bridge.compactNodeMaps(compactWhileDrainAt)
			got, ok := bridge.retiredKernelNodeIds.Get(node.nodeId)
			if !ok {
				if refs := node.localRefs.Load(); refs != 0 {
					reaped <- fmt.Sprintf("retiredKernelNodeIds reaped while localRefs = %d, want 0", refs)
					return
				}
				reaped <- ""
				return
			}
			if got != node {
				reaped <- fmt.Sprintf("retiredKernelNodeIds.Get(%d) = (%p, true), want %p", node.nodeId, got, node)
				return
			}

			select {
			case <-releasedRetired:
				bridge.compactNodeMaps(compactAfterDrainAt)
				got, ok := bridge.retiredKernelNodeIds.Get(node.nodeId)
				if ok {
					reaped <- fmt.Sprintf("retiredKernelNodeIds.Get(%d) after final release = (%p, true), want absent", node.nodeId, got)
					return
				}
				reaped <- ""
				return
			default:
				runtime.Gosched()
			}
		}
	}()
	close(releaseRetiredStart)
	releaseRetiredWG.Wait()
	if err := <-reaped; err != "" {
		t.Fatal(err)
	}

	if got := node.localRefs.Load(); got != 0 {
		t.Fatalf("localRefs after final release = %d, want 0", got)
	}
	assertBridgeRetiredMembership(t, bridge, node, stable, false)

	reapedNode, releaseReaped := bridge.acquireNode(node.nodeId)
	releaseReaped()
	if reapedNode != nil {
		t.Fatalf("acquireNode(%d) after reap = %p, want nil", node.nodeId, reapedNode)
	}
}

func newForgottenBridgeConcurrencyNodeWithLocalRef(t *testing.T, name string, ino uint64) (*rawBridge, *Inode, StableAttr, func()) {
	t.Helper()

	bridge := NewNodeFS(&Inode{}, nil).(*rawBridge)
	stable := StableAttr{Mode: syscall.S_IFREG, Ino: ino, Gen: 1}
	node := newBridgeConcurrencyNode(t, bridge, name, stable)

	acquired, release := bridge.acquireNode(node.nodeId)
	if acquired != node {
		t.Fatalf("acquireNode(%d) = %p, want %p", node.nodeId, acquired, node)
	}
	bridge.Forget(node.nodeId, 1)
	return bridge, node, stable, release
}

func newBridgeConcurrencyNode(t *testing.T, bridge *rawBridge, name string, stable StableAttr) *Inode {
	t.Helper()

	node := bridge.root.NewInode(context.Background(), &bridgeConcurrencyNode{}, stable)
	var out fuse.EntryOut
	selected, _ := bridge.addNewChild(bridge.root, name, node, nil, syscall.O_EXCL, &out)
	if selected != node {
		t.Fatalf("addNewChild(%s) selected %p, want node %p", name, selected, node)
	}
	if out.NodeId != node.nodeId {
		t.Fatalf("addNewChild(%s) NodeId = %d, want %d", name, out.NodeId, node.nodeId)
	}
	return selected
}

func assertBridgeRetiredMembership(t *testing.T, bridge *rawBridge, node *Inode, stable StableAttr, wantRetired bool) {
	t.Helper()

	if got, ok := bridge.stableAttrs.Get(stable); ok {
		t.Fatalf("stableAttrs.Get(%v) = (%p, true), want absent", stable, got)
	}
	if got, ok := bridge.kernelNodeIds.Get(node.nodeId); ok {
		t.Fatalf("kernelNodeIds.Get(%d) = (%p, true), want absent", node.nodeId, got)
	}
	got, ok := bridge.retiredKernelNodeIds.Get(node.nodeId)
	if wantRetired {
		if !ok || got != node {
			t.Fatalf("retiredKernelNodeIds.Get(%d) = (%p, %t), want (%p, true)", node.nodeId, got, ok, node)
		}
		return
	}
	if ok {
		t.Fatalf("retiredKernelNodeIds.Get(%d) = (%p, true), want absent", node.nodeId, got)
	}
}
