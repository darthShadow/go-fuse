// Copyright 2026 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fs

import (
	"context"
	"sync"
	"syscall"
	"testing"

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
			if status := bridge.Lookup(context.Background(), &header, "shared", &out); !status.Ok() {
				t.Errorf("Lookup(parent=%d, shared) status = %v, want OK", parent.nodeId, status)
				return
			}
			entry := bridge.ids.node(out.NodeId)
			ok := entry != nil
			if !ok || entry == nil || entry.inode.Load() == nil {
				t.Errorf("ids.nodes.Get(%d) after Lookup = (%p, %t), want canonical inode", out.NodeId, entry, ok)
				return
			}
			node := entry.inode.Load()
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

	got := bridge.ids.findByAttr(stable)
	ok := got != nil
	if !ok || got != canonical {
		t.Fatalf("ids.stableAttrs.Get(%v) = (%p, %t), want (%p, true)", stable, got, ok, canonical)
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
		identitySetStableAttr(&bridge.ids, stable, replacement)
	}

	bridge.Forget(canonical.nodeId, lookups)

	got = bridge.ids.findByAttr(stable)
	ok = got != nil
	if replaceStableBeforeForget {
		if !ok || got != replacement {
			t.Fatalf("ids.stableAttrs.Get(%v) after Forget = (%p, %t), want replacement %p", stable, got, ok, replacement)
		}
		return
	}
	if ok {
		t.Fatalf("ids.stableAttrs.Get(%v) after Forget = (%p, true), want absent", stable, got)
	}
}

func newBridgeHardLinkLookupParent(t *testing.T, bridge *rawBridge, name string, parentStable StableAttr, childStable StableAttr) *Inode {
	t.Helper()

	parent := bridge.root.NewInode(context.Background(), &bridgeHardLinkLookupParent{stable: childStable}, parentStable)
	var out fuse.EntryOut
	selected, _, _ := bridge.addNewChild(bridge.root, name, parent, nil, syscall.O_EXCL, &out)
	if selected != parent {
		t.Fatalf("addNewChild(%s) selected %p, want parent %p", name, selected, parent)
	}
	return selected
}

func assertBridgeLookupCount(t *testing.T, node *Inode, want uint64) {
	t.Helper()

	got, _ := identityLookupCount(&node.bridge.ids, node.nodeId)
	if got != want {
		t.Fatalf("node n%d lookupCount = %d, want %d", node.nodeId, got, want)
	}
}
