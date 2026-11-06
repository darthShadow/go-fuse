// Copyright 2026 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fs

import (
	"context"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// readdirTestEntries is what every readdirTestDir lists. Off is 0, so
// the bridge numbers the entries 1, 2, 3.
var readdirTestEntries = [...]fuse.DirEntry{
	{Name: "a", Ino: 10, Mode: syscall.S_IFREG},
	{Name: "b", Ino: 11, Mode: syscall.S_IFREG},
	{Name: "c", Ino: 12, Mode: syscall.S_IFREG},
}

// Sizes that tell which lookup path filled an EntryOut.
const (
	readdirTestGetattrSize = 42
	readdirTestLookupSize  = 43
)

type readdirTestRoot struct {
	Inode
	fileLookup bool
}

func (r *readdirTestRoot) OpendirHandle(ctx context.Context, flags uint32) (FileHandle, uint32, syscall.Errno) {
	d := &readdirTestDir{root: &r.Inode}
	if r.fileLookup {
		return &readdirTestLookupDir{d}, 0, 0
	}
	return d, 0, 0
}

type readdirTestDir struct {
	root *Inode
	next int
}

func (d *readdirTestDir) Readdirent(ctx context.Context) (*fuse.DirEntry, syscall.Errno) {
	if d.next == len(readdirTestEntries) {
		return nil, 0
	}
	d.next++
	return &readdirTestEntries[d.next-1], 0
}

func (d *readdirTestDir) Seekdir(ctx context.Context, off uint64) syscall.Errno {
	d.next = int(off)
	return 0
}

type readdirTestLookupDir struct {
	*readdirTestDir
}

func (d *readdirTestLookupDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*Inode, syscall.Errno) {
	out.Attr.Size = readdirTestLookupSize
	return d.root.GetChild(name), 0
}

// readdirTestFile is a child found by the Node lookup fallback.
type readdirTestFile struct {
	Inode
}

func (f *readdirTestFile) Getattr(ctx context.Context, fh FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Size = readdirTestGetattrSize
	return 0
}

// newReaddirTestBridge returns a bridge whose root holds the children
// of readdirTestEntries, and an open handle on the root.
func newReaddirTestBridge(t *testing.T, fileLookup bool) (*readdirTestRoot, *rawBridge, uint64) {
	t.Helper()
	root := &readdirTestRoot{fileLookup: fileLookup}
	bridge := NewNodeFS(root, nil).(*rawBridge)
	ctx := context.Background()
	for _, e := range readdirTestEntries {
		ch := root.NewPersistentInode(ctx, &readdirTestFile{}, StableAttr{Mode: e.Mode, Ino: e.Ino})
		root.AddChild(e.Name, ch, false)
	}
	in := fuse.OpenIn{}
	in.NodeId = 1
	var out fuse.OpenOut
	if status := bridge.OpenDir(ctx, &in, &out); !status.Ok() {
		t.Fatalf("OpenDir status = %v, want OK", status)
	}
	return root, bridge, out.Fh
}

func TestReadDirNoAttrAlloc(t *testing.T) {
	for _, tc := range []struct {
		name       string
		plus       bool
		fileLookup bool
		want       float64
	}{
		{"READDIR", false, false, 0},
		{"READDIRPLUS with FileLookuper", true, true, 0},
		// One AttrOut scratch per call, shared by every entry.
		{"READDIRPLUS with Node fallback", true, false, 1},
	} {
		_, bridge, fh := newReaddirTestBridge(t, tc.fileLookup)
		const runs = 100
		buf := make([]byte, 4096)
		// One list per call, built outside the measured function.
		lists := make([]*fuse.DirEntryList, runs+1)
		for i := range lists {
			lists[i] = fuse.NewDirEntryList(buf, 0)
		}
		var in fuse.ReadIn
		in.NodeId = 1
		in.Fh = fh
		ctx := context.Background()
		i := 0
		allocs := testing.AllocsPerRun(runs, func() {
			if tc.plus {
				bridge.ReadDirPlus(ctx, &in, lists[i])
			} else {
				bridge.ReadDir(ctx, &in, lists[i])
			}
			i++
		})
		if allocs != tc.want {
			t.Errorf("%s: %v allocations per call, want %v", tc.name, allocs, tc.want)
		}
	}
}

func TestReadDirPlusLookupPaths(t *testing.T) {
	for _, tc := range []struct {
		name       string
		fileLookup bool
		wantSize   uint64
	}{
		{"FileLookuper", true, readdirTestLookupSize},
		{"Node fallback", false, readdirTestGetattrSize},
	} {
		root, bridge, fh := newReaddirTestBridge(t, tc.fileLookup)
		// The second read starts after entry 1, as when the kernel
		// keeps one entry of an interrupted reply and asks again.
		for _, start := range []uint64{0, 1} {
			var in fuse.ReadIn
			in.NodeId = 1
			in.Fh = fh
			in.Offset = start
			out := fuse.NewDirEntryList(make([]byte, 4096), start)
			if status := bridge.ReadDirPlus(context.Background(), &in, out); !status.Ok() {
				t.Fatalf("%s, offset %d: status %v, want OK", tc.name, start, status)
			}
			entries, entryOuts, err := fuse.ParseDirLookupEntries(out.Bytes())
			if err != nil {
				t.Fatalf("%s, offset %d: %v", tc.name, start, err)
			}
			want := readdirTestEntries[start:]
			if len(entries) != len(want) {
				t.Fatalf("%s, offset %d: got %d entries, want %d", tc.name, start, len(entries), len(want))
			}
			for j, e := range entries {
				wantOff := start + uint64(j) + 1
				if e.Name != want[j].Name || e.Off != wantOff {
					t.Errorf("%s, offset %d: entry %d is %q at %d, want %q at %d", tc.name, start, j, e.Name, e.Off, want[j].Name, wantOff)
				}
				if got := entryOuts[j].NodeId; got != root.GetChild(e.Name).nodeId {
					t.Errorf("%s, offset %d: %q has node ID %d, want %d", tc.name, start, e.Name, got, root.GetChild(e.Name).nodeId)
				}
				if got := entryOuts[j].Attr.Size; got != tc.wantSize {
					t.Errorf("%s, offset %d: %q has size %d, want %d", tc.name, start, e.Name, got, tc.wantSize)
				}
			}
		}
	}
}

type lookupTestRoot struct {
	Inode
}

func (r *lookupTestRoot) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*Inode, syscall.Errno) {
	return r.GetChild(name), 0
}

func TestLookupNoAttrAlloc(t *testing.T) {
	root := &lookupTestRoot{}
	bridge := NewNodeFS(root, nil).(*rawBridge)
	ctx := context.Background()
	child := root.NewPersistentInode(ctx, &readdirTestFile{}, StableAttr{Mode: syscall.S_IFREG, Ino: 10})
	root.AddChild("a", child, false)
	header := fuse.InHeader{NodeId: 1}
	var out fuse.EntryOut
	allocs := testing.AllocsPerRun(100, func() {
		out = fuse.EntryOut{}
		if status := bridge.Lookup(ctx, &header, "a", &out); !status.Ok() {
			t.Fatalf("Lookup status = %v, want OK", status)
		}
	})
	if allocs != 0 {
		t.Errorf("LOOKUP with NodeLookuper: %v allocations per call, want 0", allocs)
	}
}

func TestLookupFallbackGetattr(t *testing.T) {
	_, bridge, _ := newReaddirTestBridge(t, false)
	header := fuse.InHeader{NodeId: 1}
	var out fuse.EntryOut
	if status := bridge.Lookup(context.Background(), &header, "a", &out); !status.Ok() {
		t.Fatalf("Lookup status = %v, want OK", status)
	}
	if out.Attr.Size != readdirTestGetattrSize {
		t.Errorf("Lookup Attr.Size = %d, want %d", out.Attr.Size, readdirTestGetattrSize)
	}
}
