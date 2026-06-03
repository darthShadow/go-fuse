// Copyright 2026 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build linux

package test

import (
	"path/filepath"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/hanwen/go-fuse/v2/internal/testutil"
)

type disabledXAttrNode struct {
	nodefs.Node
}

func (n *disabledXAttrNode) OnMount(fsConn *nodefs.FileSystemConnector) {
	n.Inode().NewChild("child", false, &disabledXAttrChildNode{Node: nodefs.NewDefaultNode()})
}

type disabledXAttrChildNode struct {
	nodefs.Node
}

func (n *disabledXAttrChildNode) GetXAttr(attr string, context *fuse.Context) ([]byte, fuse.Status) {
	return []byte("value"), fuse.OK
}

func (n *disabledXAttrChildNode) ListXAttr(context *fuse.Context) ([]string, fuse.Status) {
	return []string{"attr"}, fuse.OK
}

func (n *disabledXAttrChildNode) SetXAttr(attr string, data []byte, flags int, context *fuse.Context) fuse.Status {
	return fuse.OK
}

func (n *disabledXAttrChildNode) RemoveXAttr(attr string, context *fuse.Context) fuse.Status {
	return fuse.OK
}

func mountDisabledXAttrFS(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	root := &disabledXAttrNode{
		Node: nodefs.NewDefaultNode(),
	}

	opts := nodefs.NewOptions()
	opts.Debug = testutil.VerboseTest()
	mountOpts := &fuse.MountOptions{
		Debug:         opts.Debug,
		DisableXAttrs: true,
	}
	s, _, err := nodefs.Mount(dir, root, mountOpts, opts)
	if err != nil {
		t.Fatalf("Mount: %v", err)
	}
	go s.Serve()
	if err := s.WaitMount(); err != nil {
		t.Fatal("WaitMount", err)
	}

	t.Cleanup(func() {
		_ = s.Unmount()
	})

	return dir
}

func TestDisableGetXAttr(t *testing.T) {
	dir := mountDisabledXAttrFS(t)
	child := filepath.Join(dir, "child")

	var data [1024]byte
	_, err := syscall.Getxattr(child, "attr", data[:])
	if err != syscall.EOPNOTSUPP {
		t.Fatalf("Getxattr: got %v, want %v", err, syscall.EOPNOTSUPP)
	}
}

func TestDisableListXAttr(t *testing.T) {
	dir := mountDisabledXAttrFS(t)
	child := filepath.Join(dir, "child")

	var data [1024]byte
	_, err := syscall.Listxattr(child, data[:])
	if err != syscall.EOPNOTSUPP {
		t.Fatalf("Listxattr: got %v, want %v", err, syscall.EOPNOTSUPP)
	}
}

func TestDisableSetXAttr(t *testing.T) {
	dir := mountDisabledXAttrFS(t)
	child := filepath.Join(dir, "child")

	if err := syscall.Setxattr(child, "attr", []byte("value"), 0); err != syscall.EOPNOTSUPP {
		t.Fatalf("Setxattr: got %v, want %v", err, syscall.EOPNOTSUPP)
	}
}

func TestDisableRemoveXAttr(t *testing.T) {
	dir := mountDisabledXAttrFS(t)
	child := filepath.Join(dir, "child")

	if err := syscall.Removexattr(child, "attr"); err != syscall.EOPNOTSUPP {
		t.Fatalf("Removexattr: got %v, want %v", err, syscall.EOPNOTSUPP)
	}
}
