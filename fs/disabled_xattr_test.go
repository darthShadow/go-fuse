// Copyright 2026 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build linux

package fs

import (
	"context"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
)

type disabledXAttrNode struct {
	Inode
}

func (n *disabledXAttrNode) OnAdd(ctx context.Context) {
	n.AddChild("child", n.NewInode(ctx, &disabledXAttrChildNode{}, StableAttr{Mode: syscall.S_IFREG}), false)
}

type disabledXAttrChildNode struct {
	Inode
}

func (n *disabledXAttrChildNode) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	return uint32(copy(dest, "value")), 0
}

func (n *disabledXAttrChildNode) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	return uint32(copy(dest, "attr\x00")), 0
}

func (n *disabledXAttrChildNode) Setxattr(ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	return 0
}

func (n *disabledXAttrChildNode) Removexattr(ctx context.Context, attr string) syscall.Errno {
	return 0
}

func mountDisabledXAttrFS(t *testing.T) string {
	t.Helper()
	dir, _ := testMount(t, &disabledXAttrNode{}, &Options{MountOptions: fuse.MountOptions{DisableXAttrs: true}})
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
