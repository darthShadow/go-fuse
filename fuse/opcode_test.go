// Copyright 2026 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fuse

import (
	"context"
	"reflect"
	"testing"
	"unsafe"
)

// newTestProtocolServer returns a protocolServer for calling
// handlers directly, without a mount.
func newTestProtocolServer(fs RawFileSystem, opts *MountOptions) *protocolServer {
	if opts == nil {
		opts = &MountOptions{}
	}
	opts.setDefaults(fs)
	return &protocolServer{fileSystem: fs, opts: opts}
}

// newTestRequest parses in, which starts with an InHeader, into a
// request the way Server.handleRequest does.
func newTestRequest(t testing.TB, opcode uint32, nodeID uint64, in []byte) (*requestAlloc, *operationHandler) {
	t.Helper()
	hdr := (*InHeader)(unsafe.Pointer(&in[0]))
	hdr.Length = uint32(len(in))
	hdr.Opcode = opcode
	hdr.Unique = 1
	hdr.NodeId = nodeID
	ra := &requestAlloc{}
	ra.setInput(in)
	h, inSize, outSize, _, code := parseRequest(ra.inputBuf, 0)
	if !code.Ok() {
		t.Fatalf("parseRequest(%s): %v", operationName(opcode), code)
	}
	if code := ra.splitPayload(inSize, int(h.InputSize)); !code.Ok() {
		t.Fatalf("splitPayload(%s): %v", operationName(opcode), code)
	}
	ra.outHeaderBuf = ra.outHeaderInline[:]
	ra.outDataBuf = ra.outDataInline[:outSize]
	return ra, h
}

func readInBytes(off uint64, size uint32) []byte {
	in := make([]byte, unsafe.Sizeof(ReadIn{}))
	r := (*ReadIn)(unsafe.Pointer(&in[0]))
	r.Offset = off
	r.Size = size
	return in
}

type oneEntryDirFS struct {
	defaultRawFileSystem
}

func (*oneEntryDirFS) ReadDir(_ context.Context, _ *ReadIn, out *DirEntryList) Status {
	out.AddDirEntry(DirEntry{Name: "file", Ino: 7, Mode: S_IFREG})
	return OK
}

func (*oneEntryDirFS) ReadDirPlus(_ context.Context, _ *ReadIn, out *DirEntryList) Status {
	out.AddDirLookupEntry(DirEntry{Name: "file", Ino: 7, Mode: S_IFREG})
	return OK
}

func TestReadDirNoAlloc(t *testing.T) {
	ms := newTestProtocolServer(&oneEntryDirFS{}, nil)
	for _, op := range []uint32{_OP_READDIR, _OP_READDIRPLUS} {
		ra, h := newTestRequest(t, op, 1, readInBytes(0, 4096))
		buf := make([]byte, 4096)
		n := testing.AllocsPerRun(100, func() {
			ra.outPayload = buf
			h.Func(ms, &ra.request)
		})
		if n != 0 {
			t.Errorf("%s: %v allocations per call, want 0", operationName(op), n)
		}
	}
}

func TestReadDirResetsReusedList(t *testing.T) {
	ms := newTestProtocolServer(&oneEntryDirFS{}, nil)
	ra, _ := newTestRequest(t, _OP_READDIRPLUS, 1, readInBytes(5, 4096))
	buf := make([]byte, 4096)
	ra.outPayload = buf
	doReadDirPlus(ms, &ra.request)

	// A plain READDIR on the same request must start from a fresh
	// list: not in READDIRPLUS mode, at the request's offset.
	ra.outPayload = buf
	doReadDir(ms, &ra.request)
	got, err := ParseDirEntries(ra.outPayload)
	if err != nil {
		t.Fatal(err)
	}
	want := []DirEntry{{Name: "file", Ino: 7, Mode: S_IFREG, Off: 6}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestRequestClearDropsDirEntryList(t *testing.T) {
	ms := newTestProtocolServer(&oneEntryDirFS{}, nil)
	ra, _ := newTestRequest(t, _OP_READDIRPLUS, 1, readInBytes(0, 4096))
	ra.outPayload = make([]byte, 4096)
	doReadDirPlus(ms, &ra.request)
	ra.clear()
	if ra.dirList.buf != nil || ra.dirList.lastDirent != nil {
		t.Errorf("cleared request still points at its output buffer")
	}
}
