// Copyright 2026 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fuse

import (
	"bytes"
	"encoding/binary"
	"log"
	"reflect"
	"strings"
	"testing"
	"unsafe"
)

func TestFilenameEquals(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		match   string
		want    bool
	}{
		{"exact without NUL", "name", "name", true},
		{"trailing NUL", "name\x00", "name", true},
		{"second name", "name\x00second", "name", true},
		{"short payload", "nam", "name", false},
		{"non-NUL suffix", "names", "name", false},
		{"empty payload", "", "name", false},
		{"64-byte name", strings.Repeat("n", 64), strings.Repeat("n", 64), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &request{inPayload: []byte(tt.payload)}
			got := filenameEquals(req, tt.match)
			if want := req.filename() == tt.match; got != want {
				t.Errorf("filenameEquals(%q, %q) = %v, want filename() comparison %v", tt.payload, tt.match, got, want)
			}
			if got != tt.want {
				t.Errorf("filenameEquals(%q, %q) = %v, want %v", tt.payload, tt.match, got, tt.want)
			}
		})
	}
}

func TestProtocolServerParse(t *testing.T) {
	in := [][]byte{
		[]byte("A\x00\x00\x00\x16\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00.\x04\x00\x00\x00\x00\x00\x00"),
		[]byte("\x00\x00\x00\x00\x00\x00\x00\x00security.selinux\x00"),
	}
	out := [][]byte{make([]byte, 16), make([]byte, 8)}

	var logBuf bytes.Buffer
	t.Cleanup(func() {
		if logBuf.Len() > 0 {
			t.Logf("protocol-server log:\n%s", logBuf.String())
		}
	})
	opts := MountOptions{}
	opts.Debug = true
	opts.Logger = log.New(&logBuf, "", 0)
	ps := NewProtocolServer(NewDefaultRawFileSystem(), &opts)
	n, status := ps.HandleRequest(in, out)
	if status != OK {
		t.Fatalf("HandleRequest: status %v, want OK", status)
	}
	if n < int(sizeOfOutHeader) {
		t.Fatalf("HandleRequest: wrote %d bytes, want at least %d", n, sizeOfOutHeader)
	}
	length := binary.LittleEndian.Uint32(out[0][0:4])
	if int(length) != n {
		t.Errorf("OutHeader.Length = %d, want %d", length, n)
	}
	// DefaultRawFileSystem.GetXAttr returns ENOSYS, so the reply is
	// header-only with a negative status.
	gotStatus := int32(binary.LittleEndian.Uint32(out[0][4:8]))
	if gotStatus != -int32(ENOSYS) {
		t.Errorf("OutHeader.Status = %d, want %d", gotStatus, -int32(ENOSYS))
	}
}

// forgetFS records the Forget calls it gets, and how many requests
// were in flight during the last one.
type forgetFS struct {
	defaultRawFileSystem
	ms       *protocolServer
	inflight int
	forgets  [][2]uint64
	record   bool
}

func (fs *forgetFS) Forget(nodeid, nlookup uint64) {
	fs.ms.interruptMu.Lock()
	fs.inflight = len(fs.ms.reqInflight)
	fs.ms.interruptMu.Unlock()
	if fs.record {
		fs.forgets = append(fs.forgets, [2]uint64{nodeid, nlookup})
	}
}

func newForgetServer(opts *MountOptions) (*protocolServer, *forgetFS) {
	fs := &forgetFS{}
	ms := newTestProtocolServer(fs, opts)
	fs.ms = ms
	return ms, fs
}

func forgetIn(nlookup uint64) []byte {
	in := make([]byte, unsafe.Sizeof(ForgetIn{}))
	(*ForgetIn)(unsafe.Pointer(&in[0])).Nlookup = nlookup
	return in
}

// batchForgetIn returns a BATCH_FORGET input that declares count
// entries and carries the given ones.
func batchForgetIn(count uint32, entries ...[2]uint64) []byte {
	in := make([]byte, unsafe.Sizeof(_BatchForgetIn{}), unsafe.Sizeof(_BatchForgetIn{})+uintptr(len(entries))*unsafe.Sizeof(_ForgetOne{}))
	(*_BatchForgetIn)(unsafe.Pointer(&in[0])).Count = count
	for _, e := range entries {
		one := _ForgetOne{NodeId: e[0], Nlookup: e[1]}
		in = append(in, unsafe.Slice((*byte)(unsafe.Pointer(&one)), unsafe.Sizeof(one))...)
	}
	return in
}

func TestForgetSkipsInflight(t *testing.T) {
	for _, op := range []uint32{_OP_FORGET, _OP_BATCH_FORGET} {
		ms, fs := newForgetServer(nil)
		in := forgetIn(1)
		if op == _OP_BATCH_FORGET {
			in = batchForgetIn(1, [2]uint64{5, 1})
		}
		ra, h := newTestRequest(t, op, 5, in)
		fs.inflight = -1
		ms.handleRequest(h, &ra.request)
		if fs.inflight != 0 {
			t.Errorf("%s: %d requests in flight during Forget, want 0", operationName(op), fs.inflight)
		}
	}
}

func TestForgetSkipsContext(t *testing.T) {
	for _, op := range []uint32{_OP_FORGET, _OP_BATCH_FORGET} {
		ms, _ := newForgetServer(nil)
		in := forgetIn(1)
		if op == _OP_BATCH_FORGET {
			in = batchForgetIn(1, [2]uint64{5, 1})
		}
		ra, h := newTestRequest(t, op, 5, in)
		n := testing.AllocsPerRun(100, func() { ms.handleRequest(h, &ra.request) })
		if n != 0 {
			t.Errorf("%s: %v allocations per request, want 0", operationName(op), n)
		}
		if ra.ctx != nil {
			t.Errorf("%s: request has a context, want none", operationName(op))
		}
	}
}

func TestForgetLookupCounts(t *testing.T) {
	cases := []struct {
		name     string
		op       uint32
		nodeID   uint64
		in       []byte
		remember bool
		want     [][2]uint64
	}{
		{"forget", _OP_FORGET, 5, forgetIn(3), false, [][2]uint64{{5, 3}}},
		{"forget RememberInodes", _OP_FORGET, 5, forgetIn(3), true, nil},
		{"forget poll hack", _OP_FORGET, pollHackInode, forgetIn(3), false, nil},
		{"batch", _OP_BATCH_FORGET, 0, batchForgetIn(3, [2]uint64{5, 1}, [2]uint64{6, 2}, [2]uint64{pollHackInode, 4}), false, [][2]uint64{{5, 1}, {6, 2}}},
		{"batch short payload", _OP_BATCH_FORGET, 0, batchForgetIn(3, [2]uint64{5, 1}, [2]uint64{6, 2}), false, [][2]uint64{{5, 1}, {6, 2}}},
		{"batch empty", _OP_BATCH_FORGET, 0, batchForgetIn(0), false, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var logBuf bytes.Buffer
			ms, fs := newForgetServer(&MountOptions{RememberInodes: c.remember, Logger: log.New(&logBuf, "", 0)})
			fs.record = true
			ra, h := newTestRequest(t, c.op, c.nodeID, c.in)
			ms.handleRequest(h, &ra.request)
			if !reflect.DeepEqual(fs.forgets, c.want) {
				t.Errorf("Forget calls %v, want %v", fs.forgets, c.want)
			}
			if !ra.suppressReply {
				t.Errorf("suppressReply false, want true")
			}
		})
	}
}
