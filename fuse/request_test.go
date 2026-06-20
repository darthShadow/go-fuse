// Copyright 2026 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fuse

import (
	"sync"
	"testing"
)

func TestRequestAllocClear(t *testing.T) {
	// T3 catches an inc2 reversion where requestAlloc.clear stops nilling pooled
	// buffer references or stops zeroing inline request buffers before reuse.
	req := &requestAlloc{
		bufferPoolInputBuf:  []byte{1, 2, 3},
		bufferPoolOutputBuf: []byte{4, 5, 6},
	}
	fillBytes(req.outHeaderInline[:], 0xaa)
	fillBytes(req.outDataInline[:], 0xbb)
	fillBytes(req.smallInputBuf[:], 0xcc)

	req.clear()

	if req.bufferPoolInputBuf != nil {
		t.Errorf("bufferPoolInputBuf after clear: got=%v want=<nil>", req.bufferPoolInputBuf)
	}
	if req.bufferPoolOutputBuf != nil {
		t.Errorf("bufferPoolOutputBuf after clear: got=%v want=<nil>", req.bufferPoolOutputBuf)
	}
	assertZeroBytes(t, "outHeaderInline", req.outHeaderInline[:])
	assertZeroBytes(t, "outDataInline", req.outDataInline[:])
	assertZeroBytes(t, "smallInputBuf", req.smallInputBuf[:])
}

func TestSetInputBoundary(t *testing.T) {
	// T2 catches an inc2 reversion where setInput changes the cap-1/cap/cap+1
	// ownership decision or aliases inputBuf to the wrong storage.
	smallCap := cap(requestAlloc{}.smallInputBuf)
	tests := []struct {
		name           string
		inputLen       int
		wantGobbled    bool
		wantSmallAlias bool
	}{
		{
			name:           "cap minus one",
			inputLen:       smallCap - 1,
			wantGobbled:    false,
			wantSmallAlias: true,
		},
		{
			name:           "cap",
			inputLen:       smallCap,
			wantGobbled:    true,
			wantSmallAlias: false,
		},
		{
			name:           "cap plus one",
			inputLen:       smallCap + 1,
			wantGobbled:    true,
			wantSmallAlias: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := &requestAlloc{}
			input := make([]byte, tc.inputLen, tc.inputLen+1)
			for i := range input {
				input[i] = byte(i + 1)
			}

			gotGobbled := req.setInput(input)

			if gotGobbled != tc.wantGobbled {
				t.Errorf("setInput gobbled: got=%t want=%t", gotGobbled, tc.wantGobbled)
			}
			if gotLen := len(req.inputBuf); gotLen != tc.inputLen {
				t.Errorf("inputBuf length: got=%d want=%d", gotLen, tc.inputLen)
			}
			gotSmallAlias := sameSliceData(req.inputBuf, req.smallInputBuf[:])
			if gotSmallAlias != tc.wantSmallAlias {
				t.Errorf("inputBuf aliases smallInputBuf: got=%t want=%t", gotSmallAlias, tc.wantSmallAlias)
			}
			gotInputAlias := sameSliceData(req.inputBuf, input)
			wantInputAlias := !tc.wantSmallAlias
			if gotInputAlias != wantInputAlias {
				t.Errorf("inputBuf aliases passed input: got=%t want=%t", gotInputAlias, wantInputAlias)
			}
			if tc.wantGobbled {
				if req.bufferPoolInputBuf == nil {
					t.Fatalf("bufferPoolInputBuf: got=<nil> want alias of passed input")
				}
				if gotAlias := sameSliceData(req.bufferPoolInputBuf, input); !gotAlias {
					t.Errorf("bufferPoolInputBuf aliases passed input: got=%t want=true", gotAlias)
				}
			} else if req.bufferPoolInputBuf != nil {
				t.Errorf("bufferPoolInputBuf: got=%v want=<nil>", req.bufferPoolInputBuf)
			}
		})
	}
}

func TestReturnRequestSaveBeforeClear(t *testing.T) {
	// T1 catches an inc2 reversion where returnRequest clears bufferPoolInputBuf
	// before saving it, so the read buffer is not returned exactly once.
	newReadBufCalls := 0
	readPool := newBytePool(1, func() []byte {
		buf := make([]byte, 32)
		for i := range buf {
			buf[i] = byte(i + 1)
		}
		newReadBufCalls++
		return buf
	}).shard(0)
	savedInput := readPool.Get()
	newReadBufCalls = 0
	req := &requestAlloc{
		bufferPoolInputBuf: savedInput,
	}
	reqPool := &sync.Pool{}
	fd := &fuseFD{
		reqPool:              reqPool,
		readPool:             readPool,
		reqAllocBytes:        11,
		readBufBytes:         7,
		inflightRequestBytes: 18,
	}

	fd.returnRequest(req)

	if req.bufferPoolInputBuf != nil {
		t.Errorf("bufferPoolInputBuf after returnRequest: got=%v want=<nil>", req.bufferPoolInputBuf)
	}
	if got := fd.inflightRequestBytes; got != 0 {
		t.Errorf("inflightRequestBytes after returnRequest: got=%d want=0", got)
	}
	gotReadBuf := readPool.Get()
	if !sameSliceData(gotReadBuf, savedInput) {
		t.Fatalf("first readPool buffer aliases saved input: got=%t want=true", sameSliceData(gotReadBuf, savedInput))
	}
	secondReadBuf := readPool.Get()
	if sameSliceData(secondReadBuf, savedInput) {
		t.Fatalf("second readPool buffer aliases saved input: got=true want=false")
	}
	if newReadBufCalls != 1 {
		t.Errorf("readPool allocator calls after two Gets: got=%d want=1", newReadBufCalls)
	}
	gotReq := reqPool.Get().(*requestAlloc)
	if gotReq != req {
		t.Errorf("reqPool request: got=%p want=%p", gotReq, req)
	}
}

func fillBytes(buf []byte, value byte) {
	for i := range buf {
		buf[i] = value
	}
}

func assertZeroBytes(t *testing.T, name string, got []byte) {
	t.Helper()
	for i, b := range got {
		if b != 0 {
			t.Fatalf("%s[%d] after clear: got=%d want=0", name, i, b)
		}
	}
}

func sameSliceData(a, b []byte) bool {
	if len(a) == 0 || len(b) == 0 {
		return len(a) == len(b)
	}
	return &a[0] == &b[0]
}
