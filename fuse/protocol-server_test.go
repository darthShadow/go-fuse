// Copyright 2026 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fuse

import (
	"bytes"
	"encoding/binary"
	"log"
	"strings"
	"testing"
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
