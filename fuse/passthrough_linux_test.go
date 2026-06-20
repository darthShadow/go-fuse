// Copyright 2026 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fuse

import (
	"syscall"
	"testing"
)

func TestPassthroughBackingFdClosedRawConnReturnsError(t *testing.T) {
	srv := newFuseFDUnitTestServer()
	pipeFDs := []int{-1, -1}
	if err := syscall.Pipe(pipeFDs); err != nil {
		t.Fatalf("Pipe: %v", err)
	}
	t.Cleanup(func() {
		if pipeFDs[1] >= 0 {
			if err := syscall.Close(pipeFDs[1]); err != nil {
				t.Errorf("close pipe write fd: %v", err)
			}
		}
	})

	fd, err := srv.newFuseFD(pipeFDs[0])
	if err != nil {
		syscall.Close(pipeFDs[0])
		t.Fatalf("newFuseFD: %v", err)
	}
	pipeFDs[0] = -1
	if err := fd.close(); err != nil {
		t.Fatalf("fuseFD.close: %v", err)
	}

	id, errno := fd.registerBackingFd(&BackingMap{Fd: int32(pipeFDs[1])})
	if errno == 0 {
		t.Fatalf("registerBackingFd on closed fd: id=%d errno=%v, want non-zero errno", id, errno)
	}
	if errno := fd.unregisterBackingFd(1); errno == 0 {
		t.Fatalf("unregisterBackingFd on closed fd: errno=%v, want non-zero errno", errno)
	}
}
