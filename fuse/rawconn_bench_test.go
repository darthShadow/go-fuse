// Copyright 2026 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This benchmark proxies the same conn.Control(syscall.Read) pattern over
// /dev/zero because a real /dev/fuse fd is not available in this environment.
// The wrapper overhead is fd-source-independent, so /dev/zero isolates the
// per-read RawConn.Control cost from FUSE mount semantics.
//
// Interpret results as:
//   - BenchmarkRawConnControlRead - BenchmarkDirectRead = the per-read
//     conn.Control cost on top of a real syscall.
//   - BenchmarkRawConnControlNoop = the pure wrapper cost.
//
// A material delta on the metadata path would justify reopening a direct-read
// carve-out.
package fuse

import (
	"os"
	"syscall"
	"testing"
)

const rawConnBenchDevZero = "/dev/zero"

// Both read benchmarks use this same small request-representative 128-byte
// buffer, so the A/B delta is buffer-size-independent.
const rawConnBenchReadSize = 128

func BenchmarkRawConnControlNoop(b *testing.B) {
	conn := openRawConnBenchFD(b)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := conn.Control(func(uintptr) {}); err != nil {
			b.Fatalf("RawConn.Control: %v", err)
		}
	}
}

func BenchmarkRawConnControlRead(b *testing.B) {
	conn := openRawConnBenchFD(b)
	buf := make([]byte, rawConnBenchReadSize)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var n int
		var err error
		if cerr := conn.Control(func(fd uintptr) {
			n, err = syscall.Read(int(fd), buf)
		}); cerr != nil {
			b.Fatalf("RawConn.Control: %v", cerr)
		}
		if err != nil {
			b.Fatalf("read %s: %v", rawConnBenchDevZero, err)
		}
		if n != len(buf) {
			b.Fatalf("short read from %s: got %d bytes, want %d", rawConnBenchDevZero, n, len(buf))
		}
	}
}

func BenchmarkDirectRead(b *testing.B) {
	fd := openDirectBenchFD(b)
	buf := make([]byte, rawConnBenchReadSize)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		n, err := syscall.Read(fd, buf)
		if err != nil {
			b.Fatalf("read %s: %v", rawConnBenchDevZero, err)
		}
		if n != len(buf) {
			b.Fatalf("short read from %s: got %d bytes, want %d", rawConnBenchDevZero, n, len(buf))
		}
	}
}

func openRawConnBenchFD(b *testing.B) syscall.RawConn {
	b.Helper()

	file, err := os.OpenFile(rawConnBenchDevZero, os.O_RDONLY, 0)
	if err != nil {
		b.Fatalf("open %s: %v", rawConnBenchDevZero, err)
	}
	b.Cleanup(func() {
		if err := file.Close(); err != nil {
			b.Fatalf("close %s: %v", rawConnBenchDevZero, err)
		}
	})

	conn, err := file.SyscallConn()
	if err != nil {
		b.Fatalf("SyscallConn: %v", err)
	}
	return conn
}

func openDirectBenchFD(b *testing.B) int {
	b.Helper()

	fd, err := syscall.Open(rawConnBenchDevZero, syscall.O_RDONLY, 0)
	if err != nil {
		b.Fatalf("open %s: %v", rawConnBenchDevZero, err)
	}
	b.Cleanup(func() {
		if err := syscall.Close(fd); err != nil {
			b.Fatalf("close %s: %v", rawConnBenchDevZero, err)
		}
	})
	return fd
}
