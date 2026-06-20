// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fuse

import (
	"errors"
	"testing"

	"github.com/hanwen/go-fuse/v2/splice"
)

type statefulTestReadResult struct{}

func (statefulTestReadResult) Bytes(buf []byte) ([]byte, Status) {
	return buf[:0], OK
}

func (statefulTestReadResult) Size() int {
	return 0
}

func (statefulTestReadResult) Done() {}

func (statefulTestReadResult) Stateful() (uintptr, int) {
	return 0, 0
}

func TestCanSpliceReadResult(t *testing.T) {
	tests := []struct {
		name       string
		readResult ReadResult
		want       bool
	}{
		{
			name:       "nil",
			readResult: nil,
			want:       false,
		},
		{
			name:       "slice backed data",
			readResult: ReadResultData([]byte("data")),
			want:       false,
		},
		{
			name:       "seekable fd",
			readResult: ReadResultFd(0, 0, 1),
			want:       true,
		},
		{
			name:       "stateful fd",
			readResult: statefulTestReadResult{},
			want:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := canSpliceReadResult(tt.readResult); got != tt.want {
				t.Fatalf("canSpliceReadResult(%T) = %v, want %v", tt.readResult, got, tt.want)
			}
		})
	}
}

func TestTrySpliceReadResultDataSkipsPipeAcquire(t *testing.T) {
	originalGetSplicePair := getSplicePair
	t.Cleanup(func() {
		getSplicePair = originalGetSplicePair
	})

	acquireErr := errors.New("unexpected splice pair acquire")
	acquires := 0
	getSplicePair = func() (*splice.Pair, error) {
		acquires++
		return nil, acquireErr
	}

	var fd fuseFD
	err := fd.trySplice(&request{}, ReadResultData([]byte("data")))
	if err != errRecoverSplice {
		t.Fatalf("trySplice(ReadResultData) error = %v, want %v", err, errRecoverSplice)
	}
	if acquires != 0 {
		t.Fatalf("splice pair acquired %d times for ReadResultData, want 0", acquires)
	}
}
