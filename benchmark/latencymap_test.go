// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package benchmark

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestLatencyMapConcurrentAddGet needs -race: the race detector, not the
// final count check, catches Get racing Add.
func TestLatencyMapConcurrentAddGet(t *testing.T) {
	m := NewLatencyMap()
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 1000; i++ {
			m.Add("op", time.Millisecond)
			runtime.Gosched()
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 1000; i++ {
			m.Get("op")
			runtime.Gosched()
		}
	}()
	close(start)
	wg.Wait()
	if count, dur := m.Get("op"); count != 1000 || dur != time.Second {
		t.Fatalf("Get(op) = (%d, %v), want (1000, %v)", count, dur, time.Second)
	}
}

func TestLatencyMap(t *testing.T) {
	m := NewLatencyMap()
	m.Add("foo", 100*time.Millisecond)
	m.Add("foo", 200*time.Millisecond)
	c, d := m.Get("foo")
	if c != 2 || d != 300*time.Millisecond {
		t.Errorf("got %v, %d, want 2, 150ms", c, d)
	}
}
