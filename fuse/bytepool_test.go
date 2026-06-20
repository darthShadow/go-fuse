package fuse

import (
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/internal/testutil"
)

func TestBytePool(t *testing.T) {
	size := 4
	width := 10

	bufPool := newBytePool(size, func() []byte {
		return make([]byte, width)
	})
	if got := len(bufPool.shards); got != 1 {
		t.Fatalf("default bytePool shard count invalid: got %d want %d", got, 1)
	}
	shard := bufPool.shard(0)
	if got := shard.maxRetained; got != size {
		t.Fatalf("default bytePool shard cap invalid: got %d want %d", got, size)
	}

	b := shard.Get()
	if len(b) != width {
		t.Fatalf("bytepool length invalid: got %v want %v", len(b), width)
	}

	shard.Put(b[:2])
	if got := shard.NumPooled(); got != 1 {
		t.Fatalf("bytepool should have accepted short slice with sufficient capacity: got %v want %v", got, 1)
	}

	b = shard.Get()
	if len(b) != width {
		t.Fatalf("bytepool length invalid after reuse: got %v want %v", len(b), width)
	}

	held := [][]byte{b}
	for i := 0; i < size*2; i++ {
		held = append(held, shard.Get())
	}
	if got := shard.targetRetained; got != size {
		t.Fatalf("bytepool targetRetained after burst invalid: got %d want %d", got, size)
	}
	for _, b := range held {
		shard.Put(b)
	}

	if got := shard.NumPooled(); got != size {
		t.Fatalf("bytepool retained size invalid: got %v want %v", got, size)
	}
	if got := bufPool.NumPooled(); got != size {
		t.Fatalf("manager pooled size invalid: got %v want %v", got, size)
	}
}

func TestBytePoolStaticCapSplit(t *testing.T) {
	bufPool := newBytePool(5, func() []byte {
		return make([]byte, 10)
	})
	bufPool.setShardCount(3)

	wantCaps := []int{2, 2, 1}
	if got := len(bufPool.shards); got != len(wantCaps) {
		t.Fatalf("bytePool shard count invalid: got %d want %d", got, len(wantCaps))
	}
	for i, want := range wantCaps {
		shard := bufPool.shard(i)
		if got := shard.maxRetained; got != want {
			t.Fatalf("bytePool shard[%d] cap invalid: got %d want %d", i, got, want)
		}
		if got := cap(shard.buffers); got != want {
			t.Fatalf("bytePool shard[%d] buffer storage cap invalid: got %d want %d", i, got, want)
		}
		held := make([][]byte, 0, want+2)
		for j := 0; j < want+2; j++ {
			held = append(held, shard.Get())
		}
		if got := shard.targetRetained; got != want {
			t.Fatalf("bytePool shard[%d] targetRetained after burst invalid: got %d want %d", i, got, want)
		}
		for _, b := range held {
			shard.Put(b)
		}
		if got := shard.NumPooled(); got != want {
			t.Fatalf("bytePool shard[%d] retained size invalid: got %d want %d", i, got, want)
		}
	}
	if got, want := bufPool.NumPooled(), 5; got != want {
		t.Fatalf("manager pooled size invalid: got %d want %d", got, want)
	}
}

func TestBytePoolSetShardCountTrimsRetainedBuffers(t *testing.T) {
	bufPool := newBytePool(4, func() []byte {
		return make([]byte, 10)
	})
	shard := bufPool.shard(0)
	held := make([][]byte, 0, 4)
	for i := 0; i < 4; i++ {
		held = append(held, shard.Get())
	}
	for _, b := range held {
		shard.Put(b)
	}
	if got, want := bufPool.NumPooled(), 4; got != want {
		t.Fatalf("manager pooled size before split invalid: got %d want %d", got, want)
	}

	bufPool.setShardCount(3)
	if got, want := bufPool.shard(0).maxRetained, 2; got != want {
		t.Fatalf("primary shard cap after split invalid: got %d want %d", got, want)
	}
	if got, want := bufPool.shard(0).NumPooled(), 2; got != want {
		t.Fatalf("primary shard retained size after split invalid: got %d want %d", got, want)
	}
	if got, want := bufPool.NumPooled(), 2; got != want {
		t.Fatalf("manager pooled size after split invalid: got %d want %d", got, want)
	}
}

func TestBytePoolSetShardCountShrinkDropsRemovedShards(t *testing.T) {
	maxRetained := 8
	width := 10
	bufPool := newBytePool(maxRetained, func() []byte {
		return make([]byte, width)
	})
	bufPool.setShardCount(4)

	originalShards := append([]*bytePoolShard(nil), bufPool.shards...)
	for i, shard := range originalShards {
		want := shard.maxRetained
		held := make([][]byte, 0, want)
		for j := 0; j < want; j++ {
			b := shard.Get()
			if len(b) != width {
				t.Fatalf("bytePool shard[%d] allocated length invalid: got %d want %d", i, len(b), width)
			}
			held = append(held, b)
		}
		if got := shard.targetRetained; got != want {
			t.Fatalf("bytePool shard[%d] targetRetained after burst invalid: got %d want %d", i, got, want)
		}
		for _, b := range held {
			shard.Put(b)
		}
		if got := shard.NumPooled(); got != want {
			t.Fatalf("bytePool shard[%d] retained size before shrink invalid: got %d want %d", i, got, want)
		}
	}
	if got := bufPool.NumPooled(); got != maxRetained {
		t.Fatalf("manager pooled size before shrink invalid: got %d want %d", got, maxRetained)
	}

	bufPool.setShardCount(2)
	if got, want := len(bufPool.shards), 2; got != want {
		t.Fatalf("bytePool shard count after shrink invalid: got %d want %d", got, want)
	}

	wantCaps := []int{4, 4}
	totalCap := 0
	totalPooled := 0
	for i, want := range wantCaps {
		shard := bufPool.shard(i)
		totalCap += shard.maxRetained
		totalPooled += shard.NumPooled()
		if got := shard.maxRetained; got != want {
			t.Fatalf("surviving shard[%d] cap after shrink invalid: got %d want %d", i, got, want)
		}
		if got := cap(shard.buffers); got != want {
			t.Fatalf("surviving shard[%d] buffer storage cap after shrink invalid: got %d want %d", i, got, want)
		}
	}
	if totalCap != maxRetained {
		t.Fatalf("surviving shard caps after shrink invalid: got %d want %d", totalCap, maxRetained)
	}
	if got, want := totalPooled, 4; got != want {
		t.Fatalf("surviving shard pooled size after shrink invalid: got %d want %d", got, want)
	}
	if got, want := bufPool.NumPooled(), totalPooled; got != want {
		t.Fatalf("manager pooled size after shrink invalid: got %d want %d", got, want)
	}

	for i, shard := range originalShards[2:] {
		shardIndex := i + 2
		if got := shard.maxRetained; got != 0 {
			t.Fatalf("dropped shard[%d] cap after shrink invalid: got %d want %d", shardIndex, got, 0)
		}
		if got := shard.targetRetained; got != 0 {
			t.Fatalf("dropped shard[%d] targetRetained after shrink invalid: got %d want %d", shardIndex, got, 0)
		}
		if got := shard.NumPooled(); got != 0 {
			t.Fatalf("dropped shard[%d] retained size after shrink invalid: got %d want %d", shardIndex, got, 0)
		}
		if got := cap(shard.buffers); got != 0 {
			t.Fatalf("dropped shard[%d] buffer storage cap after shrink invalid: got %d want %d", shardIndex, got, 0)
		}
	}
}

func TestBytePoolZeroCapShard(t *testing.T) {
	width := 10
	allocs := 0
	bufPool := newBytePool(0, func() []byte {
		allocs++
		return make([]byte, width)
	})
	shard := bufPool.shard(0)

	b := shard.Get()
	if len(b) != width {
		t.Fatalf("zero-cap shard allocated length invalid: got %d want %d", len(b), width)
	}
	if got := shard.inUse; got != 1 {
		t.Fatalf("zero-cap shard inUse after Get invalid: got %d want %d", got, 1)
	}
	shard.Put(b[:2])
	if got := shard.inUse; got != 0 {
		t.Fatalf("zero-cap shard inUse after Put invalid: got %d want %d", got, 0)
	}
	if got := shard.NumPooled(); got != 0 {
		t.Fatalf("zero-cap shard retained buffers invalid: got %d want %d", got, 0)
	}

	_ = shard.Get()
	if got, want := allocs, 2; got != want {
		t.Fatalf("zero-cap shard allocation count invalid: got %d want %d", got, want)
	}
}

func TestBytePoolStaticSplitMoreShardsThanCap(t *testing.T) {
	width := 10
	bufPool := newBytePool(3, func() []byte {
		return make([]byte, width)
	})
	bufPool.setShardCount(5)

	wantCaps := []int{1, 1, 1, 0, 0}
	if got := len(bufPool.shards); got != len(wantCaps) {
		t.Fatalf("bytePool shard count invalid: got %d want %d", got, len(wantCaps))
	}
	for i, want := range wantCaps {
		shard := bufPool.shard(i)
		if got := shard.maxRetained; got != want {
			t.Fatalf("bytePool shard[%d] cap invalid: got %d want %d", i, got, want)
		}
		if got := cap(shard.buffers); got != want {
			t.Fatalf("bytePool shard[%d] buffer storage cap invalid: got %d want %d", i, got, want)
		}
		b := shard.Get()
		if len(b) != width {
			t.Fatalf("bytePool shard[%d] allocated length invalid: got %d want %d", i, len(b), width)
		}
		shard.Put(b)
		if got := shard.NumPooled(); got != want {
			t.Fatalf("bytePool shard[%d] retained size invalid: got %d want %d", i, got, want)
		}
	}
	if got, want := bufPool.NumPooled(), 3; got != want {
		t.Fatalf("manager pooled size invalid: got %d want %d", got, want)
	}
}

func TestBytePoolReclaimFastAttackAndSlowDecay(t *testing.T) {
	start := time.Unix(100, 0)
	bufPool := newBytePoolWithClock(3, func() []byte {
		return make([]byte, 10)
	}, func() time.Time {
		return start
	})
	shard := bufPool.shard(0)

	held := make([][]byte, 0, 5)
	for i := 0; i < 5; i++ {
		held = append(held, shard.Get())
	}
	if got, want := shard.targetRetained, 3; got != want {
		t.Fatalf("targetRetained after fast attack invalid: got %d want %d", got, want)
	}
	for _, b := range held {
		shard.Put(b)
	}
	if got, want := shard.NumPooled(), 3; got != want {
		t.Fatalf("retained buffers before reclaim invalid: got %d want %d", got, want)
	}

	checkedOut := shard.Get()
	shard.reclaim(start.Add(5 * bytePoolReclaimInterval))

	if got, want := shard.targetRetained, 1; got != want {
		t.Fatalf("targetRetained after slow decay invalid: got %d want %d", got, want)
	}
	if got, want := shard.NumPooled(), 1; got != want {
		t.Fatalf("retained buffers after slow decay invalid: got %d want %d", got, want)
	}
	if want := start.Add(5 * bytePoolReclaimInterval); !shard.lastReclaim.Equal(want) {
		t.Fatalf("lastReclaim after slow decay invalid: got %s want %s", shard.lastReclaim, want)
	}
	shard.Put(checkedOut)
}

func TestBytePoolReclaimDelayedTickTrimsMultipleBuffers(t *testing.T) {
	start := time.Unix(200, 0)
	bufPool := newBytePoolWithClock(4, func() []byte {
		return make([]byte, 10)
	}, func() time.Time {
		return start
	})
	shard := bufPool.shard(0)

	held := make([][]byte, 0, 4)
	for i := 0; i < 4; i++ {
		held = append(held, shard.Get())
	}
	for _, b := range held {
		shard.Put(b)
	}

	reclaimAt := start.Add(2*bytePoolReclaimInterval + bytePoolReclaimInterval/2)
	shard.reclaim(reclaimAt)

	if got, want := shard.targetRetained, 2; got != want {
		t.Fatalf("targetRetained after delayed reclaim invalid: got %d want %d", got, want)
	}
	if got, want := shard.NumPooled(), 2; got != want {
		t.Fatalf("retained buffers after delayed reclaim invalid: got %d want %d", got, want)
	}
	if want := start.Add(2 * bytePoolReclaimInterval); !shard.lastReclaim.Equal(want) {
		t.Fatalf("lastReclaim after delayed reclaim invalid: got %s want %s", shard.lastReclaim, want)
	}
}

func TestBytePoolReclaimZeroInUseDecaysOnePerInterval(t *testing.T) {
	start := time.Unix(250, 0)
	bufPool := newBytePoolWithClock(5, func() []byte {
		return make([]byte, 10)
	}, func() time.Time {
		return start
	})
	shard := bufPool.shard(0)

	held := make([][]byte, 0, 5)
	for i := 0; i < 5; i++ {
		held = append(held, shard.Get())
	}
	for _, b := range held {
		shard.Put(b)
	}
	if got, want := shard.targetRetained, 5; got != want {
		t.Fatalf("targetRetained before zero-in-use reclaim invalid: got %d want %d", got, want)
	}
	if got, want := shard.inUse, 0; got != want {
		t.Fatalf("inUse before zero-in-use reclaim invalid: got %d want %d", got, want)
	}

	const intervals = 2
	reclaimAt := start.Add(intervals*bytePoolReclaimInterval + bytePoolReclaimInterval/2)
	shard.reclaim(reclaimAt)

	if got, want := shard.targetRetained, 5-intervals; got != want {
		t.Fatalf("targetRetained after zero-in-use reclaim invalid: got %d want %d", got, want)
	}
	if got, want := shard.NumPooled(), 5-intervals; got != want {
		t.Fatalf("retained buffers after zero-in-use reclaim invalid: got %d want %d", got, want)
	}
	if want := start.Add(intervals * bytePoolReclaimInterval); !shard.lastReclaim.Equal(want) {
		t.Fatalf("lastReclaim after zero-in-use reclaim invalid: got %s want %s", shard.lastReclaim, want)
	}
}

func TestBytePoolReclaimDrainsIdleShardWithoutTraffic(t *testing.T) {
	start := time.Unix(300, 0)
	bufPool := newBytePoolWithClock(3, func() []byte {
		return make([]byte, 10)
	}, func() time.Time {
		return start
	})
	shard := bufPool.shard(0)

	held := make([][]byte, 0, 3)
	for i := 0; i < 3; i++ {
		held = append(held, shard.Get())
	}
	for _, b := range held {
		shard.Put(b)
	}

	bufPool.reclaim(start.Add(3 * bytePoolReclaimInterval))

	if got := shard.targetRetained; got != 0 {
		t.Fatalf("targetRetained after idle reclaim invalid: got %d want %d", got, 0)
	}
	if got := shard.NumPooled(); got != 0 {
		t.Fatalf("retained buffers after idle reclaim invalid: got %d want %d", got, 0)
	}
}

func TestBytePoolReclaimClockRewindResetsLastReclaim(t *testing.T) {
	start := time.Unix(400, 0)
	bufPool := newBytePoolWithClock(2, func() []byte {
		return make([]byte, 10)
	}, func() time.Time {
		return start
	})
	shard := bufPool.shard(0)
	b := shard.Get()
	shard.Put(b)

	reclaimAt := start.Add(-bytePoolReclaimInterval)
	shard.reclaim(reclaimAt)

	if !shard.lastReclaim.Equal(reclaimAt) {
		t.Fatalf("lastReclaim after clock rewind invalid: got %s want %s", shard.lastReclaim, reclaimAt)
	}
	if got, want := shard.targetRetained, 1; got != want {
		t.Fatalf("targetRetained after clock rewind invalid: got %d want %d", got, want)
	}
	if got, want := shard.NumPooled(), 1; got != want {
		t.Fatalf("retained buffers after clock rewind invalid: got %d want %d", got, want)
	}
}

func TestBytePoolDrainClearsRetainedBuffersAndSetsTargetToInUse(t *testing.T) {
	bufPool := newBytePool(3, func() []byte {
		return make([]byte, 10)
	})
	shard := bufPool.shard(0)

	held := make([][]byte, 0, 3)
	for i := 0; i < 3; i++ {
		held = append(held, shard.Get())
	}
	for _, b := range held {
		shard.Put(b)
	}
	checkedOut := shard.Get()

	bufPool.drain()

	if got := shard.NumPooled(); got != 0 {
		t.Fatalf("retained buffers after drain invalid: got %d want %d", got, 0)
	}
	if got, want := shard.targetRetained, 1; got != want {
		t.Fatalf("targetRetained after drain invalid: got %d want %d", got, want)
	}
	shard.Put(checkedOut)
}

func TestBytePoolUnmountDrainsRetainedBuffers(t *testing.T) {
	const maxWrite = 4096

	srv, fd := newTestFuseFD(maxWrite)
	b := fd.readPool.Get()
	fd.readPool.Put(b)
	if got := srv.readPool.NumPooled(); got != 1 {
		t.Fatalf("readPool retained size before unmount invalid: got %d want %d", got, 1)
	}

	srv.readPool.startReclaimer()
	if err := srv.Unmount(); err != nil {
		t.Fatalf("Unmount returned error: %v", err)
	}
	srv.waitLoops()

	if got := srv.readPool.NumPooled(); got != 0 {
		t.Fatalf("readPool retained size after unmount invalid: got %d want %d", got, 0)
	}
}

func TestBytePoolStopReclaimerAndDrainAreIdempotent(t *testing.T) {
	bufPool := newBytePool(2, func() []byte {
		return make([]byte, 10)
	})
	shard := bufPool.shard(0)
	b := shard.Get()
	shard.Put(b)

	bufPool.startReclaimer()
	bufPool.reclaimerMu.Lock()
	done := bufPool.reclaimerDone
	bufPool.reclaimerMu.Unlock()
	if done == nil {
		t.Fatal("reclaimerDone is nil after startReclaimer")
	}

	bufPool.stopReclaimer()
	select {
	case <-done:
	default:
		t.Fatal("reclaimerDone was not closed after stopReclaimer returned")
	}

	bufPool.drain()
	bufPool.stopReclaimer()
	bufPool.drain()
	if got := bufPool.NumPooled(); got != 0 {
		t.Fatalf("retained buffers after repeated stop/drain invalid: got %d want %d", got, 0)
	}
}

func TestBytePoolRequestHandler(t *testing.T) {
	mnt := t.TempDir()
	opts := &MountOptions{
		Debug: testutil.VerboseTest(),
	}

	rfs := readFS{}
	srv, err := NewServer(&rfs, mnt, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Unmount() })
	go srv.Serve()
	if err := srv.WaitMount(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.ReadFile(mnt + "/file"); err != nil {
		t.Fatal(err)
	}

	// The last FreeBuffer happens after returning OK for the read, so thread
	// scheduling may cause it to occur after the count check. Unmount to be sure
	// all work has finished.
	srv.Unmount()
	if count := srv.readPool.NumPooled(); count != 0 {
		t.Errorf("readPool retained buffers after unmount: got %d want %d", count, 0)
	}
}

func TestBytePoolUnmountStopsReclaimerAfterRealMount(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("real FUSE mount lifecycle test is Linux-only")
	}

	mnt := t.TempDir()
	opts := &MountOptions{
		Debug: testutil.VerboseTest(),
	}

	rfs := readFS{}
	srv, err := NewServer(&rfs, mnt, opts)
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan struct{})
	t.Cleanup(func() {
		_ = srv.Unmount()
		select {
		case <-serveDone:
		case <-time.After(5 * time.Second):
			t.Error("Serve did not exit after cleanup unmount")
		}
	})
	go func() {
		srv.Serve()
		close(serveDone)
	}()
	if err := srv.WaitMount(); err != nil {
		t.Fatal(err)
	}

	reclaimerDone := waitBytePoolReclaimerDoneSignal(t, &srv.readPool)
	for attempts := 0; attempts < 5 && srv.readPool.NumPooled() == 0; attempts++ {
		if _, err := os.ReadFile(mnt + "/file"); err != nil {
			t.Fatal(err)
		}
		runtime.Gosched()
	}
	if got := srv.readPool.NumPooled(); got == 0 {
		t.Fatalf("readPool retained buffers before unmount invalid: got %d want > 0", got)
	}

	if err := srv.Unmount(); err != nil {
		t.Fatalf("Unmount returned error: %v", err)
	}
	srv.waitLoops()

	if got := srv.readPool.NumPooled(); got != 0 {
		t.Fatalf("readPool retained buffers after real unmount invalid: got %d want %d", got, 0)
	}
	select {
	case <-reclaimerDone:
	default:
		t.Fatal("reclaimerDone was not closed after real unmount returned")
	}
	select {
	case <-serveDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not exit after real unmount")
	}
}

func waitBytePoolReclaimerDoneSignal(t *testing.T, bufPool *bytePool) <-chan struct{} {
	t.Helper()

	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		bufPool.reclaimerMu.Lock()
		done := bufPool.reclaimerDone
		bufPool.reclaimerMu.Unlock()
		if done != nil {
			return done
		}

		select {
		case <-deadline.C:
			t.Fatal("reclaimerDone was not set after Serve started")
		case <-ticker.C:
		}
	}
}

func newTestFuseFD(maxWrite int) (*Server, *fuseFD) {
	srv, fds := newTestFuseFDs(maxWrite, 1)
	return srv, fds[0]
}

func newTestFuseFDs(maxWrite int, fdCount int) (*Server, []*fuseFD) {
	_, readBufBytes, _ := requestAccountingSizes(maxWrite)
	srv := &Server{
		opts: &MountOptions{MaxWrite: maxWrite},
	}
	srv.reqPool.New = func() interface{} {
		return &requestAlloc{
			request: request{
				cancel: make(chan struct{}),
			},
		}
	}
	srv.readPool.init(readPoolMaxRetainedBuffers, func() []byte {
		return make([]byte, readBufBytes)
	}, time.Now)
	fds := make([]*fuseFD, 0, fdCount)
	for i := 0; i < fdCount; i++ {
		fds = append(fds, srv.newFuseFD(-1))
	}
	srv.fuseFDs = fds
	srv.readPool.bindFDs(srv.fuseFDs)
	return srv, fds
}

func TestBytePoolBindFDsAssignsPerFDShards(t *testing.T) {
	const maxWrite = 4096

	srv, fds := newTestFuseFDs(maxWrite, 3)
	seen := make(map[*bytePoolShard]bool, len(fds))
	for i, fd := range fds {
		if fd.readPool == nil {
			t.Fatalf("fuseFD[%d] readPool shard is nil", i)
		}
		if got, want := fd.readPool, srv.readPool.shard(i); got != want {
			t.Fatalf("fuseFD[%d] readPool shard invalid: got %p want %p", i, got, want)
		}
		if seen[fd.readPool] {
			t.Fatalf("fuseFD[%d] reused a readPool shard; want distinct shards", i)
		}
		seen[fd.readPool] = true
	}
}

func TestBytePoolBindFDsAssignsZeroCapShardsPastRetainedBudget(t *testing.T) {
	const maxWrite = 4096
	const extraFDs = 3

	srv, fds := newTestFuseFDs(maxWrite, readPoolMaxRetainedBuffers+extraFDs)
	if got, want := len(srv.readPool.shards), len(fds); got != want {
		t.Fatalf("readPool shard count invalid: got %d want %d", got, want)
	}

	for i, fd := range fds {
		if got, want := fd.readPool, srv.readPool.shard(i); got != want {
			t.Fatalf("fuseFD[%d] readPool shard invalid: got %p want %p", i, got, want)
		}

		first := fd.readPool.Get()
		if got, want := len(first), fd.readBufBytes; got != want {
			t.Fatalf("fuseFD[%d] read buffer length invalid: got %d want %d", i, got, want)
		}
		fd.readPool.Put(first[:1])

		if i < readPoolMaxRetainedBuffers {
			if got, want := fd.readPool.maxRetained, 1; got != want {
				t.Fatalf("fuseFD[%d] retained cap invalid: got %d want %d", i, got, want)
			}
			if got, want := fd.readPool.NumPooled(), 1; got != want {
				t.Fatalf("fuseFD[%d] retained buffers invalid: got %d want %d", i, got, want)
			}
			continue
		}

		if got, want := fd.readPool.maxRetained, 0; got != want {
			t.Fatalf("fuseFD[%d] retained cap invalid: got %d want %d", i, got, want)
		}
		if got, want := cap(fd.readPool.buffers), 0; got != want {
			t.Fatalf("fuseFD[%d] retained storage cap invalid: got %d want %d", i, got, want)
		}
		if got, want := fd.readPool.NumPooled(), 0; got != want {
			t.Fatalf("fuseFD[%d] retained buffers invalid: got %d want %d", i, got, want)
		}

		second := fd.readPool.Get()
		if got, want := len(second), fd.readBufBytes; got != want {
			t.Fatalf("fuseFD[%d] second read buffer length invalid: got %d want %d", i, got, want)
		}
		if &first[0] == &second[0] {
			t.Fatalf("fuseFD[%d] zero-cap shard reused a retained buffer; want fresh allocation", i)
		}
		fd.readPool.Put(second[:1])
		if got, want := fd.readPool.NumPooled(), 0; got != want {
			t.Fatalf("fuseFD[%d] retained buffers after second put invalid: got %d want %d", i, got, want)
		}
	}

	if got := srv.readPool.NumPooled(); got > readPoolMaxRetainedBuffers {
		t.Fatalf("server readPool retained too many buffers: got %d want <= %d", got, readPoolMaxRetainedBuffers)
	}
}

func TestBytePoolConcurrentMultiFDContention(t *testing.T) {
	const maxWrite = 4096
	const workersPerFD = 4
	const iterations = 200

	srv, fds := newTestFuseFDs(maxWrite, 4)
	type poolErr struct {
		fdIndex int
		worker  int
		iter    int
		got     int
		want    int
	}
	errCh := make(chan poolErr, 1)
	recordErr := func(err poolErr) {
		select {
		case errCh <- err:
		default:
		}
	}

	stopReclaim := make(chan struct{})
	var reclaimWG sync.WaitGroup
	reclaimWG.Add(1)
	go func() {
		defer reclaimWG.Done()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case now := <-ticker.C:
				srv.readPool.reclaim(now)
			case <-stopReclaim:
				return
			}
		}
	}()

	var wg sync.WaitGroup
	for fdIndex, fd := range fds {
		fdIndex, fd := fdIndex, fd
		for worker := 0; worker < workersPerFD; worker++ {
			worker := worker
			wg.Add(1)
			go func() {
				defer wg.Done()
				held := make([][]byte, 0, 8)
				for iter := 0; iter < iterations; iter++ {
					b := fd.readPool.Get()
					if got, want := len(b), fd.readBufBytes; got != want {
						recordErr(poolErr{fdIndex: fdIndex, worker: worker, iter: iter, got: got, want: want})
					}
					if (iter+worker+fdIndex)%4 == 0 {
						held = append(held, b)
						if len(held) < cap(held) {
							continue
						}
					} else {
						fd.readPool.Put(b[:1])
					}
					for _, heldBuf := range held {
						fd.readPool.Put(heldBuf[:1])
					}
					held = held[:0]
				}
				for _, heldBuf := range held {
					fd.readPool.Put(heldBuf[:1])
				}
			}()
		}
	}
	wg.Wait()
	close(stopReclaim)
	reclaimWG.Wait()

	select {
	case err := <-errCh:
		t.Fatalf("buffer length mismatch on fd %d worker %d iteration %d: got %d want %d", err.fdIndex, err.worker, err.iter, err.got, err.want)
	default:
	}
	for i, fd := range fds {
		shard := fd.readPool
		shard.mu.Lock()
		inUse := shard.inUse
		pooled := len(shard.buffers)
		targetRetained := shard.targetRetained
		maxRetained := shard.maxRetained
		shard.mu.Unlock()

		if got, want := inUse, 0; got != want {
			t.Fatalf("fuseFD[%d] shard inUse after concurrent contention invalid: got %d want %d", i, got, want)
		}
		if targetRetained > maxRetained {
			t.Fatalf("fuseFD[%d] shard targetRetained cap invariant invalid: got %d want <= %d", i, targetRetained, maxRetained)
		}
		if pooled > targetRetained {
			t.Fatalf("fuseFD[%d] shard pooled target invariant invalid: got %d want <= %d", i, pooled, targetRetained)
		}
	}
}

func TestFuseFDPutReadBufReturnsToOriginShard(t *testing.T) {
	const maxWrite = 4096

	srv, fds := newTestFuseFDs(maxWrite, 2)
	fd := fds[0]
	otherFD := fds[1]
	readBuf := fd.readPool.Get()
	fd.inflightRequestBytes = fd.readBufBytes

	fd.putReadBuf(readBuf[:2])

	if got := fd.inflightRequestBytes; got != 0 {
		t.Fatalf("inflightRequestBytes after putReadBuf invalid: got %d want %d", got, 0)
	}
	if got := srv.readPool.NumPooled(); got != 1 {
		t.Fatalf("readPool retained size after putReadBuf invalid: got %d want %d", got, 1)
	}
	if got := fd.readPool.NumPooled(); got != 1 {
		t.Fatalf("origin shard retained size after putReadBuf invalid: got %d want %d", got, 1)
	}
	if got := otherFD.readPool.NumPooled(); got != 0 {
		t.Fatalf("non-origin shard retained size after putReadBuf invalid: got %d want %d", got, 0)
	}
}

func TestReturnRequestReleasesGobbledReadBufferToOriginShard(t *testing.T) {
	const maxWrite = 4096

	srv, fds := newTestFuseFDs(maxWrite, 2)
	fd := fds[0]
	otherFD := fds[1]
	req := srv.reqPool.Get().(*requestAlloc)
	readBuf := fd.readPool.Get()
	fd.inflightRequestBytes = fd.requestBytes()

	gobbled := req.setInput(readBuf[:cap(req.smallInputBuf)])
	if !gobbled {
		t.Fatalf("setInput did not gobble input: got %v want %v", gobbled, true)
	}
	if got := srv.readPool.NumPooled(); got != 0 {
		t.Fatalf("readPool retained size before return invalid: got %d want %d", got, 0)
	}

	fd.returnRequest(req)

	if got := fd.inflightRequestBytes; got != 0 {
		t.Fatalf("inflightRequestBytes after return invalid: got %d want %d", got, 0)
	}
	if got := srv.readPool.NumPooled(); got != 1 {
		t.Fatalf("readPool retained size after gobbled request return invalid: got %d want %d", got, 1)
	}
	if got := fd.readPool.NumPooled(); got != 1 {
		t.Fatalf("origin shard retained size after gobbled request return invalid: got %d want %d", got, 1)
	}
	if got := otherFD.readPool.NumPooled(); got != 0 {
		t.Fatalf("non-origin shard retained size after gobbled request return invalid: got %d want %d", got, 0)
	}
}

func TestFuseFDReserveRequestBytesAllowsSingleRequestBelowBudget(t *testing.T) {
	const maxWrite = 4096

	srv, fd := newTestFuseFD(maxWrite)
	srv.opts.MaxInflightRequestBytes = 1

	if ok := fd.reserveRequestBytes(); !ok {
		t.Fatal("first reservation failed below request size budget")
	}
	if ok := fd.reserveRequestBytes(); ok {
		t.Fatal("second reservation succeeded below request size budget")
	}
}

func TestFuseFDReserveRequestBytesIsPerFD(t *testing.T) {
	const maxWrite = 4096

	srv, fds := newTestFuseFDs(maxWrite, 3)
	primary := fds[0]
	requestBytes := primary.requestBytes()
	srv.opts.MaxInflightRequestBytes = requestBytes

	total := 0
	for i, fd := range srv.fuseFDs {
		if ok := fd.reserveRequestBytes(); !ok {
			t.Fatalf("reserveRequestBytes failed on fuseFD[%d]", i)
		}
		total += fd.inflightRequestBytes
	}
	if got, want := total, len(srv.fuseFDs)*requestBytes; got != want {
		t.Fatalf("total inflightRequestBytes invalid: got %d want %d", got, want)
	}
	if ok := srv.fuseFDs[0].reserveRequestBytes(); ok {
		t.Fatal("second reservation on one fd succeeded; want per-fd ceiling")
	}
}
