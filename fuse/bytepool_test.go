package fuse

import (
	"os"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/internal/testutil"
)

type manualBytePoolClock struct {
	now time.Time
}

func newManualBytePoolClock() *manualBytePoolClock {
	return &manualBytePoolClock{now: time.Unix(0, 0)}
}

func (c *manualBytePoolClock) Now() time.Time {
	return c.now
}

func (c *manualBytePoolClock) Advance(d time.Duration) {
	c.now = c.now.Add(d)
}

func TestBytePool(t *testing.T) {
	size := 4
	width := 10
	clock := newManualBytePoolClock()

	bufPool := newBytePoolWithClock(size, func() interface{} {
		return make([]byte, width)
	}, clock.Now)

	b := bufPool.Get()
	if len(b) != width {
		t.Fatalf("bytepool length invalid: got %v want %v", len(b), width)
	}

	bufPool.Put(b[:2])
	if got := bufPool.NumPooled(); got != 1 {
		t.Fatalf("bytepool should have accepted short slice with sufficient capacity: got %v want %v", got, 1)
	}

	b = bufPool.Get()
	if len(b) != width {
		t.Fatalf("bytepool length invalid after reuse: got %v want %v", len(b), width)
	}

	held := [][]byte{b}
	for i := 0; i < size*2; i++ {
		held = append(held, bufPool.Get())
	}
	for _, b := range held {
		bufPool.Put(b)
	}

	if got := bufPool.NumPooled(); got != size {
		t.Fatalf("bytepool retained size invalid: got %v want %v", got, size)
	}
}

func TestBytePoolReclaimsWhileServing(t *testing.T) {
	maxRetained := 8
	workingSet := 2
	width := 10
	clock := newManualBytePoolClock()

	bufPool := newBytePoolWithClock(maxRetained, func() interface{} {
		return make([]byte, width)
	}, clock.Now)

	burst := make([][]byte, 0, maxRetained)
	for i := 0; i < maxRetained; i++ {
		burst = append(burst, bufPool.Get())
	}
	for _, b := range burst {
		bufPool.Put(b)
	}
	if got := bufPool.NumPooled(); got != maxRetained {
		t.Fatalf("bytepool burst retained size invalid: got %v want %v", got, maxRetained)
	}

	active := make([][]byte, 0, workingSet)
	for i := 0; i < workingSet; i++ {
		active = append(active, bufPool.Get())
	}
	if got, want := bufPool.NumPooled(), maxRetained-workingSet; got != want {
		t.Fatalf("bytepool retained size after steady gets invalid: got %v want %v", got, want)
	}

	for i := 0; i < maxRetained-workingSet+2; i++ {
		clock.Advance(bytePoolReclaimInterval)
		bufPool.Put(active[0])
		active[0] = bufPool.Get()
	}

	if got := bufPool.NumPooled(); got > workingSet {
		t.Fatalf("bytepool did not reclaim toward active working set while serving: got %v want <= %v", got, workingSet)
	}
	for _, b := range active {
		bufPool.Put(b)
	}
}

func TestBytePoolReclaimsTowardEmpty(t *testing.T) {
	maxRetained := 4
	width := 10
	clock := newManualBytePoolClock()

	bufPool := newBytePoolWithClock(maxRetained, func() interface{} {
		return make([]byte, width)
	}, clock.Now)

	burst := make([][]byte, 0, maxRetained)
	for i := 0; i < maxRetained; i++ {
		burst = append(burst, bufPool.Get())
	}
	for _, b := range burst {
		bufPool.Put(b)
	}
	if got := bufPool.NumPooled(); got != maxRetained {
		t.Fatalf("bytepool burst retained size invalid: got %v want %v", got, maxRetained)
	}

	for i := 0; i < maxRetained; i++ {
		b := bufPool.Get()
		clock.Advance(bytePoolReclaimInterval)
		bufPool.Put(b)
	}
	if got := bufPool.NumPooled(); got != 0 {
		t.Fatalf("bytepool did not reclaim toward empty: got %v want %v", got, 0)
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
	if count := srv.readPool.NumPooled(); count > readPoolMaxRetainedBuffers {
		t.Errorf("readPool retained too many buffers: got %d want <= %d", count, readPoolMaxRetainedBuffers)
	}
}

func newTestFuseFD(maxWrite int) (*Server, *fuseFD) {
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
	srv.readPool = newBytePool(readPoolMaxRetainedBuffers, func() interface{} {
		return make([]byte, readBufBytes)
	})
	fd := srv.newFuseFD(-1)
	return srv, fd
}

func TestReturnRequestReleasesGobbledReadBuffer(t *testing.T) {
	const maxWrite = 4096

	srv, fd := newTestFuseFD(maxWrite)
	req := srv.reqPool.Get().(*requestAlloc)
	readBuf := srv.readPool.Get()
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

	srv, primary := newTestFuseFD(maxWrite)
	requestBytes := primary.requestBytes()
	srv.opts.MaxInflightRequestBytes = requestBytes
	srv.fuseFDs = []*fuseFD{
		primary,
		srv.newFuseFD(-1),
		srv.newFuseFD(-1),
	}

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
