package fuse

import (
	"os"
	"testing"

	"github.com/hanwen/go-fuse/v2/internal/testutil"
)

func TestBytePool(t *testing.T) {
	var size = 4
	var width = 10

	bufPool := newBytePool(size, func() interface{} {
		return make([]byte, width)
	})

	// Check that retrieved buffer are of the expected width
	b := bufPool.Get()
	if len(b) != width {
		t.Fatalf("bytepool length invalid: got %v want %v", len(b), width)
	}

	// Try putting a short slice into pool
	bufPool.Put(make([]byte, width)[:2])
	if len(bufPool.channel) != 1 {
		t.Fatal("bytepool should have accepted short slice with sufficient capacity")
	}

	b = bufPool.Get()
	if len(b) != width {
		t.Fatalf("bytepool length invalid: got %v want %v", len(b), width)
	}

	// Fill the pool beyond the capped pool size.
	for i := 0; i < size*2; i++ {
		bufPool.Put(make([]byte, width))
	}

	// Close the channel so we can iterate over it.
	close(bufPool.channel)

	// Check the size of the pool.
	if bufPool.NumPooled() != size {
		t.Fatalf("bytepool size invalid: got %v want %v", len(bufPool.channel), size)
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

	// The last FreeBuffer happens after returning OK for the
	// read, so thread scheduling may cause it to happen after we
	// check.  Unmount to be sure we have finished all the work.
	srv.Unmount()
	count := srv.readPool.NumPooled()
	if count == 0 {
		t.Errorf("expected %d buffers, got %d", 1, count)
	}

	// TODO: test write as well?
}
