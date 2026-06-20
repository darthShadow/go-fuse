package fuse

import (
	"io"
	"log"
	"testing"
	"time"
	"unsafe"
)

type blockingForgetFS struct {
	RawFileSystem

	entered chan struct{}
	release chan struct{}
}

func newBlockingForgetFS() *blockingForgetFS {
	return &blockingForgetFS{
		RawFileSystem: NewDefaultRawFileSystem(),
		entered:       make(chan struct{}, 1),
		release:       make(chan struct{}),
	}
}

func (fs *blockingForgetFS) Forget(nodeID, nlookup uint64) {
	fs.entered <- struct{}{}
	<-fs.release
}

func TestReadPoolTeardownDropsLateSingleReaderReturn(t *testing.T) {
	const maxWrite = 4096

	fs := newBlockingForgetFS()
	opts := &MountOptions{
		MaxWrite: maxWrite,
		Logger:   log.New(io.Discard, "", 0),
	}
	opts.setDefaults(fs)

	readBufSize, readBufBytes, reqAllocBytes := requestAccountingSizes(opts.MaxWrite)
	srv := &Server{
		protocolServer: protocolServer{
			fileSystem:  fs,
			retrieveTab: make(map[uint64]*retrieveCacheRequest),
			opts:        opts,
		},
		opts:         opts,
		maxReaders:   1,
		singleReader: true,
	}
	srv.reqPool.New = func() interface{} {
		return &requestAlloc{
			request: request{
				cancel: make(chan struct{}),
			},
		}
	}
	srv.initReadPool(readBufSize, readBufBytes)
	fd := &fuseFD{
		server:        srv,
		reqPool:       &srv.reqPool,
		readPool:      srv.readPool.shard(0),
		buffers:       &srv.buffers,
		reqAllocBytes: reqAllocBytes,
		readBufBytes:  readBufBytes,
	}
	srv.fuseFDs = []*fuseFD{fd}

	req := srv.reqPool.Get().(*requestAlloc)
	readBuf := fd.readPool.Get()
	fd.inflightRequestBytes = fd.requestBytes()
	forget := ForgetIn{
		InHeader: InHeader{
			Length: uint32(unsafe.Sizeof(ForgetIn{})),
			Opcode: _OP_FORGET,
			Unique: 1,
			NodeId: FUSE_ROOT_ID,
		},
		Nlookup: 1,
	}
	input := readBuf[:cap(req.smallInputBuf)]
	clear(input)
	copy(input, unsafe.Slice((*byte)(unsafe.Pointer(&forget)), int(unsafe.Sizeof(forget))))
	if !req.setInput(input) {
		t.Fatalf("request input ownership invalid: got copied input, want pooled buffer ownership")
	}

	handlerDone := make(chan Status, 1)
	go func() {
		handlerDone <- srv.handleRequest(fd, req)
	}()

	select {
	case <-fs.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("singleReader handler did not enter Forget before timeout")
	}

	srv.stopAndDrainReadPool()
	if got := srv.readPool.NumPooled(); got != 0 {
		t.Fatalf("readPool retained size after teardown with blocked handler invalid: got %d want %d", got, 0)
	}

	close(fs.release)
	select {
	case code := <-handlerDone:
		if !code.Ok() {
			t.Fatalf("handleRequest returned status %v, want OK", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("singleReader handler did not return after release")
	}

	if got := srv.readPool.NumPooled(); got != 0 {
		t.Fatalf("readPool retained size after late handler return invalid: got %d want %d", got, 0)
	}
	if got := fd.inflightRequestBytes; got != 0 {
		t.Fatalf("inflightRequestBytes after late handler return invalid: got %d want %d", got, 0)
	}
}
