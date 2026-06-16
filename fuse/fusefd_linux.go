// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fuse

import (
	"os"
	"syscall"
	"unsafe"
)

const useSingleReader = false

// _FUSE_DEV_IOC_CLONE is _IOR(229, 0, uint32) -- see linux/fuse.h.
// Available since Linux 4.2. The argument is the existing FUSE fd to
// clone (i.e. bind into the same session).
const _FUSE_DEV_IOC_CLONE = 0x8004E500

// cloneFuseFD opens a fresh /dev/fuse fd and binds it to the session
// of src via FUSE_DEV_IOC_CLONE. The returned fd has its own kernel
// queue but shares session state with src.
func cloneFuseFD(src int) (int, error) {
	fd, err := syscall.Open("/dev/fuse", syscall.O_RDWR|syscall.O_CLOEXEC, 0)
	if err != nil {
		return -1, os.NewSyscallError("open /dev/fuse", err)
	}
	srcFd := uint32(src)
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), uintptr(_FUSE_DEV_IOC_CLONE), uintptr(unsafe.Pointer(&srcFd)))
	if errno != 0 {
		syscall.Close(fd)
		return -1, os.NewSyscallError("ioctl FUSE_DEV_IOC_CLONE", errno)
	}
	return fd, nil
}

func (r *fuseFD) write(req *request) Status {
	if req.outPayloadSize() == 0 {
		err := handleEINTR(func() error {
			_, err := writev(r.fd, [][]byte{req.outHeaderBuf, req.outDataBuf})
			return err
		})
		return ToStatus(err)
	}
	if req.readResult != nil {
		defer req.readResult.Done()
		if r.server.canSplice {
			err := r.trySplice(req, req.readResult)
			if err == nil {
				return OK
			}
			if err != errRecoverSplice {
				r.server.opts.Logger.Println("trySplice:", err)
			}
		}

		req.outPayload, req.status = req.readResult.Bytes(req.outPayload)
		req.serializeHeader(len(req.outPayload))
	}

	_, err := writev(r.fd, [][]byte{req.outHeaderBuf, req.outDataBuf, req.outPayload})
	return ToStatus(err)
}
