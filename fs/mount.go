// Copyright 2019 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fs

import (
	"github.com/hanwen/go-fuse/v2/fuse"
)

// Mount mounts the given NodeFS on the directory, and starts serving
// requests. This is a convenience wrapper around NewNodeFS and
// fuse.NewServer.  If nil is given as options, default settings are
// applied, which are 1 second entry and attribute timeout.
// Mount stops the NewNodeFS background compactor if server creation or mounting
// fails before normal unmount.
func Mount(dir string, root InodeEmbedder, options *Options) (*fuse.Server, error) {
	rawFS := NewNodeFS(root, options)
	var mountOptions *fuse.MountOptions
	if options != nil {
		mountOptions = &options.MountOptions
	}
	server, err := fuse.NewServer(rawFS, dir, mountOptions)
	if err != nil {
		rawFS.(*rawBridge).stopNodeMapCompactor()
		return nil, err
	}

	go server.Serve()
	if err := server.WaitMount(); err != nil {
		// we don't shutdown the serve loop. If the mount does
		// not succeed, the loop won't work and exit.
		rawFS.(*rawBridge).stopNodeMapCompactor()
		return nil, err
	}

	return server, nil
}
