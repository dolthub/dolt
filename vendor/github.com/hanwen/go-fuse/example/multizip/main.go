// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/hanwen/go-fuse/zipfs"
)

func main() {
	// Scans the arg list and sets up flags
	debug := flag.Bool("debug", false, "debug on")
	flag.Parse()
	if flag.NArg() < 1 {
		_, prog := filepath.Split(os.Args[0])
		fmt.Printf("usage: %s MOUNTPOINT\n", prog)
		os.Exit(2)
	}

	fs := zipfs.NewMultiZipFs()
	nfs := pathfs.NewPathNodeFs(fs, nil)
	opts := nodefs.NewOptions()
	opts.Debug = *debug
	state, _, err := nodefs.MountRoot(flag.Arg(0), nfs.Root(), opts)
	if err != nil {
		fmt.Printf("Mount fail: %v\n", err)
		os.Exit(1)
	}

	state.Serve()
}
