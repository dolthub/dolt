// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/hanwen/go-fuse/unionfs"
)

func main() {
	debug := flag.Bool("debug", false, "debug on")
	hardlinks := flag.Bool("hardlinks", false, "support hardlinks")
	delcache_ttl := flag.Float64("deletion_cache_ttl", 5.0, "Deletion cache TTL in seconds.")
	branchcache_ttl := flag.Float64("branchcache_ttl", 5.0, "Branch cache TTL in seconds.")
	deldirname := flag.String(
		"deletion_dirname", "GOUNIONFS_DELETIONS", "Directory name to use for deletions.")
	hide_readonly_link := flag.Bool("hide_readonly_link", true,
		"Hides READONLY link from the top mountpoints. "+
			"Enabled by default.")
	portableInodes := flag.Bool("portable-inodes", false,
		"Use sequential 32-bit inode numbers.")

	flag.Parse()

	if len(flag.Args()) < 2 {
		fmt.Println("Usage:\n  main MOUNTPOINT BASEDIR")
		os.Exit(2)
	}
	ufsOptions := unionfs.UnionFsOptions{
		DeletionCacheTTL: time.Duration(*delcache_ttl * float64(time.Second)),
		BranchCacheTTL:   time.Duration(*branchcache_ttl * float64(time.Second)),
		DeletionDirName:  *deldirname,
	}
	options := unionfs.AutoUnionFsOptions{
		UnionFsOptions: ufsOptions,
		Options: nodefs.Options{
			EntryTimeout:    time.Second,
			AttrTimeout:     time.Second,
			NegativeTimeout: time.Second,
			Owner:           fuse.CurrentOwner(),
			Debug:           *debug,
		},
		UpdateOnMount: true,
		PathNodeFsOptions: pathfs.PathNodeFsOptions{
			ClientInodes: *hardlinks,
		},
		HideReadonly: *hide_readonly_link,
	}
	fsOpts := nodefs.Options{
		PortableInodes: *portableInodes,
		Debug:          *debug,
	}
	gofs := unionfs.NewAutoUnionFs(flag.Arg(1), options)
	pathfs := pathfs.NewPathNodeFs(gofs, &pathfs.PathNodeFsOptions{
		Debug: *debug,
	})
	state, _, err := nodefs.MountRoot(flag.Arg(0), pathfs.Root(), &fsOpts)
	if err != nil {
		fmt.Printf("Mount fail: %v\n", err)
		os.Exit(1)
	}

	state.Serve()
	time.Sleep(1 * time.Second)
}
