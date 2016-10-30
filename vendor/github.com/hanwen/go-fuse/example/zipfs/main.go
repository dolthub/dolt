// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/zipfs"
)

func main() {
	// Scans the arg list and sets up flags
	debug := flag.Bool("debug", false, "print debugging messages.")
	profile := flag.String("profile", "", "record cpu profile.")
	mem_profile := flag.String("mem-profile", "", "record memory profile.")
	command := flag.String("run", "", "run this command after mounting.")
	ttl := flag.Float64("ttl", 1.0, "attribute/entry cache TTL.")
	flag.Parse()
	if flag.NArg() < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s MOUNTPOINT ZIP-FILE\n", os.Args[0])
		os.Exit(2)
	}

	var profFile, memProfFile io.Writer
	var err error
	if *profile != "" {
		profFile, err = os.Create(*profile)
		if err != nil {
			log.Fatalf("os.Create: %v", err)
		}
	}
	if *mem_profile != "" {
		memProfFile, err = os.Create(*mem_profile)
		if err != nil {
			log.Fatalf("os.Create: %v", err)
		}
	}

	root, err := zipfs.NewArchiveFileSystem(flag.Arg(1))
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewArchiveFileSystem failed: %v\n", err)
		os.Exit(1)
	}

	opts := &nodefs.Options{
		AttrTimeout:  time.Duration(*ttl * float64(time.Second)),
		EntryTimeout: time.Duration(*ttl * float64(time.Second)),
		Debug:        *debug,
	}
	state, _, err := nodefs.MountRoot(flag.Arg(0), root, opts)
	if err != nil {
		fmt.Printf("Mount fail: %v\n", err)
		os.Exit(1)
	}

	runtime.GC()
	if profFile != nil {
		pprof.StartCPUProfile(profFile)
		defer pprof.StopCPUProfile()
	}

	if *command != "" {
		args := strings.Split(*command, " ")
		cmd := exec.Command(args[0], args[1:]...)
		cmd.Stdout = os.Stdout
		cmd.Start()
	}

	state.Serve()
	if memProfFile != nil {
		pprof.WriteHeapProfile(memProfFile)
	}
}
