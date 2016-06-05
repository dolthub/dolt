// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package util

import (
	"flag"
	"os"
	"runtime/pprof"

	"github.com/attic-labs/noms/go/d"
)

var (
	cpuProfile = flag.String("cpuprofile", "", "write cpu profile to file")
	memProfile = flag.String("memprofile", "", "write memory profile to this file")
)

// MaybeStartCPUProfile checks the -cpuprofile flag and, if it is set, attempts to start writing CPU profile data to the named file. Stopping CPU profiling is left to the caller.
func MaybeStartCPUProfile() bool {
	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		d.Exp.NoError(err)
		pprof.StartCPUProfile(f)
		return true
	}
	return false
}

// StopCPUProfile is a wrapper around pprof.StopCPUProfile(), provided for consistency; callers don't have to be aware of pprof at all.
func StopCPUProfile() {
	pprof.StopCPUProfile()
}

// MaybeWriteMemProfile checks the -memprofile flag and, if it is set, attempts to write memory profiling data to the named file.
func MaybeWriteMemProfile() {
	if *memProfile != "" {
		f, err := os.Create(*memProfile)
		defer f.Close()
		d.Exp.NoError(err)
		pprof.WriteHeapProfile(f)
	}
}
