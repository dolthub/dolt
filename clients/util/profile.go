package util

import (
	"flag"
	"os"
	"runtime/pprof"
)

var (
	cpuProfile = flag.String("cpuprofile", "", "write cpu profile to file")
	memProfile = flag.String("memprofile", "", "write memory profile to this file")
)

// MaybeStartCPUProfile checks the -cpuprofile flag and, if it is set, attempts to start writing CPU profile data to the named file. Stopping CPU profiling is left to the caller.
func MaybeStartCPUProfile() (started bool, err error) {
	if *cpuProfile != "" {
		var f *os.File
		f, err = os.Create(*cpuProfile)
		if err == nil {
			pprof.StartCPUProfile(f)
			started = true
		}
	}
	return
}

// StopCPUProfile is a wrapper around pprof.StopCPUProfile(), provided for consistency; callers don't have to be aware of pprof at all.
func StopCPUProfile() {
	pprof.StopCPUProfile()
}

// MaybeStartMemProfile checks the -memprofile flag and, if it is set, attempts to write memory profiling data to the named file.
func MaybeWriteMemProfile() (err error) {
	if *memProfile != "" {
		var f *os.File
		f, err = os.Create(*memProfile)
		defer f.Close()
		if err == nil {
			pprof.WriteHeapProfile(f)
		}
	}
	return
}
