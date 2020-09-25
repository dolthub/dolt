// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package profile

import (
	"io"
	"os"
	"runtime"
	"runtime/pprof"

	flag "github.com/juju/gnuflag"

	"github.com/dolthub/dolt/go/store/d"
)

var (
	cpuProfile      string
	memProfile      string
	blockProfile    string
	flagsRegistered = false
)

func RegisterProfileFlags(flags *flag.FlagSet) {
	if !flagsRegistered {
		flagsRegistered = true
		flags.StringVar(&cpuProfile, "cpuprofile", "", "write cpu profile to file")
		flags.StringVar(&memProfile, "memprofile", "", "write memory profile to this file")
		flags.StringVar(&blockProfile, "blockprofile", "", "write block profile to this file")
	}
}

func ApplyProfileFlags(cpuProfileVal *string, memProfileVal *string, blockProfileVal *string) {
	if cpuProfileVal != nil {
		cpuProfile = *cpuProfileVal
	}
	if memProfileVal != nil {
		memProfile = *memProfileVal
	}
	if blockProfileVal != nil {
		blockProfile = *blockProfileVal
	}
}

// MaybeStartProfile checks the -blockProfile, -cpuProfile, and -memProfile flag and, for each that is set, attempts to start gathering profiling data into the appropriate files. It returns an object with one method, Stop(), that must be called in order to flush profile data to disk before the process terminates.
func MaybeStartProfile() interface {
	Stop()
} {
	p := &prof{}
	if blockProfile != "" {
		f, err := os.Create(blockProfile)
		d.PanicIfError(err)
		runtime.SetBlockProfileRate(1)
		p.bp = f
	}
	if cpuProfile != "" {
		f, err := os.Create(cpuProfile)
		d.PanicIfError(err)
		pprof.StartCPUProfile(f)
		p.cpu = f
	}
	if memProfile != "" {
		f, err := os.Create(memProfile)
		d.PanicIfError(err)
		p.mem = f
	}
	return p
}

type prof struct {
	bp  io.WriteCloser
	cpu io.Closer
	mem io.WriteCloser
}

func (p *prof) Stop() {
	if p.bp != nil {
		pprof.Lookup("block").WriteTo(p.bp, 0)
		p.bp.Close()
		runtime.SetBlockProfileRate(0)
	}
	if p.cpu != nil {
		pprof.StopCPUProfile()
		p.cpu.Close()
	}
	if p.mem != nil {
		pprof.WriteHeapProfile(p.mem)
		p.mem.Close()
	}
}
