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

package main

import (
	"fmt"
	"os"

	"github.com/dustin/go-humanize"
	flag "github.com/juju/gnuflag"

	"github.com/liquidata-inc/dolt/go/store/d"
	"github.com/liquidata-inc/dolt/go/store/nbs/benchmarks/gen"
)

var (
	genSize    = flag.Uint64("gen", 1024, "MiB of data to generate and chunk")
	chunkInput = flag.Bool("chunk", false, "Treat arg as data file to chunk")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "%s [--gen=<data in MiB>|--chunk] /path/to/file\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse(true)
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(1)
	}

	fileName := flag.Arg(0)

	var fd *os.File
	var err error
	if *chunkInput {
		fd, err = os.Open(fileName)
		d.Chk.NoError(err)
		defer fd.Close()
	} else {
		fd, err = gen.OpenOrGenerateDataFile(fileName, (*genSize)*humanize.MiByte)
		d.Chk.NoError(err)
		defer fd.Close()
	}

	cm := gen.OpenOrBuildChunkMap(fileName+".chunks", fd)
	defer cm.Close()
}
