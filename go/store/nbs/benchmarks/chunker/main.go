// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"os"

	"github.com/dustin/go-humanize"
	flag "github.com/juju/gnuflag"
	"github.com/liquidata-inc/ld/dolt/go/store/go/d"
	"github.com/liquidata-inc/ld/dolt/go/store/go/nbs/benchmarks/gen"
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
