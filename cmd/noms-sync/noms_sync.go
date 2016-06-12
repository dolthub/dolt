// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/profile"
	"github.com/attic-labs/noms/samples/go/util"
)

var (
	p = flag.Uint("p", 512, "parallelism")
)

func main() {
	cpuCount := runtime.NumCPU()
	runtime.GOMAXPROCS(cpuCount)

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Moves datasets between or within databases\n\n")
		fmt.Fprintf(os.Stderr, "noms sync [options] <source-object> <dest-dataset>\n\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nFor detailed information on spelling objects and datasets, see: at https://github.com/attic-labs/noms/blob/master/doc/spelling.md.\n\n")
	}

	spec.RegisterDatabaseFlags()
	flag.Parse()

	if flag.NArg() != 2 {
		util.CheckError(errors.New("expected a source object and destination dataset"))
	}

	sourceSpec, err := spec.ParsePathSpec(flag.Arg(0))
	util.CheckError(err)
	sourceStore, sourceObj, err := sourceSpec.Value()
	util.CheckError(err)
	defer sourceStore.Close()

	sinkSpec, err := spec.ParseDatasetSpec(flag.Arg(1))
	util.CheckError(err)

	sinkDataset, err := sinkSpec.Dataset()
	util.CheckError(err)
	defer sinkDataset.Database().Close()

	err = d.Try(func() {
		defer profile.MaybeStartProfile().Stop()

		var err error
		sinkDataset, err = sinkDataset.Pull(sourceStore, types.NewRef(sourceObj), int(*p))
		d.Exp.NoError(err)
	})

	if err != nil {
		log.Fatal(err)
	}
}
