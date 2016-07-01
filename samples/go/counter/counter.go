// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [options] <dataset>\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "Missing required dataset argument")
		return
	}

	ds, err := spec.GetDataset(flag.Arg(0))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not create dataset: %s\n", err)
		return
	}
	defer ds.Database().Close()

	newVal := uint64(1)
	if lastVal, ok := ds.MaybeHeadValue(); ok {
		newVal = uint64(lastVal.(types.Number)) + 1
	}

	_, err = ds.Commit(types.Number(newVal))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error committing: %s\n", err)
		return
	}

	fmt.Println(newVal)
}
