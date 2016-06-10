// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/samples/go/util"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [options] <dataset>\n", os.Args[0])
		flag.PrintDefaults()
	}

	spec.RegisterDatabaseFlags()
	flag.Parse()

	if flag.NArg() != 1 {
		util.CheckError(errors.New("expected dataset arg"))
	}

	ds, err := spec.GetDataset(flag.Arg(0))
	util.CheckError(err)

	defer ds.Database().Close()

	newVal := uint64(1)
	if lastVal, ok := ds.MaybeHeadValue(); ok {
		newVal = uint64(lastVal.(types.Number)) + 1
	}
	_, err = ds.Commit(types.Number(newVal))
	d.PanicIfError(err)

	fmt.Println(newVal)
}
