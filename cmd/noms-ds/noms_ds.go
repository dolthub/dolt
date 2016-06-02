// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/attic-labs/noms/clients/go/flags"
	"github.com/attic-labs/noms/clients/go/util"
	"github.com/attic-labs/noms/types"
)

var (
	toDelete = flag.String("d", "", "dataset to delete")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Noms dataset management\n")
		fmt.Fprintln(os.Stderr, "Usage: noms ds [<database> | -d <dataset>]")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nFor detailed information on spelling datastores and datasets, see: at https://github.com/attic-labs/noms/blob/master/doc/spelling.md.\n\n")
	}

	flag.Parse()

	if *toDelete != "" {
		setSpec, err := flags.ParseDatasetSpec(*toDelete)
		util.CheckError(err)

		set, err := setSpec.Dataset()
		util.CheckError(err)

		oldCommitRef, errBool := set.MaybeHeadRef()
		if !errBool {
			util.CheckError(fmt.Errorf("Dataset %v not found", set.ID()))
		}

		store, err := set.Database().Delete(set.ID())
		util.CheckError(err)
		defer store.Close()

		fmt.Printf("Deleted dataset %v (was %v)\n\n", set.ID(), oldCommitRef.TargetHash().String())
	} else {
		if flag.NArg() != 1 {
			flag.Usage()
			return
		}

		storeSpec, err := flags.ParseDatabaseSpec(flag.Arg(0))
		util.CheckError(err)

		store, err := storeSpec.Database()
		util.CheckError(err)
		defer store.Close()

		store.Datasets().IterAll(func(k, v types.Value) {
			fmt.Println(k)
		})
	}

}
