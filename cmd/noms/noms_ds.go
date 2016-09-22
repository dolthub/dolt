// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"

	"github.com/attic-labs/noms/cmd/util"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	flag "github.com/juju/gnuflag"
)

var toDelete string

var nomsDs = &util.Command{
	Run:       runDs,
	UsageLine: "ds [<database> | -d <dataset>]",
	Short:     "Noms dataset management",
	Long:      "See Spelling Objects at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the database and dataset arguments.",
	Flags:     setupDsFlags,
	Nargs:     0,
}

func setupDsFlags() *flag.FlagSet {
	dsFlagSet := flag.NewFlagSet("ds", flag.ExitOnError)
	dsFlagSet.StringVar(&toDelete, "d", "", "dataset to delete")
	return dsFlagSet
}

func runDs(args []string) int {
	spec, err := spec.NewResolver()
	d.CheckErrorNoUsage(err)
	if toDelete != "" {
		set, err := spec.GetDataset(toDelete) // append local if no local given, check for aliases
		d.CheckError(err)

		oldCommitRef, errBool := set.MaybeHeadRef()
		if !errBool {
			d.CheckError(fmt.Errorf("Dataset %v not found", set.ID()))
		}

		store, err := set.Database().Delete(set.ID())
		d.CheckError(err)
		defer store.Close()

		fmt.Printf("Deleted %v (was #%v)\n", toDelete, oldCommitRef.TargetHash().String())
	} else {
		dbSpec := ""
		if len(args) >= 1 {
			dbSpec = args[0]
		}
		store, err := spec.GetDatabase(dbSpec)
		d.CheckError(err)
		defer store.Close()

		store.Datasets().IterAll(func(k, v types.Value) {
			fmt.Println(k)
		})
	}
	return 0
}
