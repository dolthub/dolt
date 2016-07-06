// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"flag"
	"log"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/profile"
)

var (
	p int
)

var nomsSync = &nomsCommand{
	Run:       runSync,
	UsageLine: "sync [options] <source-object> <dest-dataset>",
	Short:     "Moves datasets between or within databases",
	Long:      "See Spelling Objects at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the object and dataset arguments.",
	Flags:     setupSyncFlags,
	Nargs:     2,
}

func setupSyncFlags() *flag.FlagSet {
	syncFlagSet := flag.NewFlagSet("sync", flag.ExitOnError)
	syncFlagSet.IntVar(&p, "p", 512, "parallelism")
	spec.RegisterDatabaseFlags(syncFlagSet)
	profile.RegisterProfileFlags(syncFlagSet)
	return syncFlagSet
}

func runSync(args []string) int {

	sourceStore, sourceObj, err := spec.GetPath(args[0])
	d.CheckError(err)
	defer sourceStore.Close()

	sinkDataset, err := spec.GetDataset(args[1])
	d.CheckError(err)
	defer sinkDataset.Database().Close()

	err = d.Try(func() {
		defer profile.MaybeStartProfile().Stop()

		var err error
		sinkDataset, err = sinkDataset.Pull(sourceStore, types.NewRef(sourceObj), p)
		d.PanicIfError(err)
	})

	if err != nil {
		log.Fatal(err)
	}
	return 0
}
