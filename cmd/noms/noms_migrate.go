// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"

	"github.com/attic-labs/noms/cmd/util"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	v7datas "github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/migration"
	"github.com/attic-labs/noms/go/spec"
	v7spec "github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	v7types "github.com/attic-labs/noms/go/types"
	flag "github.com/juju/gnuflag"
)

var nomsMigrate = &util.Command{
	Run:       runMigrate,
	Flags:     setupMigrateFlags,
	UsageLine: "migrate [options] <source-object> <dest-dataset>",
	Short:     "Migrates between versions of Noms",
	Long:      "",
	Nargs:     2,
}

func setupMigrateFlags() *flag.FlagSet {
	return flag.NewFlagSet("migrate", flag.ExitOnError)
}

func runMigrate(args []string) int {
	// TODO: verify source store is expected version
	// TODO: support multiple source versions
	// TODO: parallelize
	// TODO: incrementalize

	v7Path, err := v7spec.ForPath(args[0])
	d.CheckError(err)
	defer v7Path.Close()

	sourceDb, sourceValue := v7Path.GetDatabase(), v7Path.GetValue()

	if sourceValue == nil {
		d.CheckErrorNoUsage(fmt.Errorf("Value not found: %s", args[0]))
	}

	isCommit := v7datas.IsCommitType(sourceValue.Type())

	vNewDataset, err := spec.ForDataset(args[1])
	d.CheckError(err)
	defer vNewDataset.Close()

	sinkDb, sinkDataset := vNewDataset.GetDatabase(), vNewDataset.GetDataset()

	if isCommit {
		// Need to migrate both value and meta fields.
		sourceCommit := sourceValue.(v7types.Struct)

		sinkValue, err := migration.MigrateFromVersion7(sourceCommit.Get("value"), sourceDb, sinkDb)
		d.CheckError(err)
		sinkMeta, err := migration.MigrateFromVersion7(sourceCommit.Get("meta"), sourceDb, sinkDb)
		d.CheckError(err)

		// Commit will assert that we got a Commit struct.
		_, err = sinkDb.Commit(sinkDataset, sinkValue, datas.CommitOptions{
			Meta: sinkMeta.(types.Struct),
		})
		d.CheckError(err)
	} else {
		sinkValue, err := migration.MigrateFromVersion7(sourceValue, sourceDb, sinkDb)
		d.CheckError(err)

		_, err = sinkDb.CommitValue(sinkDataset, sinkValue)
		d.CheckError(err)
	}

	return 0
}
