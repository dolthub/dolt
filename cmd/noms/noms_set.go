// Copyright 2018 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"

	"github.com/attic-labs/noms/cmd/util"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/diff"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"

	"gopkg.in/alecthomas/kingpin.v2"
)

func nomsSet(noms *kingpin.Application) (*kingpin.CmdClause, util.KingpinHandler) {
	set := noms.Command("set", "interact with sets")

	setNew := set.Command("new", "creates a new set")
	newDb := setNew.Arg("database", "spec to db to create set within").Required().String()
	newEntries := setNew.Arg("items", "items to insert").Strings()

	setInsert := set.Command("insert", "inserts one or more items into a set")
	insertSpec := setInsert.Arg("spec", "value spec for the set to edit").Required().String()
	insertEntries := setInsert.Arg("items", "items to insert").Strings()

	setDel := set.Command("del", "removes one or more items from a set")
	delSpec := setDel.Arg("spec", "value spec for the set to edit").Required().String()
	delEntries := setDel.Arg("items", "items to delete").Strings()

	return set, func(input string) int {
		switch input {
		case setNew.FullCommand():
			return nomsSetNew(*newDb, *newEntries)
		case setInsert.FullCommand():
			return nomsSetInsert(*insertSpec, *insertEntries)
		case setDel.FullCommand():
			return nomsSetDel(*delSpec, *delEntries)
		}
		d.Panic("notreached")
		return 1
	}
}

func nomsSetNew(dbStr string, args []string) int {
	sp, err := spec.ForDatabase(dbStr)
	d.PanicIfError(err)
	applySetEdits(sp, types.NewSet(sp.GetDatabase()), nil, types.DiffChangeAdded, args)
	return 0
}

func nomsSetInsert(specStr string, args []string) int {
	sp, err := spec.ForPath(specStr)
	d.PanicIfError(err)
	rootVal, basePath := splitPath(sp)
	applySetEdits(sp, rootVal, basePath, types.DiffChangeAdded, args)
	return 0
}

func nomsSetDel(specStr string, args []string) int {
	sp, err := spec.ForPath(specStr)
	d.PanicIfError(err)
	rootVal, basePath := splitPath(sp)
	applySetEdits(sp, rootVal, basePath, types.DiffChangeRemoved, args)
	return 0
}

func applySetEdits(sp spec.Spec, rootVal types.Value, basePath types.Path, ct types.DiffChangeType, args []string) {
	if rootVal == nil {
		d.CheckErrorNoUsage(fmt.Errorf("No value at: %s", sp.String()))
		return
	}
	db := sp.GetDatabase()
	patch := diff.Patch{}
	for i := 0; i < len(args); i++ {
		vp, err := spec.NewAbsolutePath(args[i])
		if err != nil {
			d.CheckError(fmt.Errorf("Invalid value: %s at position %d: %s", args[i], i, err))
		}
		vv := vp.Resolve(db)
		if vv == nil {
			d.CheckError(fmt.Errorf("Invalid value: %s at position %d", args[i], i))
		}
		var pp types.PathPart
		if types.ValueCanBePathIndex(vv) {
			pp = types.NewIndexPath(vv)
		} else {
			pp = types.NewHashIndexPath(vv.Hash())
		}
		d := diff.Difference{
			Path: append(basePath, pp),
		}
		if ct == types.DiffChangeAdded {
			d.NewValue = vv
		} else {
			d.OldValue = vv
		}
		patch = append(patch, d)
	}
	appplyPatch(sp, rootVal, basePath, patch)
}
