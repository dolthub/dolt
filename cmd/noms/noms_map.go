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

func nomsMap(noms *kingpin.Application) (*kingpin.CmdClause, util.KingpinHandler) {
	maap := noms.Command("map", "interact with maps")

	mapNew := maap.Command("new", "creates a new map")
	newDb := mapNew.Arg("database", "spec to db to create map within").Required().String()
	newEntries := mapNew.Arg("entries", "key/value pairs for entries").Strings()

	return maap, func(input string) int {
		switch input {
		case mapNew.FullCommand():
			return nomsMapNew(*newDb, *newEntries)
		}
		d.Panic("notreached")
		return 1
	}
}

func nomsMapNew(dbStr string, args []string) int {
	sp, err := spec.ForDatabase(dbStr)
	d.PanicIfError(err)
	applyMapEdits(sp, types.NewMap(sp.GetDatabase()), nil, args)
	return 0
}

func applyMapEdits(sp spec.Spec, rootVal types.Value, basePath types.Path, args []string) {
	if len(args)%2 != 0 {
		d.CheckError(fmt.Errorf("Must be an even number of key/value pairs"))
	}
	if rootVal == nil {
		d.CheckErrorNoUsage(fmt.Errorf("No value at: %s", sp.String()))
		return
	}
	db := sp.GetDatabase()
	patch := diff.Patch{}
	for i := 0; i < len(args); i += 2 {
		idx, h, rem, err := types.ParsePathIndex(args[i])
		if rem != "" {
			d.CheckError(fmt.Errorf("Invalid key: %s at position %d", args[i], i))
		}
		if err != nil {
			d.CheckError(fmt.Errorf("Invalid key: %s at position %d: %s", args[i], i, err))
		}
		var kp types.PathPart
		if idx != nil {
			kp = types.NewIndexPath(idx)
		} else {
			kp = types.NewHashIndexPath(h)
		}

		vp, err := spec.NewAbsolutePath(args[i+1])
		if err != nil {
			d.CheckError(fmt.Errorf("Invalid value: %s at position %d: %s", args[i+1], i+1, err))
		}
		vv := vp.Resolve(db)
		if vv == nil {
			d.CheckError(fmt.Errorf("Invalid value: %s at position %d", args[i+1], i+1))
		}
		patch = append(patch, diff.Difference{
			Path:       append(basePath, kp),
			ChangeType: types.DiffChangeModified,
			NewValue:   vv,
		})
	}
	appplyPatch(sp, rootVal, basePath, patch)
}
