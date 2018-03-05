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

	mapSet := maap.Command("set", "sets one or more keys in a map")
	setSpec := mapSet.Arg("spec", "value spec for the map to edit").Required().String()
	setEntries := mapSet.Arg("entries", "key/value pairs for entries").Strings()

	mapDel := maap.Command("del", "removes one or more entries from a map")
	delSpec := mapDel.Arg("spec", "value spec for the map to edit").Required().String()
	delKeys := mapDel.Arg("keys", "keys for the entries to be removed").Strings()

	return maap, func(input string) int {
		switch input {
		case mapNew.FullCommand():
			return nomsMapNew(*newDb, *newEntries)
		case mapSet.FullCommand():
			return nomsMapSet(*setSpec, *setEntries)
		case mapDel.FullCommand():
			return nomsMapDel(*delSpec, *delKeys)
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

func nomsMapSet(specStr string, args []string) int {
	sp, err := spec.ForPath(specStr)
	d.PanicIfError(err)
	rootVal, basePath := splitPath(sp)
	applyMapEdits(sp, rootVal, basePath, args)
	return 0
}

func nomsMapDel(specStr string, args []string) int {
	sp, err := spec.ForPath(specStr)
	d.PanicIfError(err)

	rootVal, basePath := splitPath(sp)
	patch := diff.Patch{}
	for i := 0; i < len(args); i++ {
		kp := parseKeyPart(args, i)
		patch = append(patch, diff.Difference{
			Path:       append(basePath, kp),
			ChangeType: types.DiffChangeRemoved,
		})
	}

	appplyPatch(sp, rootVal, basePath, patch)
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
		kp := parseKeyPart(args, i)
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

func parseKeyPart(args []string, i int) (res types.PathPart) {
	idx, h, rem, err := types.ParsePathIndex(args[i])
	if rem != "" {
		d.CheckError(fmt.Errorf("Invalid key: %s at position %d", args[i], i))
	}
	if err != nil {
		d.CheckError(fmt.Errorf("Invalid key: %s at position %d: %s", args[i], i, err))
	}
	if idx != nil {
		res = types.NewIndexPath(idx)
	} else {
		res = types.NewHashIndexPath(h)
	}
	return
}
