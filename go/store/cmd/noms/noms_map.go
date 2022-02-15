// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2018 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"fmt"

	"github.com/attic-labs/kingpin"

	"github.com/dolthub/dolt/go/store/cmd/noms/util"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/diff"
	"github.com/dolthub/dolt/go/store/spec"
	"github.com/dolthub/dolt/go/store/types"
)

func nomsMap(ctx context.Context, noms *kingpin.Application) (*kingpin.CmdClause, util.KingpinHandler) {
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
			return nomsMapNew(ctx, *newDb, *newEntries)
		case mapSet.FullCommand():
			return nomsMapSet(ctx, *setSpec, *setEntries)
		case mapDel.FullCommand():
			return nomsMapDel(ctx, *delSpec, *delKeys)
		}
		d.Panic("notreached")
		return 1
	}
}

func nomsMapNew(ctx context.Context, dbStr string, args []string) int {
	sp, err := spec.ForDatabase(dbStr)
	d.PanicIfError(err)
	db := sp.GetDatabase(ctx)
	vrw := sp.GetVRW(ctx)
	m, err := types.NewMap(ctx, vrw)
	d.PanicIfError(err)
	applyMapEdits(ctx, db, sp, m, nil, args)
	return 0
}

func nomsMapSet(ctx context.Context, specStr string, args []string) int {
	sp, err := spec.ForPath(specStr)
	d.PanicIfError(err)
	db := sp.GetDatabase(ctx)
	rootVal, basePath := splitPath(ctx, db, sp)
	applyMapEdits(ctx, db, sp, rootVal, basePath, args)
	return 0
}

func nomsMapDel(ctx context.Context, specStr string, args []string) int {
	sp, err := spec.ForPath(specStr)
	d.PanicIfError(err)

	db := sp.GetDatabase(ctx)
	rootVal, basePath := splitPath(ctx, db, sp)
	patch := diff.Patch{}
	for i := 0; i < len(args); i++ {
		kp := parseKeyPart(args, i)
		patch = append(patch, diff.Difference{
			Path:       append(basePath, kp),
			ChangeType: types.DiffChangeRemoved,
		})
	}

	appplyPatch(ctx, db, sp, rootVal, basePath, patch)
	return 0
}

func applyMapEdits(ctx context.Context, db datas.Database, sp spec.Spec, rootVal types.Value, basePath types.Path, args []string) {
	if len(args)%2 != 0 {
		util.CheckError(fmt.Errorf("Must be an even number of key/value pairs"))
	}
	if rootVal == nil {
		util.CheckErrorNoUsage(fmt.Errorf("No value at: %s", sp.String()))
		return
	}
	patch := diff.Patch{}
	for i := 0; i < len(args); i += 2 {
		kp := parseKeyPart(args, i)
		vv, err := argumentToValue(ctx, args[i+1], db, sp.GetVRW(ctx))
		if err != nil {
			util.CheckError(fmt.Errorf("Invalid value: %s at position %d: %s", args[i+1], i+1, err))
		}
		patch = append(patch, diff.Difference{
			Path:       append(basePath, kp),
			ChangeType: types.DiffChangeModified,
			NewValue:   vv,
		})
	}
	appplyPatch(ctx, db, sp, rootVal, basePath, patch)
}

func parseKeyPart(args []string, i int) (res types.PathPart) {
	idx, h, rem, err := types.ParsePathIndex(args[i])
	if rem != "" {
		util.CheckError(fmt.Errorf("Invalid key: %s at position %d", args[i], i))
	}
	if err != nil {
		util.CheckError(fmt.Errorf("Invalid key: %s at position %d: %s", args[i], i, err))
	}
	if idx != nil {
		res = types.NewIndexPath(idx)
	} else {
		res = types.NewHashIndexPath(h)
	}
	return
}
