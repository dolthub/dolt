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

func nomsStruct(ctx context.Context, noms *kingpin.Application) (*kingpin.CmdClause, util.KingpinHandler) {
	strukt := noms.Command("struct", "interact with structs")

	struktNew := strukt.Command("new", "creates a new struct")
	newDb := struktNew.Arg("database", "spec to db to create struct within").Required().String()
	newName := struktNew.Flag("name", "name for new struct").String()
	newFields := struktNew.Arg("fields", "key/value pairs for field names and values").Strings()

	struktSet := strukt.Command("set", "sets one or more fields of a struct")
	setSpec := struktSet.Arg("spec", "value spec for the struct to edit").Required().String()
	setFields := struktSet.Arg("fields", "key/value pairs for field names and values").Strings()

	struktDel := strukt.Command("del", "removes one or more fields from a struct")
	delSpec := struktDel.Arg("spec", "value spec for the struct to edit").Required().String()
	delFields := struktDel.Arg("fields", "fields to be removed").Strings()

	return strukt, func(input string) int {
		switch input {
		case struktNew.FullCommand():
			return nomsStructNew(ctx, *newDb, *newName, *newFields)
		case struktSet.FullCommand():
			return nomsStructSet(ctx, *setSpec, *setFields)
		case struktDel.FullCommand():
			return nomsStructDel(ctx, *delSpec, *delFields)
		}
		d.Panic("notreached")
		return 1
	}
}

func nomsStructNew(ctx context.Context, dbStr string, name string, args []string) int {
	sp, err := spec.ForDatabase(dbStr)
	d.PanicIfError(err)
	vrw := sp.GetVRW(ctx)
	st, err := types.NewStruct(vrw.Format(), name, nil)
	d.PanicIfError(err)
	applyStructEdits(ctx, sp.GetDatabase(ctx), sp, st, nil, args)
	return 0
}

func nomsStructSet(ctx context.Context, specStr string, args []string) int {
	sp, err := spec.ForPath(specStr)
	d.PanicIfError(err)
	db := sp.GetDatabase(ctx)
	rootVal, basePath := splitPath(ctx, db, sp)
	applyStructEdits(ctx, db, sp, rootVal, basePath, args)
	return 0
}

func nomsStructDel(ctx context.Context, specStr string, args []string) int {
	sp, err := spec.ForPath(specStr)
	d.PanicIfError(err)
	db := sp.GetDatabase(ctx)
	rootVal, basePath := splitPath(ctx, db, sp)
	patch := diff.Patch{}
	for i := 0; i < len(args); i++ {
		if !types.IsValidStructFieldName(args[i]) {
			util.CheckError(fmt.Errorf("Invalid field name: %s at position: %d", args[i], i))
		}
		patch = append(patch, diff.Difference{
			Path:       append(basePath, types.FieldPath{Name: args[i]}),
			ChangeType: types.DiffChangeRemoved,
		})
	}

	appplyPatch(ctx, db, sp, rootVal, basePath, patch)
	return 0
}

func splitPath(ctx context.Context, db datas.Database, sp spec.Spec) (rootVal types.Value, basePath types.Path) {
	rootPath := sp.Path
	rootPath.Path = types.Path{}
	var err error
	rootVal, err = rootPath.Resolve(ctx, db, sp.GetVRW(ctx))
	if err != nil {
		panic(err)
	}

	if rootVal == nil {
		util.CheckError(fmt.Errorf("Invalid path: %s", sp.String()))
		return
	}
	basePath = sp.Path.Path
	return
}

func applyStructEdits(ctx context.Context, db datas.Database, sp spec.Spec, rootVal types.Value, basePath types.Path, args []string) {
	if len(args)%2 != 0 {
		util.CheckError(fmt.Errorf("Must be an even number of key/value pairs"))
	}
	if rootVal == nil {
		util.CheckErrorNoUsage(fmt.Errorf("No value at: %s", sp.String()))
		return
	}
	patch := diff.Patch{}
	for i := 0; i < len(args); i += 2 {
		if !types.IsValidStructFieldName(args[i]) {
			util.CheckError(fmt.Errorf("Invalid field name: %s at position: %d", args[i], i))
		}
		nv, err := argumentToValue(ctx, args[i+1], db, sp.GetVRW(ctx))
		if err != nil {
			util.CheckError(fmt.Errorf("Invalid field value: %s at position %d: %s", args[i+1], i+1, err))
		}
		patch = append(patch, diff.Difference{
			Path:       append(basePath, types.FieldPath{Name: args[i]}),
			ChangeType: types.DiffChangeModified,
			NewValue:   nv,
		})
	}
	appplyPatch(ctx, db, sp, rootVal, basePath, patch)
}

func appplyPatch(ctx context.Context, db datas.Database, sp spec.Spec, rootVal types.Value, basePath types.Path, patch diff.Patch) {
	vrw := sp.GetVRW(ctx)
	baseVal, err := basePath.Resolve(ctx, rootVal, vrw)
	util.CheckError(err)
	if baseVal == nil {
		util.CheckErrorNoUsage(fmt.Errorf("No value at: %s", sp.String()))
	}

	newRootVal, err := diff.Apply(ctx, vrw.Format(), rootVal, patch)
	util.CheckError(err)
	d.Chk.NotNil(newRootVal)
	r, err := vrw.WriteValue(ctx, newRootVal)
	util.CheckError(err)
	newAbsPath := spec.AbsolutePath{
		Hash: r.TargetHash(),
		Path: basePath,
	}
	newSpec := sp
	newSpec.Path = newAbsPath
	// TODO: This value is not actually in the database.
	fmt.Println(newSpec.String())
}
