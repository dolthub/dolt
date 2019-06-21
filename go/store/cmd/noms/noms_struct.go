// Copyright 2018 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"fmt"

	"github.com/attic-labs/kingpin"

	"github.com/liquidata-inc/ld/dolt/go/store/cmd/noms/util"
	"github.com/liquidata-inc/ld/dolt/go/store/d"
	"github.com/liquidata-inc/ld/dolt/go/store/diff"
	"github.com/liquidata-inc/ld/dolt/go/store/spec"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
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
	applyStructEdits(ctx, sp, types.NewStruct(name, nil), nil, args)
	return 0
}

func nomsStructSet(ctx context.Context, specStr string, args []string) int {
	sp, err := spec.ForPath(specStr)
	d.PanicIfError(err)

	rootVal, basePath := splitPath(ctx, sp)
	applyStructEdits(ctx, sp, rootVal, basePath, args)
	return 0
}

func nomsStructDel(ctx context.Context, specStr string, args []string) int {
	sp, err := spec.ForPath(specStr)
	d.PanicIfError(err)

	rootVal, basePath := splitPath(ctx, sp)
	patch := diff.Patch{}
	for i := 0; i < len(args); i++ {
		if !types.IsValidStructFieldName(args[i]) {
			d.CheckError(fmt.Errorf("Invalid field name: %s at position: %d", args[i], i))
		}
		patch = append(patch, diff.Difference{
			Path:       append(basePath, types.FieldPath{Name: args[i]}),
			ChangeType: types.DiffChangeRemoved,
		})
	}

	appplyPatch(ctx, sp, rootVal, basePath, patch)
	return 0
}

func splitPath(ctx context.Context, sp spec.Spec) (rootVal types.Value, basePath types.Path) {
	db := sp.GetDatabase(ctx)
	rootPath := sp.Path
	rootPath.Path = types.Path{}
	rootVal = rootPath.Resolve(ctx, db)
	if rootVal == nil {
		d.CheckError(fmt.Errorf("Invalid path: %s", sp.String()))
		return
	}
	basePath = sp.Path.Path
	return
}

func applyStructEdits(ctx context.Context, sp spec.Spec, rootVal types.Value, basePath types.Path, args []string) {
	if len(args)%2 != 0 {
		d.CheckError(fmt.Errorf("Must be an even number of key/value pairs"))
	}
	if rootVal == nil {
		d.CheckErrorNoUsage(fmt.Errorf("No value at: %s", sp.String()))
		return
	}
	db := sp.GetDatabase(ctx)
	patch := diff.Patch{}
	for i := 0; i < len(args); i += 2 {
		if !types.IsValidStructFieldName(args[i]) {
			d.CheckError(fmt.Errorf("Invalid field name: %s at position: %d", args[i], i))
		}
		nv, err := argumentToValue(ctx, args[i+1], db)
		if err != nil {
			d.CheckError(fmt.Errorf("Invalid field value: %s at position %d: %s", args[i+1], i+1, err))
		}
		patch = append(patch, diff.Difference{
			Path:       append(basePath, types.FieldPath{Name: args[i]}),
			ChangeType: types.DiffChangeModified,
			NewValue:   nv,
		})
	}
	appplyPatch(ctx, sp, rootVal, basePath, patch)
}

func appplyPatch(ctx context.Context, sp spec.Spec, rootVal types.Value, basePath types.Path, patch diff.Patch) {
	db := sp.GetDatabase(ctx)
	baseVal := basePath.Resolve(ctx, rootVal, db)
	if baseVal == nil {
		d.CheckErrorNoUsage(fmt.Errorf("No value at: %s", sp.String()))
	}

	newRootVal := diff.Apply(ctx, rootVal, patch)
	d.Chk.NotNil(newRootVal)
	r := db.WriteValue(ctx, newRootVal)
	db.Flush(ctx)
	newAbsPath := spec.AbsolutePath{
		Hash: r.TargetHash(),
		Path: basePath,
	}
	newSpec := sp
	newSpec.Path = newAbsPath
	fmt.Println(newSpec.String())
}
