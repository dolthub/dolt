// Copyright 2018 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"

	"github.com/attic-labs/noms/cmd/util"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/diff"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"

	"gopkg.in/alecthomas/kingpin.v2"
)

func nomsStruct(noms *kingpin.Application) (*kingpin.CmdClause, util.KingpinHandler) {
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

	struktName := strukt.Command("name", "updates the name of a struct")
	nameSpec := struktName.Arg("spec", "value spec for the struct to edit").Required().String()
	nameName := struktName.Arg("name", "new name for the struct").String()

	return strukt, func(input string) int {
		switch input {
		case struktNew.FullCommand():
			return nomsStructNew(*newDb, *newName, *newFields)
		case struktSet.FullCommand():
			return nomsStructSet(*setSpec, *setFields)
		case struktDel.FullCommand():
			return nomsStructDel(*delSpec, *delFields)
		case struktName.FullCommand():
			return nomsStructName(*nameSpec, *nameName)
		}
		d.Panic("notreached")
		return 1
	}
}

func nomsStructNew(dbStr string, name string, args []string) int {
	sp, err := spec.ForDatabase(dbStr)
	d.PanicIfError(err)

	if len(args)%2 != 0 {
		d.CheckError(fmt.Errorf("Must be an even number of key/value pairs"))
	}

	db := sp.GetDatabase()
	st := types.NewStruct(name, nil)

	applyStructEdits(db, st, args)
	return 0
}

func nomsStructSet(specStr string, args []string) int {
	sp, err := spec.ForPath(specStr)
	d.PanicIfError(err)

	if len(args)%2 != 0 {
		d.CheckError(fmt.Errorf("Must be an even number of key/value pairs"))
	}

	db := sp.GetDatabase()
	rootPath := sp.Path
	rootPath.Path = types.Path{}
	rootVal := rootPath.Resolve(db)
	if rootVal == nil {
		d.CheckError(fmt.Errorf("Invalid path: %s", specStr))
		return 1
	}

	basePath := sp.Path.Path
	patch := diff.Patch{}
	for i := 0; i < len(args); i += 2 {
		if !types.IsValidStructFieldName(args[i]) {
			d.CheckError(fmt.Errorf("Invalid field name: %s at position: %d", args[i], i))
		}
		p, err := spec.NewAbsolutePath(args[i+1])
		if err != nil {
			d.CheckError(fmt.Errorf("Invalid field value: %s at position %d: %s", args[i+1], i+1, err))
		}
		nv := p.Resolve(db)
		if nv == nil {
			d.CheckError(fmt.Errorf("Invalid field value: %s at position: %d", args[i+1], i+1))
		}
		patch = append(patch, diff.Difference{
			Path:       append(basePath, types.FieldPath{Name: args[i]}),
			ChangeType: types.DiffChangeModified,
			NewValue:   nv,
		})
	}
	newRootVal := diff.Apply(rootVal, patch)
	d.Chk.NotNil(newRootVal)
	r := db.WriteValue(newRootVal)
	db.Flush()
	newAbsPath := spec.AbsolutePath{
		Hash: r.TargetHash(),
		Path: basePath,
	}
	newSpec := sp
	newSpec.Path = newAbsPath
	fmt.Println(newSpec.String())
	return 0
}

func nomsStructDel(specStr string, args []string) int {
	sp, err := spec.ForPath(specStr)
	d.PanicIfError(err)

	if len(args)%2 != 0 {
		d.CheckError(fmt.Errorf("Must be an even number of key/value pairs"))
	}

	db := sp.GetDatabase()
	val := sp.GetValue()
	if st, ok := val.(types.Struct); ok {
		for i := 0; i < len(args); i++ {
			if !types.IsValidStructFieldName(args[i]) {
				d.CheckError(fmt.Errorf("Invalid field name: %s at position: %d", args[i], i))
			}
			st = st.Delete(args[i])
		}
		r := db.WriteValue(st)
		db.Flush()
		fmt.Println(r.TargetHash().String())
		return 0
	} else {
		d.CheckError(fmt.Errorf("Path does not resolve to a struct: %s", specStr))
		return 1
	}
}

func nomsStructName(specStr string, name string) int {
	sp, err := spec.ForPath(specStr)
	d.PanicIfError(err)
	db := sp.GetDatabase()
	val := sp.GetValue()
	if st, ok := val.(types.Struct); ok {
		sd := types.StructData{}
		st.IterFields(func(name string, val types.Value) {
			sd[name] = val
		})
		r := db.WriteValue(types.NewStruct(name, sd))
		db.Flush()
		fmt.Println(r.TargetHash().String())
		return 0
	} else {
		d.CheckError(fmt.Errorf("Path does not resolve to a struct: %s", specStr))
		return 1
	}
}

func applyStructEdits(db datas.Database, st types.Struct, args []string) {
	for i := 0; i < len(args); i += 2 {
		if !types.IsValidStructFieldName(args[i]) {
			d.CheckError(fmt.Errorf("Invalid field name: %s at position: %d", args[i], i))
		}
		ap, err := spec.NewAbsolutePath(args[i+1])
		if err != nil {
			d.CheckError(fmt.Errorf("Invalid field value: %s at position %d: %s", args[i+1], i+1, err))
		}
		v := ap.Resolve(db)
		if v == nil {
			d.CheckError(fmt.Errorf("Invalid field value: %s at position: %d", args[i+1], i+1))
		}
		st = st.Set(args[i], v)
	}

	r := db.WriteValue(st)
	db.Flush()
	fmt.Println(r.TargetHash().String())
}
