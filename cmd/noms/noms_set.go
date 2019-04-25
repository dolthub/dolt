// Copyright 2018 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bytes"
	"context"
	"fmt"
	"strconv"

	"github.com/attic-labs/kingpin"

	"github.com/attic-labs/noms/cmd/util"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/diff"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
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
	applySetEdits(sp, types.NewSet(context.Background(), sp.GetDatabase()), nil, types.DiffChangeAdded, args)
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
		vv, err := argumentToValue(args[i], db)
		if err != nil {
			d.CheckErrorNoUsage(err)
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

func argumentToValue(arg string, db datas.Database) (types.Value, error) {
	d.PanicIfTrue(arg == "")

	if arg == "true" {
		return types.Bool(true), nil
	}
	if arg == "false" {
		return types.Bool(false), nil
	}
	if arg[0] == '"' {
		buf := bytes.Buffer{}
		for i := 1; i < len(arg); i++ {
			c := arg[i]
			if c == '"' {
				if i != len(arg)-1 {
					break
				}
				return types.String(buf.String()), nil
			}
			if c == '\\' {
				i++
				c = arg[i]
				if c != '\\' && c != '"' {
					return nil, fmt.Errorf("Invalid string argument: %s: Only '\\' and '\"' can be escaped", arg)
				}
			}
			buf.WriteByte(c)
		}
		return nil, fmt.Errorf("Invalid string argument: %s", arg)
	}
	if arg[0] == '@' {
		p, err := spec.NewAbsolutePath(arg[1:])
		d.PanicIfError(err)
		return p.Resolve(context.Background(), db), nil
	}
	if n, err := strconv.ParseFloat(arg, 64); err == nil {
		return types.Float(n), nil
	}

	return types.String(arg), nil
}
