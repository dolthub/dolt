// Copyright 2018 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/random"
)

func nomsStructNew(dbStr string, name string, args []string) int {
	sp, err := spec.ForDatabase(dbStr)
	d.PanicIfError(err)

	if len(args)%2 != 0 {
		d.CheckError(fmt.Errorf("Must be an even number of key/value pairs"))
	}

	db := sp.GetDatabase()
	sd := types.StructData{}
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
		sd[args[i]] = v
	}

	// TODO: Committing to this temporary dataset is ghetto, but we lack any
	// other way to flush changes.
	// See: https://github.com/attic-labs/noms/issues/3530
	ds := db.GetDataset(fmt.Sprintf("nomscmd/struct/new/%s", random.Id()))
	r := db.WriteValue(types.NewStruct(name, sd))
	ds, err = db.CommitValue(ds, r)
	d.PanicIfError(err)
	_, err = db.Delete(ds)
	d.PanicIfError(err)
	fmt.Println(r.TargetHash().String())
	return 0
}
