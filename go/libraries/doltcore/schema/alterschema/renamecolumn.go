// Copyright 2019 Liquidata, Inc.
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

package alterschema

import (
	"context"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema/encoding"
)

// RenameColumn takes a table and renames a column from oldName to newName
func RenameColumn(ctx context.Context, doltDB *doltdb.DoltDB, tbl *doltdb.Table, oldName, newName string) (*doltdb.Table, error) {
	if newName == oldName {
		return tbl, nil
	} else if tbl == nil || doltDB == nil {
		panic("invalid parameters")
	}

	tblSch := tbl.GetSchema(ctx)
	allCols := tblSch.GetAllCols()

	if _, ok := allCols.GetByName(newName); ok {
		return nil, schema.ErrColNameCollision
	}

	if _, ok := allCols.GetByName(oldName); !ok {
		return nil, schema.ErrColNotFound
	}

	cols := make([]schema.Column, 0)
	allCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
		if col.Name == oldName {
			col.Name = newName
		}
		cols = append(cols, col)
		return false
	})

	colColl, err := schema.NewColCollection(cols...)
	if err != nil {
		return nil, err
	}

	newSch := schema.SchemaFromCols(colColl)

	vrw := doltDB.ValueReadWriter()
	schemaVal, err := encoding.MarshalAsNomsValue(ctx, vrw, newSch)

	if err != nil {
		return nil, err
	}

	newTable := doltdb.NewTable(ctx, vrw, schemaVal, tbl.GetRowData(ctx))

	return newTable, nil
}
