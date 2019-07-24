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
	"errors"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
)

// DropColumn drops a column from a table. No existing rows are modified, but a new schema entry is written.
func DropColumn(ctx context.Context, doltDB *doltdb.DoltDB, tbl *doltdb.Table, colName string) (*doltdb.Table, error) {
	if tbl == nil || doltDB == nil {
		panic("invalid parameters")
	}

	tblSch := tbl.GetSchema(ctx)
	allCols := tblSch.GetAllCols()

	if col, ok := allCols.GetByName(colName); !ok {
		return nil, schema.ErrColNotFound
	} else if col.IsPartOfPK {
		return nil, errors.New("Cannot drop column in primary key")
	}

	cols := make([]schema.Column, 0)
	allCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
		if col.Name != colName {
			cols = append(cols, col)
		}
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
