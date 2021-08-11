// Copyright 2021 Dolthub, Inc.
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
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/types"
)

func AddPrimaryKeyToTable(ctx *sql.Context, table *doltdb.Table, nbf *types.NomsBinFormat, columns []sql.IndexColumn) (*doltdb.Table, error) {
	sch, err := table.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	if sch.GetPKCols().Size() > 0 {
		return nil, sql.ErrMultiplePrimaryKeysDefined.New() // Also caught in GMS
	}

	// Map function for converting columns to a primary key
	newCollection := schema.MapColCollection(sch.GetAllCols(), func(col schema.Column) schema.Column {
		for _, c := range columns {
			if strings.ToLower(c.Name) == strings.ToLower(col.Name) {
				col.IsPartOfPK = true
				return col
			}
		}

		return col
	})

	newSchema, err := schema.SchemaFromCols(newCollection)
	if err != nil {
		return nil, err
	}

	newSchema.Indexes().AddIndex(sch.Indexes().AllIndexes()...)

	table, err = table.UpdateSchema(ctx, newSchema)
	if err != nil {
		return nil, err
	}

	// Convert the row data to match the new schema format
	rowData, err := table.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	newRowData, err := keylessRowDataToKeyedRowData(ctx, nbf, table.ValueReadWriter(), rowData, newSchema)
	if err != nil {
		return nil, err
	}

	table, err = table.UpdateRows(ctx, newRowData)
	if err != nil {
		return nil, err
	}

	// Rebuild all of the indexes now that the primary key has been changed
	return editor.RebuildAllIndexes(ctx, table)
}

func keylessRowDataToKeyedRowData(ctx *sql.Context, nbf *types.NomsBinFormat, vrw types.ValueReadWriter, rowData types.Map, newSch schema.Schema) (types.Map, error) {
	newMap, err := types.NewMap(ctx, vrw)
	if err != nil {
		return types.Map{}, err
	}

	mapEditor := newMap.Edit()

	err = rowData.Iter(ctx, func(key types.Value, value types.Value) (stop bool, err error) {
		keyless, card, err := row.KeylessRowsFromTuples(key.(types.Tuple), value.(types.Tuple))
		if err != nil {
			return true, err
		}

		if card > 1 {
			return true, fmtPrimaryKeyError(newSch, keyless)
		}

		taggedVals, err := keyless.TaggedValues()
		if err != nil {
			return true, err
		}

		keyedRow, err := row.New(nbf, newSch, taggedVals)
		if err != nil {
			return true, err
		}

		mapEditor = mapEditor.Set(keyedRow.NomsMapKey(newSch), keyedRow.NomsMapValue(newSch))

		return false, nil
	})

	if err != nil {
		return types.Map{}, err
	}

	return mapEditor.Map(ctx)
}

func fmtPrimaryKeyError(sch schema.Schema, keylessRow row.Row) error {
	pkTags := sch.GetPKCols().Tags

	vals := make([]string, len(pkTags))
	for i, tg := range sch.GetPKCols().Tags {
		val, ok := keylessRow.GetColVal(tg)
		if !ok {
			panic("tag for primary key wasn't found")
		}

		vals[i] = val.HumanReadableString()
	}

	return sql.NewUniqueKeyErr(fmt.Sprintf("[%s]", strings.Join(vals, ",")), true, sql.Row{vals})
}
