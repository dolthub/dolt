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
	"context"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/types"
)

func AddPrimaryKeyToTable(ctx context.Context, table *doltdb.Table, tableName string, nbf *types.NomsBinFormat, columns []sql.IndexColumn, opts editor.Options) (*doltdb.Table, error) {
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
				// Add not null constraint if currently nullable
				if col.IsNullable() {
					col.Constraints = append(col.Constraints, schema.NotNullConstraint{})
				}
				col.IsPartOfPK = true
				return col
			}
		}

		return col
	})

	// Get Row Data out of Table
	rowData, err := table.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	// Go through every row
	err = rowData.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
		r, err := row.FromNoms(sch, key.(types.Tuple), value.(types.Tuple))
		if err != nil {
			return false, err
		}

		// Go through every column of row
		err = newCollection.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			// Skip if they are not part of primary key
			if !col.IsPartOfPK {
				return false, nil
			}

			// Check if column value is null
			val, ok := r.GetColVal(tag)
			if !ok || val == nil || val == types.NullValue {
				return true, fmt.Errorf("primary key cannot have NULL values")
			}
			return false, nil
		})

		if err != nil {
			return true, err
		}

		return false, nil
	})

	if err != nil {
		return nil, err
	}

	newSchema, err := schema.SchemaFromCols(newCollection)
	if err != nil {
		return nil, err
	}

	if !pkInCorrectOrder(newSchema, columns) {
		newSchema, err = rearrangeSchema(newSchema, columns)
		if err != nil {
			return nil, err
		}
	}

	newSchema.Indexes().AddIndex(sch.Indexes().AllIndexes()...)

	// Rebuild all of the indexes now that the primary key has been changed
	return insertKeyedData(ctx, nbf, table, newSchema, tableName, opts)
}

func pkInCorrectOrder(sch schema.Schema, cols []sql.IndexColumn) bool {
	pks := sch.GetPKCols()

	if pks.Size() != len(cols) {
		return false
	}

	for i, c := range cols {
		if strings.ToLower(c.Name) != strings.ToLower(pks.GetAtIndex(i).Name) {
			return false
		}
	}

	return true
}

func rearrangeSchema(sch schema.Schema, cols []sql.IndexColumn) (schema.Schema, error) {
	currPks := sch.GetPKCols()
	newPks := schema.NewColCollection()

	for _, c := range cols {
		foundCol, ok := currPks.GetByNameCaseInsensitive(c.Name)
		if !ok {
			return nil, fmt.Errorf("error: column %s was not found", foundCol.Name)
		}

		newPks = newPks.Append(foundCol)
	}

	return schema.SchemaFromCols(newPks.AppendColl(sch.GetNonPKCols()))
}

func insertKeyedData(ctx context.Context, nbf *types.NomsBinFormat, oldTable *doltdb.Table, newSchema schema.Schema, name string, opts editor.Options) (*doltdb.Table, error) {
	marshalledSchema, err := encoding.MarshalSchemaAsNomsValue(context.Background(), oldTable.ValueReadWriter(), newSchema)
	if err != nil {
		return nil, err
	}

	empty, err := types.NewMap(ctx, oldTable.ValueReadWriter())
	if err != nil {
		return nil, err
	}

	// Create the new Table and rebuild all the indexes
	newTable, err := doltdb.NewTable(ctx, oldTable.ValueReadWriter(), marshalledSchema, empty, empty, nil)
	if err != nil {
		return nil, err
	}

	newTable, err = editor.RebuildAllIndexes(ctx, newTable, opts)
	if err != nil {
		return nil, err
	}

	// Create the table editor and insert all of the new data into it
	tableEditor, err := editor.NewTableEditor(ctx, newTable, newSchema, name, opts)
	if err != nil {
		return nil, err
	}

	oldRowData, err := oldTable.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	err = oldRowData.Iter(ctx, func(key types.Value, value types.Value) (stop bool, err error) {
		keyless, card, err := row.KeylessRowsFromTuples(key.(types.Tuple), value.(types.Tuple))
		if err != nil {
			return true, err
		}

		// A row that exists more than once must be a duplicate.
		if card > 1 {
			return true, fmtPrimaryKeyError(newSchema, keyless)
		}

		taggedVals, err := keyless.TaggedValues()
		if err != nil {
			return true, err
		}

		keyedRow, err := row.New(nbf, newSchema, taggedVals)
		if err != nil {
			return true, err
		}

		err = tableEditor.InsertRow(ctx, keyedRow, duplicatePkFunction)
		if err != nil {
			return true, err
		}

		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return tableEditor.Table(ctx)
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

func duplicatePkFunction(keyString, indexName string, k, v types.Tuple, isPk bool) error {
	return sql.NewUniqueKeyErr(fmt.Sprintf("%s", keyString), true, sql.Row{})
}
