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
	"fmt"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// Nullable represents whether a column can have a null value.
type Nullable bool

const (
	NotNull Nullable = false
	Null    Nullable = true
)

// Adds a new column to the schema given and returns the new table value. Non-null column additions rewrite the entire
// table, since we must write a value for each row. If the column is not nullable, a default value must be provided.
//
// Returns an error if the column added conflicts with the existing schema in tag or name.
func AddColumnToTable(ctx context.Context, db *doltdb.DoltDB, tbl *doltdb.Table, tag uint64, newColName string, colKind types.NomsKind, nullable Nullable, defaultVal types.Value) (*doltdb.Table, error) {
	sch, err := tbl.GetSchema(ctx)

	if err != nil {
		return nil, err
	}

	if err := validateNewColumn(ctx, tbl, tag, newColName, colKind, nullable, defaultVal); err != nil {
		return nil, err
	}

	newSchema, err := createNewSchema(sch, tag, newColName, colKind, nullable)
	if err != nil {
		return nil, err
	}

	return updateTableWithNewSchema(ctx, db, tbl, tag, newSchema, defaultVal)
}

// updateTableWithNewSchema updates the existing table with a new schema and new values for the new column as necessary,
// and returns the new table.
func updateTableWithNewSchema(ctx context.Context, db *doltdb.DoltDB, tbl *doltdb.Table, tag uint64, newSchema schema.Schema, defaultVal types.Value) (*doltdb.Table, error) {
	vrw := db.ValueReadWriter()
	newSchemaVal, err := encoding.MarshalAsNomsValue(ctx, vrw, newSchema)
	if err != nil {
		return nil, err
	}

	rowData, err := tbl.GetRowData(ctx)

	if err != nil {
		return nil, err
	}

	if defaultVal == nil {
		return doltdb.NewTable(ctx, vrw, newSchemaVal, rowData)
	}

	me := rowData.Edit()

	err = rowData.Iter(ctx, func(k, v types.Value) (stop bool, err error) {
		oldRow, _, err := tbl.GetRow(ctx, k.(types.Tuple), newSchema)
		if err != nil {
			return false, err
		}

		newRow, err := oldRow.SetColVal(tag, defaultVal, newSchema)
		if err != nil {
			return false, err
		}

		me.Set(newRow.NomsMapKey(newSchema), newRow.NomsMapValue(newSchema))
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	m, err := me.Map(ctx)

	if err != nil {
		return nil, err
	}

	return doltdb.NewTable(ctx, vrw, newSchemaVal, m)
}

// createNewSchema Creates a new schema with a column as specified by the params.
func createNewSchema(sch schema.Schema, tag uint64, newColName string, colKind types.NomsKind, nullable Nullable) (schema.Schema, error) {
	var col schema.Column
	if nullable {
		col = schema.NewColumn(newColName, tag, colKind, false)
	} else {
		col = schema.NewColumn(newColName, tag, colKind, false, schema.NotNullConstraint{})
	}

	updatedCols, err := sch.GetAllCols().Append(col)
	if err != nil {
		return nil, err
	}

	return schema.SchemaFromCols(updatedCols), nil
}

// validateNewColumn returns an error if the column as specified cannot be added to the schema given.
func validateNewColumn(ctx context.Context, tbl *doltdb.Table, tag uint64, newColName string, colKind types.NomsKind, nullable Nullable, defaultVal types.Value) error {
	sch, err := tbl.GetSchema(ctx)

	if err != nil {
		return err
	}

	cols := sch.GetAllCols()
	err = cols.Iter(func(currColTag uint64, currCol schema.Column) (stop bool, err error) {
		if currColTag == tag {
			return false, fmt.Errorf("A column with the tag %d already exists.", tag)
		} else if currCol.Name == newColName {

			return true, fmt.Errorf("A column with the name %s already exists.", newColName)
		}

		return false, nil
	})

	if err != nil {
		return err
	}

	rd, err := tbl.GetRowData(ctx)

	if err != nil {
		return err
	}

	if !nullable && defaultVal == nil && rd.Len() > 0 {
		return errors.New("When adding a column that may not be null to a table with existing rows, a default value must be provided.")
	}

	if !types.IsNull(defaultVal) && defaultVal.Kind() != colKind {
		return fmt.Errorf("Type of default value (%v) doesn't match type of column (%v)", types.KindToString[defaultVal.Kind()], types.KindToString[colKind])
	}

	return nil
}
