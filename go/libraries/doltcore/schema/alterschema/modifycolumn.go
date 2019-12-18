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
	"fmt"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// ModifyColumn modifies the column with the name given, replacing it with the new definition provided. A column with
// the name given must exist in the schema of the table.
func ModifyColumn(
	ctx context.Context,
	tbl *doltdb.Table,
	existingCol schema.Column,
	newCol schema.Column,
	defaultVal types.Value,
	order *ColumnOrder,
)(*doltdb.Table, error) {

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	if err := validateModifyColumn(ctx, tbl, newCol, defaultVal); err != nil {
		return nil, err
	}

	// Modify statements won't include key info, so fill it in from the old column
	if existingCol.IsPartOfPK {
		newCol.IsPartOfPK = true
	}

	newSchema, err := replaceColumnInSchema(sch, existingCol.Name, newCol, order)
	if err != nil {
		return nil, err
	}

	return updateTable(ctx, tbl, newSchema)
}

// validateNewColumn returns an error if the column as specified cannot be added to the schema given.
func validateModifyColumn(ctx context.Context, tbl *doltdb.Table, col schema.Column, defaultVal types.Value) error {
	sch, err := tbl.GetSchema(ctx)

	if err != nil {
		return err
	}

	cols := sch.GetAllCols()
	err = cols.Iter(func(currColTag uint64, currCol schema.Column) (stop bool, err error) {
		if currColTag == col.Tag {
			return false, nil
		} else if currCol.Name == col.Name {
			return true, fmt.Errorf("A column with the name %s already exists.", col.Name)
		}

		return false, nil
	})

	if err != nil {
		return err
	}

	if !types.IsNull(defaultVal) && defaultVal.Kind() != col.Kind {
		return fmt.Errorf("Type of default value (%v) doesn't match type of column (%v)", types.KindToString[defaultVal.Kind()], types.KindToString[col.Kind])
	}

	return nil
}


// updateTable updates the existing table with the new schema. No data is changed.
// TODO: type change, default values
func updateTable(ctx context.Context, tbl *doltdb.Table, newSchema schema.Schema) (*doltdb.Table, error) {
	vrw := tbl.ValueReadWriter()
	newSchemaVal, err := encoding.MarshalAsNomsValue(ctx, vrw, newSchema)
	if err != nil {
		return nil, err
	}

	rowData, err := tbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	return doltdb.NewTable(ctx, vrw, newSchemaVal, rowData)
}


// createNewSchema Creates a new schema with a column as specified by the params.
func replaceColumnInSchema(sch schema.Schema, oldColName string, newCol schema.Column, order *ColumnOrder) (schema.Schema, error) {
	// If no order is specified, insert in the same place as the existing column
	if order == nil {
		prevColumn := ""
		sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			if col.Name == oldColName {
				if prevColumn == "" {
					order = &ColumnOrder{First: true}
				}
				return true, nil
			} else {
				prevColumn = col.Name
			}
			return false, nil
		})

		if order == nil {
			if prevColumn != "" {
				order = &ColumnOrder{After: prevColumn}
			} else {
				return nil, fmt.Errorf("Couldn't find column %s", oldColName)
			}
		}
	}

	var newCols []schema.Column
	if order.First {
		newCols = append(newCols, newCol)
	}
	sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if col.Name != oldColName {
			newCols = append(newCols, col)
		}

		if order.After == col.Name {
			newCols = append(newCols, newCol)
		}

		return false, nil
	})

	collection, err := schema.NewColCollection(newCols...)
	if err != nil {
		return nil, err
	}

	return schema.SchemaFromCols(collection), nil
}

