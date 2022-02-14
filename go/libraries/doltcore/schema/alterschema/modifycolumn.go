// Copyright 2019 Dolthub, Inc.
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
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/types"
)

var ErrPrimaryKeySetsIncompatible = errors.New("primary key sets incompatible")

// ModifyColumn modifies the column with the name given, replacing it with the new definition provided. A column with
// the name given must exist in the schema of the table.
func ModifyColumn(
	ctx context.Context,
	tbl *doltdb.Table,
	existingCol schema.Column,
	newCol schema.Column,
	order *ColumnOrder,
	opts editor.Options,
) (*doltdb.Table, error) {
	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	if strings.ToLower(existingCol.Name) == strings.ToLower(newCol.Name) {
		newCol.Name = existingCol.Name
	}
	if err := validateModifyColumn(ctx, tbl, existingCol, newCol); err != nil {
		return nil, err
	}

	// Modify statements won't include key info, so fill it in from the old column
	if existingCol.IsPartOfPK {
		newCol.IsPartOfPK = true
		if schema.IsColSpatialType(newCol) {
			return nil, fmt.Errorf("can't use Spatial Types as Primary Key for table")
		}
		foundNotNullConstraint := false
		for _, constraint := range newCol.Constraints {
			if _, ok := constraint.(schema.NotNullConstraint); ok {
				foundNotNullConstraint = true
				break
			}
		}
		if !foundNotNullConstraint {
			newCol.Constraints = append(newCol.Constraints, schema.NotNullConstraint{})
		}
	}

	newSchema, err := replaceColumnInSchema(sch, existingCol, newCol, order)
	if err != nil {
		return nil, err
	}

	updatedTable, err := updateTableWithModifiedColumn(ctx, tbl, sch, newSchema, existingCol, newCol, opts)
	if err != nil {
		return nil, err
	}

	return updatedTable, nil
}

// validateModifyColumn returns an error if the column as specified cannot be added to the schema given.
func validateModifyColumn(ctx context.Context, tbl *doltdb.Table, existingCol schema.Column, modifiedCol schema.Column) error {
	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return err
	}

	if existingCol.Name != modifiedCol.Name {
		cols := sch.GetAllCols()
		err = cols.Iter(func(currColTag uint64, currCol schema.Column) (stop bool, err error) {
			if currColTag == modifiedCol.Tag {
				return false, nil
			} else if strings.ToLower(currCol.Name) == strings.ToLower(modifiedCol.Name) {
				return true, fmt.Errorf("A column with the name %s already exists.", modifiedCol.Name)
			}

			return false, nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// updateTableWithModifiedColumn updates the existing table with the new schema. If the column type has changed, then
// the data is updated.
func updateTableWithModifiedColumn(ctx context.Context, tbl *doltdb.Table, oldSch, newSch schema.Schema, oldCol, modifiedCol schema.Column, opts editor.Options) (*doltdb.Table, error) {
	vrw := tbl.ValueReadWriter()

	rowData, err := tbl.GetNomsRowData(ctx)
	if err != nil {
		return nil, err
	}

	if !oldCol.TypeInfo.Equals(modifiedCol.TypeInfo) {
		if schema.IsKeyless(newSch) {
			return nil, fmt.Errorf("keyless table column type alteration is not yet supported")
		}
		rowData, err = updateRowDataWithNewType(ctx, rowData, tbl.ValueReadWriter(), oldSch, newSch, oldCol, modifiedCol)
		if err != nil {
			return nil, err
		}
	} else if !modifiedCol.IsNullable() {
		err = rowData.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
			r, err := row.FromNoms(newSch, key.(types.Tuple), value.(types.Tuple))
			if err != nil {
				return false, err
			}
			val, ok := r.GetColVal(modifiedCol.Tag)
			if !ok || val == nil || val == types.NullValue {
				return true, fmt.Errorf("cannot change column to NOT NULL when one or more values is NULL")
			}
			return false, nil
		})
		if err != nil {
			return nil, err
		}
	}

	indexData, err := tbl.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	var autoVal types.Value
	if schema.HasAutoIncrement(newSch) && schema.HasAutoIncrement(oldSch) {
		autoVal, err = tbl.GetAutoIncrementValue(ctx)
		if err != nil {
			return nil, err
		}
	}

	updatedTable, err := doltdb.NewNomsTable(ctx, vrw, newSch, rowData, indexData, autoVal)
	if err != nil {
		return nil, err
	}

	if !oldCol.TypeInfo.Equals(modifiedCol.TypeInfo) {
		// If we're modifying the primary key then all indexes are affected. Otherwise we just want to update the
		// touched ones.
		if modifiedCol.IsPartOfPK {
			for _, index := range newSch.Indexes().AllIndexes() {
				indexRowData, err := editor.RebuildIndex(ctx, updatedTable, index.Name(), opts)
				if err != nil {
					return nil, err
				}
				updatedTable, err = updatedTable.SetNomsIndexRows(ctx, index.Name(), indexRowData)
				if err != nil {
					return nil, err
				}
			}
		} else {
			for _, index := range newSch.Indexes().IndexesWithTag(modifiedCol.Tag) {
				indexRowData, err := editor.RebuildIndex(ctx, updatedTable, index.Name(), opts)
				if err != nil {
					return nil, err
				}
				updatedTable, err = updatedTable.SetNomsIndexRows(ctx, index.Name(), indexRowData)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	return updatedTable, nil
}

// updateRowDataWithNewType returns a new map of row data containing the updated rows from the changed schema column type.
func updateRowDataWithNewType(
	ctx context.Context,
	rowData types.Map,
	vrw types.ValueReadWriter,
	oldSch, newSch schema.Schema,
	oldCol, newCol schema.Column,
) (types.Map, error) {
	// If there are no rows then we can immediately return. All type conversions are valid for tables without rows, but
	// when rows are present then it is no longer true. GetTypeConverter assumes that there are rows present, so it
	// will return a failure on a type conversion that should work for the empty table.
	if rowData.Len() == 0 {
		return rowData, nil
	}
	convFunc, _, err := typeinfo.GetTypeConverter(ctx, oldCol.TypeInfo, newCol.TypeInfo)
	if err != nil {
		return types.EmptyMap, err
	}

	if !newCol.IsNullable() {
		originalConvFunc := convFunc
		convFunc = func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
			if v == nil || v == types.NullValue {
				return nil, fmt.Errorf("cannot change column to NOT NULL when one or more values is NULL")
			}
			return originalConvFunc(ctx, vrw, v)
		}
	}

	var lastKey types.Value
	mapEditor := rowData.Edit()
	err = rowData.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
		r, err := row.FromNoms(oldSch, key.(types.Tuple), value.(types.Tuple))
		if err != nil {
			return true, err
		}
		taggedVals, err := r.TaggedValues()
		if err != nil {
			return true, err
		}
		// We skip the "ok" check as nil is returned if the value does not exist, and we still want to check nil.
		// The underscore is important, otherwise a missing value would result in a panic.
		val, _ := taggedVals[oldCol.Tag]
		delete(taggedVals, oldCol.Tag) // If there was no value then delete is a no-op so this is safe
		newVal, err := convFunc(ctx, vrw, val)
		if err != nil {
			return true, err
		}
		// convFunc returns types.NullValue rather than nil so it's always safe to compare
		if newVal.Equals(val) {
			newRowKey, err := r.NomsMapKey(newSch).Value(ctx)
			if err != nil {
				return true, err
			}
			if newCol.IsPartOfPK && newRowKey.Equals(lastKey) {
				return true, fmt.Errorf("pk violation when altering column type and rewriting values")
			}
			lastKey = newRowKey
			return false, nil
		} else if newVal != types.NullValue {
			taggedVals[newCol.Tag] = newVal
		}
		r, err = row.New(rowData.Format(), newSch, taggedVals)
		if err != nil {
			return true, err
		}

		newRowKey, err := r.NomsMapKey(newSch).Value(ctx)
		if err != nil {
			return true, err
		}
		if newCol.IsPartOfPK {
			mapEditor.Remove(key)
			if newRowKey.Equals(lastKey) {
				return true, fmt.Errorf("pk violation when altering column type and rewriting values")
			}
		}
		lastKey = newRowKey
		mapEditor.Set(newRowKey, r.NomsMapValue(newSch))
		return false, nil
	})
	if err != nil {
		return types.EmptyMap, err
	}
	return mapEditor.Map(ctx)
}

// replaceColumnInSchema replaces the column with the name given with its new definition, optionally reordering it.
func replaceColumnInSchema(sch schema.Schema, oldCol schema.Column, newCol schema.Column, order *ColumnOrder) (schema.Schema, error) {
	// If no order is specified, insert in the same place as the existing column
	prevColumn := ""
	for _, col := range sch.GetAllCols().GetColumns() {
		if col.Name == oldCol.Name {
			if prevColumn == "" {
				if order == nil {
					order = &ColumnOrder{First: true}
				}
			}
			break
		} else {
			prevColumn = col.Name
		}
	}

	if order == nil {
		if prevColumn != "" {
			order = &ColumnOrder{After: prevColumn}
		} else {
			return nil, fmt.Errorf("Couldn't find column %s", oldCol.Name)
		}
	}

	var newCols []schema.Column
	if order.First {
		newCols = append(newCols, newCol)
	}

	for _, col := range sch.GetAllCols().GetColumns() {
		if col.Name != oldCol.Name {
			newCols = append(newCols, col)
		}

		if order.After == col.Name {
			newCols = append(newCols, newCol)
		}
	}

	collection := schema.NewColCollection(newCols...)

	err := schema.ValidateForInsert(collection)
	if err != nil {
		return nil, err
	}

	newSch, err := schema.SchemaFromCols(collection)
	if err != nil {
		return nil, err
	}
	for _, index := range sch.Indexes().AllIndexes() {
		tags := index.IndexedColumnTags()
		for i := range tags {
			if tags[i] == oldCol.Tag {
				tags[i] = newCol.Tag
			}
		}
		_, err = newSch.Indexes().AddIndexByColTags(index.Name(), tags, schema.IndexProperties{
			IsUnique:      index.IsUnique(),
			IsUserDefined: index.IsUserDefined(),
			Comment:       index.Comment(),
		})
		if err != nil {
			return nil, err
		}
	}

	// Copy over all checks from the old schema
	for _, check := range sch.Checks().AllChecks() {
		_, err := newSch.Checks().AddCheck(check.Name(), check.Expression(), check.Enforced())
		if err != nil {
			return nil, err
		}
	}

	pkOrds, err := modifyPkOrdinals(sch, newSch)
	if err != nil {
		return nil, err
	}
	err = newSch.SetPkOrdinals(pkOrds)
	if err != nil {
		return nil, err
	}
	return newSch, nil
}

// modifyPkOrdinals tries to create primary key ordinals for a newSch maintaining
// the relative positions of PKs from the oldSch. Return an ErrPrimaryKeySetsIncompatible
// error if the two schemas have a different number of primary keys, or a primary
// key column's tag changed between the two sets.
func modifyPkOrdinals(oldSch, newSch schema.Schema) ([]int, error) {
	if newSch.GetPKCols().Size() != oldSch.GetPKCols().Size() {
		return nil, ErrPrimaryKeySetsIncompatible
	}

	newPkOrdinals := make([]int, len(newSch.GetPkOrdinals()))
	for _, newCol := range newSch.GetPKCols().GetColumns() {
		// ordIdx is the relative primary key order (that stays the same)
		ordIdx, ok := oldSch.GetPKCols().TagToIdx[newCol.Tag]
		if !ok {
			// if pk tag changed, use name to find the new newCol tag
			oldCol, ok := oldSch.GetPKCols().NameToCol[newCol.Name]
			if !ok {
				return nil, ErrPrimaryKeySetsIncompatible
			}
			ordIdx = oldSch.GetPKCols().TagToIdx[oldCol.Tag]
		}

		// ord is the schema ordering index, which may have changed in newSch
		ord := newSch.GetAllCols().TagToIdx[newCol.Tag]
		newPkOrdinals[ordIdx] = ord
	}

	return newPkOrdinals, nil
}
