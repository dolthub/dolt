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
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
)

// Nullable represents whether a column can have a null value.
type Nullable bool

const (
	NotNull Nullable = false
	Null    Nullable = true
)

// Clone of sql.ColumnOrder to avoid a dependency on sql here
type ColumnOrder struct {
	First bool
	After string
}

// AddColumnToTable adds a new column to the schema given and returns the new table value. Non-null column additions
// rewrite the entire table, since we must write a value for each row. If the column is not nullable, a default value
// must be provided.
//
// Returns an error if the column added conflicts with the existing schema in tag or name.
func AddColumnToTable(
	ctx context.Context,
	root *doltdb.RootValue,
	tbl *doltdb.Table,
	tblName string,
	tag uint64,
	newColName string,
	typeInfo typeinfo.TypeInfo,
	nullable Nullable,
	defaultVal *sql.ColumnDefaultValue,
	comment string,
	order *ColumnOrder,
) (*doltdb.Table, error) {
	oldSchema, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	if schema.IsKeyless(oldSchema) {
		return nil, ErrKeylessAltTbl
	}

	if err := validateNewColumn(ctx, root, tbl, tblName, tag, newColName, typeInfo); err != nil {
		return nil, err
	}

	newSchema, err := addColumnToSchema(oldSchema, tag, newColName, typeInfo, nullable, order, defaultVal, comment)
	if err != nil {
		return nil, err
	}

	return tbl.UpdateSchema(ctx, newSchema)
}

// addColumnToSchema creates a new schema with a column as specified by the params.
func addColumnToSchema(
	sch schema.Schema,
	tag uint64,
	newColName string,
	typeInfo typeinfo.TypeInfo,
	nullable Nullable,
	order *ColumnOrder,
	defaultVal sql.Expression,
	comment string,
) (schema.Schema, error) {
	newCol, err := createColumn(nullable, newColName, tag, typeInfo, defaultVal.String(), comment)
	if err != nil {
		return nil, err
	}

	var newCols []schema.Column
	if order != nil && order.First {
		newCols = append(newCols, newCol)
	}
	for _, col := range sch.GetAllCols().GetColumns() {
		newCols = append(newCols, col)
		if order != nil && order.After == col.Name {
			newCols = append(newCols, newCol)
		}
	}

	if order == nil {
		newCols = append(newCols, newCol)
	}

	collection := schema.NewColCollection(newCols...)

	err = schema.ValidateForInsert(collection)
	if err != nil {
		return nil, err
	}

	newSch, err := schema.SchemaFromCols(collection)
	if err != nil {
		return nil, err
	}
	newSch.Indexes().AddIndex(sch.Indexes().AllIndexes()...)

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

func createColumn(nullable Nullable, newColName string, tag uint64, typeInfo typeinfo.TypeInfo, defaultVal, comment string) (schema.Column, error) {
	if nullable {
		return schema.NewColumnWithTypeInfo(newColName, tag, typeInfo, false, defaultVal, false, comment)
	} else {
		return schema.NewColumnWithTypeInfo(newColName, tag, typeInfo, false, defaultVal, false, comment, schema.NotNullConstraint{})
	}
}

// ValidateNewColumn returns an error if the column as specified cannot be added to the schema given.
func validateNewColumn(
	ctx context.Context,
	root *doltdb.RootValue,
	tbl *doltdb.Table,
	tblName string,
	tag uint64,
	newColName string,
	typeInfo typeinfo.TypeInfo,
) error {
	if typeInfo == nil {
		return fmt.Errorf(`typeinfo may not be nil`)
	}

	sch, err := tbl.GetSchema(ctx)

	if err != nil {
		return err
	}

	cols := sch.GetAllCols()
	err = cols.Iter(func(currColTag uint64, currCol schema.Column) (stop bool, err error) {
		if currColTag == tag {
			return false, schema.ErrTagPrevUsed(tag, newColName, tblName)
		} else if strings.ToLower(currCol.Name) == strings.ToLower(newColName) {
			return true, fmt.Errorf("A column with the name %s already exists in table %s.", newColName, tblName)
		}

		return false, nil
	})

	if err != nil {
		return err
	}

	_, tblName, found, err := root.GetTableByColTag(ctx, tag)
	if err != nil {
		return err
	}
	if found {
		return schema.ErrTagPrevUsed(tag, newColName, tblName)
	}

	return nil
}
