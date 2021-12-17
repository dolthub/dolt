// Copyright 2020 Dolthub, Inc.
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

package sqle

import (
	"context"
	"io"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/types"
)

// SetupFunc can be run to perform additional setup work before a test case
type SetupFn func(t *testing.T, dEnv *env.DoltEnv)

// Runs the query given and returns the result. The schema result of the query's execution is currently ignored, and
// the targetSchema given is used to prepare all rows.
func executeSelect(t *testing.T, ctx context.Context, dEnv *env.DoltEnv, root *doltdb.RootValue, query string) ([]sql.Row, sql.Schema, error) {
	var err error
	opts := editor.Options{Deaf: dEnv.DbEaFactory()}
	db := NewDatabase("dolt", dEnv.DbData(), opts)
	engine, sqlCtx, err := NewTestEngine(t, dEnv, ctx, db, root)
	if err != nil {
		return nil, nil, err
	}

	sch, iter, err := engine.Query(sqlCtx, query)
	if err != nil {
		return nil, nil, err
	}

	sqlRows := make([]sql.Row, 0)
	var r sql.Row
	for r, err = iter.Next(sqlCtx); err == nil; r, err = iter.Next(sqlCtx) {
		sqlRows = append(sqlRows, r)
	}

	if err != io.EOF {
		return nil, nil, err
	}

	return sqlRows, sch, nil
}

// Runs the query given and returns the error (if any).
func executeModify(t *testing.T, ctx context.Context, dEnv *env.DoltEnv, root *doltdb.RootValue, query string) (*doltdb.RootValue, error) {
	opts := editor.Options{Deaf: dEnv.DbEaFactory()}
	db := NewDatabase("dolt", dEnv.DbData(), opts)
	engine, sqlCtx, err := NewTestEngine(t, dEnv, ctx, db, root)

	if err != nil {
		return nil, err
	}

	_, iter, err := engine.Query(sqlCtx, query)
	if err != nil {
		return nil, err
	}

	for {
		_, err := iter.Next(sqlCtx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}

	err = iter.Close(sqlCtx)
	if err != nil {
		return nil, err
	}

	return db.GetRoot(sqlCtx)
}

func schemaNewColumn(t *testing.T, name string, tag uint64, sqlType sql.Type, partOfPK bool, constraints ...schema.ColConstraint) schema.Column {
	return schemaNewColumnWDefVal(t, name, tag, sqlType, partOfPK, "", constraints...)
}

func schemaNewColumnWDefVal(t *testing.T, name string, tag uint64, sqlType sql.Type, partOfPK bool, defaultVal string, constraints ...schema.ColConstraint) schema.Column {
	typeInfo, err := typeinfo.FromSqlType(sqlType)
	require.NoError(t, err)
	col, err := schema.NewColumnWithTypeInfo(name, tag, typeInfo, partOfPK, defaultVal, false, "", constraints...)
	require.NoError(t, err)
	return col
}

func equalSchemas(t *testing.T, expectedSch schema.Schema, sch schema.Schema) {
	require.NotNil(t, expectedSch)
	require.NotNil(t, sch)
	require.Equal(t, expectedSch.GetAllCols().Size(), sch.GetAllCols().Size())
	cols := sch.GetAllCols().GetColumns()
	for i, expectedCol := range expectedSch.GetAllCols().GetColumns() {
		col := cols[i]
		col.Tag = expectedCol.Tag
		assert.Equal(t, expectedCol, col)
	}
}

// TODO: this shouldn't be here
func CreateWorkingRootUpdate() map[string]TableUpdate {
	return map[string]TableUpdate{
		TableWithHistoryName: {
			RowUpdates: []row.Row{
				mustRow(row.New(types.Format_Default, ReaddAgeAt5HistSch, row.TaggedValues{
					0: types.Int(6), 1: types.String("Katie"), 2: types.String("McCulloch"),
				})),
			},
		},
	}
}

// Returns the dolt schema given as a sql.Schema, or panics.
func mustSqlSchema(sch schema.Schema) sql.Schema {
	sqlSchema, err := sqlutil.FromDoltSchema("", sch)
	if err != nil {
		panic(err)
	}

	return sqlSchema.Schema
}

// Convenience function to return a row or panic on an error
func mustRow(r row.Row, err error) row.Row {
	if err != nil {
		panic(err)
	}

	return r
}

// Returns the schema given reduced to just its column names and types.
func reduceSchema(sch sql.Schema) sql.Schema {
	newSch := make(sql.Schema, len(sch))
	for i, column := range sch {
		newSch[i] = &sql.Column{
			Name: column.Name,
			Type: column.Type,
		}
	}
	return newSch
}

// Asserts that the two schemas are equal, comparing only names and types of columns.
func assertSchemasEqual(t *testing.T, expected, actual sql.Schema) {
	assert.Equal(t, reduceSchema(expected), reduceSchema(actual))
}

// CreateTableFn returns a SetupFunc that creates a table with the rows given
func CreateTableFn(tableName string, tableSchema schema.Schema, initialRows ...row.Row) SetupFn {
	return func(t *testing.T, dEnv *env.DoltEnv) {
		dtestutils.CreateTestTable(t, dEnv, tableName, tableSchema, initialRows...)
	}
}

// CreateTableWithRowsFn returns a SetupFunc that creates a table with the rows given, creating the rows on the fly
// from Value types conforming to the schema given.
func CreateTableWithRowsFn(tableName string, tableSchema schema.Schema, initialRows ...[]types.Value) SetupFn {
	return func(t *testing.T, dEnv *env.DoltEnv) {
		rows := make([]row.Row, len(initialRows))
		for i, r := range initialRows {
			rows[i] = NewRowWithSchema(tableSchema, r...)
		}
		dtestutils.CreateTestTable(t, dEnv, tableName, tableSchema, rows...)
	}
}

// Compose takes an arbitrary number of SetupFns and composes them into a single func which executes all funcs given.
func Compose(fns ...SetupFn) SetupFn {
	return func(t *testing.T, dEnv *env.DoltEnv) {
		for _, f := range fns {
			f(t, dEnv)
		}
	}
}
