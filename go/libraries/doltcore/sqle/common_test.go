// Copyright 2020 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqle

import (
	"context"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/envtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sql/sqltestutil"
	"github.com/liquidata-inc/dolt/go/store/types"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/typeinfo"
)

// SetupFunc can be run to perform additional setup work before a test case
type SetupFn func(t *testing.T, dEnv *env.DoltEnv)

// Runs the query given and returns the result. The schema result of the query's execution is currently ignored, and
// the targetSchema given is used to prepare all rows.
func executeSelect(ctx context.Context, dEnv *env.DoltEnv, root *doltdb.RootValue, query string) ([]sql.Row, sql.Schema, error) {
	var err error
	db := NewDatabase("dolt", dEnv.DoltDB, dEnv.RepoState, dEnv.RepoStateWriter())
	engine, sqlCtx, err := NewTestEngine(ctx, db, root)
	if err != nil {
		return nil, nil, err
	}

	sch, iter, err := engine.Query(sqlCtx, query)
	if err != nil {
		return nil, nil, err
	}

	sqlRows := make([]sql.Row, 0)
	var r sql.Row
	for r, err = iter.Next(); err == nil; r, err = iter.Next() {
		sqlRows = append(sqlRows, r)
	}

	if err != io.EOF {
		return nil, nil, err
	}

	return sqlRows, sch, nil
}

// Runs the query given and returns the error (if any).
func executeModify(ctx context.Context, dEnv *env.DoltEnv, root *doltdb.RootValue, query string) (*doltdb.RootValue, error) {
	db := NewDatabase("dolt", dEnv.DoltDB, dEnv.RepoState, dEnv.RepoStateWriter())
	engine, sqlCtx, err := NewTestEngine(ctx, db, root)

	if err != nil {
		return nil, err
	}

	_, _, err = engine.Query(sqlCtx, query)

	if err != nil {
		return nil, err
	}

	return db.GetRoot(sqlCtx)
}

// Returns the dolt rows given transformed to sql rows. Exactly the columns in the schema provided are present in the
// final output rows, even if the input rows contain different columns. The tag numbers for columns in the row and
// schema given must match.
func ToSqlRows(sch schema.Schema, rs ...row.Row) []sql.Row {
	sqlRows := make([]sql.Row, len(rs))
	compressedSch := CompressSchema(sch)
	for i := range rs {
		sqlRows[i], _ = DoltRowToSqlRow(CompressRow(sch, rs[i]), compressedSch)
	}
	return sqlRows
}

// SubsetSchema returns a schema that is a subset of the schema given, with keys and constraints removed. Column names
// must be verified before subsetting. Unrecognized column names will cause a panic.
func SubsetSchema(sch schema.Schema, colNames ...string) schema.Schema {
	srcColls := sch.GetAllCols()

	var cols []schema.Column
	for _, name := range colNames {
		if col, ok := srcColls.GetByName(name); !ok {
			panic("Unrecognized name " + name)
		} else {
			cols = append(cols, col)
		}
	}
	colColl, _ := schema.NewColCollection(cols...)
	return schema.UnkeyedSchemaFromCols(colColl)
}

func schemaNewColumn(t *testing.T, name string, tag uint64, sqlType sql.Type, partOfPK bool, constraints ...schema.ColConstraint) schema.Column {
	typeInfo, err := typeinfo.FromSqlType(sqlType)
	require.NoError(t, err)
	col, err := schema.NewColumnWithTypeInfo(name, tag, typeInfo, partOfPK, constraints...)
	require.NoError(t, err)
	return col
}

// TODO: this shouldn't be here
func CreateWorkingRootUpdate() map[string]envtestutils.TableUpdate {
	return map[string]envtestutils.TableUpdate{
		sqltestutil.TableWithHistoryName: {
			RowUpdates: []row.Row{
				mustRow(row.New(types.Format_Default, sqltestutil.ReaddAgeAt5HistSch, row.TaggedValues{
					0: types.Int(6), 1: types.String("Katie"), 2: types.String("McCulloch"),
				})),
			},
		},
	}
}

// Returns the dolt schema given as a sql.Schema, or panics.
func mustSqlSchema(sch schema.Schema) sql.Schema {
	sqlSchema, err := doltSchemaToSqlSchema("", sch)
	if err != nil {
		panic(err)
	}

	return sqlSchema
}

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
			Name:       column.Name,
			Type:       column.Type,
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
