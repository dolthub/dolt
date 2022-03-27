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

package sqle

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	config2 "github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/store/types"
)

// ExecuteSql executes all the SQL non-select statements given in the string against the root value given and returns
// the updated root, or an error. Statements in the input string are split by `;\n`
func ExecuteSql(t *testing.T, dEnv *env.DoltEnv, root *doltdb.RootValue, statements string) (*doltdb.RootValue, error) {
	opts := editor.Options{Deaf: dEnv.DbEaFactory(), Tempdir: dEnv.TempTableFilesDir()}
	db := NewDatabase("dolt", dEnv.DbData(), opts)

	engine, ctx, err := NewTestEngine(t, dEnv, context.Background(), db, root)
	dsess.DSessFromSess(ctx.Session).EnableBatchedMode()
	err = ctx.Session.SetSessionVariable(ctx, sql.AutoCommitSessionVar, false)
	if err != nil {
		return nil, err
	}

	for _, query := range strings.Split(statements, ";\n") {
		if len(strings.Trim(query, " ")) == 0 {
			continue
		}

		sqlStatement, err := sqlparser.Parse(query)
		if err != nil {
			return nil, err
		}

		var execErr error
		switch sqlStatement.(type) {
		case *sqlparser.Show:
			return nil, errors.New("Show statements aren't handled")
		case *sqlparser.Select, *sqlparser.OtherRead:
			return nil, errors.New("Select statements aren't handled")
		case *sqlparser.Insert:
			var rowIter sql.RowIter
			_, rowIter, execErr = engine.Query(ctx, query)
			if execErr == nil {
				execErr = drainIter(ctx, rowIter)
			}
		case *sqlparser.DDL, *sqlparser.MultiAlterDDL:
			var rowIter sql.RowIter
			_, rowIter, execErr = engine.Query(ctx, query)
			if execErr == nil {
				execErr = drainIter(ctx, rowIter)
			}
			if err = db.Flush(ctx); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("Unsupported SQL statement: '%v'.", query)
		}

		if execErr != nil {
			return nil, execErr
		}
	}

	err = db.CommitTransaction(ctx, ctx.GetTransaction())
	if err != nil {
		return nil, err
	}

	return db.GetRoot(ctx)
}

// NewTestSQLCtx returns a new *sql.Context with a default DoltSession, a new IndexRegistry, and a new ViewRegistry
func NewTestSQLCtx(ctx context.Context) *sql.Context {
	return NewTestSQLCtxWithProvider(ctx, dsess.EmptyDatabaseProvider())
}

func NewTestSQLCtxWithProvider(ctx context.Context, pro dsess.RevisionDatabaseProvider) *sql.Context {
	s, err := dsess.NewDoltSession(
		sql.NewEmptyContext(),
		sql.NewBaseSession(),
		pro,
		config2.NewMapConfig(make(map[string]string)),
	)
	if err != nil {
		panic(err)
	}

	return sql.NewContext(
		ctx,
		sql.WithSession(s),
	).WithCurrentDB("dolt")
}

// NewTestEngine creates a new default engine, and a *sql.Context and initializes indexes and schema fragments.
func NewTestEngine(t *testing.T, dEnv *env.DoltEnv, ctx context.Context, db Database, root *doltdb.RootValue) (*sqle.Engine, *sql.Context, error) {
	b := env.GetDefaultInitBranch(dEnv.Config)
	pro := NewDoltDatabaseProvider(b, dEnv.FS, db)
	engine := sqle.NewDefault(pro)
	sqlCtx := NewTestSQLCtxWithProvider(ctx, pro)

	err := dsess.DSessFromSess(sqlCtx.Session).AddDB(sqlCtx, getDbState(t, db, dEnv))
	if err != nil {
		return nil, nil, err
	}

	sqlCtx.SetCurrentDatabase(db.Name())
	err = db.SetRoot(sqlCtx, root)
	if err != nil {
		return nil, nil, err
	}

	return engine, sqlCtx, nil
}

func getDbState(t *testing.T, db sql.Database, dEnv *env.DoltEnv) dsess.InitialDbState {
	ctx := context.Background()

	head := dEnv.RepoStateReader().CWBHeadSpec()
	headCommit, err := dEnv.DoltDB.Resolve(ctx, head, dEnv.RepoStateReader().CWBHeadRef())
	require.NoError(t, err)

	ws, err := dEnv.WorkingSet(ctx)
	require.NoError(t, err)

	return dsess.InitialDbState{
		Db:         db,
		HeadCommit: headCommit,
		WorkingSet: ws,
		DbData:     dEnv.DbData(),
		Remotes:    dEnv.RepoState.Remotes,
	}
}

// ExecuteSelect executes the select statement given and returns the resulting rows, or an error if one is encountered.
func ExecuteSelect(t *testing.T, dEnv *env.DoltEnv, ddb *doltdb.DoltDB, root *doltdb.RootValue, query string) ([]sql.Row, error) {

	dbData := env.DbData{
		Ddb: ddb,
		Rsw: dEnv.RepoStateWriter(),
		Rsr: dEnv.RepoStateReader(),
		Drw: dEnv.DocsReadWriter(),
	}

	opts := editor.Options{Deaf: dEnv.DbEaFactory(), Tempdir: dEnv.TempTableFilesDir()}
	db := NewDatabase("dolt", dbData, opts)
	engine, ctx, err := NewTestEngine(t, dEnv, context.Background(), db, root)
	if err != nil {
		return nil, err
	}

	_, rowIter, err := engine.Query(ctx, query)
	if err != nil {
		return nil, err
	}

	var (
		rows   []sql.Row
		rowErr error
		row    sql.Row
	)
	for row, rowErr = rowIter.Next(ctx); rowErr == nil; row, rowErr = rowIter.Next(ctx) {
		rows = append(rows, row)
	}

	if rowErr != io.EOF {
		return nil, rowErr
	}

	return rows, nil
}

// Returns the dolt rows given transformed to sql rows. Exactly the columns in the schema provided are present in the
// final output rows, even if the input rows contain different columns. The tag numbers for columns in the row and
// schema given must match.
func ToSqlRows(sch schema.Schema, rs ...row.Row) []sql.Row {
	sqlRows := make([]sql.Row, len(rs))
	compressedSch := CompressSchema(sch)
	for i := range rs {
		sqlRows[i], _ = sqlutil.DoltRowToSqlRow(CompressRow(sch, rs[i]), compressedSch)
	}
	return sqlRows
}

// Rewrites the tag numbers for the schema given to start at 0, just like result set schemas. If one or more column
// names are given, only those column names are included in the compressed schema. The column list can also be used to
// reorder the columns as necessary.
func CompressSchema(sch schema.Schema, colNames ...string) schema.Schema {
	var itag uint64
	var cols []schema.Column

	if len(colNames) > 0 {
		cols = make([]schema.Column, len(colNames))
		for _, colName := range colNames {
			column, ok := sch.GetAllCols().GetByName(colName)
			if !ok {
				panic("No column found for column name " + colName)
			}
			column.Tag = itag
			cols[itag] = column
			itag++
		}
	} else {
		cols = make([]schema.Column, sch.GetAllCols().Size())
		sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			col.Tag = itag
			cols[itag] = col
			itag++
			return false, nil
		})
	}

	colCol := schema.NewColCollection(cols...)
	return schema.UnkeyedSchemaFromCols(colCol)
}

// Rewrites the tag numbers for the schemas given to start at 0, just like result set schemas.
func CompressSchemas(schs ...schema.Schema) schema.Schema {
	var itag uint64
	var cols []schema.Column

	cols = make([]schema.Column, 0)
	for _, sch := range schs {
		sch.GetAllCols().IterInSortedOrder(func(tag uint64, col schema.Column) (stop bool) {
			col.Tag = itag
			cols = append(cols, col)
			itag++
			return false
		})
	}

	colCol := schema.NewColCollection(cols...)
	return schema.UnkeyedSchemaFromCols(colCol)
}

// Compresses each of the rows given ala compressRow
func CompressRows(sch schema.Schema, rs ...row.Row) []row.Row {
	compressed := make([]row.Row, len(rs))
	for i := range rs {
		compressed[i] = CompressRow(sch, rs[i])
	}
	return compressed
}

// Rewrites the tag numbers for the row given to begin at zero and be contiguous, just like result set schemas. We don't
// want to just use the field mappings in the result set schema used by sqlselect, since that would only demonstrate
// that the code was consistent with itself, not actually correct.
func CompressRow(sch schema.Schema, r row.Row) row.Row {
	var itag uint64
	compressedRow := make(row.TaggedValues)

	// TODO: this is probably incorrect and will break for schemas where the tag numbering doesn't match the declared order
	sch.GetAllCols().IterInSortedOrder(func(tag uint64, col schema.Column) (stop bool) {
		if val, ok := r.GetColVal(tag); ok {
			compressedRow[itag] = val
		}
		itag++
		return false
	})

	// call to compress schema is a no-op in most cases
	r, err := row.New(types.Format_Default, CompressSchema(sch), compressedRow)

	if err != nil {
		panic(err)
	}

	return r
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
	colColl := schema.NewColCollection(cols...)
	return schema.UnkeyedSchemaFromCols(colColl)
}

// DoltSchemaFromAlterableTable is a utility for integration tests
func DoltSchemaFromAlterableTable(t *AlterableDoltTable) schema.Schema {
	return t.sch
}

// DoltTableFromAlterableTable is a utility for integration tests
func DoltTableFromAlterableTable(ctx *sql.Context, t *AlterableDoltTable) *doltdb.Table {
	dt, err := t.doltTable(ctx)
	if err != nil {
		panic(err)
	}
	return dt
}

func drainIter(ctx *sql.Context, iter sql.RowIter) error {
	for {
		_, err := iter.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			closeErr := iter.Close(ctx)
			if closeErr != nil {
				panic(fmt.Errorf("%v\n%v", err, closeErr))
			}
			return err
		}
	}
	return iter.Close(ctx)
}
