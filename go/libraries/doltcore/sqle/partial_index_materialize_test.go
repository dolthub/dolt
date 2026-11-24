// Copyright 2026 Dolthub, Inc.
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
	"fmt"
	"strings"
	"testing"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

// workingSetReadingFunctions reads the working set whenever a function is looked up, as Doltgres does when it resolves
// casts while planning an expression. It provides no functions of its own.
type workingSetReadingFunctions struct {
	err *error
}

func (f workingSetReadingFunctions) Function(ctx *sql.Context, _, _ string) (sql.Function, bool) {
	state, ok, err := dsess.DSessFromSess(ctx.Session).LookupDbState(ctx, ctx.GetCurrentDatabase())
	if err == nil && ok {
		_, err = state.WorkingRoot(ctx)
	}
	if err != nil && *f.err == nil {
		*f.err = err
	}
	return nil, false
}

// TestPartialIndexPredicateReadsWorkingSetWhileMaterializing writes to a table with a partial index, then to more
// tables than the session caches writer schemas for, then to the first table again. Materializing that table's edits
// rebuilds its writer schema, whose predicate reads the working set when its function is looked up.
func TestPartialIndexPredicateReadsWorkingSetWhileMaterializing(t *testing.T) {
	const otherTables = 70
	ctx := context.Background()
	dEnv := CreateTestEnv()
	defer dEnv.DoltDB(ctx).Close()

	db, err := NewDatabase(ctx, "dolt", dEnv.DbData(ctx), editor.Options{})
	require.NoError(t, err)
	var lookupErr error
	overrides := sql.EngineOverrides{Builder: sql.BuilderOverrides{FunctionProvider: workingSetReadingFunctions{err: &lookupErr}}}
	pro, err := NewDoltDatabaseProviderWithDatabase(env.GetDefaultInitBranch(dEnv.Config), dEnv.FS, db, dEnv.FS, overrides)
	require.NoError(t, err)
	vars := NewEngineVariables()
	engine := sqle.New(analyzer.NewDefault(pro), &sqle.Config{EngineVariables: vars})
	config, _ := dEnv.Config.GetConfig(env.GlobalConfig)
	sqlCtx := NewTestSQLCtxWithProvider(ctx, pro, config, nil, nil, vars)
	sqlCtx.SetCurrentDatabase(db.Name())

	setUp := []string{"create table t (id int primary key, v int)", "create index t_v on t (v)"}
	for i := 0; i < otherTables; i++ {
		setUp = append(setUp, fmt.Sprintf("create table f%d (id int primary key)", i))
	}
	require.NoError(t, ExecuteSqlOnEngine(sqlCtx, engine, strings.Join(setUp, ";\n")))
	addIndexPredicate(t, sqlCtx, "t", "t_v", "abs(v) = 1")

	// ExecuteSqlOnEngine runs these in a single transaction with autocommit off.
	writes := []string{"insert into t values (1, 1)"}
	for i := 0; i < otherTables; i++ {
		writes = append(writes, fmt.Sprintf("insert into f%d values (1)", i))
	}
	writes = append(writes, "insert into t values (2, 2)")
	require.NoError(t, ExecuteSqlOnEngine(sqlCtx, engine, strings.Join(writes, ";\n")))
	require.NoError(t, lookupErr)

	_, iter, _, err := engine.Query(sqlCtx, "select id from t order by id")
	require.NoError(t, err)
	rows, err := sql.RowIterToRows(sqlCtx, iter)
	require.NoError(t, err)
	require.Equal(t, []sql.Row{{int32(1)}, {int32(2)}}, rows)
}

// addIndexPredicate gives the index |idxName| of the empty table |tblName| the predicate |predicate|. MySQL syntax
// can't create a partial index.
func addIndexPredicate(t *testing.T, ctx *sql.Context, tblName, idxName, predicate string) {
	sess := dsess.DSessFromSess(ctx.Session)
	tx, err := sess.StartTransaction(ctx, sql.ReadWrite)
	require.NoError(t, err)
	ctx.SetTransaction(tx)
	roots, ok := sess.GetRoots(ctx, ctx.GetCurrentDatabase())
	require.True(t, ok)
	tbl, ok, err := roots.Working.GetTable(ctx, doltdb.TableName{Name: tblName})
	require.NoError(t, err)
	require.True(t, ok)
	sch, err := tbl.GetSchema(ctx)
	require.NoError(t, err)

	idx, err := sch.Indexes().RemoveIndex(idxName)
	require.NoError(t, err)
	_, err = sch.Indexes().AddIndexByColNames(idxName, idx.ColumnNames(), nil, schema.IndexProperties{
		IsUserDefined: true,
		Predicate:     predicate,
	})
	require.NoError(t, err)

	tbl, err = tbl.UpdateSchema(ctx, sch)
	require.NoError(t, err)
	root, err := roots.Working.PutTable(ctx, doltdb.TableName{Name: tblName}, tbl)
	require.NoError(t, err)
	require.NoError(t, sess.SetWorkingRoot(ctx, ctx.GetCurrentDatabase(), root))
	require.NoError(t, sess.CommitTransaction(ctx, tx))
	ctx.SetTransaction(nil)
}
