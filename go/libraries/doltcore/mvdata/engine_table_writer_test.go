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

package mvdata_test

import (
	"context"
	"io"
	"testing"

	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/gcctx"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/mvdata"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/types"
)

// startTxnHook runs onStart the nth time a START TRANSACTION is built, before it executes.
type startTxnHook struct {
	n       int
	seen    int
	onStart func() error
}

func (h *startTxnHook) Build(_ *sql.Context, n sql.Node, _ sql.Row) (sql.RowIter, error) {
	if _, ok := n.(*plan.StartTransaction); ok {
		h.seen++
		if h.seen == h.n {
			return nil, h.onStart()
		}
	}
	return nil, nil
}

// TestCreateImportKeepsConcurrentCommit commits a change from another session just before the
// CreateOp import starts its insert transaction. The import must not revert it.
func TestCreateImportKeepsConcurrentCommit(t *testing.T) {
	ctx := context.Background()
	dEnv := sqle.CreateTestEnv()
	defer dEnv.DoltDB(ctx).Close()

	db, err := sqle.NewDatabase(ctx, "dolt", dEnv.DbData(ctx), editor.Options{})
	require.NoError(t, err)
	pro, err := sqle.NewDoltDatabaseProviderWithDatabase(env.GetDefaultInitBranch(dEnv.Config), dEnv.FS, db, dEnv.FS, sql.EngineOverrides{})
	require.NoError(t, err)
	vars := sqle.NewEngineVariables()
	engine := gms.New(analyzer.NewDefault(pro), &gms.Config{EngineVariables: vars})
	cfg, _ := dEnv.Config.GetConfig(env.GlobalConfig)
	gcSafepoints := gcctx.NewGCSafepointController()
	newCtx := func() *sql.Context {
		sqlCtx := sqle.NewTestSQLCtxWithProvider(ctx, pro, cfg, nil, gcSafepoints, vars)
		sqlCtx.SetCurrentDatabase(db.Name())
		return sqlCtx
	}

	importCtx := newCtx()
	otherCtx := newCtx()
	_, err = sqle.QueryRows(otherCtx, engine, "CREATE TABLE other (pk int primary key, v int)")
	require.NoError(t, err)
	_, err = sqle.QueryRows(otherCtx, engine, "INSERT INTO other VALUES (1, 1)")
	require.NoError(t, err)

	// The import's second START TRANSACTION follows its CREATE TABLE.
	engine.Analyzer.ExecBuilder.PriorityBuilder = &startTxnHook{n: 2, onStart: func() error {
		_, err := sqle.QueryRows(otherCtx, engine, "UPDATE other SET v = 2 WHERE pk = 1")
		return err
	}}

	sch := schema.MustSchemaFromCols(schema.NewColCollection(schema.NewColumn("pk", 0, types.IntKind, true)))
	wr, err := mvdata.NewSqlEngineTableWriter(importCtx, engine, sch, sch, &mvdata.MoverOptions{
		TableToWriteTo: "imported",
		Operation:      mvdata.CreateOp,
	}, nil)
	require.NoError(t, err)

	rows := make(chan sql.Row, 1)
	rows <- sql.Row{int64(1)}
	close(rows)
	err = wr.WriteRows(ctx, rows, func(sql.Row, sql.PrimaryKeySchema, string, int, error) bool { return true })
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, wr.Commit(ctx))

	got, err := sqle.QueryRows(otherCtx, engine, "SELECT v FROM other WHERE pk = 1")
	require.NoError(t, err)
	require.Equal(t, []sql.Row{{int32(2)}}, got)
	got, err = sqle.QueryRows(otherCtx, engine, "SELECT pk FROM imported")
	require.NoError(t, err)
	require.Equal(t, []sql.Row{{int64(1)}}, got)
}
