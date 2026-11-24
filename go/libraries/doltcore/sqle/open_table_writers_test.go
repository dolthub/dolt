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
	"errors"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

// TestRollbackWithOpenTableWriter checks that a table writer is open from when it's handed out until it's closed, and
// that the session won't drop its branch state in between.
func TestRollbackWithOpenTableWriter(t *testing.T) {
	ctx := context.Background()
	dEnv := CreateTestEnv()
	defer dEnv.DoltDB(ctx).Close()

	db, err := NewDatabase(ctx, "dolt", dEnv.DbData(ctx), editor.Options{})
	require.NoError(t, err)
	engine, sqlCtx, err := NewTestEngine(dEnv, ctx, db)
	require.NoError(t, err)
	require.NoError(t, ExecuteSqlOnEngine(sqlCtx, engine, "create table t (pk int primary key);"))

	sess := dsess.DSessFromSess(sqlCtx.Session)
	state, ok, err := sess.LookupDbState(sqlCtx, "dolt")
	require.NoError(t, err)
	require.True(t, ok)

	tbl, ok, err := db.GetTableInsensitive(sqlCtx, "t")
	require.NoError(t, err)
	require.True(t, ok)
	require.False(t, state.WriteSession().HasOpenTableWriters())
	// Being handed out opens a writer, before any statement begins on it.
	inserter := tbl.(sql.InsertableTable).Inserter(sqlCtx)
	require.True(t, state.WriteSession().HasOpenTableWriters())

	require.Panics(t, func() {
		_ = sess.Rollback(sqlCtx, sqlCtx.GetTransaction())
	})
	after, ok, err := sess.LookupDbState(sqlCtx, "dolt")
	require.NoError(t, err)
	require.True(t, ok)
	assert.True(t, state == after, "branch state was dropped")

	inserter.StatementBegin(sqlCtx)
	require.NoError(t, inserter.Insert(sqlCtx, sql.Row{1}))
	require.NoError(t, inserter.StatementComplete(sqlCtx))
	require.NoError(t, inserter.Close(sqlCtx))
	require.False(t, state.WriteSession().HasOpenTableWriters())
	require.NoError(t, sess.Rollback(sqlCtx, sqlCtx.GetTransaction()))

	// Discarding a failed statement's changes doesn't close the writer.
	state, ok, err = sess.LookupDbState(sqlCtx, "dolt")
	require.NoError(t, err)
	require.True(t, ok)
	inserter = tbl.(sql.InsertableTable).Inserter(sqlCtx)
	inserter.StatementBegin(sqlCtx)
	failed := errors.New("statement failed")
	require.NoError(t, inserter.DiscardChanges(sqlCtx, failed))
	require.True(t, state.WriteSession().HasOpenTableWriters())
	require.ErrorIs(t, inserter.Close(sqlCtx), failed)
	require.False(t, state.WriteSession().HasOpenTableWriters())
}

// TestTableRewritesCloseTableWriters checks that rewriting a table with an auto_increment column, or adding one,
// leaves no table writer of the session open.
func TestTableRewritesCloseTableWriters(t *testing.T) {
	ctx := context.Background()
	dEnv := CreateTestEnv()
	defer dEnv.DoltDB(ctx).Close()

	db, err := NewDatabase(ctx, "dolt", dEnv.DbData(ctx), editor.Options{})
	require.NoError(t, err)
	engine, sqlCtx, err := NewTestEngine(dEnv, ctx, db)
	require.NoError(t, err)
	require.NoError(t, ExecuteSqlOnEngine(sqlCtx, engine, `
create table ai (pk int auto_increment primary key, v int);
insert into ai (v) values (1), (2);
create table plain (pk int primary key, v int);
insert into plain values (1, 1), (2, 2);`))

	for _, q := range []string{
		"alter table ai modify column v bigint",
		"alter table plain add column id int auto_increment unique",
	} {
		t.Run(q, func(t *testing.T) {
			require.NoError(t, ExecuteSqlOnEngine(sqlCtx, engine, q))
			state, ok, err := dsess.DSessFromSess(sqlCtx.Session).LookupDbState(sqlCtx, "dolt")
			require.NoError(t, err)
			require.True(t, ok)
			assert.False(t, state.WriteSession().HasOpenTableWriters())
		})
	}
}
