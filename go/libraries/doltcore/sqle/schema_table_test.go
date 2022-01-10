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
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/types"
)

func TestSchemaTableRecreation(t *testing.T) {
	ctx := NewTestSQLCtx(context.Background())
	dEnv := dtestutils.CreateTestEnv()
	opts := editor.Options{Deaf: dEnv.DbEaFactory()}
	db := NewDatabase("dolt", dEnv.DbData(), opts)
	dbState := getDbState(t, db, dEnv)
	err := dsess.DSessFromSess(ctx.Session).AddDB(ctx, dbState)
	require.NoError(t, err)
	ctx.SetCurrentDatabase(db.Name())

	err = db.createSqlTable(ctx, doltdb.SchemasTableName, sql.NewPrimaryKeySchema(sql.Schema{ // schema of dolt_schemas table before the change
		{Name: doltdb.SchemasTablesTypeCol, Type: sql.Text, Source: doltdb.SchemasTableName, PrimaryKey: true},
		{Name: doltdb.SchemasTablesNameCol, Type: sql.Text, Source: doltdb.SchemasTableName, PrimaryKey: true},
		{Name: doltdb.SchemasTablesFragmentCol, Type: sql.Text, Source: doltdb.SchemasTableName, PrimaryKey: false},
	}))
	require.NoError(t, err)
	sqlTbl, found, err := db.GetTableInsensitive(ctx, doltdb.SchemasTableName)
	require.NoError(t, err)
	require.True(t, found)
	inserter := sqlTbl.(*WritableDoltTable).Inserter(ctx)
	err = inserter.Insert(ctx, sql.Row{"view", "view1", "SELECT v1 FROM test;"})
	require.NoError(t, err)
	err = inserter.Insert(ctx, sql.Row{"view", "view2", "SELECT v2 FROM test;"})
	require.NoError(t, err)
	err = inserter.Close(ctx)
	require.NoError(t, err)

	table, err := sqlTbl.(*WritableDoltTable).doltTable(ctx)
	require.NoError(t, err)

	rowData, err := table.GetNomsRowData(ctx)
	require.NoError(t, err)
	expectedVals := []sql.Row{
		{"view", "view1", "SELECT v1 FROM test;"},
		{"view", "view2", "SELECT v2 FROM test;"},
	}
	index := 0
	_ = rowData.IterAll(ctx, func(keyTpl, valTpl types.Value) error {
		dRow, err := row.FromNoms(sqlTbl.(*WritableDoltTable).sch, keyTpl.(types.Tuple), valTpl.(types.Tuple))
		require.NoError(t, err)
		sqlRow, err := sqlutil.DoltRowToSqlRow(dRow, sqlTbl.(*WritableDoltTable).sch)
		require.NoError(t, err)
		assert.Equal(t, expectedVals[index], sqlRow)
		index++
		return nil
	})

	tbl, err := GetOrCreateDoltSchemasTable(ctx, db) // removes the old table and recreates it with the new schema
	require.NoError(t, err)

	table, err = tbl.doltTable(ctx)
	require.NoError(t, err)

	rowData, err = table.GetNomsRowData(ctx)
	require.NoError(t, err)
	expectedVals = []sql.Row{
		{"view", "view1", "SELECT v1 FROM test;", int64(1)},
		{"view", "view2", "SELECT v2 FROM test;", int64(2)},
	}
	index = 0
	_ = rowData.IterAll(ctx, func(keyTpl, valTpl types.Value) error {
		dRow, err := row.FromNoms(tbl.sch, keyTpl.(types.Tuple), valTpl.(types.Tuple))
		require.NoError(t, err)
		sqlRow, err := sqlutil.DoltRowToSqlRow(dRow, tbl.sch)
		require.NoError(t, err)
		assert.Equal(t, expectedVals[index], sqlRow)
		index++
		return nil
	})

	indexes := tbl.sch.Indexes().AllIndexes()
	require.Len(t, indexes, 1)
	assert.Equal(t, true, indexes[0].IsUnique())
	assert.Equal(t, doltdb.SchemasTablesIndexName, indexes[0].Name())
}
