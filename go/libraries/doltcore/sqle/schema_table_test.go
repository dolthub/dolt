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
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

func TestAncientSchemaTableMigration(t *testing.T) {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()
	tmpDir, err := dEnv.TempTableFilesDir()
	require.NoError(t, err)
	opts := editor.Options{Deaf: dEnv.DbEaFactory(ctx), Tempdir: tmpDir}
	db, err := NewDatabase(context.Background(), "dolt", dEnv.DbData(ctx), opts)
	require.NoError(t, err)

	_, sqlCtx, err := NewTestEngine(dEnv, context.Background(), db)
	require.NoError(t, err)

	err = db.createSqlTable(sqlCtx, doltdb.SchemasTableName, "", sql.NewPrimaryKeySchema(sql.Schema{ // original schema of dolt_schemas table
		{Name: doltdb.SchemasTablesTypeCol, Type: gmstypes.Text, Source: doltdb.SchemasTableName, PrimaryKey: true},
		{Name: doltdb.SchemasTablesNameCol, Type: gmstypes.Text, Source: doltdb.SchemasTableName, PrimaryKey: true},
		{Name: doltdb.SchemasTablesFragmentCol, Type: gmstypes.Text, Source: doltdb.SchemasTableName, PrimaryKey: false},
	}), sql.Collation_Default, "")
	require.NoError(t, err)

	sqlTbl, found, err := db.GetTableInsensitive(sqlCtx, doltdb.SchemasTableName)
	require.NoError(t, err)
	require.True(t, found)

	wrapper, ok := sqlTbl.(*SchemaTable)
	require.True(t, ok)
	require.NotNil(t, wrapper.backingTable)
	// unmodified dolt_schemas table.
	require.Equal(t, 3, len(wrapper.backingTable.Schema()))

	inserter := wrapper.backingTable.Inserter(sqlCtx)
	err = inserter.Insert(sqlCtx, sql.Row{"view", "view1", "SELECT v1 FROM test;"})
	require.NoError(t, err)
	err = inserter.Insert(sqlCtx, sql.Row{"view", "view2", "SELECT v2 FROM test;"})
	require.NoError(t, err)
	err = inserter.Close(sqlCtx)
	require.NoError(t, err)

	tbl, err := getOrCreateDoltSchemasTable(sqlCtx, db) // removes the old table and recreates it with the new schema
	require.NoError(t, err)

	iter, err := SqlTableToRowIter(sqlCtx, tbl.DoltTable, nil)
	require.NoError(t, err)

	var rows []sql.Row
	for {
		row, err := iter.Next(sqlCtx)
		if err == io.EOF {
			break
		}

		require.NoError(t, err)
		rows = append(rows, row)
	}

	require.NoError(t, iter.Close(sqlCtx))
	expectedRows := []sql.Row{
		{"view", "view1", "SELECT v1 FROM test;", nil, nil},
		{"view", "view2", "SELECT v2 FROM test;", nil, nil},
	}
	assert.Equal(t, expectedRows, rows)
}

func TestV1SchemasTable(t *testing.T) {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()
	tmpDir, err := dEnv.TempTableFilesDir()
	require.NoError(t, err)
	opts := editor.Options{Deaf: dEnv.DbEaFactory(ctx), Tempdir: tmpDir}
	db, err := NewDatabase(context.Background(), "dolt", dEnv.DbData(ctx), opts)
	require.NoError(t, err)

	_, sqlCtx, err := NewTestEngine(dEnv, context.Background(), db)
	require.NoError(t, err)

	err = db.createSqlTable(sqlCtx, doltdb.SchemasTableName, "", sql.NewPrimaryKeySchema(sql.Schema{ // original schema of dolt_schemas table
		{Name: doltdb.SchemasTablesTypeCol, Type: gmstypes.Text, Source: doltdb.SchemasTableName, PrimaryKey: true},
		{Name: doltdb.SchemasTablesNameCol, Type: gmstypes.Text, Source: doltdb.SchemasTableName, PrimaryKey: true},
		{Name: doltdb.SchemasTablesFragmentCol, Type: gmstypes.Text, Source: doltdb.SchemasTableName, PrimaryKey: false},
		{Name: doltdb.SchemasTablesExtraCol, Type: gmstypes.JSON, Source: doltdb.SchemasTableName, PrimaryKey: false},
	}), sql.Collation_Default, "")
	require.NoError(t, err)

	tbl, _, err := db.GetTableInsensitive(sqlCtx, doltdb.SchemasTableName)
	require.NoError(t, err)

	wrapper, ok := tbl.(*SchemaTable)
	require.True(t, ok)
	require.NotNil(t, wrapper.backingTable)

	// unmodified dolt_schemas table.
	require.Equal(t, 4, len(wrapper.backingTable.Schema()))

	tbl, err = getOrCreateDoltSchemasTable(sqlCtx, db)
	require.NoError(t, err)
	require.NotNil(t, tbl)

	// modified dolt_schemas table.
	require.Equal(t, 5, len(tbl.Schema()))

	tbl, _, err = db.GetTableInsensitive(sqlCtx, doltdb.SchemasTableName)
	require.NoError(t, err)
	wrapper, ok = tbl.(*SchemaTable)
	require.True(t, ok)
	require.NotNil(t, wrapper.backingTable)

	// modified dolt_schemas table.
	require.Equal(t, 5, len(wrapper.backingTable.Schema()))

}
