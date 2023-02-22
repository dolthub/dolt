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
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/json"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

func TestSchemaTableMigrationOriginal(t *testing.T) {
	ctx := NewTestSQLCtx(context.Background())
	dEnv := dtestutils.CreateTestEnv()
	tmpDir, err := dEnv.TempTableFilesDir()
	require.NoError(t, err)
	opts := editor.Options{Deaf: dEnv.DbEaFactory(), Tempdir: tmpDir}
	db, err := NewDatabase(ctx, "dolt", dEnv.DbData(), opts)
	require.NoError(t, err)

	dbState, err := getDbState(db, dEnv)
	require.NoError(t, err)

	err = dsess.DSessFromSess(ctx.Session).AddDB(ctx, dbState)
	require.NoError(t, err)
	ctx.SetCurrentDatabase(db.Name())

	err = db.createSqlTable(ctx, doltdb.SchemasTableName, sql.NewPrimaryKeySchema(sql.Schema{ // original schema of dolt_schemas table
		{Name: doltdb.SchemasTablesTypeCol, Type: gmstypes.Text, Source: doltdb.SchemasTableName, PrimaryKey: true},
		{Name: doltdb.SchemasTablesNameCol, Type: gmstypes.Text, Source: doltdb.SchemasTableName, PrimaryKey: true},
		{Name: doltdb.SchemasTablesFragmentCol, Type: gmstypes.Text, Source: doltdb.SchemasTableName, PrimaryKey: false},
	}), sql.Collation_Default)
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

	tbl, err := getOrCreateDoltSchemasTable(ctx, db) // removes the old table and recreates it with the new schema
	require.NoError(t, err)

	iter, err := SqlTableToRowIter(ctx, tbl.DoltTable, nil)
	require.NoError(t, err)

	var rows []sql.Row
	for {
		row, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}

		require.NoError(t, err)
		rows = append(rows, row)
	}

	require.NoError(t, iter.Close(ctx))
	expectedRows := []sql.Row{
		{"view", "view1", "SELECT v1 FROM test;", nil},
		{"view", "view2", "SELECT v2 FROM test;", nil},
	}

	assert.Equal(t, expectedRows, rows)
}

func TestSchemaTableMigrationV1(t *testing.T) {
	ctx := NewTestSQLCtx(context.Background())
	dEnv := dtestutils.CreateTestEnv()
	tmpDir, err := dEnv.TempTableFilesDir()
	require.NoError(t, err)
	opts := editor.Options{Deaf: dEnv.DbEaFactory(), Tempdir: tmpDir}
	db, err := NewDatabase(ctx, "dolt", dEnv.DbData(), opts)
	require.NoError(t, err)

	dbState, err := getDbState(db, dEnv)
	require.NoError(t, err)

	err = dsess.DSessFromSess(ctx.Session).AddDB(ctx, dbState)
	require.NoError(t, err)
	ctx.SetCurrentDatabase(db.Name())

	// original schema of dolt_schemas table with the ID column
	err = db.createSqlTable(ctx, doltdb.SchemasTableName, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: doltdb.SchemasTablesTypeCol, Type: gmstypes.Text, Source: doltdb.SchemasTableName, PrimaryKey: false},
		{Name: doltdb.SchemasTablesNameCol, Type: gmstypes.Text, Source: doltdb.SchemasTableName, PrimaryKey: false},
		{Name: doltdb.SchemasTablesFragmentCol, Type: gmstypes.Text, Source: doltdb.SchemasTableName, PrimaryKey: false},
		{Name: doltdb.SchemasTablesIdCol, Type: gmstypes.Int64, Source: doltdb.SchemasTableName, PrimaryKey: true},
		{Name: doltdb.SchemasTablesExtraCol, Type: gmstypes.JsonType{}, Source: doltdb.SchemasTableName, PrimaryKey: false, Nullable: true},
	}), sql.Collation_Default)
	require.NoError(t, err)

	sqlTbl, found, err := db.GetTableInsensitive(ctx, doltdb.SchemasTableName)
	require.NoError(t, err)
	require.True(t, found)

	inserter := sqlTbl.(*WritableDoltTable).Inserter(ctx)
	// JSON string has no spaces because our various JSON libraries don't agree on how to marshall it
	err = inserter.Insert(ctx, sql.Row{"view", "view1", "SELECT v1 FROM test;", 1, `{"extra":"data"}`})
	require.NoError(t, err)
	err = inserter.Insert(ctx, sql.Row{"view", "view2", "SELECT v2 FROM test;", 2, nil})
	require.NoError(t, err)
	err = inserter.Close(ctx)
	require.NoError(t, err)

	tbl, err := getOrCreateDoltSchemasTable(ctx, db) // removes the old table and recreates it with the new schema
	require.NoError(t, err)

	iter, err := SqlTableToRowIter(ctx, tbl.DoltTable, nil)
	require.NoError(t, err)

	var rows []sql.Row
	for {
		row, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}

		require.NoError(t, err)
		// convert the JSONDocument to a string for comparison
		if row[3] != nil {
			// Annoying difference in representation between storage versions here
			jsonDoc, ok := row[3].(gmstypes.JSONDocument)
			if ok {
				row[3], err = jsonDoc.ToString(nil)
				row[3] = strings.ReplaceAll(row[3].(string), " ", "") // remove spaces
			}

			nomsJson, ok := row[3].(json.NomsJSON)
			if ok {
				row[3], err = nomsJson.ToString(ctx)
				row[3] = strings.ReplaceAll(row[3].(string), " ", "") // remove spaces
			}

			require.NoError(t, err)
		}

		rows = append(rows, row)
	}

	require.NoError(t, iter.Close(ctx))

	expectedRows := []sql.Row{
		{"view", "view1", "SELECT v1 FROM test;", `{"extra":"data"}`},
		{"view", "view2", "SELECT v2 FROM test;", nil},
	}

	assert.Equal(t, expectedRows, rows)
}
