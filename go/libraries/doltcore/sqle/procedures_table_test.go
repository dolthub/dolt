// Copyright 2023 Dolthub, Inc.
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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

// Tests the codepath for migrating the dolt_procedures system table from an older schema
// to the latest schema
func TestProceduresMigration(t *testing.T) {
	dEnv := dtestutils.CreateTestEnv()
	tmpDir, err := dEnv.TempTableFilesDir()
	require.NoError(t, err)
	opts := editor.Options{Deaf: dEnv.DbEaFactory(ctx), Tempdir: tmpDir}

	timestamp := time.Now().Truncate(time.Minute).UTC()

	ctx, db := newDatabaseWithProcedures(t, dEnv, opts, timestamp)

	t.Run("test migration logic", func(t *testing.T) {
		// Call the logic to migrate it to the latest schema
		tbl, err := DoltProceduresGetTable(ctx, *db)
		require.NoError(t, err)

		// Assert that the data was migrated correctly
		rows := readAllRows(ctx, t, tbl)
		expectedRows := []sql.Row{
			{"proc1", "create procedure proc1() SELECT 42 as pk from dual;", timestamp, timestamp, nil},
			{"proc2", "create procedure proc2() SELECT 'HELLO' as greeting from dual;", timestamp, timestamp, nil},
		}
		assert.Equal(t, expectedRows, rows)
	})

	t.Run("test that fetching stored procedure triggers the migration logic", func(t *testing.T) {
		// Call the logic to migrate it to the latest schema
		_, found, err := db.GetStoredProcedure(ctx, "proc1")
		require.NoError(t, err)
		require.True(t, found)

		// Assert that the data was migrated correctly
		tbl, found, err := db.GetTableInsensitive(ctx, doltdb.ProceduresTableName)
		require.NoError(t, err)
		require.True(t, found)

		wrapper, ok := tbl.(*ProceduresTable)
		require.True(t, ok)
		require.NotNil(t, wrapper.backingTable)

		rows := readAllRows(ctx, t, wrapper.backingTable)
		expectedRows := []sql.Row{
			{"proc1", "create procedure proc1() SELECT 42 as pk from dual;", timestamp, timestamp, nil},
			{"proc2", "create procedure proc2() SELECT 'HELLO' as greeting from dual;", timestamp, timestamp, nil},
		}
		assert.Equal(t, expectedRows, rows)
	})

	t.Run("test that adding a new stored procedure triggers the migration logic", func(t *testing.T) {
		// Call the logic to migrate it to the latest schema
		proc3 := sql.StoredProcedureDetails{
			Name:            "proc3",
			CreateStatement: "create procedure proc3() SELECT 47 as pk from dual;",
			CreatedAt:       timestamp,
			ModifiedAt:      timestamp,
			SqlMode:         "NO_ENGINE_SUBSTITUTION",
		}
		err := db.SaveStoredProcedure(ctx, proc3)
		require.NoError(t, err)

		// Assert that the data was migrated correctly
		tbl, found, err := db.GetTableInsensitive(ctx, doltdb.ProceduresTableName)
		require.NoError(t, err)
		require.True(t, found)

		wrapper, ok := tbl.(*ProceduresTable)
		require.True(t, ok)
		require.NotNil(t, wrapper.backingTable)

		rows := readAllRows(ctx, t, wrapper.backingTable)
		expectedRows := []sql.Row{
			{"proc1", "create procedure proc1() SELECT 42 as pk from dual;", timestamp, timestamp, nil},
			{"proc2", "create procedure proc2() SELECT 'HELLO' as greeting from dual;", timestamp, timestamp, nil},
			{"proc3", "create procedure proc3() SELECT 47 as pk from dual;", timestamp, timestamp, "NO_ENGINE_SUBSTITUTION"},
		}
		assert.Equal(t, expectedRows, rows)
	})

}

func newDatabaseWithProcedures(t *testing.T, dEnv *env.DoltEnv, opts editor.Options, timestamp time.Time) (*sql.Context, *Database) {
	db, err := NewDatabase(context.Background(), "dolt", dEnv.DbData(ctx), opts)
	require.NoError(t, err)

	_, ctx, err := NewTestEngine(dEnv, context.Background(), db)
	require.NoError(t, err)

	// Create the dolt_procedures table with its original schema
	err = db.createSqlTable(ctx, doltdb.ProceduresTableName, "", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: doltdb.ProceduresTableNameCol, Type: gmstypes.Text, Source: doltdb.ProceduresTableName, PrimaryKey: true},
		{Name: doltdb.ProceduresTableCreateStmtCol, Type: gmstypes.Text, Source: doltdb.ProceduresTableName, PrimaryKey: false},
		{Name: doltdb.ProceduresTableCreatedAtCol, Type: gmstypes.Timestamp, Source: doltdb.ProceduresTableName, PrimaryKey: false},
		{Name: doltdb.ProceduresTableModifiedAtCol, Type: gmstypes.Timestamp, Source: doltdb.ProceduresTableName, PrimaryKey: false},
	}), sql.Collation_Default, "")
	require.NoError(t, err)

	sqlTbl, found, err := db.GetTableInsensitive(ctx, doltdb.ProceduresTableName)
	require.NoError(t, err)
	require.True(t, found)

	wrapper, ok := sqlTbl.(*ProceduresTable)
	require.True(t, ok)
	require.NotNil(t, wrapper.backingTable)

	// Insert some test data for procedures
	inserter := wrapper.backingTable.Inserter(ctx)
	require.NoError(t, inserter.Insert(ctx, sql.Row{"proc1", "create procedure proc1() SELECT 42 as pk from dual;", timestamp, timestamp}))
	require.NoError(t, inserter.Insert(ctx, sql.Row{"proc2", "create procedure proc2() SELECT 'HELLO' as greeting from dual;", timestamp, timestamp}))
	require.NoError(t, inserter.Close(ctx))

	return ctx, &db
}

func readAllRows(ctx *sql.Context, t *testing.T, tbl *WritableDoltTable) []sql.Row {
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

	return rows
}
