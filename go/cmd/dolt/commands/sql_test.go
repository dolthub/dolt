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

package commands

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	_ "github.com/dolthub/go-mysql-server/sql/variables"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

var tableName = "people"

// Smoke test: Console opens and exits
func TestSqlConsole(t *testing.T) {
	t.Run("SQL console opens and exits", func(t *testing.T) {
		ctx := context.TODO()

		dEnv, err := sqle.CreateEnvWithSeedData()
		require.NoError(t, err)
		defer dEnv.DoltDB(ctx).Close()

		cliCtx, err := NewArgFreeCliContext(ctx, dEnv, dEnv.FS)
		require.NoError(t, err)

		args := []string{}
		commandStr := "dolt sql"

		result := SqlCmd{}.Exec(ctx, commandStr, args, dEnv, cliCtx)
		assert.Equal(t, 0, result)
	})

}

func TestSqlBatchMode(t *testing.T) {
	tests := []struct {
		query       string
		expectedRes int
	}{
		{
			"create table test (a int primary key);" +
				"insert into test values (1),(2),(3);" +
				"select * from test;",
			0,
		},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			ctx := context.TODO()

			dEnv, err := sqle.CreateEnvWithSeedData()
			require.NoError(t, err)
			defer dEnv.DoltDB(ctx).Close()

			cliCtx, err := NewArgFreeCliContext(ctx, dEnv, dEnv.FS)
			require.NoError(t, err)

			args := []string{"-b", "-q", test.query}

			commandStr := "dolt sql"
			result := SqlCmd{}.Exec(ctx, commandStr, args, dEnv, cliCtx)
			assert.Equal(t, test.expectedRes, result)
		})
	}
}

// Smoke tests, values are printed to console
func TestSqlSelect(t *testing.T) {
	tests := []struct {
		query       string
		expectedRes int
	}{
		{"select * from doesnt_exist where age = 32", 1},
		{"select * from people", 0},
		{"select * from people where age = 32", 0},
		{"select * from people where title = 'Senior Dufus'", 0},
		{"select * from people where name = 'Bill Billerson'", 0},
		{"select * from people where name = 'John Johnson'", 0},
		{"select * from people where age = 25", 0},
		{"select * from people where 25 = age", 0},
		{"select * from people where is_married = false", 0},
		{"select * from people where age < 30", 0},
		{"select * from people where age > 24", 0},
		{"select * from people where age >= 25", 0},
		{"select * from people where name <= 'John Johnson'", 0},
		{"select * from people where name <> 'John Johnson'", 0},
		{"select age, is_married from people where name <> 'John Johnson'", 0},
		{"select age, is_married from people where name <> 'John Johnson' limit 1", 0},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			ctx := context.TODO()
			dEnv, err := sqle.CreateEnvWithSeedData()
			require.NoError(t, err)
			defer dEnv.DoltDB(ctx).Close()

			cliCtx, err := NewArgFreeCliContext(ctx, dEnv, dEnv.FS)
			require.NoError(t, err)

			args := []string{"-q", test.query}

			commandStr := "dolt sql"
			result := SqlCmd{}.Exec(ctx, commandStr, args, dEnv, cliCtx)
			assert.Equal(t, test.expectedRes, result)
		})
	}
}

// Smoke tests, values are printed to console
func TestSqlShow(t *testing.T) {
	tests := []struct {
		query       string
		expectedRes int
	}{
		{"show tables", 0},
		{"show create table people", 0},
		{"show all tables", 1},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			ctx := context.TODO()
			dEnv, err := sqle.CreateEnvWithSeedData()
			require.NoError(t, err)
			defer dEnv.DoltDB(ctx).Close()

			cliCtx, err := NewArgFreeCliContext(ctx, dEnv, dEnv.FS)
			require.NoError(t, err)

			args := []string{"-q", test.query}

			commandStr := "dolt sql"
			result := SqlCmd{}.Exec(ctx, commandStr, args, dEnv, cliCtx)
			assert.Equal(t, test.expectedRes, result)
		})
	}
}

// Tests of the create table SQL command, mostly a smoke test for errors in the command line handler. Most tests of
// create table SQL command are in the sql package.
func TestCreateTable(t *testing.T) {
	tests := []struct {
		query       string
		expectedRes int
	}{
		{"create table", 1},          // bad syntax
		{"create table (id int ", 1}, // bad syntax
		{"create table people (id int primary key)", 0},
		{"create table people (id int primary key, age int)", 0},
		{"create table people (id int primary key, age int, first_name varchar(80), is_married bit)", 0},
		{"create table people (`id` int, `age` int, `first_name` varchar(80), `last_name` varchar(80), `title` varchar(80), `is_married` bit, primary key (`id`, `age`))", 0},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			ctx := context.TODO()

			dEnv := dtestutils.CreateTestEnv()
			defer dEnv.DoltDB(ctx).Close()
			working, err := dEnv.WorkingRoot(context.Background())
			assert.Nil(t, err, "Unexpected error")
			has, err := working.HasTable(context.Background(), doltdb.TableName{Name: tableName})
			assert.NoError(t, err)
			assert.False(t, has, "table exists before creating it")

			cliCtx, err := NewArgFreeCliContext(ctx, dEnv, dEnv.FS)
			require.NoError(t, err)

			args := []string{"-q", test.query}
			commandStr := "dolt sql"
			result := SqlCmd{}.Exec(ctx, commandStr, args, dEnv, cliCtx)
			assert.Equal(t, test.expectedRes, result)

			working, err = dEnv.WorkingRoot(context.Background())
			assert.Nil(t, err, "Unexpected error")
			if test.expectedRes == 0 {
				has, err := working.HasTable(context.Background(), doltdb.TableName{Name: tableName})
				assert.NoError(t, err)
				assert.True(t, has, "table doesn't exist after creating it")
			} else {
				has, err := working.HasTable(context.Background(), doltdb.TableName{Name: tableName})
				assert.NoError(t, err)
				assert.False(t, has, "table shouldn't exist after error")
			}
		})
	}
}

// Tests of the create table SQL command, mostly a smoke test for errors in the command line handler. Most tests of
// create table SQL command are in the sql package.
func TestShowTables(t *testing.T) {
	tests := []struct {
		query       string
		expectedRes int
	}{
		{"show ", 1},      // bad syntax
		{"show table", 1}, // bad syntax
		{"show tables", 0},
		{"show create table people", 0},
		{"show create table dne", 1},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			ctx := context.TODO()
			dEnv, err := sqle.CreateEnvWithSeedData()
			require.NoError(t, err)
			defer dEnv.DoltDB(ctx).Close()

			cliCtx, err := NewArgFreeCliContext(ctx, dEnv, dEnv.FS)
			require.NoError(t, err)

			args := []string{"-q", test.query}
			commandStr := "dolt sql"
			result := SqlCmd{}.Exec(ctx, commandStr, args, dEnv, cliCtx)
			assert.Equal(t, test.expectedRes, result)
		})
	}
}

// Tests of the alter table SQL command, mostly a smoke test for errors in the command line handler. Most tests of
// create table SQL command are in the sql package.
func TestAlterTable(t *testing.T) {
	tests := []struct {
		query       string
		expectedRes int
	}{
		{"alter table", 1},                               // bad syntax
		{"alter table people rename", 1},                 // bad syntax
		{"alter table dne rename column id to newId", 1}, // unknown column
		{"alter table people rename column name to appelation", 0},
		{"alter table people rename to newPeople", 0},
		{"rename table people to newPeople", 0},
		{"alter table people add column (newCol int not null default 10)", 0},
		{"alter table people drop column title", 0},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			ctx := context.TODO()

			dEnv, err := sqle.CreateEnvWithSeedData()
			require.NoError(t, err)
			defer dEnv.DoltDB(ctx).Close()

			cliCtx, err := NewArgFreeCliContext(ctx, dEnv, dEnv.FS)
			require.NoError(t, err)

			args := []string{"-q", test.query}
			commandStr := "dolt sql"
			result := SqlCmd{}.Exec(ctx, commandStr, args, dEnv, cliCtx)
			assert.Equal(t, test.expectedRes, result)
		})
	}
}

// Tests of the drop table SQL command, mostly a smoke test for errors in the command line handler. Most tests of
// create table SQL command are in the sql package.
func TestDropTable(t *testing.T) {
	tests := []struct {
		query       string
		expectedRes int
	}{
		{"drop table", 1},
		{"drop table people", 0},
		{"drop table dne", 1},
		{"drop table if exists dne", 0},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			ctx := context.TODO()
			dEnv, err := sqle.CreateEnvWithSeedData()
			require.NoError(t, err)
			defer dEnv.DoltDB(ctx).Close()

			cliCtx, err := NewArgFreeCliContext(ctx, dEnv, dEnv.FS)
			require.NoError(t, err)

			args := []string{"-q", test.query}
			commandStr := "dolt sql"
			result := SqlCmd{}.Exec(ctx, commandStr, args, dEnv, cliCtx)
			assert.Equal(t, test.expectedRes, result)
		})
	}
}

// Tests of the insert SQL command, mostly a smoke test for errors in the command line handler. Most tests of
// insert SQL command are in the sql package.
func TestInsert(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		expectedRes int
		expectedIds []uuid.UUID
	}{
		{
			name:        "no primary key",
			query:       "insert into people (title) values ('hello')",
			expectedRes: 1,
		},
		{
			name:  "bad syntax",
			query: "insert into table", expectedRes: 1,
		},
		{
			name:  "bad syntax",
			query: "insert into people (id) values", expectedRes: 1,
		},
		{
			name:  "table doesn't exist",
			query: "insert into dne (id) values (00000000-0000-0000-0000-000000000005)", expectedRes: 1,
		},
		{
			name: "insert one row",
			query: `insert into people (id, name, age, is_married) values
				('00000000-0000-0000-0000-000000000005', 'Frank Frankerson', 10, false)`,
			expectedIds: []uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000005")},
		},
		{
			name: "includes functions",
			query: `insert into people (id, name, age, is_married, title) values
				('00000000-0000-0000-0000-000000000005', UPPER('FirsNam LasNam'), 33, false, TO_BASE64('Super-Resident'))`,
			expectedIds: []uuid.UUID{
				uuid.MustParse("00000000-0000-0000-0000-000000000005"),
			},
		},
		{
			name:  "no column names",
			query: `insert into people values ('00000000-0000-0000-0000-000000000005', 'FirsNam LasNam', 33, false, 'Super-Resident')`,
			expectedIds: []uuid.UUID{
				uuid.MustParse("00000000-0000-0000-0000-000000000005"),
			},
		},
		{
			name: "insert one row all columns",
			query: `insert into people (id, name, age, is_married, title) values
				('00000000-0000-0000-0000-000000000005', 'Frank Frankerson', 10, false, 'Goon')`,
			expectedIds: []uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000005")},
		},
		{
			name: "insert two rows all columns",
			query: `insert into people (id, name, age, is_married, title) values
				('00000000-0000-0000-0000-000000000005', 'Frank Frankerson', 10, false, 'Goon'),
				('00000000-0000-0000-0000-000000000006', 'Kobe Buffalomeat', 30, false, 'Linebacker')`,
			expectedIds: []uuid.UUID{
				uuid.MustParse("00000000-0000-0000-0000-000000000005"),
				uuid.MustParse("00000000-0000-0000-0000-000000000006"),
			},
		},
		{
			name: "mixed order",
			query: `insert into people (name, id, age, is_married, title) values
				('FirsNam LasNam', '00000000-0000-0000-0000-000000000005', 33, false, 'Super-Resident')`,
			expectedIds: []uuid.UUID{
				uuid.MustParse("00000000-0000-0000-0000-000000000005"),
			},
		},
		{
			name: "too many values",
			query: `insert into people (name, id, age, is_married) values
				('FirsNam LasNam', '00000000-0000-0000-0000-000000000005', 33, false, 'Super-Resident')`,
			expectedRes: 1,
		},
		{
			name: "not enough values",
			query: `insert into people (name, id, age, is_married, title) values
				('FirsNam LasNam', '00000000-0000-0000-0000-000000000005', 33, false)`,
			expectedRes: 1,
		},
		{
			name: "missing required column",
			query: `insert into people (id, title, age) values
				('00000000-0000-0000-0000-000000000005', 'Frank Frankerson', 10)`,
			expectedRes: 1,
		},
		{
			name: "existing primary key",
			query: `insert into people (id, name, age, is_married, title) values
				('00000000-0000-0000-0000-000000000000', 'Frank Frankerson', 10, false, 'Goon')`,
			expectedRes: 1,
		},
		//{
		//	name: "insert ignore",
		//	query: `insert ignore into people (id, name, age, is_married, title) values
		//		('00000000-0000-0000-0000-000000000000', 'Frank Frankerson', 10, false, 'Goon')`,
		//	expectedIds: []uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000000")},
		//},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			dEnv, err := sqle.CreateEnvWithSeedData()
			require.NoError(t, err)
			defer dEnv.DoltDB(ctx).Close()

			cliCtx, err := NewArgFreeCliContext(ctx, dEnv, dEnv.FS)
			require.NoError(t, err)

			args := []string{"-q", test.query}

			commandStr := "dolt sql"
			result := SqlCmd{}.Exec(ctx, commandStr, args, dEnv, cliCtx)
			assert.Equal(t, test.expectedRes, result)

			if result == 0 {
				root, err := dEnv.WorkingRoot(ctx)
				assert.Nil(t, err)

				// Assert that all expected IDs exist after the insert
				for _, expectedid := range test.expectedIds {
					q := fmt.Sprintf("SELECT * FROM %s WHERE id = '%s'", tableName, expectedid.String())
					rows, err := sqle.ExecuteSelect(ctx, dEnv, root, q)
					assert.NoError(t, err)
					assert.True(t, len(rows) > 0)
				}
			}
		})
	}
}

// Tests of the update SQL command, mostly a smoke test for errors in the command line handler. Most tests of
// update SQL command are in the sql package.
func TestUpdate(t *testing.T) {
	tests := []struct {
		name         string
		query        string
		expectedRes  int
		expectedIds  []uuid.UUID
		expectedAges []uint
	}{
		{
			name:  "bad syntax",
			query: "update table", expectedRes: 1,
		},
		{
			name:  "bad syntax",
			query: "update people set id", expectedRes: 1,
		},
		{
			name:  "table doesn't exist",
			query: "update dne set id = '00000000-0000-0000-0000-000000000005'", expectedRes: 1,
		},
		{
			name:         "update one row",
			query:        `update people set age = 1 where id = '00000000-0000-0000-0000-000000000002'`,
			expectedIds:  []uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000002")},
			expectedAges: []uint{1},
		},
		{
			name:  "insert two rows, two columns",
			query: `update people set age = 1, is_married = true where age > 21`,
			expectedIds: []uuid.UUID{
				uuid.MustParse("00000000-0000-0000-0000-000000000000"),
				uuid.MustParse("00000000-0000-0000-0000-000000000001"),
			},
			expectedAges: []uint{1, 1},
		},
		{
			name:        "null constraint violation",
			query:       `update people set name = null where id ='00000000-0000-0000-0000-000000000000'`,
			expectedRes: 1,
		},
		//{
		//	name:  "on duplicate update",
		//	query: `insert into people (id, name, age, is_married) values
		//		('00000000-0000-0000-0000-000000000000', 'Bill Billerson', 99, true)
		//		ON DUPLICATE KEY UPDATE age=99`,
		//	expectedIds: []uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000000")},
		//	expectedAges: []uint{99},
		//},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			ctx := context.Background()
			dEnv, err := sqle.CreateEnvWithSeedData()
			require.NoError(t, err)
			defer dEnv.DoltDB(ctx).Close()

			cliCtx, err := NewArgFreeCliContext(ctx, dEnv, dEnv.FS)
			require.NoError(t, err)

			args := []string{"-q", test.query}

			commandStr := "dolt sql"
			result := SqlCmd{}.Exec(ctx, commandStr, args, dEnv, cliCtx)
			assert.Equal(t, test.expectedRes, result)

			if result == 0 {
				root, err := dEnv.WorkingRoot(ctx)
				assert.Nil(t, err)

				// Assert that all rows have been updated
				for i, expectedid := range test.expectedIds {
					q := fmt.Sprintf("SELECT * FROM %s WHERE id = '%s'", tableName, expectedid.String())
					rows, err := sqle.ExecuteSelect(ctx, dEnv, root, q)
					assert.NoError(t, err)
					assert.True(t, len(rows) > 0)
					assert.Equal(t, uint32(test.expectedAges[i]), rows[0][2])
				}
			}
		})
	}
}

// Tests of the delete SQL command, mostly a smoke test for errors in the command line handler. Most tests of
// delete SQL command are in the sql package.
func TestDelete(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		expectedRes int
		deletedIds  []uuid.UUID
	}{
		{
			name:  "bad syntax",
			query: "delete table", expectedRes: 1,
		},
		{
			name:  "bad syntax",
			query: "delete from people where", expectedRes: 1,
		},
		{
			name:  "table doesn't exist",
			query: "delete from dne", expectedRes: 1,
		},
		{
			name:       "delete one row",
			query:      `delete from people where id = '00000000-0000-0000-0000-000000000002'`,
			deletedIds: []uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000002")},
		},
		{
			name:  "delete two rows",
			query: `delete from people where age > 21`,
			deletedIds: []uuid.UUID{
				uuid.MustParse("00000000-0000-0000-0000-000000000000"),
				uuid.MustParse("00000000-0000-0000-0000-000000000001"),
			},
		},
		{
			name:  "delete everything",
			query: `delete from people`,
			deletedIds: []uuid.UUID{
				uuid.MustParse("00000000-0000-0000-0000-000000000000"),
				uuid.MustParse("00000000-0000-0000-0000-000000000001"),
				uuid.MustParse("00000000-0000-0000-0000-000000000002"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			ctx := context.Background()
			dEnv, err := sqle.CreateEnvWithSeedData()
			require.NoError(t, err)
			defer dEnv.DoltDB(ctx).Close()

			cliCtx, err := NewArgFreeCliContext(ctx, dEnv, dEnv.FS)
			require.NoError(t, err)

			args := []string{"-q", test.query}

			commandStr := "dolt sql"
			result := SqlCmd{}.Exec(ctx, commandStr, args, dEnv, cliCtx)
			assert.Equal(t, test.expectedRes, result)

			if result == 0 {
				root, err := dEnv.WorkingRoot(ctx)
				assert.Nil(t, err)

				// Assert that all rows have been deleted
				for _, expectedid := range test.deletedIds {
					q := fmt.Sprintf("SELECT * FROM %s WHERE id = '%s'", tableName, expectedid.String())
					rows, err := sqle.ExecuteSelect(ctx, dEnv, root, q)
					assert.NoError(t, err)
					assert.True(t, len(rows) == 0)
				}
			}
		})
	}
}

func TestCommitHooksNoErrors(t *testing.T) {
	dEnv, err := sqle.CreateEnvWithSeedData()
	require.NoError(t, err)
	defer dEnv.DoltDB(ctx).Close()

	sql.SystemVariables.SetGlobal(dsess.ReplicateToRemote, "unknown")
	hooks, err := sqle.GetCommitHooks(context.Background(), nil, dEnv, io.Discard)
	assert.NoError(t, err)
	if len(hooks) < 1 {
		t.Error("failed to produce noop hook")
	} else {
		switch h := hooks[0].(type) {
		case *doltdb.LogHook:
		default:
			t.Errorf("expected LogHook, found: %s", h)
		}
	}
}
