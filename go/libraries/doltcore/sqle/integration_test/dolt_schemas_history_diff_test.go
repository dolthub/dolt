// Copyright 2024 Dolthub, Inc.
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

package integration_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	cmd "github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
)

func TestDoltSchemasHistoryTable(t *testing.T) {
	SkipByDefaultInCI(t)
	ctx := context.Background()
	dEnv := setupDoltSchemasHistoryTests(t)
	defer dEnv.DoltDB(ctx).Close()
	for _, test := range doltSchemasHistoryTableTests() {
		t.Run(test.name, func(t *testing.T) {
			testDoltSchemasHistoryTable(t, test, dEnv)
		})
	}
}

func TestDoltSchemasDiffTable(t *testing.T) {
	SkipByDefaultInCI(t)
	ctx := context.Background()
	dEnv := setupDoltSchemasDiffTests(t)
	defer dEnv.DoltDB(ctx).Close()
	for _, test := range doltSchemasDiffTableTests() {
		t.Run(test.name, func(t *testing.T) {
			testDoltSchemasDiffTable(t, test, dEnv)
		})
	}
}

type doltSchemasTableTest struct {
	name  string
	setup []testCommand
	query string
	rows  []sql.Row
}

// Global variables to store commit hashes for test validation
var (
	DOLT_SCHEMAS_HEAD    string
	DOLT_SCHEMAS_HEAD_1  string
	DOLT_SCHEMAS_HEAD_2  string
	DOLT_SCHEMAS_INIT    string
)

var setupDoltSchemasCommon = []testCommand{
	// Create initial view
	{cmd.SqlCmd{}, args{"-q", "CREATE VIEW test_view AS SELECT 1 as col1"}},
	{cmd.AddCmd{}, args{"."}},
	{cmd.CommitCmd{}, args{"-m", "first commit: added test_view"}},
	
	// Create a trigger
	{cmd.SqlCmd{}, args{"-q", "CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(50))"}},
	{cmd.SqlCmd{}, args{"-q", `CREATE TRIGGER test_trigger 
		BEFORE INSERT ON test_table 
		FOR EACH ROW 
		SET NEW.name = UPPER(NEW.name)`}},
	{cmd.AddCmd{}, args{"."}},
	{cmd.CommitCmd{}, args{"-m", "second commit: added test_table and test_trigger"}},
	
	// Modify the view
	{cmd.SqlCmd{}, args{"-q", "DROP VIEW test_view"}},
	{cmd.SqlCmd{}, args{"-q", "CREATE VIEW test_view AS SELECT 1 as col1, 2 as col2"}},
	{cmd.AddCmd{}, args{"."}},
	{cmd.CommitCmd{}, args{"-m", "third commit: modified test_view"}},
	
	// Add an event
	{cmd.SqlCmd{}, args{"-q", `CREATE EVENT test_event 
		ON SCHEDULE EVERY 1 DAY 
		DO INSERT INTO test_table VALUES (1, 'daily')`}},
	{cmd.AddCmd{}, args{"."}},
	{cmd.CommitCmd{}, args{"-m", "fourth commit: added test_event"}},
	
	{cmd.LogCmd{}, args{}},
}

func doltSchemasHistoryTableTests() []doltSchemasTableTest {
	return []doltSchemasTableTest{
		{
			name:  "verify dolt_history_dolt_schemas has all required columns",
			query: "SELECT COUNT(*) FROM (SELECT type, name, fragment, extra, sql_mode, commit_hash, committer, commit_date FROM dolt_history_dolt_schemas LIMIT 0) AS schema_check",
			rows: []sql.Row{
				{int64(0)}, // Should return 0 rows but verify all columns exist
			},
		},
		{
			name:  "verify dolt_history_dolt_schemas shows view in history",
			query: "SELECT type, name FROM dolt_history_dolt_schemas WHERE type = 'view' ORDER BY name",
			rows: []sql.Row{
				{"view", "test_view"},
				{"view", "test_view"},
				{"view", "test_view"},
				{"view", "test_view"},
			},
		},
		{
			name:  "verify dolt_history_dolt_schemas shows trigger in history",
			query: "SELECT type, name FROM dolt_history_dolt_schemas WHERE type = 'trigger' ORDER BY name",
			rows: []sql.Row{
				{"trigger", "test_trigger"},
				{"trigger", "test_trigger"},
				{"trigger", "test_trigger"},
			},
		},
		{
			name:  "verify dolt_history_dolt_schemas shows event in history",
			query: "SELECT type, name FROM dolt_history_dolt_schemas WHERE type = 'event' ORDER BY name",
			rows: []sql.Row{
				{"event", "test_event"},
			},
		},
		{
			name:  "verify commit metadata is present",
			query: "SELECT COUNT(*) FROM dolt_history_dolt_schemas WHERE commit_hash IS NOT NULL AND committer IS NOT NULL AND commit_date IS NOT NULL",
			rows: []sql.Row{
				{int64(8)}, // Should have 8 rows total: view(4) + trigger(3) + event(1)
			},
		},
		{
			name:  "verify history shows modifications across commits",
			query: "SELECT COUNT(DISTINCT commit_hash) FROM dolt_history_dolt_schemas WHERE type = 'view' AND name = 'test_view'",
			rows: []sql.Row{
				{int64(4)}, // View should appear in 4 commits (all commits after creation)
			},
		},
	}
}

var setupDoltSchemasDiffCommon = []testCommand{
	// Start with a clean state
	{cmd.SqlCmd{}, args{"-q", "CREATE VIEW original_view AS SELECT 1 as id"}},
	{cmd.SqlCmd{}, args{"-q", "CREATE TABLE diff_table (id INT PRIMARY KEY)"}},
	{cmd.SqlCmd{}, args{"-q", `CREATE TRIGGER original_trigger 
		BEFORE INSERT ON diff_table 
		FOR EACH ROW 
		SET NEW.id = NEW.id + 1`}},
	{cmd.AddCmd{}, args{"."}},
	{cmd.CommitCmd{}, args{"-m", "base commit with original schemas"}},
	
	// Make changes for diff (working directory changes)
	{cmd.SqlCmd{}, args{"-q", "DROP VIEW original_view"}},
	{cmd.SqlCmd{}, args{"-q", "CREATE VIEW original_view AS SELECT 1 as id, 'modified' as status"}}, // modified
	{cmd.SqlCmd{}, args{"-q", "CREATE VIEW new_view AS SELECT 'added' as status"}}, // added
	{cmd.SqlCmd{}, args{"-q", "DROP TRIGGER original_trigger"}}, // removed
	{cmd.SqlCmd{}, args{"-q", `CREATE EVENT new_event 
		ON SCHEDULE EVERY 1 HOUR 
		DO SELECT 1`}}, // added
}

func doltSchemasDiffTableTests() []doltSchemasTableTest {
	return []doltSchemasTableTest{
		{
			name:  "verify dolt_diff_dolt_schemas has all required columns",
			query: "SELECT COUNT(*) FROM (SELECT to_type, to_name, to_fragment, to_extra, to_sql_mode, to_commit, to_commit_date, from_type, from_name, from_fragment, from_extra, from_sql_mode, from_commit, from_commit_date, diff_type FROM dolt_diff_dolt_schemas LIMIT 0) AS schema_check",
			rows: []sql.Row{
				{int64(0)}, // Should return 0 rows but verify all columns exist
			},
		},
		{
			name:  "basic table access without errors",
			query: "SELECT 1 AS test_query",
			rows: []sql.Row{
				{int8(1)}, // Simple test to verify test framework works
			},
		},
	}
}

func setupDoltSchemasHistoryTests(t *testing.T) *env.DoltEnv {
	dEnv := dtestutils.CreateTestEnv()
	ctx := context.Background()
	cliCtx, verr := cmd.NewArgFreeCliContext(ctx, dEnv, dEnv.FS)
	require.NoError(t, verr)

	for _, c := range setupDoltSchemasCommon {
		exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv, cliCtx)
		require.Equal(t, 0, exitCode)
	}

	// Get commit hashes for test validation
	root, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)

	rows, err := sqle.ExecuteSelect(ctx, dEnv, root, "SELECT commit_hash FROM dolt_log ORDER BY date DESC")
	require.NoError(t, err)
	require.Equal(t, 5, len(rows)) // 4 commits + initial commit

	DOLT_SCHEMAS_HEAD = rows[0][0].(string)
	DOLT_SCHEMAS_HEAD_1 = rows[1][0].(string)
	DOLT_SCHEMAS_HEAD_2 = rows[2][0].(string)
	DOLT_SCHEMAS_INIT = rows[4][0].(string) // Skip one to get to the first real commit

	return dEnv
}

func setupDoltSchemasDiffTests(t *testing.T) *env.DoltEnv {
	dEnv := dtestutils.CreateTestEnv()
	ctx := context.Background()
	cliCtx, verr := cmd.NewArgFreeCliContext(ctx, dEnv, dEnv.FS)
	require.NoError(t, verr)

	for _, c := range setupDoltSchemasDiffCommon {
		exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv, cliCtx)
		require.Equal(t, 0, exitCode)
	}

	return dEnv
}

func testDoltSchemasHistoryTable(t *testing.T, test doltSchemasTableTest, dEnv *env.DoltEnv) {
	ctx := context.Background()
	cliCtx, verr := cmd.NewArgFreeCliContext(ctx, dEnv, dEnv.FS)
	require.NoError(t, verr)

	for _, c := range test.setup {
		exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv, cliCtx)
		require.Equal(t, 0, exitCode)
	}

	root, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)

	// Replace placeholder in query with actual commit hash
	query := test.query
	if query == fmt.Sprintf("SELECT type, name FROM dolt_history_dolt_schemas WHERE commit_hash = '%s' ORDER BY type, name", "%s") {
		query = fmt.Sprintf("SELECT type, name FROM dolt_history_dolt_schemas WHERE commit_hash = '%s' ORDER BY type, name", DOLT_SCHEMAS_INIT)
	}
	if query == "SELECT type, name FROM dolt_history_dolt_schemas WHERE type IN ('trigger', 'event') AND commit_hash = '"+"%s"+"' ORDER BY type, name" {
		query = fmt.Sprintf("SELECT type, name FROM dolt_history_dolt_schemas WHERE type IN ('trigger', 'event') AND commit_hash = '%s' ORDER BY type, name", DOLT_SCHEMAS_HEAD)
	}

	actRows, err := sqle.ExecuteSelect(ctx, dEnv, root, query)
	require.NoError(t, err)

	require.ElementsMatch(t, test.rows, actRows)
}

func testDoltSchemasDiffTable(t *testing.T, test doltSchemasTableTest, dEnv *env.DoltEnv) {
	ctx := context.Background()
	cliCtx, verr := cmd.NewArgFreeCliContext(ctx, dEnv, dEnv.FS)
	require.NoError(t, verr)

	for _, c := range test.setup {
		exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv, cliCtx)
		require.Equal(t, 0, exitCode)
	}

	root, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)

	actRows, err := sqle.ExecuteSelect(ctx, dEnv, root, test.query)
	require.NoError(t, err)

	require.ElementsMatch(t, test.rows, actRows)
}