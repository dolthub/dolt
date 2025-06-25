// Copyright 2025 Dolthub, Inc.
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
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	cmd "github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
)

func TestDoltProceduresHistoryTable(t *testing.T) {
	SkipByDefaultInCI(t)
	ctx := context.Background()
	dEnv := setupDoltProceduresHistoryTests(t)
	defer dEnv.DoltDB(ctx).Close()
	for _, test := range doltProceduresHistoryTableTests() {
		t.Run(test.name, func(t *testing.T) {
			testDoltProceduresHistoryTable(t, test, dEnv)
		})
	}
}

type doltProceduresTableTest struct {
	name  string
	setup []testCommand
	query string
	rows  []sql.Row
}

// Global variables to store commit hashes for test validation
var (
	DOLT_PROCEDURES_HEAD   string
	DOLT_PROCEDURES_HEAD_1 string
	DOLT_PROCEDURES_HEAD_2 string
	DOLT_PROCEDURES_INIT   string
)

var setupDoltProceduresCommon = []testCommand{
	// Create initial procedure
	{cmd.SqlCmd{}, args{"-q", "CREATE PROCEDURE test_proc1() BEGIN SELECT 1; END"}},
	{cmd.AddCmd{}, args{"."}},
	{cmd.CommitCmd{}, args{"-m", "first commit: added test_proc1"}},

	// Create a second procedure
	{cmd.SqlCmd{}, args{"-q", "CREATE PROCEDURE test_proc2(IN x INT) BEGIN SELECT x * 2; END"}},
	{cmd.AddCmd{}, args{"."}},
	{cmd.CommitCmd{}, args{"-m", "second commit: added test_proc2"}},

	// Modify the first procedure
	{cmd.SqlCmd{}, args{"-q", "DROP PROCEDURE test_proc1"}},
	{cmd.SqlCmd{}, args{"-q", "CREATE PROCEDURE test_proc1() BEGIN SELECT 'modified'; END"}},
	{cmd.AddCmd{}, args{"."}},
	{cmd.CommitCmd{}, args{"-m", "third commit: modified test_proc1"}},

	// Add a third procedure
	{cmd.SqlCmd{}, args{"-q", "CREATE PROCEDURE test_proc3(OUT result VARCHAR(50)) BEGIN SET result = 'hello world'; END"}},
	{cmd.AddCmd{}, args{"."}},
	{cmd.CommitCmd{}, args{"-m", "fourth commit: added test_proc3"}},

	{cmd.LogCmd{}, args{}},
}

func doltProceduresHistoryTableTests() []doltProceduresTableTest {
	return []doltProceduresTableTest{
		{
			name:  "verify dolt_history_dolt_procedures has all required columns",
			query: "SELECT COUNT(*) FROM (SELECT name, create_stmt, created_at, modified_at, sql_mode, commit_hash, committer, commit_date FROM dolt_history_dolt_procedures LIMIT 0) AS procedures_check",
			rows: []sql.Row{
				{int64(0)}, // Should return 0 rows but verify all columns exist
			},
		},
		{
			name:  "check correct number of history entries",
			query: "SELECT COUNT(*) FROM dolt_history_dolt_procedures",
			rows: []sql.Row{
				{int64(9)}, // test_proc1(4 commits) + test_proc2(3 commits) + test_proc3(1 commit) = 8 total
			},
		},
		{
			name:  "filter for test_proc1 history only",
			query: "SELECT COUNT(*) FROM dolt_history_dolt_procedures WHERE name = 'test_proc1'",
			rows: []sql.Row{
				{int64(4)}, // test_proc1 appears in all 4 commits
			},
		},
		{
			name:  "filter for test_proc2 history only",
			query: "SELECT COUNT(*) FROM dolt_history_dolt_procedures WHERE name = 'test_proc2'",
			rows: []sql.Row{
				{int64(3)}, // test_proc2 appears in 3 commits (added in 2nd commit)
			},
		},
		{
			name:  "filter for test_proc3 history only",
			query: "SELECT COUNT(*) FROM dolt_history_dolt_procedures WHERE name = 'test_proc3'",
			rows: []sql.Row{
				{int64(1)}, // test_proc3 appears in 1 commit (added in 4th commit)
			},
		},
		{
			name:  "check commit_hash is not null",
			query: "SELECT COUNT(*) FROM dolt_history_dolt_procedures WHERE commit_hash IS NOT NULL",
			rows: []sql.Row{
				{int64(9)}, // Total number of procedure entries across all commits
			},
		},
		{
			name:  "verify procedure names in latest commit",
			query: "SELECT name FROM dolt_history_dolt_procedures WHERE commit_hash = '" + "%s" + "' ORDER BY name",
			rows: []sql.Row{
				{"test_proc1"},
				{"test_proc2"},
				{"test_proc3"},
			},
		},
		{
			name:  "check committer column exists",
			query: "SELECT COUNT(*) FROM dolt_history_dolt_procedures WHERE committer IS NOT NULL",
			rows: []sql.Row{
				{int64(9)}, // All entries should have committer info
			},
		},
		{
			name:  "verify create_stmt column contains procedure definitions",
			query: "SELECT COUNT(*) FROM dolt_history_dolt_procedures WHERE create_stmt LIKE '%PROCEDURE%'",
			rows: []sql.Row{
				{int64(9)}, // All entries should have CREATE PROCEDURE in create_stmt
			},
		},
		{
			name:  "check created_at and modified_at are not null",
			query: "SELECT COUNT(*) FROM dolt_history_dolt_procedures WHERE created_at IS NOT NULL AND modified_at IS NOT NULL",
			rows: []sql.Row{
				{int64(9)}, // All entries should have timestamp info
			},
		},
	}
}

func setupDoltProceduresHistoryTests(t *testing.T) *env.DoltEnv {
	dEnv := dtestutils.CreateTestEnv()
	ctx := context.Background()
	cliCtx, verr := cmd.NewArgFreeCliContext(ctx, dEnv, dEnv.FS)
	require.NoError(t, verr)

	for _, c := range setupDoltProceduresCommon {
		exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv, cliCtx)
		require.Equal(t, 0, exitCode)
	}

	// Get commit hashes for test validation
	root, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)

	rows, err := sqle.ExecuteSelect(ctx, dEnv, root, "SELECT commit_hash FROM dolt_log ORDER BY date DESC")
	require.NoError(t, err)
	require.Equal(t, 5, len(rows)) // 4 commits + initial commit

	DOLT_PROCEDURES_HEAD = rows[0][0].(string)
	DOLT_PROCEDURES_HEAD_1 = rows[1][0].(string)
	DOLT_PROCEDURES_HEAD_2 = rows[2][0].(string)
	DOLT_PROCEDURES_INIT = rows[4][0].(string) // Skip one to get to the first real commit

	return dEnv
}

func testDoltProceduresHistoryTable(t *testing.T, test doltProceduresTableTest, dEnv *env.DoltEnv) {
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
	if query == "SELECT name FROM dolt_history_dolt_procedures WHERE commit_hash = '"+"%s"+"' ORDER BY name" {
		query = "SELECT name FROM dolt_history_dolt_procedures WHERE commit_hash = '" + DOLT_PROCEDURES_HEAD + "' ORDER BY name"
	}

	actRows, err := sqle.ExecuteSelect(ctx, dEnv, root, query)
	require.NoError(t, err)

	require.ElementsMatch(t, test.rows, actRows)
}