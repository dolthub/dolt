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

package integration_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	cmd "github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
)

func TestHistoryTable(t *testing.T) {
	SkipByDefaultInCI(t)
	ctx := context.Background()
	dEnv := setupHistoryTests(t)
	defer dEnv.DoltDB(ctx).Close()
	for _, test := range historyTableTests() {
		t.Run(test.name, func(t *testing.T) {
			testHistoryTable(t, test, dEnv)
		})
	}
}

// SkipByDefaultInCI skips the currently executing test as long as the CI env var is set
// (GitHub Actions sets this automatically) and the DOLT_TEST_RUN_NON_RACE_TESTS env var
// is not set. This is useful for filtering out tests that cause race detection to fail.
func SkipByDefaultInCI(t *testing.T) {
	if os.Getenv("CI") != "" && os.Getenv("DOLT_TEST_RUN_NON_RACE_TESTS") == "" {
		t.Skip()
	}
}

type historyTableTest struct {
	name  string
	setup []testCommand
	query string
	rows  []sql.Row
}

type testCommand struct {
	cmd  cli.Command
	args args
}

type args []string

var setupCommon = []testCommand{
	{cmd.SqlCmd{}, args{"-q", "create table test (" +
		"pk int not null primary key," +
		"c0 int);"}},
	{cmd.AddCmd{}, args{"."}},
	{cmd.CommitCmd{}, args{"-m", "first"}},
	{cmd.SqlCmd{}, args{"-q", "insert into test values " +
		"(0,0)," +
		"(1,1);"}},
	{cmd.AddCmd{}, args{"."}},
	{cmd.CommitCmd{}, args{"-m", "second"}},
	{cmd.SqlCmd{}, args{"-q", "insert into test values " +
		"(2,2)," +
		"(3,3);"}},
	{cmd.AddCmd{}, args{"."}},
	{cmd.CommitCmd{}, args{"-m", "third"}},
	{cmd.SqlCmd{}, args{"-q", "update test set c0 = c0+10 where c0 % 2 = 0"}},
	{cmd.AddCmd{}, args{"."}},
	{cmd.CommitCmd{}, args{"-m", "fourth"}},
	{cmd.LogCmd{}, args{}},
}

func historyTableTests() []historyTableTest {
	return []historyTableTest{
		{
			name:  "select pk, c0 from dolt_history_test",
			query: "select pk, c0 from dolt_history_test",
			rows: []sql.Row{
				{int32(0), int32(10)},
				{int32(1), int32(1)},
				{int32(2), int32(12)},
				{int32(3), int32(3)},
				{int32(0), int32(0)},
				{int32(1), int32(1)},
				{int32(2), int32(2)},
				{int32(3), int32(3)},
				{int32(0), int32(0)},
				{int32(1), int32(1)},
			},
		},
		{
			name:  "select commit_hash from dolt_history_test",
			query: "select commit_hash from dolt_history_test",
			rows: []sql.Row{
				{HEAD},
				{HEAD},
				{HEAD},
				{HEAD},
				{HEAD_1},
				{HEAD_1},
				{HEAD_1},
				{HEAD_1},
				{HEAD_2},
				{HEAD_2},
			},
		},
		{
			name:  "filter for a specific commit hash",
			query: fmt.Sprintf("select pk, c0, commit_hash from dolt_history_test where commit_hash = '%s';", HEAD_1),
			rows: []sql.Row{
				{int32(0), int32(0), HEAD_1},
				{int32(1), int32(1), HEAD_1},
				{int32(2), int32(2), HEAD_1},
				{int32(3), int32(3), HEAD_1},
			},
		},
		{
			name:  "filter out a specific commit hash",
			query: fmt.Sprintf("select pk, c0, commit_hash from dolt_history_test where commit_hash != '%s';", HEAD_1),
			rows: []sql.Row{
				{int32(0), int32(10), HEAD},
				{int32(1), int32(1), HEAD},
				{int32(2), int32(12), HEAD},
				{int32(3), int32(3), HEAD},
				{int32(0), int32(0), HEAD_2},
				{int32(1), int32(1), HEAD_2},
			},
		},
		{
			name: "compound or filter on commit hash",
			query: fmt.Sprintf("select pk, c0, commit_hash from dolt_history_test "+
				"where commit_hash = '%s' or commit_hash = '%s';", HEAD_1, HEAD_2),
			rows: []sql.Row{
				{int32(0), int32(0), HEAD_1},
				{int32(1), int32(1), HEAD_1},
				{int32(2), int32(2), HEAD_1},
				{int32(3), int32(3), HEAD_1},
				{int32(0), int32(0), HEAD_2},
				{int32(1), int32(1), HEAD_2},
			},
		},
		{
			name: "commit hash in value set",
			query: fmt.Sprintf("select pk, c0, commit_hash from dolt_history_test "+
				"where commit_hash in ('%s', '%s');", HEAD_1, HEAD_2),
			rows: []sql.Row{
				{int32(0), int32(0), HEAD_1},
				{int32(1), int32(1), HEAD_1},
				{int32(2), int32(2), HEAD_1},
				{int32(3), int32(3), HEAD_1},
				{int32(0), int32(0), HEAD_2},
				{int32(1), int32(1), HEAD_2},
			},
		},
		{
			name: "commit hash not in value set",
			query: fmt.Sprintf("select pk, c0, commit_hash from dolt_history_test "+
				"where commit_hash not in ('%s','%s');", HEAD_1, HEAD_2),
			rows: []sql.Row{
				{int32(0), int32(10), HEAD},
				{int32(1), int32(1), HEAD},
				{int32(2), int32(12), HEAD},
				{int32(3), int32(3), HEAD},
			},
		},
		{
			name:  "commit is not null",
			query: "select pk, c0, commit_hash from dolt_history_test where commit_hash is not null;",
			rows: []sql.Row{
				{int32(0), int32(10), HEAD},
				{int32(1), int32(1), HEAD},
				{int32(2), int32(12), HEAD},
				{int32(3), int32(3), HEAD},
				{int32(0), int32(0), HEAD_1},
				{int32(1), int32(1), HEAD_1},
				{int32(2), int32(2), HEAD_1},
				{int32(3), int32(3), HEAD_1},
				{int32(0), int32(0), HEAD_2},
				{int32(1), int32(1), HEAD_2},
			},
		},
		{
			name:  "commit is null",
			query: "select * from dolt_history_test where commit_hash is null;",
			rows:  []sql.Row{},
		},
	}
}

var HEAD = ""   // HEAD
var HEAD_1 = "" // HEAD~1
var HEAD_2 = "" // HEAD~2
var HEAD_3 = "" // HEAD~3
var INIT = ""   // HEAD~4

func setupHistoryTests(t *testing.T) *env.DoltEnv {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()
	cliCtx, verr := cmd.NewArgFreeCliContext(ctx, dEnv, dEnv.FS)
	require.NoError(t, verr)

	for _, c := range setupCommon {
		exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv, cliCtx)
		require.Equal(t, 0, exitCode)
	}

	root, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)

	// get commit hashes from the log table
	q := "select commit_hash, date from dolt_log order by date desc;"
	rows, err := sqle.ExecuteSelect(ctx, dEnv, root, q)
	require.NoError(t, err)
	require.Equal(t, 5, len(rows))
	HEAD = rows[0][0].(string)
	HEAD_1 = rows[1][0].(string)
	HEAD_2 = rows[2][0].(string)
	HEAD_3 = rows[3][0].(string)
	INIT = rows[4][0].(string)

	return dEnv
}

func testHistoryTable(t *testing.T, test historyTableTest, dEnv *env.DoltEnv) {
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
