// Copyright 2021 Dolthub, Inc.
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

package rebase_test

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	cmd "github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
)

func TestFilterBranch(t *testing.T) {
	for _, test := range filterBranchTests() {
		t.Run(test.name, func(t *testing.T) {
			testFilterBranch(t, test)
		})
	}
}

type filterBranchTest struct {
	name    string
	setup   []testCommand
	asserts []testAssertion
}

type testCommand struct {
	cmd  cli.Command
	args args
}

type args []string

type testAssertion struct {
	setup []testCommand
	query string
	rows  []sql.Row
}

var setupCommon = []testCommand{
	{cmd.SqlCmd{}, args{"-q",
		`create table test (
			pk int not null primary key,
			c0 int);`},
	},
	{cmd.SqlCmd{}, args{"-q", "insert into test values (0,0),(1,1),(2,2);"}},
	{cmd.SqlCmd{}, args{"-q",
		`create table to_drop (
			pk int not null primary key,
			c0 int);`},
	},
	{cmd.AddCmd{}, args{"-A"}},
	{cmd.CommitCmd{}, args{"-m", "added test tables"}},
}

func filterBranchTests() []filterBranchTest {
	return []filterBranchTest{
		{
			name: "smoke test",
			asserts: []testAssertion{
				{
					query: "select * from test",
					rows: []sql.Row{
						{int32(0), int32(0)},
						{int32(1), int32(1)},
						{int32(2), int32(2)},
					},
				},
			},
		},
		{
			name: "filter-branch with single branch",
			setup: []testCommand{
				{cmd.SqlCmd{}, args{"-q", "INSERT INTO test VALUES (4,4),(5,5),(6,6);"}},
				{cmd.AddCmd{}, args{"-A"}},
				{cmd.CommitCmd{}, args{"-m", "added more rows"}},
				{cmd.SqlCmd{}, args{"-q", "INSERT INTO test VALUES (7,7),(8,8),(9,9);"}},
				{cmd.AddCmd{}, args{"-A"}},
				{cmd.CommitCmd{}, args{"-m", "added more rows againg"}},
				{cmd.FilterBranchCmd{}, args{"--all", "-q", "DELETE FROM test WHERE pk IN (5,8);"}},
			},
			asserts: []testAssertion{
				{
					query: "SELECT * FROM test",
					rows: []sql.Row{
						{int32(0), int32(0)},
						{int32(1), int32(1)},
						{int32(2), int32(2)},
						{int32(4), int32(4)},
						{int32(6), int32(6)},
						{int32(7), int32(7)},
						{int32(9), int32(9)},
					},
				},
			},
		},
		{
			name: "filter-branch with multiple branches",
			setup: []testCommand{
				{cmd.CheckoutCmd{}, args{"-b", "other"}},
				{cmd.SqlCmd{}, args{"-q", "INSERT INTO test VALUES (4,4),(5,5),(6,6);"}},
				{cmd.AddCmd{}, args{"-A"}},
				{cmd.CommitCmd{}, args{"-m", "added more rows on other"}},
				{cmd.CheckoutCmd{}, args{env.DefaultInitBranch}},
				{cmd.SqlCmd{}, args{"-q", "INSERT INTO test VALUES (7,7),(8,8),(9,9);"}},
				{cmd.AddCmd{}, args{"-A"}},
				{cmd.CommitCmd{}, args{"-m", "added more rows on main"}},
				{cmd.FilterBranchCmd{}, args{"--all", "-q", "DELETE FROM test WHERE pk > 4;"}},
			},
			asserts: []testAssertion{
				{
					query: "SELECT pk,c0 FROM dolt_history_test ORDER BY pk, c0",
					rows: []sql.Row{
						{int32(0), int32(0)},
						{int32(0), int32(0)},
						{int32(1), int32(1)},
						{int32(1), int32(1)},
						{int32(2), int32(2)},
						{int32(2), int32(2)},
					},
				},
				{
					query: "SELECT pk,c0 FROM dolt_history_test ORDER BY pk",
					rows: []sql.Row{
						{int32(0), int32(0)},
						{int32(0), int32(0)},
						{int32(1), int32(1)},
						{int32(1), int32(1)},
						{int32(2), int32(2)},
						{int32(2), int32(2)},
					},
				},
				{
					setup: []testCommand{
						{cmd.CheckoutCmd{}, args{"other"}},
					},
					query: "SELECT * FROM test;",
					rows: []sql.Row{
						{int32(0), int32(0)},
						{int32(1), int32(1)},
						{int32(2), int32(2)},
						{int32(4), int32(4)},
					},
				},
				{
					query: "SELECT pk,c0 FROM dolt_history_test ORDER BY pk,c0",
					rows: []sql.Row{
						{int32(0), int32(0)},
						{int32(0), int32(0)},
						{int32(1), int32(1)},
						{int32(1), int32(1)},
						{int32(2), int32(2)},
						{int32(2), int32(2)},
						{int32(4), int32(4)},
					},
				},
			},
		},
		{
			name: "filter-branch with missing table",
			setup: []testCommand{
				{cmd.SqlCmd{}, args{"-q", "DROP TABLE test;"}},
				{cmd.AddCmd{}, args{"-A"}},
				{cmd.CommitCmd{}, args{"-m", "dropped test"}},
			},
			asserts: []testAssertion{
				{
					setup: []testCommand{
						// expected error: "table not found: test"
						{cmd.FilterBranchCmd{}, args{"--continue", "-q", "DELETE FROM test WHERE pk > 1;"}},
					},
				},
				{
					query: "SELECT count(*) FROM test AS OF 'HEAD~1';",
					rows: []sql.Row{
						{int64(2)},
					},
				},
			},
		},
	}
}

func setupFilterBranchTests(t *testing.T) *env.DoltEnv {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()
	cliCtx, err := cmd.NewArgFreeCliContext(ctx, dEnv, dEnv.FS)
	require.NoError(t, err)

	for _, c := range setupCommon {
		exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv, cliCtx)
		require.Equal(t, 0, exitCode)
	}

	return dEnv
}

func testFilterBranch(t *testing.T, test filterBranchTest) {
	ctx := context.Background()
	dEnv := setupFilterBranchTests(t)
	defer dEnv.DoltDB.Close()
	cliCtx, err := cmd.NewArgFreeCliContext(ctx, dEnv, dEnv.FS)
	require.NoError(t, err)

	for _, c := range test.setup {
		exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv, cliCtx)
		require.Equal(t, 0, exitCode)
	}

	for _, a := range test.asserts {
		for _, c := range a.setup {
			exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv, cliCtx)
			require.Equal(t, 0, exitCode)
		}

		t.Run(a.query, func(t *testing.T) {
			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)

			actRows, err := sqle.ExecuteSelect(dEnv, root, a.query)
			require.NoError(t, err)
			require.Equal(t, a.rows, actRows)
		})
	}
}
