// Copyright 2020 Liquidata, Inc.
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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"
	"github.com/liquidata-inc/go-mysql-server/sql"
)

func TestHistoryTable(t *testing.T) {
	for _, test := range historyTableTests {
		t.Run(test.name, func(t *testing.T) {
			testHistoryTable(t, test)
		})
	}
}

type historyTableTest struct {
	name     string
	query    string
	setup    []testCommand
	rows     []sql.Row
}

type testCommand struct {
	cmd  cli.Command
	args []string
}

var setupCommon = []testCommand{
	{commands.SqlCmd{}, []string{"-q", "create table test (" +
		"pk int not null primary key," +
		"c0 int);"}},
	{commands.AddCmd{}, []string{"."}},
	{commands.CommitCmd{}, []string{"-m", "first"}},
	{commands.SqlCmd{}, []string{"-q", "insert into test values " +
		"(0,0)," +
		"(1,1)," +
		"(2,2)," +
		"(3,3);"}},
	{commands.AddCmd{}, []string{"."}},
	{commands.CommitCmd{}, []string{"-m", "second"}},
	{commands.SqlCmd{}, []string{"-q", "insert into test values " +
		"(4,4)," +
		"(5,5)," +
		"(6,6)," +
		"(7,7);"}},
	{commands.AddCmd{}, []string{"."}},
	{commands.CommitCmd{}, []string{"-m", "third"}},
	{commands.SqlCmd{}, []string{"-q", "update test set c0 = c0+10 where c0 % 2 = 0"}},
	{commands.AddCmd{}, []string{"."}},
	{commands.CommitCmd{}, []string{"-m", "fourth"}},
}

var historyTableTests = []historyTableTest{
	{
		name: "select pk, c0 from dolt_history_test",
		query: "select pk, c0 from dolt_history_test",
		rows: []sql.Row{
			{int32(0), int32(10)},
			{int32(1), int32(1)},
			{int32(2), int32(12)},
			{int32(3), int32(3)},
			{int32(4), int32(14)},
			{int32(5), int32(5)},
			{int32(6), int32(16)},
			{int32(7), int32(7)},
			{int32(0), int32(0)},
			{int32(1), int32(1)},
			{int32(2), int32(2)},
			{int32(3), int32(3)},
			{int32(4), int32(4)},
			{int32(5), int32(5)},
			{int32(6), int32(6)},
			{int32(7), int32(7)},
			{int32(0), int32(0)},
			{int32(1), int32(1)},
			{int32(2), int32(2)},
			{int32(3), int32(3)},

		},
	},
}

func testHistoryTable(t *testing.T, test historyTableTest) {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()

	for _, c := range setupCommon {
		exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv)
		require.Equal(t, 0, exitCode)
	}
	for _, c := range test.setup {
		exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv)
		require.Equal(t, 0, exitCode)
	}

	root, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)

	actRows, err := sqle.ExecuteSelect(dEnv, dEnv.DoltDB, root, test.query)
	require.NoError(t, err)

	require.Equal(t, len(test.rows), len(actRows))
	for i := range test.rows {
		assert.Equal(t, test.rows[i], actRows[i])
	}
}
