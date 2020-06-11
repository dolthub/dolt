// Copyright 2020 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed toIter in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package querydiff_test

import (
	"context"
	"io"
	"testing"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/diff/querydiff"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
)

type queryDifferTest struct {
	name     string
	query    string
	setup    []testCommand
	diffRows []diffRow
}

type testCommand struct {
	cmd  cli.Command
	args []string
}

type diffRow struct {
	from sql.Row
	to   sql.Row
}

var setupCommon = []testCommand{
	{commands.SqlCmd{}, []string{"-q", "create table test (pk int not null primary key, c0 int)"}},
	{commands.SqlCmd{}, []string{"-q", "insert into test values (0,0), (1,1), (2,2), (3,3)"}},
	{commands.SqlCmd{}, []string{"-q", "create table quiz (pk int not null primary key, c0 int)"}},
	{commands.SqlCmd{}, []string{"-q", "insert into quiz values (0,10), (1,11), (2,22), (3,33)"}},
	{commands.AddCmd{}, []string{"."}},
}

var queryDifferTests = []queryDifferTest{
	{
		name:  "query diff",
		query: "select * from test order by pk",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", "delete from test where pk = 1"}},
			{commands.SqlCmd{}, []string{"-q", "insert into test values (9,9)"}},
		},
		diffRows: []diffRow{
			{from: sql.Row{int32(1), int32(1)}, to: nil},
			{from: nil, to: sql.Row{int32(9), int32(9)}},
		},
	},
	{
		name:  "more from rows",
		query: "select * from test order by pk",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", "delete from test where pk > 0"}},
			{commands.SqlCmd{}, []string{"-q", "insert into test values (9,9)"}},
		},
		diffRows: []diffRow{
			{from: sql.Row{int32(1), int32(1)}, to: nil},
			{from: sql.Row{int32(2), int32(2)}, to: nil},
			{from: sql.Row{int32(3), int32(3)}, to: nil},
			{from: nil, to: sql.Row{int32(9), int32(9)}},
		},
	},
	{
		name:  "more to rows",
		query: "select * from test order by pk",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", "delete from test where pk = 1"}},
			{commands.SqlCmd{}, []string{"-q", "insert into test values (7,7)"}},
			{commands.SqlCmd{}, []string{"-q", "insert into test values (8,8)"}},
			{commands.SqlCmd{}, []string{"-q", "insert into test values (9,9)"}},
		},
		diffRows: []diffRow{
			{from: sql.Row{int32(1), int32(1)}, to: nil},
			{from: nil, to: sql.Row{int32(7), int32(7)}},
			{from: nil, to: sql.Row{int32(8), int32(8)}},
			{from: nil, to: sql.Row{int32(9), int32(9)}},
		},
	},
	{
		name:  "sort column masked out by project",
		query: "select c0 from test order by pk",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", "delete from test where pk = 1"}},
			{commands.SqlCmd{}, []string{"-q", "insert into test values (9,9)"}},
		},
		diffRows: []diffRow{
			{from: sql.Row{int32(1)}, to: nil},
			{from: nil, to: sql.Row{int32(9)}},
		},
	},
}

func TestQueryDiffer(t *testing.T) {
	for _, test := range queryDifferTests {
		t.Run(test.name, func(t *testing.T) {
			testQueryDiffer(t, test)
		})
	}
}

func testQueryDiffer(t *testing.T, test queryDifferTest) {
	dEnv := dtestutils.CreateTestEnv()
	ctx := context.Background()

	for _, c := range setupCommon {
		exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv)
		assert.Equal(t, 0, exitCode)
	}

	for _, c := range test.setup {
		exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv)
		assert.Equal(t, 0, exitCode)
	}

	fromRoot, err := dEnv.StagedRoot(ctx)
	require.NoError(t, err)
	toRoot, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)

	qd, err := querydiff.MakeQueryDiffer(ctx, dEnv, fromRoot, toRoot, test.query)
	require.NoError(t, err)

	for _, expected := range test.diffRows {
		from, to, err := qd.NextDiff()
		assert.NoError(t, err)
		assert.Equal(t, expected.from, from)
		assert.Equal(t, expected.to, to)
	}
	from, to, err := qd.NextDiff()
	assert.Nil(t, from)
	assert.Nil(t, to)
	assert.Equal(t, io.EOF, err)
}
