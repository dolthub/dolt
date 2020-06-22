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
	diffRows []rowDiff
}

type testCommand struct {
	cmd  cli.Command
	args []string
}

type rowDiff struct {
	from sql.Row
	to   sql.Row
}

func TestQueryDiffer(t *testing.T) {
	for _, test := range queryDifferTests {
		t.Run(test.name, func(t *testing.T) {
			testQueryDiffer(t, test)
		})
	}
}

var setupCommon = []testCommand{
	{commands.SqlCmd{}, []string{"-q", "create table test (pk int not null primary key, c0 int)"}},
	{commands.SqlCmd{}, []string{"-q", "insert into test values (0,0), (1,1), (2,2), (3,3)"}},
	{commands.SqlCmd{}, []string{"-q", "create table quiz (pk int not null primary key, c0 int)"}},
	{commands.SqlCmd{}, []string{"-q", "insert into quiz values (0,10), (1,11), (2,22), (3,33)"}},
	{commands.SqlCmd{}, []string{"-q", "create view squared as select c0*c0 as c0c0 from test order by pk"}},
	{commands.AddCmd{}, []string{"."}},
	{commands.CommitCmd{}, []string{"-m", "setup common"}},
}

var queryDifferTests = []queryDifferTest{
	{
		name:  "query diff",
		query: "select * from test",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", "delete from test where pk = 1"}},
			{commands.SqlCmd{}, []string{"-q", "insert into test values (9,9)"}},
		},
		diffRows: []rowDiff{
			{from: sql.Row{int32(1), int32(1)}, to: nil},
			{from: nil, to: sql.Row{int32(9), int32(9)}},
		},
	},
	{
		name:  "more from rows",
		query: "select * from test",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", "delete from test where pk > 0"}},
			{commands.SqlCmd{}, []string{"-q", "insert into test values (9,9)"}},
		},
		diffRows: []rowDiff{
			{from: sql.Row{int32(1), int32(1)}, to: nil},
			{from: sql.Row{int32(2), int32(2)}, to: nil},
			{from: sql.Row{int32(3), int32(3)}, to: nil},
			{from: nil, to: sql.Row{int32(9), int32(9)}},
		},
	},
	{
		name:  "more to rows",
		query: "select * from test",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", "delete from test where pk = 1"}},
			{commands.SqlCmd{}, []string{"-q", "insert into test values (7,7)"}},
			{commands.SqlCmd{}, []string{"-q", "insert into test values (8,8)"}},
			{commands.SqlCmd{}, []string{"-q", "insert into test values (9,9)"}},
		},
		diffRows: []rowDiff{
			{from: sql.Row{int32(1), int32(1)}, to: nil},
			{from: nil, to: sql.Row{int32(7), int32(7)}},
			{from: nil, to: sql.Row{int32(8), int32(8)}},
			{from: nil, to: sql.Row{int32(9), int32(9)}},
		},
	},
	{
		name:  "sort column masked out by project",
		query: "select c0 from test",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", "delete from test where pk = 1"}},
			{commands.SqlCmd{}, []string{"-q", "insert into test values (9,9)"}},
		},
		diffRows: []rowDiff{
			{from: sql.Row{int32(1)}, to: nil},
			{from: nil, to: sql.Row{int32(9)}},
		},
	},
	{
		name:  "select from join",
		query: "select * from test join quiz on test.pk = quiz.pk",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", "delete from test where pk = 1"}},
			{commands.SqlCmd{}, []string{"-q", "insert into test values (9,9)"}},
			{commands.SqlCmd{}, []string{"-q", "insert into quiz values (9,99)"}},
		},
		diffRows: []rowDiff{
			{from: sql.Row{int32(1), int32(1), int32(1), int32(11)}, to: nil},
			{from: nil, to: sql.Row{int32(9), int32(9), int32(9), int32(99)}},
		},
	},
	{
		name:  "project a join",
		query: "select test.pk*quiz.pk, test.c0, quiz.c0 from test join quiz on test.pk = quiz.pk",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", "delete from test where pk = 1"}},
			{commands.SqlCmd{}, []string{"-q", "insert into test values (9,9)"}},
			{commands.SqlCmd{}, []string{"-q", "insert into quiz values (9,99)"}},
		},
		diffRows: []rowDiff{
			{from: sql.Row{int64(1), int32(1), int32(11)}, to: nil},
			{from: nil, to: sql.Row{int64(81), int32(9), int32(99)}},
		},
	},
	{
		name:  "select from view",
		query: "select * from squared",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", "delete from test where pk = 1"}},
			{commands.SqlCmd{}, []string{"-q", "insert into test values (9,9)"}},
		},
		diffRows: []rowDiff{
			{from: sql.Row{int64(1)}, to: nil},
			{from: nil, to: sql.Row{int64(81)}},
		},
	},
	{
		name:  "filter a view",
		query: "select * from squared where c0c0 % 2 = 0",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", "delete from test where pk = 1"}},
			{commands.SqlCmd{}, []string{"-q", "delete from test where pk = 2"}},
			{commands.SqlCmd{}, []string{"-q", "insert into test values (9,9)"}},
			{commands.SqlCmd{}, []string{"-q", "insert into test values (10,10)"}},
		},
		diffRows: []rowDiff{
			{from: sql.Row{int64(4)}, to: nil},
			{from: nil, to: sql.Row{int64(100)}},
		},
	},
	{
		name:  "project a view",
		query: "select sqrt(c0c0), c0c0 from squared",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", "delete from test where pk = 1"}},
			{commands.SqlCmd{}, []string{"-q", "insert into test values (9,9)"}},
		},
		diffRows: []rowDiff{
			{from: sql.Row{float64(1), int64(1)}, to: nil},
			{from: nil, to: sql.Row{float64(9), int64(81)}},
		},
	},
	{
		name:  "reorder a view",
		query: "select * from squared order by c0c0",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", "delete from test where pk = 1"}},
			{commands.SqlCmd{}, []string{"-q", "insert into test values (-2,-2)"}},
			{commands.SqlCmd{}, []string{"-q", "insert into test values (9,9)"}},
		},
		diffRows: []rowDiff{
			{from: sql.Row{int64(1)}, to: nil},
			{from: nil, to: sql.Row{int64(4)}},
			{from: nil, to: sql.Row{int64(81)}},
		},
	},
	{
		name:  "join a view",
		query: "select c0c0, pk, c0 from squared join quiz on sqrt(squared.c0c0) = quiz.pk",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", "delete from test where pk = 1"}},
			{commands.SqlCmd{}, []string{"-q", "insert into test values (-2,-2)"}},
			{commands.SqlCmd{}, []string{"-q", "insert into test values (9,9)"}},
		},
		diffRows: []rowDiff{
			{from: nil, to: sql.Row{int64(4), int32(2), int32(22)}},
			{from: sql.Row{int64(1), int32(1), int32(11)}, to: nil},
		},
	},
	{
		name:  "join two views with explosions",
		query: "select * from v1 join v2 on v1.one = v2.one;",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", "create view v1 as select c0 as one, c0*c0 as two, c0*c0*c0 as three from test"}},
			{commands.SqlCmd{}, []string{"-q", "create view v2 as select c0 as one, c0*c0 as two, c0*c0*c0 as three from test"}},
			{commands.AddCmd{}, []string{"."}},
			{commands.CommitCmd{}, []string{"-m", "create two views v1 and v2"}},
			{commands.SqlCmd{}, []string{"-q", "delete from test where pk = 1"}},
			{commands.SqlCmd{}, []string{"-q", "insert into test values (9,9)"}},
		},
		diffRows: []rowDiff{
			{from: sql.Row{int32(1), int64(1), int64(1), int32(1), int64(1), int64(1)}, to: nil},
			{from: nil, to: sql.Row{int32(9), int64(81), int64(729), int32(9), int64(81), int64(729)}},
		},
	},
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

	fromRoot, err := dEnv.HeadRoot(ctx)
	require.NoError(t, err)
	toRoot, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)

	qd, err := querydiff.MakeQueryDiffer(ctx, dEnv, fromRoot, toRoot, test.query)
	require.NoError(t, err)

	qd.Start()
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
