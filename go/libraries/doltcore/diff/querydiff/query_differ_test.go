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
	"sort"
	"strings"
	"testing"

	"github.com/liquidata-inc/go-mysql-server/enginetest"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/diff/querydiff"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	det "github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle/enginetest"
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

var queryDifferTestSetup = []testCommand{
	{commands.SqlCmd{}, []string{"-q", "create table test (" +
		"pk int not null primary key," +
		"c0 int);"}},
	{commands.SqlCmd{}, []string{"-q", "insert into test values " +
		"(0,0)," +
		"(1,1)," +
		"(2,2)," +
		"(3,3);"}},
	{commands.SqlCmd{}, []string{"-q", "create table quiz (" +
		"pk int not null primary key," +
		"c0 int);"}},
	{commands.SqlCmd{}, []string{"-q", "insert into quiz values " +
		"(0,10)," +
		"(1,11)," +
		"(2,22)," +
		"(3,33);"}},
	{commands.SqlCmd{}, []string{"-q", "create view squared as select c0*c0 as c0c0 from test order by pk;"}},
	{commands.AddCmd{}, []string{"."}},
	{commands.CommitCmd{}, []string{"-m", "setup common"}},
}

var queryDiffTests = [][]queryDifferTest{
	selectTests,
	joinTests,
	groupByTests,
}

var selectTests = []queryDifferTest{
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
		name:  "query: select from view",
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
}

var joinTests = []queryDifferTest{
	{
		name:  "query: select from join",
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
		query: "select v1.three, v1.one*test.c0, POW(test.c0,3) from v1 join test on v1.one = test.c0;",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", "create view v1 as select c0 as one, c0*c0*c0 as three from test;"}},
			{commands.AddCmd{}, []string{"."}},
			{commands.CommitCmd{}, []string{"-m", "create two views v1 and v2;"}},
			{commands.SqlCmd{}, []string{"-q", "delete from test where pk = 1;"}},
			{commands.SqlCmd{}, []string{"-q", "insert into test values (9,9);"}},
		},
		diffRows: []rowDiff{
			{from: sql.Row{int64(1), int64(1), float64(1)}, to: nil},
			{from: nil, to: sql.Row{int64(729), int64(81), float64(729)}},
		},
	},
}

var groupByTests = []queryDifferTest{
	{
		name:  "sum groups",
		query: "select sum(pk) from test",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", "delete from test where pk = 1"}},
			{commands.SqlCmd{}, []string{"-q", "insert into test values (100,0)"}},
		},
		diffRows: []rowDiff{
			{from: sql.Row{float64(6)}, to: sql.Row{float64(105)}},
		},
	},
	{
		name:  "sum groups",
		query: "select sum(pk),c0 from test group by c0",
		setup: []testCommand{
			// from queryDifferTestSetup: "insert into test values (0,0),(1,1),(2,2),(3,3);"
			{commands.SqlCmd{}, []string{"-q", "insert into test values (10,0),(11,1),(12,2),(13,3)"}},
			{commands.AddCmd{}, []string{"."}},
			{commands.CommitCmd{}, []string{"-m", "added extra rows"}},
			{commands.SqlCmd{}, []string{"-q", "delete from test where pk = 1"}},
			{commands.SqlCmd{}, []string{"-q", "insert into test values (19,9)"}},
		},
		diffRows: []rowDiff{
			{from: sql.Row{float64(12), int32(1)}, to: sql.Row{float64(11), int32(1)}},
			{from: nil, to: sql.Row{float64(19), int32(9)}},
		},
	},
	{
		name:  "sum groups having",
		query: "select sum(pk),c0 from test group by c0 having sum(pk) > 11",
		setup: []testCommand{
			// from queryDifferTestSetup: "insert into test values (0,0),(1,1),(2,2),(3,3);"
			{commands.SqlCmd{}, []string{"-q", "insert into test values (10,0),(11,1),(12,2),(13,3)"}},
			{commands.AddCmd{}, []string{"."}},
			{commands.CommitCmd{}, []string{"-m", "added extra rows"}},
			{commands.SqlCmd{}, []string{"-q", "delete from test where pk = 11"}},
			{commands.SqlCmd{}, []string{"-q", "insert into test values (100,0)"}},
			{commands.SqlCmd{}, []string{"-q", "insert into test values (22,2)"}},
		},
		diffRows: []rowDiff{
			{from: nil, to: sql.Row{float64(110), int32(0)}},
			{from: sql.Row{float64(12), int32(1)}, to: nil},
			{from: sql.Row{float64(14), int32(2)}, to: sql.Row{float64(36), int32(2)}},
		},
	},
	{
		name:  "join on sum groups",
		query: "select * from quiz join (select sum(pk), c0 from test group by c0) as subq on quiz.pk = subq.c0",
		setup: []testCommand{
			// from queryDifferTestSetup: "insert into test values (0,0),(1,1),(2,2),(3,3);"
			// from queryDifferTestSetup: "insert into quiz values (0,10),(1,11),(2,22),(3,33);"
			{commands.SqlCmd{}, []string{"-q", "insert into test values (10,0),(11,1),(12,2),(13,3)"}},
			{commands.AddCmd{}, []string{"."}},
			{commands.CommitCmd{}, []string{"-m", "added extra rows"}},
			{commands.SqlCmd{}, []string{"-q", "delete from quiz where pk = 1"}},
			{commands.SqlCmd{}, []string{"-q", "insert into test values (100,0)"}},
		},
		diffRows: []rowDiff{
			{from: sql.Row{int32(0), int32(10), float64(10), int32(0)}, to: sql.Row{int32(0), int32(10), float64(110), int32(0)}},
			{from: sql.Row{int32(1), int32(11), float64(12), int32(1)}, to: nil},
		},
	},
}

var engineTestSetup = []testCommand{
	{commands.SqlCmd{}, []string{"-q", "create table mytable (" +
		"i bigint primary key," +
		"s text);"}},
	{commands.SqlCmd{}, []string{"-q", "create table one_pk (" +
		"pk tinyint primary key, " +
		"c1 tinyint," +
		"c2 tinyint," +
		"c3 tinyint," +
		"c4 tinyint," +
		"c5 tinyint);"}},
	{commands.SqlCmd{}, []string{"-q", "create table two_pk (" +
		"pk1 tinyint," +
		"pk2 tinyint," +
		"c1 tinyint," +
		"c2 tinyint," +
		"c3 tinyint," +
		"c4 tinyint," +
		"c5 tinyint," +
		"primary key (pk1, pk2));"}},
	{commands.SqlCmd{}, []string{"-q", "create table othertable (" +
		"s2 text primary key," +
		"i2 bigint);"}},
	{commands.SqlCmd{}, []string{"-q", "create table tabletest (" +
		"i int primary key," +
		"s text);"}},
	{commands.SqlCmd{}, []string{"-q", "create table emptytable (" +
		"i int primary key," +
		"s text);"}},
	{commands.SqlCmd{}, []string{"-q", "create table other_table (" +
		"`text` text primary key," +
		"number int);"}},
	{commands.SqlCmd{}, []string{"-q", "create table bigtable (" +
		"t text primary key," +
		"n bigint);"}},
	{commands.SqlCmd{}, []string{"-q", "create table floattable (" +
		"i bigint primary key," +
		"f32 float," +
		"f64 double);"}},
	{commands.SqlCmd{}, []string{"-q", "create table niltable (" +
		"i bigint primary key," +
		"i2 bigint," +
		"b bool," +
		"f float);"}},
	{commands.SqlCmd{}, []string{"-q", "create table newlinetable (" +
		"i bigint primary key," +
		"s text);"}},
	{commands.SqlCmd{}, []string{"-q", "create table stringandtable (" +
		"k bigint primary key," +
		"i bigint," +
		"v text);"}},
	{commands.SqlCmd{}, []string{"-q", "create table reservedWordsTable (" +
		"`Timestamp` text primary key," +
		"`and` text," +
		"`or` text," +
		"`select` text);"}},
	{commands.SqlCmd{}, []string{"-q", `create view myview as select * from mytable`}},
	//{commands.SqlCmd{}, []string{"-q", "create view myview1 as select * from myhistorytable"}},
	{commands.SqlCmd{}, []string{"-q", "create view myview2 as select * from myview where i = 1"}},
	{commands.AddCmd{}, []string{"."}},
	{commands.CommitCmd{}, []string{"-m", "setup enginetest test tables"}},
	{commands.SqlCmd{}, []string{"-q", "insert into mytable values " +
		"(1, 'first row'), " +
		"(2, 'second row'), " +
		"(3, 'third row');"}},
	{commands.SqlCmd{}, []string{"-q", "insert into one_pk values " +
		"(0, 0, 0, 0, 0, 0)," +
		"(1, 10, 10, 10, 10, 10)," +
		"(2, 20, 20, 20, 20, 20)," +
		"(3, 30, 30, 30, 30, 30);"}},
	{commands.SqlCmd{}, []string{"-q", "insert into two_pk values " +
		"(0, 0, 0, 0, 0, 0, 0)," +
		"(0, 1, 10, 10, 10, 10, 10)," +
		"(1, 0, 20, 20, 20, 20, 20)," +
		"(1, 1, 30, 30, 30, 30, 30);"}},
	{commands.SqlCmd{}, []string{"-q", "insert into othertable values " +
		"('first', 3)," +
		"('second', 2)," +
		"('third', 1);"}},
	{commands.SqlCmd{}, []string{"-q", "insert into tabletest values " +
		"(1, 'first row')," +
		"(2, 'second row')," +
		"(3, 'third row');"}},
	{commands.SqlCmd{}, []string{"-q", "insert into other_table values " +
		"('a', 4)," +
		"('b', 2)," +
		"('c', 0);"}},
	{commands.SqlCmd{}, []string{"-q", "insert into bigtable values " +
		"('a', 1)," +
		"('s', 2)," +
		"('f', 3)," +
		"('g', 1)," +
		"('h', 2)," +
		"('j', 3)," +
		"('k', 1)," +
		"('l', 2)," +
		"('ñ', 4)," +
		"('z', 5)," +
		"('x', 6)," +
		"('c', 7)," +
		"('v', 8)," +
		"('b', 9);"}},
	{commands.SqlCmd{}, []string{"-q", "insert into floattable values " +
		"(1, 1.0, 1.0)," +
		"(2, 1.5, 1.5)," +
		"(3, 2.0, 2.0)," +
		"(4, 2.5, 2.5)," +
		"(-1, -1.0, -1.0)," +
		"(-2, -1.5, -1.5);"}},
	{commands.SqlCmd{}, []string{"-q", "insert into niltable values " +
		"(1, NULL, NULL, NULL)," +
		"(2, 2, 1, NULL)," +
		"(3, NULL, 0, NULL)," +
		"(4, 4, NULL, 4)," +
		"(5, NULL, 1, 5)," +
		"(6, 6, 0, 6);"}},
	{commands.SqlCmd{}, []string{"-q", "insert into newlinetable values " +
		"(1, '\nthere is some text in here')," +
		"(2, 'there is some\ntext in here')," +
		"(3, 'there is some text\nin here')," +
		"(4, 'there is some text in here\n')," +
		"(5, 'there is some text in here');"}},
	{commands.SqlCmd{}, []string{"-q", "insert into stringandtable values " +
		"(0, 0, '0')," +
		"(1, 1, '1')," +
		"(2, 2, '')," +
		"(3, 3, 'true')," +
		"(4, 4, 'false')," +
		"(5, 5, NULL)," +
		"(6, NULL, '2');"}},
	{commands.SqlCmd{}, []string{"-q", `insert into reservedWordsTable values 
		("1", "1.1", "aaa", "create");`}},
}

func setupEngineTests(t *testing.T) *env.DoltEnv {
	dEnv := dtestutils.CreateTestEnv()
	for _, c := range engineTestSetup {
		exitCode := c.cmd.Exec(context.Background(), c.cmd.Name(), c.args, dEnv)
		assert.Equal(t, 0, exitCode)
	}
	return dEnv
}

func commitData(t *testing.T, dEnv *env.DoltEnv) {
	exitCode := commands.AddCmd{}.Exec(context.Background(), "Add", []string{"."}, dEnv)
	assert.Equal(t, 0, exitCode)
	exitCode = commands.CommitCmd{}.Exec(context.Background(), "Commit", []string{"-m", "setup enginetest test tables"}, dEnv)
	assert.Equal(t, 0, exitCode)
}

var engineQueryTests = [][]enginetest.QueryTest{
	enginetest.QueryTests,
	enginetest.ViewTests,
}

var engineTestSkipSet = []string{
	// query diff doesn't handle mutlidb queries
	`SELECT * FROM foo.other_table`,
}

func skipEngineTest(test enginetest.QueryTest) bool {
	h := det.DoltHarness{}
	if h.SkipQueryTest(test.Query) {
		return true
	}

	lowerQuery := strings.ToLower(test.Query)
	if strings.Contains(lowerQuery, "myview1") {
		// todo: support for history table
		return true
	}

	for _, q := range engineTestSkipSet {
		if strings.Contains(lowerQuery, strings.ToLower(q)) {
			return true
		}
	}

	return false
}

func TestQueryDiffer(t *testing.T) {
	inner := func(t *testing.T, test queryDifferTest) {
		dEnv := dtestutils.CreateTestEnv()
		ctx := context.Background()
		for _, c := range queryDifferTestSetup {
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

	for _, testSet := range queryDiffTests {
		for _, test := range testSet {
			t.Run(test.name, func(t *testing.T) {
				inner(t, test)
			})
		}
	}
}

func TestEngineTestQueryDifferBefore(t *testing.T) {
	inner := func(t *testing.T, test enginetest.QueryTest, dEnv *env.DoltEnv) {
		if skipEngineTest(test) {
			t.Skip()
		}
		ctx := context.Background()
		fromRoot, err := dEnv.StagedRoot(ctx)
		require.NoError(t, err)
		toRoot, err := dEnv.WorkingRoot(ctx)
		require.NoError(t, err)

		qd, err := querydiff.MakeQueryDiffer(ctx, dEnv, fromRoot, toRoot, test.Query)
		if err != nil {
			if qde, ok := err.(querydiff.QueryDiffError); ok {
				t.Skipf(qde.Error())
			}
			t.Fatalf("unexpected error creating QueryDiffer: %s", err.Error())
		}

		qd.Start()
		var actual []sql.Row
		for {
			_, to, err := qd.NextDiff()
			if err == io.EOF {
				break
			}
			assert.NoError(t, err)
			actual = append(actual, to)
		}
		actual = sortExpectedResults(qd.Schema(), actual)
		test.Expected = sortExpectedResults(qd.Schema(), test.Expected)
		require.Equal(t, len(test.Expected), len(actual))

		for i, exp := range test.Expected {
			act := actual[i]
			exp, act = enginetest.WidenRow(exp), enginetest.WidenRow(act)
			assert.Equal(t, exp, act)
		}
	}

	// engineTestQueries are read-only, sharing a dEnv speeds up tests
	dEnv := setupEngineTests(t)
	for _, testSet := range engineQueryTests {
		for _, test := range testSet {
			t.Run(test.Query, func(t *testing.T) {
				inner(t, test, dEnv)
			})
		}
	}
}

func TestEngineTestQueryDifferAfter(t *testing.T) {
	inner := func(t *testing.T, test enginetest.QueryTest, dEnv *env.DoltEnv) {
		if skipEngineTest(test) {
			t.Skip()
		}

		ctx := context.Background()
		fromRoot, err := dEnv.StagedRoot(ctx)
		require.NoError(t, err)
		toRoot, err := dEnv.WorkingRoot(ctx)
		require.NoError(t, err)

		qd, err := querydiff.MakeQueryDiffer(ctx, dEnv, fromRoot, toRoot, test.Query)
		if err != nil {
			if qde, ok := err.(querydiff.QueryDiffError); ok {
				t.Skip(qde.Error())
			}
			t.Fatalf("unexpected error creating QueryDiffer: %s", err.Error())
		}

		qd.Start()
		from, to, err := qd.NextDiff()
		assert.Nil(t, from)
		assert.Nil(t, to)
		assert.Equal(t, io.EOF, err)
	}

	// engineTestQueries are read-only, sharing a dEnv speeds up tests
	dEnv := setupEngineTests(t)
	commitData(t, dEnv)
	for _, testSet := range engineQueryTests {
		for _, test := range testSet {
			t.Run(test.Query, func(t *testing.T) {
				inner(t, test, dEnv)
			})
		}
	}
}

func sortExpectedResults(sch sql.Schema, rows []sql.Row) []sql.Row {
	order := make([]plan.SortField, len(sch))
	for i, col := range sch {
		order[i] = plan.SortField{
			Column: expression.NewGetField(i, col.Type, col.Name, col.Nullable),
		}
	}
	s := &plan.Sorter{
		SortFields: order,
		Rows:       rows,
		Ctx:        sql.NewContext(context.Background()),
	}
	sort.Stable(s)
	return s.Rows
}
