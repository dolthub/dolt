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
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
)

func TestQueryDiffer(t *testing.T) {
	queryDiffTests := [][]queryDifferTest{
		selectTests,
		joinTests,
		groupByTests,
	}
	for _, testSet := range queryDiffTests {
		for _, test := range testSet {
			t.Run(test.name, func(t *testing.T) {
				testQueryDiffer(t, test)
			})
		}
	}

	dEnv := setupEngineTests(t)
	engineTestQueries := [][]engineTestQuery{
		engineTestSelectQueries,
		engineTestAggregateQueries,
	}
	for _, testSet := range engineTestQueries {
		for _, test := range testSet {
			t.Run(test.query, func(t *testing.T) {
				engineTestQueryDiffer(t, test, dEnv)
			})
		}
	}
}

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

func testQueryDiffer(t *testing.T, test queryDifferTest) {
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

// subset of test data from go-mysql-server/enginetest/testdata.go:CreateSubsetTestData()
var engineTestSetup = []testCommand{
	{commands.SqlCmd{}, []string{"-q", "create table mytable (" +
		"i bigint primary key," +
		"s text);"}},
	{commands.SqlCmd{}, []string{"-q", "insert into mytable values " +
		"(1, 'first row'), " +
		"(2, 'second row'), " +
		"(3, 'third row');"}},
	{commands.SqlCmd{}, []string{"-q", "create table one_pk (" +
		"pk tinyint primary key, " +
		"c1 tinyint," +
		"c2 tinyint," +
		"c3 tinyint," +
		"c4 tinyint," +
		"c5 tinyint);"}},
	{commands.SqlCmd{}, []string{"-q", "insert into one_pk values " +
		"(0, 0, 0, 0, 0, 0)," +
		"(1, 10, 10, 10, 10, 10)," +
		"(2, 20, 20, 20, 20, 20)," +
		"(3, 30, 30, 30, 30, 30);"}},
	{commands.SqlCmd{}, []string{"-q", "create table two_pk (" +
		"pk1 tinyint," +
		"pk2 tinyint," +
		"c1 tinyint," +
		"c2 tinyint," +
		"c3 tinyint," +
		"c4 tinyint," +
		"c5 tinyint," +
		"primary key (pk1, pk2));"}},
	{commands.SqlCmd{}, []string{"-q", "insert into two_pk values " +
		"(0, 0, 0, 0, 0, 0, 0)," +
		"(0, 1, 10, 10, 10, 10, 10)," +
		"(1, 0, 20, 20, 20, 20, 20)," +
		"(1, 1, 30, 30, 30, 30, 30);"}},
	{commands.SqlCmd{}, []string{"-q", "create table othertable (" +
		"s2 text primary key," +
		"i2 bigint);"}},
	{commands.SqlCmd{}, []string{"-q", "insert into othertable values " +
		"('first', 3)," +
		"('second', 2)," +
		"('third', 1);"}},
	{commands.SqlCmd{}, []string{"-q", "create table tabletest (" +
		"i int primary key," +
		"s text);"}},
	{commands.SqlCmd{}, []string{"-q", "insert into tabletest values " +
		"(1, 'first row')," +
		"(2, 'second row')," +
		"(3, 'third row');"}},
	{commands.SqlCmd{}, []string{"-q", "create table emptytable (" +
		"i int primary key," +
		"s text);"}},
	{commands.SqlCmd{}, []string{"-q", "create table other_table (" +
		"`text` text primary key," +
		"number int);"}},
	{commands.SqlCmd{}, []string{"-q", "insert into other_table values " +
		"('a', 4)," +
		"('b', 2)," +
		"('c', 0);"}},
	{commands.SqlCmd{}, []string{"-q", "create table bigtable (" +
		"t text primary key," +
		"n bigint);"}},
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
	{commands.SqlCmd{}, []string{"-q", "create table floattable (" +
		"i bigint primary key," +
		"f32 float," +
		"f64 double);"}},
	{commands.SqlCmd{}, []string{"-q", "insert into floattable values " +
		"(1, 1.0, 1.0)," +
		"(2, 1.5, 1.5)," +
		"(3, 2.0, 2.0)," +
		"(4, 2.5, 2.5)," +
		"(-1, -1.0, -1.0)," +
		"(-2, -1.5, -1.5);"}},
	{commands.SqlCmd{}, []string{"-q", "create table niltable (" +
		"i bigint primary key," +
		"i2 bigint," +
		"b bool," +
		"f float);"}},
	{commands.SqlCmd{}, []string{"-q", "insert into niltable values " +
		"(1, NULL, NULL, NULL)," +
		"(2, 2, 1, NULL)," +
		"(3, NULL, 0, NULL)," +
		"(4, 4, NULL, 4)," +
		"(5, NULL, 1, 5)," +
		"(6, 6, 0, 6);"}},
	{commands.SqlCmd{}, []string{"-q", "create table newlinetable (" +
		"i bigint primary key," +
		"s text);"}},
	{commands.SqlCmd{}, []string{"-q", "insert into newlinetable values " +
		"(1, '\nthere is some text in here')," +
		"(2, 'there is some\ntext in here')," +
		"(3, 'there is some text\nin here')," +
		"(4, 'there is some text in here\n')," +
		"(5, 'there is some text in here');"}},
	{commands.SqlCmd{}, []string{"-q", "create table stringandtable (" +
		"k bigint primary key," +
		"i bigint," +
		"v text);"}},
	{commands.SqlCmd{}, []string{"-q", "insert into stringandtable values " +
		"(0, 0, '0')," +
		"(1, 1, '1')," +
		"(2, 2, '')," +
		"(3, 3, 'true')," +
		"(4, 4, 'false')," +
		"(5, 5, NULL)," +
		"(6, NULL, '2');"}},
	{commands.AddCmd{}, []string{"."}},
	{commands.CommitCmd{}, []string{"-m", "setup enginetest test data"}},
}

func setupEngineTests(t *testing.T) *env.DoltEnv {
	// engineTestQueries are read-only, sharing a dEnv speeds up tests
	dEnv := dtestutils.CreateTestEnv()
	for _, c := range engineTestSetup {
		exitCode := c.cmd.Exec(context.Background(), c.cmd.Name(), c.args, dEnv)
		assert.Equal(t, 0, exitCode)
	}
	return dEnv
}

type engineTestQuery struct {
	query string
}

var engineTestSelectQueries = []engineTestQuery{
	{query: "SELECT * FROM mytable INNER JOIN othertable ON i = i2 ORDER BY i"},
	{query: "SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 ORDER BY i"},
	{query: "SELECT s2, i2, i FROM mytable INNER JOIN othertable ON i = i2 ORDER BY i"},
	{query: "SELECT * FROM mytable;"},
	{query: "SELECT * FROM mytable ORDER BY i DESC;"},
	{query: "SELECT i FROM mytable;"},
	{query: "SELECT i FROM mytable AS mt;"},
	{query: "SELECT s,i FROM mytable;"},
	{query: "SELECT mytable.s,i FROM mytable;"},
	{query: "SELECT t.s,i FROM mytable AS t;"},
	{query: "SELECT s,i FROM (select i,s FROM mytable) mt;"},
	{query: "SELECT s,i FROM MyTable ORDER BY 2"},
	{query: "SELECT S,I FROM MyTable ORDER BY 2"},
	{query: "SELECT mt.s,mt.i FROM MyTable MT ORDER BY 2;"},
	{query: "SELECT mT.S,Mt.I FROM MyTable MT ORDER BY 2;"},
	{query: "SELECT mt.* FROM MyTable MT ORDER BY mT.I;"},
	{query: "SELECT MyTABLE.s,myTable.i FROM MyTable ORDER BY 2;"},
	{query: "SELECT myTable.* FROM MYTABLE ORDER BY myTable.i;"},
	{query: "SELECT MyTABLE.S,myTable.I FROM MyTable ORDER BY mytable.i;"},
	{query: "SELECT i + 1 FROM mytable;"},
	{query: "SELECT i div 2 FROM mytable order by 1;"},
	{query: "SELECT i DIV 2 FROM mytable order by 1;"},
	{query: "SELECT -i FROM mytable;"},
	{query: "SELECT +i FROM mytable;"},
	{query: "SELECT + - i FROM mytable;"},
	{query: "SELECT i FROM mytable WHERE -i = -2;"},
	{query: "SELECT i FROM mytable WHERE i = 2;"},
	{query: "SELECT i FROM mytable WHERE 2 = i;"},
	{query: "SELECT i FROM mytable WHERE i > 2;"},
	{query: "SELECT i FROM mytable WHERE 2 < i;"},
	{query: "SELECT i FROM mytable WHERE i < 2;"},
	{query: "SELECT i FROM mytable WHERE 2 > i;"},
	{query: "SELECT i FROM mytable WHERE i <> 2;"},
	{query: "SELECT i FROM mytable WHERE i IN (1, 3)"},
	{query: "SELECT i FROM mytable WHERE i = 1 OR i = 3"},
	{query: "SELECT i FROM mytable WHERE i >= 2 ORDER BY 1"},
	{query: "SELECT i FROM mytable WHERE 2 <= i ORDER BY 1"},
	{query: "SELECT i FROM mytable WHERE i <= 2 ORDER BY 1"},
	{query: "SELECT i FROM mytable WHERE 2 >= i ORDER BY 1"},
	{query: "SELECT i FROM mytable WHERE i > 2"},
	{query: "SELECT i FROM mytable WHERE i < 2"},
	{query: "SELECT i FROM mytable WHERE i >= 2 OR i = 1 ORDER BY 1"},
	{query: "SELECT f32 FROM floattable WHERE f64 = 2.0;"},
	{query: "SELECT f32 FROM floattable WHERE f64 < 2.0;"},
	{query: "SELECT f32 FROM floattable WHERE f64 > 2.0;"},
	{query: "SELECT f32 FROM floattable WHERE f64 <> 2.0;"},
	{query: "SELECT f64 FROM floattable WHERE f32 = 2.0;"},
	{query: "SELECT f64 FROM floattable WHERE f32 = -1.5;"},
	{query: "SELECT f64 FROM floattable WHERE -f32 = -2.0;"},
	{query: "SELECT f64 FROM floattable WHERE f32 < 2.0;"},
	{query: "SELECT f64 FROM floattable WHERE f32 > 2.0;"},
	{query: "SELECT f64 FROM floattable WHERE f32 <> 2.0;"},
	{query: "SELECT f32 FROM floattable ORDER BY f64;"},
	{query: "SELECT i FROM mytable ORDER BY i DESC;"},
	{query: "SELECT i FROM mytable WHERE 'hello';"},
	{query: "SELECT i FROM mytable WHERE NOT 'hello';"},
	{query: "SELECT i FROM mytable WHERE s = 'first row' ORDER BY i DESC;"},
	{query: "SELECT i FROM mytable WHERE s = 'first row' ORDER BY i DESC LIMIT 1;"},
	{query: "SELECT i FROM mytable ORDER BY i LIMIT 1 OFFSET 1;"},
	{query: "SELECT i FROM mytable ORDER BY i LIMIT 1,1;"},
	{query: "SELECT i FROM mytable ORDER BY i LIMIT 3,1;"},
	{query: "SELECT i FROM mytable ORDER BY i LIMIT 2,100;"},
	{query: "SELECT i FROM niltable WHERE b IS NULL"},
	{query: "SELECT i FROM niltable WHERE b IS NOT NULL"},
	{query: "SELECT i FROM niltable WHERE b"},
	{query: "SELECT i FROM niltable WHERE NOT b"},
	{query: "SELECT i FROM niltable WHERE b IS TRUE"},
	{query: "SELECT i FROM niltable WHERE b IS NOT TRUE"},
	{query: "SELECT f FROM niltable WHERE b IS FALSE"},
	{query: "SELECT i FROM niltable WHERE f < 5"},
	{query: "SELECT i FROM niltable WHERE f > 5"},
	{query: "SELECT i FROM niltable WHERE b IS NOT FALSE"},
	{query: "SELECT substring(s, 2, 3) FROM mytable"},
	{query: `SELECT substring("foo", 2, 2)`},
	{query: `SELECT SUBSTRING_INDEX('a.b.c.d.e.f', '.', 2)`},
	{query: `SELECT SUBSTRING_INDEX('a.b.c.d.e.f', '.', -2)`},
	{query: `SELECT SUBSTRING_INDEX(SUBSTRING_INDEX('source{d}', '{d}', 1), 'r', -1)`},
	{query: "SELECT YEAR('2007-12-11') FROM mytable"},
	{query: "SELECT MONTH('2007-12-11') FROM mytable"},
	{query: "SELECT DAY('2007-12-11') FROM mytable"},
	{query: "SELECT HOUR('2007-12-11 20:21:22') FROM mytable"},
	{query: "SELECT MINUTE('2007-12-11 20:21:22') FROM mytable"},
	{query: "SELECT SECOND('2007-12-11 20:21:22') FROM mytable"},
	{query: "SELECT DAYOFYEAR('2007-12-11 20:21:22') FROM mytable"},
	{query: "SELECT SECOND('2007-12-11T20:21:22Z') FROM mytable"},
	{query: "SELECT DAYOFYEAR('2007-12-11') FROM mytable"},
	{query: "SELECT DAYOFYEAR('20071211') FROM mytable"},
	{query: "SELECT YEARWEEK('0000-01-01')"},
	{query: "SELECT YEARWEEK('9999-12-31')"},
	{query: "SELECT YEARWEEK('2008-02-20', 1)"},
	{query: "SELECT YEARWEEK('1987-01-01')"},
	{query: "SELECT YEARWEEK('1987-01-01', 20), YEARWEEK('1987-01-01', 1), YEARWEEK('1987-01-01', 2), YEARWEEK('1987-01-01', 3), YEARWEEK('1987-01-01', 4), YEARWEEK('1987-01-01', 5), YEARWEEK('1987-01-01', 6), YEARWEEK('1987-01-01', 7)"},
	{query: "SELECT i FROM mytable WHERE i BETWEEN 1 AND 2"},
	{query: "SELECT i FROM mytable WHERE i NOT BETWEEN 1 AND 2"},
	{query: "SELECT i,v from stringandtable WHERE i"},
	{query: "SELECT i,v from stringandtable WHERE i AND i"},
	{query: "SELECT i,v from stringandtable WHERE i OR i"},
	{query: "SELECT i,v from stringandtable WHERE NOT i"},
	{query: "SELECT i,v from stringandtable WHERE NOT i AND NOT i"},
	{query: "SELECT i,v from stringandtable WHERE NOT i OR NOT i"},
	{query: "SELECT i,v from stringandtable WHERE i OR NOT i"},
	{query: "SELECT i,v from stringandtable WHERE v"},
	{query: "SELECT i,v from stringandtable WHERE v AND v"},
	{query: "SELECT i,v from stringandtable WHERE v OR v"},
	{query: "SELECT i,v from stringandtable WHERE NOT v"},
	{query: "SELECT i,v from stringandtable WHERE NOT v AND NOT v"},
	{query: "SELECT i,v from stringandtable WHERE NOT v OR NOT v"},
	{query: "SELECT i,v from stringandtable WHERE v OR NOT v"},
	{query: "SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 ORDER BY i"},
	{query: "SELECT s2, i2, i FROM mytable INNER JOIN othertable ON i = i2 ORDER BY i"},
	{query: "SELECT i, i2, s2 FROM othertable JOIN mytable  ON i = i2 ORDER BY i"},
	{query: "SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2 ORDER BY i"},
	{query: "SELECT substring(s2, 1), substring(s2, 2), substring(s2, 3) FROM othertable ORDER BY i2"},
	{query: `SELECT substring("first", 1), substring("second", 2), substring("third", 3)`},
	{query: "SELECT substring(s2, -1), substring(s2, -2), substring(s2, -3) FROM othertable ORDER BY i2"},
	{query: `SELECT substring("first", -1), substring("second", -2), substring("third", -3)`},
	{query: "SELECT s FROM mytable INNER JOIN othertable ON substring(s2, 1, 2) != '' AND i = i2 ORDER BY 1"},
	{query: `SELECT i FROM mytable NATURAL JOIN tabletest`},
	{query: `SELECT i FROM mytable AS t NATURAL JOIN tabletest AS test`},
	// TODO: (from enginetest) this should work: either table alias should be usable in the select clause
	// {query: `SELECT t.i, test.s FROM mytable AS t NATURAL JOIN tabletest AS test`},
	{query: "SELECT CAST(-3 AS UNSIGNED) FROM mytable"},
	{query: "SELECT CONVERT(-3, UNSIGNED) FROM mytable"},
	{query: "SELECT '3' > 2 FROM tabletest"},
	{query: "SELECT s > 2 FROM tabletest"},
	{query: "SELECT * FROM tabletest WHERE s > 0"},
	{query: "SELECT * FROM tabletest WHERE s = 0"},
	{query: "SELECT * FROM tabletest WHERE s = 'first row'"},
	{query: "SELECT s FROM mytable WHERE i IN (1, 2, 5)"},
	{query: "SELECT s FROM mytable WHERE i NOT IN (1, 2, 5)"},
	{query: "SELECT 1 + 2"},
	{query: `SELECT i AS foo FROM mytable WHERE foo NOT IN (1, 2, 5)`},
	{query: `SELECT * FROM tabletest, mytable mt INNER JOIN othertable ot ON mt.i = ot.i2`},
	{query: `SELECT split(s," ") FROM mytable`},
	{query: `SELECT split(s,"s") FROM mytable`},
	// todo: (from enginetest) data alignment issue?
	//{query: `SELECT * FROM mytable mt INNER JOIN othertable ot ON mt.i = ot.i2 AND mt.i > 2`},
	{query: `SELECT i AS foo FROM mytable ORDER BY i DESC`},
	{query: `SELECT CONCAT("a", "b", "c")`},
	{query: `SELECT COALESCE(NULL, NULL, NULL, 'example', NULL, 1234567890)`},
	{query: `SELECT COALESCE(NULL, NULL, NULL, COALESCE(NULL, 1234567890))`},
	{query: "SELECT concat(s, i) FROM mytable"},
	{query: "SELECT version()"},
	{query: `SELECT RAND(100)`},
	{query: `SELECT RAND(100) = RAND(100)`},
	{query: `SELECT RAND() = RAND()`},
	{query: `SELECT s FROM mytable WHERE s LIKE '%d row'`},
	{query: `SELECT s FROM mytable WHERE s NOT LIKE '%d row'`},
	// todo: multi-db queries
	//{query: `SELECT * FROM foo.other_table`},  // error: "database not found: foo"
	{query: "SELECT i FROM mytable WHERE NULL > 10;"},
	{query: "SELECT i FROM mytable WHERE NULL IN (10);"},
	{query: "SELECT i FROM mytable WHERE NULL IN (NULL, NULL);"},
	{query: "SELECT i FROM mytable WHERE NOT NULL NOT IN (NULL);"},
	{query: "SELECT i FROM mytable WHERE NOT (NULL) <> 10;"},
	{query: "SELECT i FROM mytable WHERE NOT NULL <> NULL;"},
	{query: "SELECT substring(s, 1, 1) FROM mytable ORDER BY substring(s, 1, 1)"},
	{query: "SELECT left(s, 1) as l FROM mytable ORDER BY l"},
	{query: "SELECT left(s, 2) as l FROM mytable ORDER BY l"},
	{query: "SELECT left(s, 0) as l FROM mytable ORDER BY l"},
	{query: "SELECT left(s, NULL) as l FROM mytable ORDER BY l"},
	{query: "SELECT left(s, 100) as l FROM mytable ORDER BY l"},
	{query: "SELECT instr(s, 'row') as l FROM mytable ORDER BY i"},
	{query: "SELECT instr(s, 'first') as l FROM mytable ORDER BY i"},
	{query: "SELECT instr(s, 'o') as l FROM mytable ORDER BY i"},
	{query: "SELECT instr(s, NULL) as l FROM mytable ORDER BY l"},
	{query: `SELECT i AS i FROM mytable ORDER BY i`},
	{query: `SELECT i AS foo FROM mytable ORDER BY mytable.i`},
	{query: "SELECT i, i2, s2 FROM mytable LEFT JOIN othertable ON i = i2 - 1"},
	{query: "SELECT i, i2, s2 FROM mytable RIGHT JOIN othertable ON i = i2 - 1"},
	{query: "SELECT i, i2, s2 FROM mytable LEFT OUTER JOIN othertable ON i = i2 - 1"},
	{query: "SELECT i, i2, s2 FROM mytable RIGHT OUTER JOIN othertable ON i = i2 - 1"},
	{query: "SELECT i FROM mytable WHERE NOT s ORDER BY 1 DESC"},
	{query: "SELECT i FROM mytable WHERE NOT(NOT i) ORDER BY 1 DESC"},
	{query: `SELECT * FROM mytable WHERE NULL AND i = 3`},
	{query: `SELECT FIRST(i) FROM (SELECT i FROM mytable ORDER BY i) t`},
	{query: `SELECT LAST(i) FROM (SELECT i FROM mytable ORDER BY i) t`},
	{query: "SELECT * FROM newlinetable WHERE s LIKE '%text%'"},
	{query: `SELECT i FROM mytable WHERE i = (SELECT 1)`},
	{query: `SELECT i FROM mytable WHERE i IN (SELECT i FROM mytable)`},
	{query: `SELECT i FROM mytable WHERE i NOT IN (SELECT i FROM mytable ORDER BY i ASC LIMIT 2)`},
	{query: `SELECT (SELECT i FROM mytable ORDER BY i ASC LIMIT 1) AS x`},
	{query: `SELECT DISTINCT n FROM bigtable ORDER BY t`},
	{query: "SELECT pk,pk1,pk2 FROM one_pk, two_pk ORDER BY 1,2,3"},
	{query: "SELECT t1.c1,t2.c2 FROM one_pk t1, two_pk t2 WHERE pk1=1 AND pk2=1 ORDER BY 1,2"},
	{query: "SELECT t1.c1,t2.c2 FROM one_pk t1, two_pk t2 WHERE t2.pk1=1 AND t2.pk2=1 ORDER BY 1,2"},
	{query: "SELECT t1.c1,t2.c2 FROM one_pk t1, two_pk t2 WHERE pk1=1 OR pk2=1 ORDER BY 1,2"},
	{query: "SELECT pk,pk2 FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 ORDER BY 1,2"},
	{query: "SELECT pk,pk1,pk2 FROM one_pk,two_pk WHERE pk=0 AND pk1=0 OR pk2=1 ORDER BY 1,2,3"},
	{query: "SELECT pk,pk1,pk2 FROM one_pk,two_pk WHERE one_pk.c1=two_pk.c1 ORDER BY 1,2,3"},
	{query: "SELECT one_pk.c5,pk1,pk2 FROM one_pk,two_pk WHERE pk=pk1 ORDER BY 1,2,3"},
	{query: "SELECT opk.c5,pk1,pk2 FROM one_pk opk, two_pk tpk WHERE pk=pk1 ORDER BY 1,2,3"},
	{query: "SELECT one_pk.c5,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1 ORDER BY 1,2,3"},
	{query: "SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON pk=pk1 ORDER BY 1,2,3"},
	{query: "SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 ORDER BY 1,2,3"},
	{query: "SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 WHERE pk=1 ORDER BY 1,2,3"},
	{query: "SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3"},
	{query: "SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 AND opk.pk=tpk.pk2 ORDER BY 1,2,3"},
	{query: "SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON pk=tpk.pk1 AND pk=tpk.pk2 ORDER BY 1,2,3"},
	{query: "SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3"},
	{query: "SELECT pk,pk1,pk2 FROM one_pk RIGHT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3"},
	{query: "SELECT i,pk1,pk2 FROM mytable JOIN two_pk ON i-1=pk1 AND i-2=pk2 ORDER BY 1,2,3"},
	{query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1=b.pk2 AND a.pk2=b.pk1 ORDER BY 1,2,3"},
	{query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1=b.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3"},
	{query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a, two_pk b WHERE a.pk1=b.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3"},
	{query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON b.pk1=a.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3"},
	{query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1+1=b.pk1 AND a.pk2+1=b.pk2 ORDER BY 1,2,3"},
	{query: "SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON pk=pk1 ORDER BY 1,2,3"},
	{query: "SELECT pk,i2,f FROM one_pk LEFT JOIN niltable ON pk=i2 ORDER BY 1"},
	{query: "SELECT pk,i2,f FROM one_pk RIGHT JOIN niltable ON pk=i2 ORDER BY 2,3"},
	{query: "SELECT pk,i2,f FROM one_pk LEFT JOIN niltable ON pk=i2 AND f IS NOT NULL ORDER BY 1"}, // AND clause causes right table join mis},
	{query: "SELECT pk,i2,f FROM one_pk RIGHT JOIN niltable ON pk=i2 and pk > 0 ORDER BY 2,3"},     // > 0 clause in join condition is ignore},
	{query: "SELECT pk,i2,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE i2 IS NOT NULL ORDER BY 1"},
	{query: "SELECT pk,i2,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE f IS NOT NULL ORDER BY 2,3"},
	{query: "SELECT pk,i2,f FROM one_pk LEFT JOIN niltable ON pk=i2 WHERE pk > 1 ORDER BY 1"},
	{query: "SELECT pk,i2,f FROM one_pk RIGHT JOIN niltable ON pk=i2 WHERE pk > 0 ORDER BY 2,3"},
	{query: "SELECT GREATEST(CAST(i AS CHAR), CAST(b AS CHAR)) FROM niltable order by i"},
	{query: "SELECT pk,pk1,pk2,one_pk.c1 AS foo, two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 ORDER BY 1,2,3"},
	{query: "SELECT pk,pk1,pk2,one_pk.c1 AS foo,two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 WHERE one_pk.c1=10"},
	{query: "SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk1-pk>0 AND pk2<1"},
	{query: "SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ORDER BY 1,2,3"},
	{query: "SELECT a.pk,b.pk FROM one_pk a JOIN one_pk b ON a.pk = b.pk order by a.pk"},
	{query: "SELECT a.pk,b.pk FROM one_pk a, one_pk b WHERE a.pk = b.pk order by a.pk"},
	{query: "SELECT one_pk.pk,b.pk FROM one_pk JOIN one_pk b ON one_pk.pk = b.pk order by one_pk.pk"},
	{query: "SELECT one_pk.pk,b.pk FROM one_pk, one_pk b WHERE one_pk.pk = b.pk order by one_pk.pk"},
	{query: "SELECT (CASE WHEN i THEN i ELSE 0 END) as cases_i from mytable"},
}

var engineTestUnionQueries = []engineTestQuery{
	{query: "SELECT i FROM mytable UNION SELECT i+10 FROM mytable;"},
	{query: "SELECT i FROM mytable UNION DISTINCT SELECT i+10 FROM mytable;"},
	{query: "SELECT i FROM mytable UNION SELECT i FROM mytable;"},
	{query: "SELECT i FROM mytable UNION DISTINCT SELECT i FROM mytable;"},
	{query: "SELECT i FROM mytable UNION SELECT s FROM mytable;"},
}

var engineTestAggregateQueries = []engineTestQuery{
	{query: "SELECT * FROM mytable GROUP BY i,s;"},
	{query: "SELECT COUNT(*) FROM mytable;"},
	{query: "SELECT COUNT(*) FROM mytable LIMIT 1;"},
	{query: "SELECT COUNT(*) AS c FROM mytable;"},
	{query: `
		SELECT COUNT(*) AS cnt, fi FROM (
			SELECT tbl.s AS fi
			FROM mytable tbl
		) t
		GROUP BY fi`,
	},
	{query: `
		SELECT fi, COUNT(*) FROM (
			SELECT tbl.s AS fi
			FROM mytable tbl
		) t
		GROUP BY fi
		ORDER BY COUNT(*) ASC`,
	},
	{query: `
		SELECT COUNT(*), fi  FROM (
			SELECT tbl.s AS fi
			FROM mytable tbl
		) t
		GROUP BY fi
		ORDER BY COUNT(*) ASC`,
	},
	{query: `
		SELECT COUNT(*) AS cnt, fi FROM (
			SELECT tbl.s AS fi
			FROM mytable tbl
		) t
		GROUP BY 2`,
	},
	{query: `SELECT COUNT(*) AS cnt, s AS fi FROM mytable GROUP BY fi`},
	{query: `SELECT COUNT(*) AS cnt, s AS fi FROM mytable GROUP BY 2`},
	{query: `SELECT COUNT(*) c, i AS foo FROM mytable GROUP BY i ORDER BY i DESC`},
	{query: `SELECT COUNT(*) c, i AS foo FROM mytable GROUP BY 2 ORDER BY 2 DESC`},
	{query: `SELECT COUNT(*) c, i AS foo FROM mytable GROUP BY i ORDER BY foo DESC`},
	{query: `SELECT COUNT(*) c, i AS foo FROM mytable GROUP BY 2 ORDER BY foo DESC`},
	{query: `SELECT COUNT(*) c, i AS i FROM mytable GROUP BY 2`},
	{query: `SELECT i AS i FROM mytable GROUP BY 1`},
	{query: "SELECT SUM(i) + 1, i FROM mytable GROUP BY i ORDER BY i"},
	{query: "SELECT SUM(i), i FROM mytable GROUP BY i ORDER BY 1+SUM(i) ASC"},
	{query: "SELECT i, SUM(i) FROM mytable GROUP BY i ORDER BY SUM(i) DESC"},
	{query: `SELECT AVG(23.222000)`},
	{query: "SELECT substring(s, 1, 1), count(*) FROM mytable GROUP BY substring(s, 1, 1)"},
	{query: `
		SELECT
			i,
			foo
		FROM (
			SELECT
				i,
				COUNT(s) AS foo
			FROM mytable
			GROUP BY i
		) AS q
		ORDER BY foo DESC
		`,
	},
	{query: "SELECT n, COUNT(n) FROM bigtable GROUP BY n HAVING COUNT(n) > 2"},
	{query: "SELECT n, MAX(n) FROM bigtable GROUP BY n HAVING COUNT(n) > 2"},
	{query: "SELECT substring(mytable.s, 1, 5) AS s FROM mytable INNER JOIN othertable ON (substring(mytable.s, 1, 5) = SUBSTRING(othertable.s2, 1, 5)) GROUP BY 1 HAVING s = \"secon\""},
	{query: "SELECT s,  i FROM mytable GROUP BY i ORDER BY SUBSTRING(s, 1, 1) DESC"},
	{query: "SELECT s, i FROM mytable GROUP BY i HAVING count(*) > 0 ORDER BY SUBSTRING(s, 1, 1) DESC"},
	{query: `SELECT t.date_col FROM (SELECT CONVERT('2019-06-06 00:00:00', DATETIME) as date_col) t GROUP BY t.date_col`},
	{query: "SELECT i, COUNT(i) AS `COUNT(i)` FROM (SELECT i FROM mytable) t GROUP BY i ORDER BY i, `COUNT(i)` DESC"},
	{query: `SELECT 1 FROM mytable GROUP BY i HAVING i > 1`},
	{query: `SELECT avg(i) FROM mytable GROUP BY i HAVING avg(i) > 1`},
	{query: `
		SELECT s AS s, COUNT(*) AS count,  AVG(i) AS ` + "`AVG(i)`" + `
		FROM  (
			SELECT * FROM mytable
		) AS expr_qry
		GROUP BY s
		HAVING ((AVG(i) > 0))
		ORDER BY count DESC
		LIMIT 10000`,
	},
	{query: `
		SELECT
			table_schema,
			table_name,
			CASE
				WHEN table_type = 'BASE TABLE' THEN
					CASE
						WHEN table_schema = 'mysql'
							OR table_schema = 'performance_schema' THEN 'SYSTEM TABLE'
						ELSE 'TABLE'
					END
				WHEN table_type = 'TEMPORARY' THEN 'LOCAL_TEMPORARY'
				ELSE table_type
			END AS TABLE_TYPE
		FROM information_schema.tables
		WHERE table_schema = 'mydb'
			AND table_name = 'mytable'
		HAVING table_type IN ('TABLE', 'VIEW')
		ORDER BY table_type, table_schema, table_name`,
	},
	{query: "SELECT substring(mytable.s, 1, 5) AS s FROM mytable INNER JOIN othertable ON (substring(mytable.s, 1, 5) = SUBSTRING(othertable.s2, 1, 5)) GROUP BY 1"},
	{query: `SELECT SUBSTRING_INDEX(mytable.s, "d", 1) AS s FROM mytable INNER JOIN othertable ON (SUBSTRING_INDEX(mytable.s, "d", 1) = SUBSTRING_INDEX(othertable.s2, "d", 1)) GROUP BY 1 HAVING s = 'secon'`},
	{query: `SELECT SUBSTRING(s, -3, 3) AS s FROM mytable WHERE s LIKE '%d row' GROUP BY 1`},
	{query: `SELECT SUM(i) FROM mytable`},
}

// runs a subset of SELECT queries from enginetest/queries.go as a sanity check
func engineTestQueryDiffer(t *testing.T, test engineTestQuery, dEnv *env.DoltEnv) {
	ctx := context.Background()
	fromRoot, err := dEnv.HeadRoot(ctx)
	require.NoError(t, err)
	toRoot, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)

	qd, err := querydiff.MakeQueryDiffer(ctx, dEnv, fromRoot, toRoot, test.query)
	if err != nil {
		t.Fatalf("unexpected error creating QueryDiffer: %s", err.Error())
	}

	qd.Start()
	from, to, err := qd.NextDiff()
	assert.Nil(t, from)
	assert.Nil(t, to)
	assert.Equal(t, io.EOF, err)
}
