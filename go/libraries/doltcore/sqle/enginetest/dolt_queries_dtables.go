// Copyright 2026 Dolthub, Inc.
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

package enginetest

import (
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
)

var DoltStatusTableScripts = []queries.ScriptTest{
	{
		Name: "dolt_status detached head is read-only clean",
		SetUpScript: []string{
			"CALL DOLT_COMMIT('--allow-empty', '-m', 'empty commit');",
			"CALL DOLT_TAG('tag1');",
			"SET @head_hash = (SELECT HASHOF('main') LIMIT 1);",
			"SET @status_by_hash = CONCAT('SELECT * FROM `mydb/', @head_hash, '`.dolt_status;');",
			"PREPARE status_by_hash FROM @status_by_hash;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM `mydb/tag1`.dolt_status;",
				Expected: []sql.Row{},
			},
			{
				Query:    "EXECUTE status_by_hash;",
				Expected: []sql.Row{},
			},
			{
				Query:       "SELECT * FROM `information_schema`.dolt_status;",
				ExpectedErr: sql.ErrTableNotFound,
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/8283
		Name: "dolt_status tests",
		SetUpScript: []string{
			"CALL DOLT_COMMIT('--allow-empty', '-m', 'empty commit');",
			"SET @commit1 = HASHOF('HEAD');",
			"CALL DOLT_TAG('tag1');",
			"CALL DOLT_CHECKOUT('-b', 'branch1');",
			"CREATE TABLE abc (pk int);",
			"CALL DOLT_ADD('abc');",
			"CALL DOLT_CHECKOUT('main');",
			"CREATE TABLE t (pk int primary key, v varchar(100));",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_status;",
				Expected: []sql.Row{{"t", byte(0), "new table"}},
			},
			{
				Query:    "SELECT * FROM `mydb/main`.dolt_status;",
				Expected: []sql.Row{{"t", byte(0), "new table"}},
			},
			{
				Query:    "SELECT * FROM dolt_status AS OF 'tag1';",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * FROM dolt_status AS OF @commit1;",
				Expected: []sql.Row{},
			},
			{
				// HEAD is a special revision spec
				Query:    "SELECT * FROM dolt_status AS OF 'head';",
				Expected: []sql.Row{{"t", byte(0), "new table"}},
			},
			{
				Query:    "SELECT * FROM dolt_status AS OF 'HEAD~1';",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * FROM dolt_status AS OF 'branch1';",
				Expected: []sql.Row{{"abc", byte(1), "new table"}},
			},
			{
				Query:    "SELECT * FROM `mydb/branch1`.dolt_status;",
				Expected: []sql.Row{{"abc", byte(1), "new table"}},
			},
		},
	},
}

// HistorySystemTableScriptTests holds scripts for the history system tables,
// such as dolt_history_<table> and dolt_commit_ancestors. Each script runs on
// the engine and the server engine.
var HistorySystemTableScriptTests = []queries.ScriptTest{
	{
		Name: "empty table",
		SetUpScript: []string{
			"create table t (n int, c varchar(20));",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'creating table t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select count(*) from DOLT_HISTORY_t;",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "keyless table",
		SetUpScript: []string{
			"create table foo1 (n int, de varchar(20));",
			"insert into foo1 values (1, 'Ein'), (2, 'Zwei'), (3, 'Drei');",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'inserting into foo1', '--date', '2022-08-06T12:00:00');",
			"set @Commit1 = (select hashof('HEAD'));",

			"update foo1 set de='Eins' where n=1;",
			"call dolt_commit('-am', 'updating data in foo1', '--date', '2022-08-06T12:00:01');",
			"set @Commit2 = (select hashof('HEAD'));",

			"insert into foo1 values (4, 'Vier');",
			"call dolt_commit('-am', 'inserting data in foo1', '--date', '2022-08-06T12:00:02');",
			"set @Commit3 = (select hashof('HEAD'));",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select count(*) from DOLT_HISTORY_foO1;",
				Expected: []sql.Row{{10}},
			},
			{
				Query:    "select n, de from dolt_history_foo1 where commit_hash=@Commit1;",
				Expected: []sql.Row{{1, "Ein"}, {2, "Zwei"}, {3, "Drei"}},
			},
			{
				Query:    "select n, de from dolt_history_Foo1 where commit_hash=@Commit2;",
				Expected: []sql.Row{{1, "Eins"}, {2, "Zwei"}, {3, "Drei"}},
			},
			{
				Query:    "select n, de from dolt_history_foo1 where commit_hash=@Commit3;",
				Expected: []sql.Row{{1, "Eins"}, {2, "Zwei"}, {3, "Drei"}, {4, "Vier"}},
			},
		},
	},
	{
		Name: "primary key table: basic cases",
		SetUpScript: []string{
			"create table t1 (n int primary key, de varchar(20));",
			"call dolt_add('.')",
			"insert into t1 values (1, 'Eins'), (2, 'Zwei'), (3, 'Drei');",
			"call dolt_commit('-am', 'inserting into t1', '--date', '2022-08-06T12:00:01');",
			"SET @Commit1 = (select hashof('HEAD'));",

			"alter table t1 add column fr varchar(20);",
			"insert into t1 values (4, 'Vier', 'Quatre');",
			"call dolt_commit('-am', 'adding column and inserting data in t1', '--date', '2022-08-06T12:00:02');",
			"SET @Commit2 = (select hashof('HEAD'));",

			"update t1 set fr='Un' where n=1;",
			"update t1 set fr='Deux' where n=2;",
			"call dolt_commit('-am', 'updating data in t1', '--date', '2022-08-06T12:00:03');",
			"SET @Commit3 = (select hashof('HEAD'));",

			"update t1 set de=concat(de, ', meine herren') where n>1;",
			"call dolt_commit('-am', 'be polite when you address a gentleman', '--date', '2022-08-06T12:00:04');",
			"SET @Commit4 = (select hashof('HEAD'));",

			"delete from t1 where n=2;",
			"call dolt_commit('-am', 'we don''t need the number 2', '--date', '2022-08-06T12:00:05');",
			"SET @Commit5 = (select hashof('HEAD'));",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select count(*) from Dolt_History_t1;",
				Expected: []sql.Row{{18}},
			},
			{
				Query:    "select n, de, fr from dolt_history_T1 where commit_hash = @Commit1;",
				Expected: []sql.Row{{1, "Eins", nil}, {2, "Zwei", nil}, {3, "Drei", nil}},
			},
			{
				Query:    "select de, fr from dolt_history_T1 where commit_hash = @Commit1;",
				Expected: []sql.Row{{"Eins", nil}, {"Zwei", nil}, {"Drei", nil}},
			},
			{
				Query:    "select n, de, fr from dolt_history_T1 where commit_hash = @Commit2;",
				Expected: []sql.Row{{1, "Eins", nil}, {2, "Zwei", nil}, {3, "Drei", nil}, {4, "Vier", "Quatre"}},
			},
			{
				Query:    "select n, de, fr from dolt_history_T1 where commit_hash = @Commit3;",
				Expected: []sql.Row{{1, "Eins", "Un"}, {2, "Zwei", "Deux"}, {3, "Drei", nil}, {4, "Vier", "Quatre"}},
			},
			{
				Query: "select n, de, fr from dolt_history_T1 where commit_hash = @Commit4;",
				Expected: []sql.Row{
					{1, "Eins", "Un"},
					{2, "Zwei, meine herren", "Deux"},
					{3, "Drei, meine herren", nil},
					{4, "Vier, meine herren", "Quatre"},
				},
			},
			{
				Query: "select n, de, fr from dolt_history_T1 where commit_hash = @Commit5;",
				Expected: []sql.Row{
					{1, "Eins", "Un"},
					{3, "Drei, meine herren", nil},
					{4, "Vier, meine herren", "Quatre"},
				},
			},
			{
				Query: "select de, fr, commit_hash=@commit1, commit_hash=@commit2, commit_hash=@commit3, commit_hash=@commit4" +
					" from dolt_history_T1 where n=2 order by commit_date",
				Expected: []sql.Row{
					{"Zwei", nil, true, false, false, false},
					{"Zwei", nil, false, true, false, false},
					{"Zwei", "Deux", false, false, true, false},
					{"Zwei, meine herren", "Deux", false, false, false, true},
				},
			},
		},
	},
	{
		Name: "index by primary key",
		SetUpScript: []string{
			"create table t1 (pk int primary key, c int);",
			"call dolt_add('.')",
			"insert into t1 values (1,2), (3,4)",
			"call dolt_commit('-am', 'initial table');",
			"set @Commit1 = (select hashof('HEAD'));",

			"insert into t1 values (5,6), (7,8)",
			"call dolt_commit('-am', 'two more rows');",
			"set @Commit2 = (select hashof('HEAD'));",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select pk, c, commit_hash = @Commit1, commit_hash = @Commit2 from dolt_history_t1",
				Expected: []sql.Row{
					{1, 2, false, true},
					{3, 4, false, true},
					{5, 6, false, true},
					{7, 8, false, true},
					{1, 2, true, false},
					{3, 4, true, false},
				},
			},
			{
				Query: "select pk, c from dolt_history_t1 order by pk",
				Expected: []sql.Row{
					{1, 2},
					{1, 2},
					{3, 4},
					{3, 4},
					{5, 6},
					{7, 8},
				},
			},
			{
				Query: "select pk, c from dolt_history_t1 order by pk, c",
				Expected: []sql.Row{
					{1, 2},
					{1, 2},
					{3, 4},
					{3, 4},
					{5, 6},
					{7, 8},
				},
			},
			{
				Query: "select pk, c from dolt_history_t1 where pk = 3",
				Expected: []sql.Row{
					{3, 4},
					{3, 4},
				},
			},
			{
				Query: "select pk, c from dolt_history_t1 where pk = 3 and commit_hash = @Commit2",
				Expected: []sql.Row{
					{3, 4},
				},
			},
			{
				Query: "explain plan select pk, c from dolt_history_t1 where pk = 3",
				Expected: []sql.Row{
					{"Filter"},
					{" ├─ (dolt_history_t1.pk = 3)"},
					{" └─ IndexedTableAccess(dolt_history_t1)"},
					{"     ├─ index: [dolt_history_t1.pk]"},
					{"     ├─ filters: [{[3, 3]}]"},
					{"     └─ columns: [pk c]"},
				},
			},
			{
				Query: "explain plan select pk, c from dolt_history_t1 where pk = 3 and committer = 'someguy'",
				Expected: []sql.Row{
					{"Project"},
					{" ├─ columns: [dolt_history_t1.pk, dolt_history_t1.c]"},
					{" └─ Filter"},
					{"     ├─ ((dolt_history_t1.pk = 3) AND (dolt_history_t1.committer = 'someguy'))"},
					{"     └─ IndexedTableAccess(dolt_history_t1)"},
					{"         ├─ index: [dolt_history_t1.pk]"},
					{"         ├─ filters: [{[3, 3]}]"},
					{"         └─ columns: [pk c committer]"},
				},
			},
		},
	},
	{
		Name: "adding an index",
		SetUpScript: []string{
			"create table t1 (pk int primary key, c int);",
			"call dolt_add('.')",
			"insert into t1 values (1,2), (3,4)",
			"call dolt_commit('-am', 'initial table');",
			"set @Commit1 = (select hashof('HEAD'));",

			"insert into t1 values (5,6), (7,8)",
			"call dolt_commit('-am', 'two more rows');",

			"insert into t1 values (9,10), (11,12)",
			"create index t1_c on t1(c)",
			"call dolt_commit('-am', 'two more rows and an index');",
			"set @Commit2 = (select hashof('HEAD'));",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select pk, c from dolt_history_t1 order by pk",
				Expected: []sql.Row{
					{1, 2},
					{1, 2},
					{1, 2},
					{3, 4},
					{3, 4},
					{3, 4},
					{5, 6},
					{5, 6},
					{7, 8},
					{7, 8},
					{9, 10},
					{11, 12},
				},
			},
			{
				Query: "select pk, c from dolt_history_t1 where c = 4 order by pk",
				Expected: []sql.Row{
					{3, 4},
					{3, 4},
					{3, 4},
				},
			},
			{
				Query: "select pk, c from dolt_history_t1 where c = 10 order by pk",
				Expected: []sql.Row{
					{9, 10},
				},
			},
			{
				Query: "explain plan select pk, c from dolt_history_t1 where c = 4",
				Expected: []sql.Row{
					{"Filter"},
					{" ├─ (dolt_history_t1.c = 4)"},
					{" └─ IndexedTableAccess(dolt_history_t1)"},
					{"     ├─ index: [dolt_history_t1.c]"},
					{"     ├─ filters: [{[4, 4]}]"},
					{"     └─ columns: [pk c]"},
				},
			},
			{
				Query: "explain plan select pk, c from dolt_history_t1 where c = 10 and committer = 'someguy'",
				Expected: []sql.Row{
					{"Project"},
					{" ├─ columns: [dolt_history_t1.pk, dolt_history_t1.c]"},
					{" └─ Filter"},
					{"     ├─ ((dolt_history_t1.c = 10) AND (dolt_history_t1.committer = 'someguy'))"},
					{"     └─ IndexedTableAccess(dolt_history_t1)"},
					{"         ├─ index: [dolt_history_t1.c]"},
					{"         ├─ filters: [{[10, 10]}]"},
					{"         └─ columns: [pk c committer]"},
				},
			},
		},
	},
	{
		Name: "primary key table: non-pk column drops and adds",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 varchar(20));",
			"call dolt_add('.')",
			"insert into t values (1, 2, '3'), (4, 5, '6');",
			"CALL DOLT_COMMIT('-am', 'creating table t');",
			"set @Commit1 = (select hashof('HEAD'));",

			"alter table t drop column c2;",
			"CALL DOLT_COMMIT('-am', 'dropping column c2');",
			"set @Commit2 = (select hashof('HEAD'));",

			"alter table t rename column c1 to c2;",
			"CALL DOLT_COMMIT('-am', 'renaming c1 to c2');",
			"set @Commit3 = (select hashof('HEAD'));",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select count(*) from dolt_history_t;",
				Expected: []sql.Row{{6}},
			},
			{
				// TODO: Instead of just spot checking the non-existence of c1, it would be useful to be able to
				//       assert the full schema of the result set. ScriptTestAssertion doesn't support that currently,
				//       but the code from QueryTest could be ported over to ScriptTestAssertion.
				Query:       "select c1 from dolt_history_t;",
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query:    "select pk, c2 from dolt_history_t where commit_hash=@Commit1 order by pk;",
				Expected: []sql.Row{{1, nil}, {4, nil}},
			},
			{
				Query:    "select pk, c2 from dolt_history_t where commit_hash=@Commit2 order by pk;",
				Expected: []sql.Row{{1, nil}, {4, nil}},
			},
			{
				Query:    "select pk, c2 from dolt_history_t where commit_hash=@Commit3 order by pk;",
				Expected: []sql.Row{{1, 2}, {4, 5}},
			},
		},
	},
	{
		Name: "primary key table: non-pk column type changes",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 varchar(20));",
			"CALL DOLT_COMMIT('-Am', 'creating table t');",
			"set @Commit1 = dolt_hashof('HEAD');",

			"insert into t values (1, 2, '3'), (4, 5, '6');",
			"CALL DOLT_COMMIT('-Am', 'inserting two rows');",
			"set @Commit2 = dolt_hashof('HEAD');",

			"CALL DOLT_COMMIT('--allow-empty', '-m', 'empty commit');",
			"set @Commit3 = dolt_hashof('HEAD');",

			"alter table t modify column c2 int;",
			"CALL DOLT_COMMIT('-am', 'changed type of c2');",
			"set @Commit4 = dolt_hashof('HEAD');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select count(*) from dolt_history_t;",
				Expected: []sql.Row{{6}},
			},
			// Can't represent the old schema in the current one, so it gets nil valued
			{
				Query:    "select pk, c2 from dolt_history_t where commit_hash=@Commit2 order by pk;",
				Expected: []sql.Row{{1, nil}, {4, nil}},
			},
			{
				Query:    "select pk, c2 from dolt_history_t where commit_hash=@Commit4 order by pk;",
				Expected: []sql.Row{{1, 3}, {4, 6}},
			},
			{
				// When filtering on a column from the original table, we use the primary index here, but if column
				// tags have changed in previous versions of the table, the index tags won't match up completely.
				// https://github.com/dolthub/dolt/issues/6891
				// NOTE: {4,5,nil} shows up as a row from the first commit, when c2 was a varchar type. The schema
				//       for dolt_history_t uses the current table schema, and we can't extract an int from the older
				//       version's tuple, so it shows up as a NULL and a SQL warning in the session. In the future,
				//       we could consider using a different tuple descriptor based on the version of the row and
				//       pull the data out and try to convert it to the new type.
				Query:                 "select pk, c1, c2 from dolt_history_t where pk=4;",
				Expected:              []sql.Row{{4, 5, 6}, {4, 5, nil}, {4, 5, nil}},
				ExpectedWarning:       1246,
				ExpectedWarningsCount: 1,
				ExpectedWarningMessageSubstring: "Unable to convert field c2 in historical rows because " +
					"its type (int) doesn't match current schema's type (varchar(20))",
			},
		},
	},
	{
		Name: "primary key table: rename table",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 varchar(20));",
			"call dolt_add('.')",
			"insert into t values (1, 2, '3'), (4, 5, '6');",

			"CALL DOLT_COMMIT('-am', 'creating table t');",
			"set @Commit1 = dolt_hashof('HEAD');",

			"alter table t rename to t2;",
			"call dolt_add('.')",
			"CALL DOLT_COMMIT('-am', 'renaming table to t2');",
			"set @commit2 = dolt_hashof('HEAD');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "select count(*) from dolt_history_t;",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:    "select count(*) from dolt_history_T2;",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "select pk, c1, c2 from dolt_history_t2 where commit_hash != @Commit1;",
				Expected: []sql.Row{{1, 2, "3"}, {4, 5, "6"}},
			},
		},
	},
	{
		Name: "primary key table: delete and recreate table",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 varchar(20));",
			"call dolt_add('.')",
			"insert into t values (1, 2, '3'), (4, 5, '6');",
			"CALL DOLT_COMMIT('-am', 'creating table t');",
			"set @Commit1 = dolt_hashof('HEAD');",

			"drop table t;",
			"CALL DOLT_COMMIT('-am', 'dropping table t');",
			"SET @Commit2 = dolt_hashof('HEAD');",

			"create table t (pk int primary key, c1 int);",
			"call dolt_add('.')",
			"CALL DOLT_COMMIT('-am', 'recreating table t');",
			"SET @Commit3 = dolt_hashof('HEAD');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// TODO: The history system table processes history in parallel and pulls the rows for the
				//       user table at all commits. This means we can't currently detect when a table was dropped
				//       and if a different table with the same name exists at earlier commits, those results will
				//       be included in the history table. It may make more sense to have history scoped only
				//       to the current instance of the table, which would require changing the history system table
				//       to use something like an iterator approach where it goes back sequentially until it detects
				//       the current table doesn't exist any more and then stop.
				Query:    "select count(*) from dolt_history_t;",
				Expected: []sql.Row{{2}},
			},
		},
	},
	{
		// TODO(#12191): drop SkipPrepared when prepared AS OF takes a bind variable.
		SkipPrepared: true,
		Name:         "dolt_history table with AS OF",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 varchar(20));",
			"call dolt_add('-A');",
			"call dolt_commit('-m', 'creating table t');",
			"insert into t values (1, 2, '3'), (4, 5, '6');",
			"call dolt_commit('-am', 'added values');",
			"insert into t values (11, 22, '3'), (44, 55, '6');",
			"call dolt_commit('-am', 'added values again');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select count(*) from dolt_history_t;",
				Expected: []sql.Row{{6}}, // 2 + 4
			},
			{
				Query:    "select count(*) from dolt_history_t AS OF 'head^';",
				Expected: []sql.Row{{2}}, // 2
			},
			{
				Query: "select message from dolt_log;",
				Expected: []sql.Row{
					{"added values again"},
					{"added values"},
					{"creating table t"},
					{"checkpoint enginetest database mydb"},
					{"Initialize data repository"},
				},
			},
		},
	},
	{
		SkipPrepared: true,
		Name:         "dolt_history table with AS OF",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 varchar(20));",
			"call dolt_add('-A');",
			"call dolt_commit('-m', 'creating table t');",
			"insert into t values (1, 2, '3'), (4, 5, '6');",
			"call dolt_commit('-am', 'added values');",
			"insert into t values (11, 22, '3'), (44, 55, '6');",
			"call dolt_commit('-am', 'added values again');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select message from dolt_log AS OF 'head^';",
				Expected: []sql.Row{
					{"added values"},
					{"creating table t"},
					{"checkpoint enginetest database mydb"},
					{"Initialize data repository"},
				},
			},
		},
	},
	{
		Name: "dolt_history table with enums",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 enum('foo','bar'));",
			"call dolt_add('-A');",
			"call dolt_commit('-m', 'creating table t');",
			"insert into t values (1, 'foo');",
			"call dolt_commit('-am', 'added values');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select c1 from dolt_history_t;",
				Expected: []sql.Row{
					{"foo"},
				},
			},
		},
	},
	{
		Name: "dolt_history table index lookup",
		SetUpScript: []string{
			"create table yx (y int, x int primary key);",
			"call dolt_add('.');",
			"call dolt_commit('-m', 'creating table');",
			"insert into yx values (0, 1);",
			"call dolt_commit('-am', 'add data');",
			"insert into yx values (2, 3);",
			"call dolt_commit('-am', 'add data');",
			"insert into yx values (4, 5);",
			"call dolt_commit('-am', 'add data');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select count(x) from dolt_history_yx where x = 1;",
				Expected: []sql.Row{
					{3},
				},
			},
		},
	},
	{
		Name: "dolt_history table filter correctness",
		SetUpScript: []string{
			"create table xy (x int primary key, y int);",
			"call dolt_add('.');",
			"call dolt_commit('-m', 'creating table');",
			"insert into xy values (0, 1);",
			"call dolt_commit('-am', 'add data');",
			"insert into xy values (2, 3);",
			"call dolt_commit('-am', 'add data');",
			"insert into xy values (4, 5);",
			"call dolt_commit('-am', 'add data');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select count(*) from dolt_history_xy where commit_hash = (select dolt_log.commit_hash from dolt_log limit 1 offset 1)",
				Expected: []sql.Row{
					{2},
				},
			},
		},
	},
	{
		Name: "dolt_history table primary key with join",
		SetUpScript: []string{
			"create table xyz (x int, y int, z int, primary key(x, y));",
			"call dolt_add('.');",
			"call dolt_commit('-m', 'creating table');",
			"insert into xyz values (0, 1, 100);",
			"call dolt_commit('-am', 'add data');",
			"insert into xyz values (2, 3, 200);",
			"call dolt_commit('-am', 'add data');",
			"insert into xyz values (4, 5, 300);",
			"call dolt_commit('-am', 'add data');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{

				Query: `
SELECT
  dolt_history_xyz.x as x,
  dolt_history_xyz.y as y,
  dolt_history_xyz.z as z,
  dolt_commits.commit_hash as comm
FROM
  dolt_history_xyz
  LEFT JOIN
  dolt_commits
  ON
  dolt_history_xyz.commit_hash = dolt_commits.commit_hash
ORDER BY
  dolt_history_xyz.x,
  dolt_history_xyz.y,
  dolt_history_xyz.z;`,
				Expected: []sql.Row{
					{0, 1, 100, doltCommit},
					{0, 1, 100, doltCommit},
					{0, 1, 100, doltCommit},
					{2, 3, 200, doltCommit},
					{2, 3, 200, doltCommit},
					{4, 5, 300, doltCommit},
				},
			},
			{
				Query: `
SELECT
  dolt_history_xyz.y as y,
  dolt_history_xyz.z as z,
  dolt_commits.commit_hash as comm
FROM
  dolt_history_xyz
  LEFT JOIN
  dolt_commits
  ON
  dolt_history_xyz.commit_hash = dolt_commits.commit_hash
ORDER BY
  dolt_history_xyz.y,
  dolt_history_xyz.z;`,
				Expected: []sql.Row{
					{1, 100, doltCommit},
					{1, 100, doltCommit},
					{1, 100, doltCommit},
					{3, 200, doltCommit},
					{3, 200, doltCommit},
					{5, 300, doltCommit},
				},
			},
			{
				Query: `
SELECT
  dolt_history_xyz.z as z,
  dolt_commits.commit_hash as comm
FROM
  dolt_history_xyz
  LEFT JOIN
  dolt_commits
  ON
  dolt_history_xyz.commit_hash = dolt_commits.commit_hash
ORDER BY
  dolt_history_xyz.z;`,
				Expected: []sql.Row{
					{100, doltCommit},
					{100, doltCommit},
					{100, doltCommit},
					{200, doltCommit},
					{200, doltCommit},
					{300, doltCommit},
				},
			},
			{
				Query: `
SELECT z
FROM xyz
WHERE z IN (
  SELECT z
  FROM dolt_history_xyz
  LEFT JOIN dolt_commits
  ON dolt_history_xyz.commit_hash = dolt_commits.commit_hash
);`,
				Expected: []sql.Row{
					{100},
					{200},
					{300},
				},
			},
		},
	},
	{
		Name:        "can sort by dolt_log.commit",
		SetUpScript: []string{},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select 'something' from dolt_log order by commit_hash;",
				Expected: []sql.Row{
					{"something"},
					{"something"},
				},
			},
			{
				Query:    "select 'something' from dolt_diff order by commit_hash;",
				Expected: []sql.Row{},
			},
			{
				Query: "select 'something' from dolt_commits order by commit_hash;",
				Expected: []sql.Row{
					{"something"},
					{"something"},
				},
			},
			{
				Query: "select 'something' from dolt_commit_ancestors order by commit_hash;",
				Expected: []sql.Row{
					{"something"},
					{"something"},
				},
			},
		},
	},
	{
		Name: "dolt_history table non-unique pk columns ignored for max1row optimization",
		SetUpScript: []string{
			"CREATE TABLE t1 (id INT PRIMARY KEY, message TEXT);",
			"INSERT INTO t1 (id, message) VALUES (1, 'test1');",
			"INSERT INTO t1 (id, message) VALUES (2, 'irrelevant');",
			"CALL DOLT_COMMIT('-A', '-m', 'test commit 1');",
			"UPDATE t1 SET message='test2' WHERE id=1;",
			"CALL DOLT_COMMIT('-a', '-m', 'test commit 2');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT id, message FROM dolt_history_t1 where id = 1 order by commit_date desc;",
				Expected: []sql.Row{{1, "test2"}, {1, "test1"}},
			},
			{
				Query:    "SELECT id, message FROM dolt_history_t1 where id = 2;",
				Expected: []sql.Row{{2, "irrelevant"}, {2, "irrelevant"}},
			},
		},
	},
	{
		Name: "dolt_commit_ancestors table with commit_hash filter ignored for max1row optimization",
		SetUpScript: []string{
			"CALL DOLT_CHECKOUT('-b', 'branch1');",
			"CREATE TABLE t1 (id INT PRIMARY KEY, message TEXT);",
			"INSERT INTO t1 (id, message) VALUES (1, 'test1');",
			"CALL DOLT_COMMIT('-A', '-m', 'test commit 1');",

			"CALL DOLT_CHECKOUT('-b', 'branch2');",
			"UPDATE t1 SET message='test2' WHERE id=1;",
			"CALL DOLT_COMMIT('-A', '-m', 'test commit 2');",

			"CALL DOLT_CHECKOUT('branch1');",
			"INSERT INTO t1 (id, message) VALUES (2, 'test3');",
			"CALL DOLT_COMMIT('-A', '-m', 'test commit 3');",

			"CALL DOLT_MERGE('--no-ff', 'branch2');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT parent_index FROM dolt_commit_ancestors WHERE commit_hash = (SELECT hashof('HEAD'));",
				Expected: []sql.Row{{0}, {1}},
			},
			{
				Query:    "SELECT dca.parent_index, (SELECT message FROM dolt_log WHERE commit_hash = dca.parent_hash) AS message FROM dolt_commit_ancestors dca WHERE dca.commit_hash = (SELECT hashof('HEAD')) ORDER BY dca.parent_index;",
				Expected: []sql.Row{{0, "test commit 3"}, {1, "test commit 2"}},
			},
		},
	},
	{
		Name:    "dolt_history_dolt_schemas committer column when unset reflects --author",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE VIEW v1 AS SELECT 1",
			"CALL DOLT_COMMIT('-Am', 'create view', '--author', 'Test Author <test@example.com>')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT committer FROM dolt_history_dolt_schemas WHERE name = 'v1'",
				Expected: []sql.Row{{"Test Author <test@example.com>"}},
			},
		},
	},
	{
		Name:    "dolt_history_dolt_procedures committer column when unset reflects --author",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE PROCEDURE p1() SELECT 1",
			"CALL DOLT_COMMIT('-Am', 'create procedure', '--author', 'Test Author <test@example.com>')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT committer FROM dolt_history_dolt_procedures WHERE name = 'p1'",
				Expected: []sql.Row{{"Test Author <test@example.com>"}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/12178
		Name: "dolt_commit_ancestors returns one row per parent for a merge commit looked up by commit_hash",
		SetUpScript: []string{
			"CREATE TABLE t (id INT PRIMARY KEY, v INT);",
			"INSERT INTO t VALUES (1, 1);",
			"CALL DOLT_ADD('-A');",
			"CALL DOLT_COMMIT('-A', '-m', 'base');",
			"CALL DOLT_CHECKOUT('-b', 'other');",
			"INSERT INTO t VALUES (2, 2);",
			"CALL DOLT_COMMIT('-A', '-m', 'other');",
			"SET @other = hashof('other');",
			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO t VALUES (3, 3);",
			"CALL DOLT_COMMIT('-A', '-m', 'main');",
			"SET @main = hashof('main');",
			"CALL DOLT_MERGE('--no-ff', '-m', 'merged', 'other');",
			"SET @merge = hashof('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT parent_index FROM dolt_commit_ancestors WHERE commit_hash = @merge;",
				Expected: []sql.Row{{0}, {1}},
			},
			{
				Query:    "SELECT IF(parent_hash IN (@main, @other), 'ok', 'bad') FROM dolt_commit_ancestors WHERE commit_hash = @merge;",
				Expected: []sql.Row{{"ok"}, {"ok"}},
			},
		},
	},
}
