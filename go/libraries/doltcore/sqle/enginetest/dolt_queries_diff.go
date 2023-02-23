// Copyright 2022 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
)

var DiffSystemTableScriptTests = []queries.ScriptTest{
	{
		Name: "base case: added rows",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 int);",
			"call dolt_add('.')",
			"insert into t values (1, 2, 3), (4, 5, 6);",
			"set @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'creating table t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t;",
				Expected: []sql.Row{{2}},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit1 ORDER BY to_pk, to_c2, to_c2, from_pk, from_c1, from_c2, diff_type;",
				Expected: []sql.Row{
					{1, 2, 3, nil, nil, nil, "added"},
					{4, 5, 6, nil, nil, nil, "added"},
				},
			},
		},
	},
	{
		Name: "base case: modified rows",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 int);",
			"call dolt_add('.')",
			"insert into t values (1, 2, 3), (4, 5, 6);",
			"set @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'creating table t');",

			"update t set c2=0 where pk=1",
			"set @Commit2 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit2, '-am', 'modifying row');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t;",
				Expected: []sql.Row{{3}},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit2 ORDER BY to_pk, to_c2, to_c2, from_pk, from_c1, from_c2, diff_type;",
				Expected: []sql.Row{
					{1, 2, 0, 1, 2, 3, "modified"},
				},
			},
		},
	},
	{
		Name: "base case: deleted row",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 int);",
			"call dolt_add('.')",
			"insert into t values (1, 2, 3), (4, 5, 6);",
			"set @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'creating table t');",

			"delete from t where pk=1",
			"set @Commit2 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit2, '-am', 'modifying row');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t;",
				Expected: []sql.Row{{3}},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.Row{
					{nil, nil, nil, 1, 2, 3, "removed"},
				},
			},
		},
	},
	{
		// In this case, we do not expect to see the old/dropped table included in the dolt_diff_table output
		Name: "table drop and recreate with overlapping schema",
		SetUpScript: []string{
			"create table t (pk int primary key, c int);",
			"call dolt_add('.')",
			"insert into t values (1, 2), (3, 4);",
			"set @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'creating table t');",

			"drop table t;",
			"set @Commit2 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit2, '-am', 'dropping table t');",

			"create table t (pk int primary key, c int);",
			"call dolt_add('.')",
			"insert into t values (100, 200), (300, 400);",
			"set @Commit3 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit3, '-am', 'recreating table t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t",
				Expected: []sql.Row{{2}},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit3 ORDER BY to_pk;",
				Expected: []sql.Row{
					{100, 200, nil, nil, "added"},
					{300, 400, nil, nil, "added"},
				},
			},
		},
	},
	{
		// When a column is dropped we should see the column's value set to null in that commit
		Name: "column drop",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 int);",
			"call dolt_add('.')",
			"insert into t values (1, 2, 3), (4, 5, 6);",
			"set @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'creating table t');",

			"alter table t drop column c1;",
			"set @Commit2 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit2, '-am', 'dropping column c');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t;",
				Expected: []sql.Row{{4}},
			},
			{
				Query: "SELECT to_pk, to_c2, from_pk, from_c2 FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, 3, nil, nil},
					{4, 6, nil, nil},
				},
			},
			{
				Query: "SELECT to_pk, to_c2, from_pk, from_c2 FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, 3, 1, 3},
					{4, 6, 4, 6},
				},
			},
		},
	},
	{
		// When a column is dropped and recreated with the same type, we expect it to be included in dolt_diff output
		Name: "column drop and recreate with same type",
		SetUpScript: []string{
			"create table t (pk int primary key, c int);",
			"call dolt_add('.')",
			"insert into t values (1, 2), (3, 4);",
			"set @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'creating table t');",

			"alter table t drop column c;",
			"set @Commit2 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit2, '-am', 'dropping column c');",

			"alter table t add column c int;",
			"insert into t values (100, 101);",
			"set @Commit3 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit3, '-am', 'inserting into t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t;",
				Expected: []sql.Row{{5}},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, 2, nil, nil, "added"},
					{3, 4, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, nil, 1, 2, "modified"},
					{3, nil, 3, 4, "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit3 ORDER BY to_pk;",
				Expected: []sql.Row{
					{100, 101, nil, nil, "added"},
				},
			},
		},
	},
	{
		// When a column is dropped and then another column with the same type is renamed to that name, we expect it to be included in dolt_diff output
		Name: "column drop, then rename column with same type to same name",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 int);",
			"call dolt_add('.')",
			"insert into t values (1, 2, 3), (4, 5, 6);",
			"set @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'creating table t');",

			"alter table t drop column c1;",
			"set @Commit2 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit2, '-am', 'dropping column c1');",

			"alter table t rename column c2 to c1;",
			"insert into t values (100, 101);",
			"set @Commit3 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit3, '-am', 'inserting into t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t;",
				Expected: []sql.Row{{5}},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, 2, nil, nil, "added"},
					{4, 5, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, nil, 1, 2, "modified"},
					{4, nil, 4, 5, "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit3 ORDER BY to_pk;",
				Expected: []sql.Row{
					{100, 101, nil, nil, "added"},
					// TODO: It's more correct to also return the following rows.
					//{1, 3, 1, nil, "modified"},
					//{4, 6, 4, nil, "modified"}

					// To explain why, let's inspect table t at each of the commits:
					//
					//     @Commit1          @Commit2         @Commit3
					// +----+----+----+     +----+----+     +-----+-----+
					// | pk | c1 | c2 |     | pk | c2 |     | pk  | c1  |
					// +----+----+----+     +----+----+     +-----+-----+
					// | 1  | 2  | 3  |     | 1  | 3  |     | 1   | 3   |
					// | 4  | 5  | 6  |     | 4  | 6  |     | 4   | 6   |
					// +----+----+----+     +----+----+     | 100 | 101 |
					//                                      +-----+-----+
					//
					// If you were to interpret each table using the schema at
					// @Commit3, (pk, c1), you would see the following:
					//
					//   @Commit1            @Commit2         @Commit3
					// +----+----+         +----+------+     +-----+-----+
					// | pk | c1 |         | pk | c1   |     | pk  | c1  |
					// +----+----+         +----+------+     +-----+-----+
					// | 1  | 2  |         | 1  | NULL |     | 1   | 3   |
					// | 4  | 5  |         | 4  | NULL |     | 4   | 6   |
					// +----+----+         +----+------+     | 100 | 101 |
					//                                       +-----+-----+
					//
					// The corresponding diffs for the interpreted tables:
					//
					// Diff between init and @Commit1:
					// + (1, 2)
					// + (4, 5)
					//
					// Diff between @Commit1 and @Commit2:
					// ~ (1, NULL)
					// ~ (4, NULL)
					//
					// Diff between @Commit2 and @Commit3:
					// ~ (1, 3) <- currently not outputted
					// ~ (4, 6) <- currently not outputted
					// + (100, 101)
					//
					// The missing rows are not produced by diff since the
					// underlying value of the prolly trees are not modified during a column rename.
				},
			},
		},
	},
	{
		// When a column is dropped and recreated with a different type, we expect only the new column
		// to be included in dolt_diff output, with previous values coerced (with any warnings reported) to the new type
		Name: "column drop and recreate with different type that can be coerced (int -> string)",
		SetUpScript: []string{
			"create table t (pk int primary key, c int);",
			"call dolt_add('.')",
			"insert into t values (1, 2), (3, 4);",
			"set @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'creating table t');",

			"alter table t drop column c;",
			"set @Commit2 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit2, '-am', 'dropping column c');",

			"alter table t add column c varchar(20);",
			"insert into t values (100, '101');",
			"set @Commit3 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit3, '-am', 're-adding column c');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t;",
				Expected: []sql.Row{{5}},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, "2", nil, nil, "added"},
					{3, "4", nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, nil, 1, "2", "modified"},
					{3, nil, 3, "4", "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit3 ORDER BY to_pk;",
				Expected: []sql.Row{
					{100, "101", nil, nil, "added"},
				},
			},
		},
	},
	{
		Name: "column drop and recreate with different type that can NOT be coerced (string -> int)",
		SetUpScript: []string{
			"create table t (pk int primary key, c varchar(20));",
			"call dolt_add('.')",
			"insert into t values (1, 'two'), (3, 'four');",
			"set @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'creating table t');",

			"alter table t drop column c;",
			"set @Commit2 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit2, '-am', 'dropping column c');",

			"alter table t add column c int;",
			"insert into t values (100, 101);",
			"set @Commit3 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit3, '-am', 're-adding column c');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t;",
				Expected: []sql.Row{{5}},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, nil, nil, nil, "added"},
					{3, nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, nil, 1, nil, "modified"},
					{3, nil, 3, nil, "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit3 ORDER BY to_pk;",
				Expected: []sql.Row{
					{100, 101, nil, nil, "added"},
				},
			},
			{
				Query:                           "select * from dolt_diff_t;",
				ExpectedWarning:                 1105,
				ExpectedWarningsCount:           4,
				ExpectedWarningMessageSubstring: "unable to coerce value from field",
				SkipResultsCheck:                true,
			},
		},
	},
	{
		Name: "multiple column renames",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int);",
			"call dolt_add('.')",
			"insert into t values (1, 2);",
			"set @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'creating table t');",

			"alter table t rename column c1 to c2;",
			"insert into t values (3, 4);",
			"set @Commit2 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit2, '-am', 'renaming c1 to c2');",

			"alter table t drop column c2;",
			"set @Commit3 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit3, '-am', 'dropping column c2');",

			"alter table t add column c2 int;",
			"insert into t values (100, '101');",
			"set @Commit4 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit4, '-am', 'recreating column c2');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t;",
				Expected: []sql.Row{{5}},
			},
			{
				Query: "SELECT to_pk, to_c2, from_pk, from_c2, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c2, from_pk, from_c2, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.Row{
					{3, 4, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c2, from_pk, from_c2, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit3 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, nil, 1, 2, "modified"},
					{3, nil, 3, 4, "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c2, from_pk, from_c2, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit4 ORDER BY to_pk;",
				Expected: []sql.Row{
					{100, 101, nil, nil, "added"},
				},
			},
		},
	},
	{
		Name: "primary key change",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int);",
			"call dolt_add('.')",
			"insert into t values (1, 2), (3, 4);",
			"set @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'creating table t');",

			"alter table t drop primary key;",
			"insert into t values (5, 6);",
			"set @Commit2 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit2, '-am', 'dropping primary key');",

			"alter table t add primary key (c1);",
			"set @Commit3 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit3, '-am', 'adding primary key');",

			"insert into t values (7, 8);",
			"set @Commit4 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit4, '-am', 'adding more data');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:                           "select * from dolt_diff_t;",
				ExpectedWarning:                 1105,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "cannot render full diff between commits",
				SkipResultsCheck:                true,
			},
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t;;",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM DOLT_DIFF_t where to_commit=@Commit4;",
				Expected: []sql.Row{{7, 8, nil, nil, "added"}},
			},
		},
	},
	{
		Name: "table with commit column should maintain its data in diff",
		SetUpScript: []string{
			"CREATE TABLE t (pk int PRIMARY KEY, commit_msg varchar(20));",
			"CALL DOLT_ADD('.')",
			"CALL dolt_commit('-am', 'creating table t');",
			"INSERT INTO t VALUES (1, 'hi');",
			"CALL dolt_commit('-am', 'insert data');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT to_pk, char_length(to_commit), from_pk, char_length(from_commit), diff_type from dolt_diff_t;",
				Expected: []sql.Row{{1, 32, nil, 32, "added"}},
			},
		},
	},
	{
		Name: "selecting to_pk columns",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 int);",
			"call dolt_add('.')",
			"insert into t values (1, 2, 3), (4, 5, 6);",
			"set @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'first commit');",
			"insert into t values (7, 8, 9);",
			"set @Commit2 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit2, '-am', 'second commit');",
			"update t set c1 = 0 where pk > 5;",
			"set @Commit3 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit3, '-am', 'third commit');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t;",
				Expected: []sql.Row{{4}},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_DIFF_t WHERE to_pk = 1 ORDER BY to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type;",
				Expected: []sql.Row{
					{1, 2, 3, nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_DIFF_t WHERE to_pk > 1 ORDER BY to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type;",
				Expected: []sql.Row{
					{4, 5, 6, nil, nil, nil, "added"},
					{7, 0, 9, 7, 8, 9, "modified"},
					{7, 8, 9, nil, nil, nil, "added"},
				},
			},
		},
	},
	{
		Name: "selecting to_pk1 and to_pk2 columns",
		SetUpScript: []string{
			"create table t (pk1 int, pk2 int, c1 int, primary key (pk1, pk2));",
			"call dolt_add('.')",
			"insert into t values (1, 2, 3), (4, 5, 6);",
			"set @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'first commit');",
			"insert into t values (7, 8, 9);",
			"set @Commit2 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit2, '-am', 'second commit');",
			"update t set c1 = 0 where pk1 > 5;",
			"set @Commit3 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit3, '-am', 'third commit');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t;",
				Expected: []sql.Row{{4}},
			},
			{
				Query: "SELECT to_pk1, to_pk2, to_c1, from_pk1, from_pk2, from_c1, diff_type FROM DOLT_DIFF_t WHERE to_pk1 = 1 ORDER BY to_pk1, to_pk2, to_c1, from_pk1, from_pk2, from_c1, diff_type;",
				Expected: []sql.Row{
					{1, 2, 3, nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk1, to_pk2, to_c1, from_pk1, from_pk2, from_c1, diff_type FROM DOLT_DIFF_t WHERE to_pk1 = 1 and to_pk2 = 2 ORDER BY to_pk1, to_pk2, to_c1, from_pk1, from_pk2, from_c1, diff_type;",
				Expected: []sql.Row{
					{1, 2, 3, nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk1, to_pk2, to_c1, from_pk1, from_pk2, from_c1, diff_type FROM DOLT_DIFF_t WHERE to_pk1 > 1 and to_pk2 < 10 ORDER BY to_pk1, to_pk2, to_c1, from_pk1, from_pk2, from_c1, diff_type;",
				Expected: []sql.Row{
					{4, 5, 6, nil, nil, nil, "added"},
					{7, 8, 0, 7, 8, 9, "modified"},
					{7, 8, 9, nil, nil, nil, "added"},
				},
			},
		},
	},
	{
		Name: "Diff table shows diffs across primary key renames",
		SetUpScript: []string{
			"CREATE TABLE t (pk1 int PRIMARY KEY);",
			"INSERT INTO t values (1);",
			"CREATE table t2 (pk1a int, pk1b int, PRIMARY KEY (pk1a, pk1b));",
			"CALL DOLT_ADD('.')",
			"INSERT INTO t2 values (2, 2);",
			"CALL DOLT_COMMIT('-am', 'initial');",

			"ALTER TABLE t RENAME COLUMN pk1 to pk2",
			"ALTER TABLE t2 RENAME COLUMN pk1a to pk2a",
			"ALTER TABLE t2 RENAME COLUMN pk1b to pk2b",
			"CALL DOLT_COMMIT('-am', 'rename primary key')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT from_pk2, to_pk2, diff_type from dolt_diff_t;",
				Expected: []sql.Row{{nil, 1, "added"}},
			},
			{
				Query:    "SELECT from_pk2a, from_pk2b, to_pk2a, to_pk2b, diff_type from dolt_diff_t2;",
				Expected: []sql.Row{{nil, nil, 2, 2, "added"}},
			},
		},
	},
	{
		Name: "add multiple columns, then set and unset a value. Should not show a diff",
		SetUpScript: []string{
			"CREATE table t (pk int primary key);",
			"Insert into t values (1);",
			"alter table t add column col1 int;",
			"alter table t add column col2 int;",
			"CALL DOLT_ADD('.');",
			"CALL DOLT_COMMIT('-am', 'setup');",
			"UPDATE t set col1 = 1 where pk = 1;",
			"UPDATE t set col1 = null where pk = 1;",
			"CALL DOLT_COMMIT('--allow-empty', '-am', 'fix short tuple');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT to_pk, to_col1, from_pk, from_col1, diff_type from dolt_diff_t;",
				Expected: []sql.Row{{1, nil, nil, nil, "added"}},
			},
		},
	},
}

var Dolt1DiffSystemTableScripts = []queries.ScriptTest{
	{
		Name: "Diff table stops creating diff partitions when any primary key type has changed",
		SetUpScript: []string{
			"CREATE TABLE t (pk1 VARCHAR(100), pk2 VARCHAR(100), PRIMARY KEY (pk1, pk2));",
			"CALL DOLT_ADD('.')",
			"INSERT INTO t VALUES ('1', '1');",
			"CALL DOLT_COMMIT('-am', 'setup');",

			"ALTER TABLE t MODIFY COLUMN pk2 VARCHAR(101)",
			"CALL DOLT_COMMIT('-am', 'modify column type');",

			"INSERT INTO t VALUES ('2', '2');",
			"CALL DOLT_COMMIT('-am', 'insert new row');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT to_pk1, to_pk2, from_pk1, from_pk2, diff_type from dolt_diff_t;",
				Expected: []sql.Row{{"2", "2", nil, nil, "added"}},
			},
		},
	},
}

var DiffTableFunctionScriptTests = []queries.ScriptTest{
	{
		Name: "invalid arguments",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"call dolt_add('.')",
			"set @Commit1 = '';",
			"call dolt_commit_hash_out(@Commit1, '-am', 'creating table t');",

			"insert into t values(1, 'one', 'two'), (2, 'two', 'three');",
			"set @Commit2 = '';",
			"call dolt_commit_hash_out(@Commit2, '-am', 'inserting into t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "SELECT * from dolt_diff();",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_diff('t');",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_diff(@Commit1, 't');",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_diff(@Commit1, @Commit2, 'extra', 't');",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_diff(null, null, null);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_diff(@Commit1, @Commit2, 123);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_diff(123, @Commit2, 't');",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_diff(@Commit1, 123, 't');",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_diff(@Commit1, @Commit2, 'doesnotexist');",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:          "SELECT * from dolt_diff('fakefakefakefakefakefakefakefake', @Commit2, 't');",
				ExpectedErrStr: "target commit not found",
			},
			{
				Query:          "SELECT * from dolt_diff(@Commit1, 'fake-branch', 't');",
				ExpectedErrStr: "branch not found: fake-branch",
			},
			{
				Query:       "SELECT * from dolt_diff(@Commit1, concat('fake', '-', 'branch'), 't');",
				ExpectedErr: sqle.ErrInvalidNonLiteralArgument,
			},
			{
				Query:       "SELECT * from dolt_diff(hashof('main'), @Commit2, 't');",
				ExpectedErr: sqle.ErrInvalidNonLiteralArgument,
			},
			{
				Query:       "SELECT * from dolt_diff(hashof('main'), @Commit2, LOWER('T'));",
				ExpectedErr: sqle.ErrInvalidNonLiteralArgument,
			},

			{
				Query:       "SELECT * from dolt_diff('main..main~');",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_diff('main..main~', 'extra', 't');",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_diff('main..main^', 123);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_diff('main..main~', 'doesnotexist');",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:          "SELECT * from dolt_diff('fakefakefakefakefakefakefakefake..main', 't');",
				ExpectedErrStr: "target commit not found",
			},
			{
				Query:          "SELECT * from dolt_diff('main..fakefakefakefakefakefakefakefake', 't');",
				ExpectedErrStr: "target commit not found",
			},
			{
				Query:          "SELECT * from dolt_diff('fakefakefakefakefakefakefakefake...main', 't');",
				ExpectedErrStr: "target commit not found",
			},
			{
				Query:          "SELECT * from dolt_diff('main...fakefakefakefakefakefakefakefake', 't');",
				ExpectedErrStr: "target commit not found",
			},
			{
				Query:       "SELECT * from dolt_diff('main..main~', LOWER('T'));",
				ExpectedErr: sqle.ErrInvalidNonLiteralArgument,
			},
		},
	},
	{
		Name: "basic case",
		SetUpScript: []string{
			"set @Commit0 = HashOf('HEAD');",

			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"call dolt_add('.')",
			"set @Commit1 = '';",
			"call dolt_commit_hash_out(@Commit1, '-am', 'creating table t');",

			"insert into t values(1, 'one', 'two');",
			"set @Commit2 = '';",
			"call dolt_commit_hash_out(@Commit2, '-am', 'inserting into table t');",

			"create table t2 (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"call dolt_add('.')",
			"insert into t2 values(100, 'hundred', 'hundert');",
			"set @Commit3 = '';",
			"call dolt_commit_hash_out(@Commit3, '-am', 'inserting into table t2');",

			"insert into t values(2, 'two', 'three'), (3, 'three', 'four');",
			"update t set c1='uno', c2='dos' where pk=1;",
			"set @Commit4 = '';",
			"call dolt_commit_hash_out(@Commit4, '-am', 'inserting into table t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit1, @Commit2, 't');",
				Expected: []sql.Row{{1, "one", "two", nil, nil, nil, "added"}},
			},
			{
				Query:    "SELECT COUNT(*) from dolt_diff(@Commit2, @Commit3, 't');",
				Expected: []sql.Row{{0}},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit3, @Commit4, 't');",
				Expected: []sql.Row{
					{1, "uno", "dos", 1, "one", "two", "modified"},
					{2, "two", "three", nil, nil, nil, "added"},
					{3, "three", "four", nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit4, @Commit3, 't');",
				Expected: []sql.Row{
					{1, "one", "two", 1, "uno", "dos", "modified"},
					{nil, nil, nil, 2, "two", "three", "removed"},
					{nil, nil, nil, 3, "three", "four", "removed"},
				},
			},
			{
				// Table t2 had no changes between Commit3 and Commit4, so results should be empty
				Query:    "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type  from dolt_diff(@Commit3, @Commit4, 'T2');",
				Expected: []sql.Row{},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type  from dolt_diff(@Commit1, @Commit4, 't');",
				Expected: []sql.Row{
					{1, "uno", "dos", nil, nil, nil, "added"},
					{2, "two", "three", nil, nil, nil, "added"},
					{3, "three", "four", nil, nil, nil, "added"},
				},
			},
			{
				// Reverse the to/from commits to see the diff from the other direction
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type  from dolt_diff(@Commit4, @Commit1, 'T');",
				Expected: []sql.Row{
					{nil, nil, nil, 1, "uno", "dos", "removed"},
					{nil, nil, nil, 2, "two", "three", "removed"},
					{nil, nil, nil, 3, "three", "four", "removed"},
				},
			},
			{
				Query: `
SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type
from dolt_diff(@Commit1, @Commit2, 't')
inner join t on to_pk = t.pk;`,
				Expected: []sql.Row{{1, "one", "two", nil, nil, nil, "added"}},
			},
		},
	},
	{
		Name: "WORKING and STAGED",
		SetUpScript: []string{
			"set @Commit0 = HashOf('HEAD');",

			"create table t (pk int primary key, c1 text, c2 text);",
			"call dolt_add('.')",
			"insert into t values (1, 'one', 'two'), (2, 'three', 'four');",
			"set @Commit1 = '';",
			"call dolt_commit_hash_out(@Commit1, '-am', 'inserting two rows into table t');",

			"insert into t values (3, 'five', 'six');",
			"delete from t where pk = 2",
			"update t set c2 = '100' where pk = 1",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT from_pk, from_c1, from_c2, to_pk, to_c1, to_c2, diff_type from dolt_diff(@Commit1, 'WORKING', 't') order by coalesce(from_pk, to_pk)",
				Expected: []sql.Row{
					{1, "one", "two", 1, "one", "100", "modified"},
					{2, "three", "four", nil, nil, nil, "removed"},
					{nil, nil, nil, 3, "five", "six", "added"},
				},
			},
			{
				Query: "SELECT from_pk, from_c1, from_c2, to_pk, to_c1, to_c2, diff_type from dolt_diff('STAGED', 'WORKING', 't') order by coalesce(from_pk, to_pk);",
				Expected: []sql.Row{
					{1, "one", "two", 1, "one", "100", "modified"},
					{2, "three", "four", nil, nil, nil, "removed"},
					{nil, nil, nil, 3, "five", "six", "added"},
				},
			},
			{
				Query: "SELECT from_pk, from_c1, from_c2, to_pk, to_c1, to_c2, diff_type from dolt_diff('STAGED..WORKING', 't') order by coalesce(from_pk, to_pk);",
				Expected: []sql.Row{
					{1, "one", "two", 1, "one", "100", "modified"},
					{2, "three", "four", nil, nil, nil, "removed"},
					{nil, nil, nil, 3, "five", "six", "added"},
				},
			},
			{
				Query: "SELECT from_pk, from_c1, from_c2, to_pk, to_c1, to_c2, diff_type from dolt_diff('WORKING', 'STAGED', 't') order by coalesce(from_pk, to_pk);",
				Expected: []sql.Row{
					{1, "one", "100", 1, "one", "two", "modified"},
					{nil, nil, nil, 2, "three", "four", "added"},
					{3, "five", "six", nil, nil, nil, "removed"},
				},
			},
			{
				Query:    "SELECT from_pk, from_c1, from_c2, to_pk, to_c1, to_c2, diff_type from dolt_diff('WORKING', 'WORKING', 't') order by coalesce(from_pk, to_pk);",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT from_pk, from_c1, from_c2, to_pk, to_c1, to_c2, diff_type from dolt_diff('WORKING..WORKING', 't') order by coalesce(from_pk, to_pk);",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT from_pk, from_c1, from_c2, to_pk, to_c1, to_c2, diff_type from dolt_diff('STAGED', 'STAGED', 't') order by coalesce(from_pk, to_pk);",
				Expected: []sql.Row{},
			},
			{
				Query:            "call dolt_add('.')",
				SkipResultsCheck: true,
			},
			{
				Query:    "SELECT from_pk, from_c1, from_c2, to_pk, to_c1, to_c2, diff_type from dolt_diff('WORKING', 'STAGED', 't') order by coalesce(from_pk, to_pk);",
				Expected: []sql.Row{},
			},
			{
				Query: "SELECT from_pk, from_c1, from_c2, to_pk, to_c1, to_c2, diff_type from dolt_diff('HEAD', 'STAGED', 't') order by coalesce(from_pk, to_pk);",
				Expected: []sql.Row{
					{1, "one", "two", 1, "one", "100", "modified"},
					{2, "three", "four", nil, nil, nil, "removed"},
					{nil, nil, nil, 3, "five", "six", "added"},
				},
			},
		},
	},
	{
		Name: "diff with branch refs",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"call dolt_add('.')",
			"set @Commit1 = '';",
			"call dolt_commit_hash_out(@Commit1, '-am', 'creating table t');",

			"insert into t values(1, 'one', 'two');",
			"set @Commit2 = '';",
			"call dolt_commit_hash_out(@Commit2, '-am', 'inserting row 1 into t in main');",

			"CALL DOLT_checkout('-b', 'branch1');",
			"alter table t drop column c2;",
			"set @Commit3 = '';",
			"call dolt_commit_hash_out(@Commit3, '-am', 'dropping column c2 in branch1');",

			"delete from t where pk=1;",
			"set @Commit4 = '';",
			"call dolt_commit_hash_out(@Commit4, '-am', 'deleting row 1 in branch1');",

			"insert into t values (2, 'two');",
			"set @Commit5 = '';",
			"call dolt_commit_hash_out(@Commit5, '-am', 'inserting row 2 in branch1');",

			"CALL DOLT_checkout('main');",
			"insert into t values (2, 'two', 'three');",
			"set @Commit6 = '';",
			"call dolt_commit_hash_out(@Commit6, '-am', 'inserting row 2 in main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff('main', 'branch1', 't');",
				Expected: []sql.Row{
					{nil, nil, 1, "one", "two", "removed"},
					{2, "two", 2, "two", "three", "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff('main..branch1', 't');",
				Expected: []sql.Row{
					{nil, nil, 1, "one", "two", "removed"},
					{2, "two", 2, "two", "three", "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, diff_type from dolt_diff('branch1', 'main', 't');",
				Expected: []sql.Row{
					{1, "one", "two", nil, nil, "added"},
					{2, "two", "three", 2, "two", "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, diff_type from dolt_diff('branch1..main', 't');",
				Expected: []sql.Row{
					{1, "one", "two", nil, nil, "added"},
					{2, "two", "three", 2, "two", "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff('main~', 'branch1', 't');",
				Expected: []sql.Row{
					{nil, nil, 1, "one", "two", "removed"},
					{2, "two", nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff('main~..branch1', 't');",
				Expected: []sql.Row{
					{nil, nil, 1, "one", "two", "removed"},
					{2, "two", nil, nil, nil, "added"},
				},
			},

			// Three dot
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff('main...branch1', 't');",
				Expected: []sql.Row{
					{nil, nil, 1, "one", "two", "removed"},
					{2, "two", nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, diff_type from dolt_diff('branch1...main', 't');",
				Expected: []sql.Row{
					{2, "two", "three", nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff('main~...branch1', 't');",
				Expected: []sql.Row{
					{nil, nil, 1, "one", "two", "removed"},
					{2, "two", nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff('main...branch1~', 't');",
				Expected: []sql.Row{
					{nil, nil, 1, "one", "two", "removed"},
				},
			},
		},
	},
	{
		Name: "schema modification: drop and recreate column with same type",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"call dolt_add('.')",
			"set @Commit1 = '';",
			"call dolt_commit_hash_out(@Commit1, '-am', 'creating table t');",

			"insert into t values(1, 'one', 'two'), (2, 'two', 'three');",
			"set @Commit2 = '';",
			"call dolt_commit_hash_out(@Commit2, '-am', 'inserting into t');",

			"alter table t drop column c2;",
			"set @Commit3 = '';",
			"call dolt_commit_hash_out(@Commit3, '-am', 'dropping column c2');",

			"alter table t add column c2 varchar(20);",
			"insert into t values (3, 'three', 'four');",
			"update t set c2='foo' where pk=1;",
			"set @Commit4 = '';",
			"call dolt_commit_hash_out(@Commit4, '-am', 'adding column c2, inserting, and updating data');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit1, @Commit2, 't');",
				Expected: []sql.Row{
					{1, "one", "two", nil, nil, nil, "added"},
					{2, "two", "three", nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit2, @Commit3, 't');",
				Expected: []sql.Row{
					{1, "one", 1, "one", "two", "modified"},
					{2, "two", 2, "two", "three", "modified"},
				},
			},
			{
				Query:       "SELECT to_c2 from dolt_diff(@Commit2, @Commit3, 't');",
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, diff_type from dolt_diff(@Commit3, @Commit4, 't');",
				Expected: []sql.Row{
					{1, "one", "foo", 1, "one", "modified"},
					// This row doesn't show up as changed because adding a column doesn't touch the row data.
					//{2, "two", nil, 2, "two", "modified"},
					{3, "three", "four", nil, nil, "added"},
				},
			},
			{
				Query:       "SELECT from_c2 from dolt_diff(@Commit3, @Commit4, 't');",
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit1, @Commit4, 't');",
				Expected: []sql.Row{
					{1, "one", "foo", nil, nil, nil, "added"},
					{2, "two", nil, nil, nil, nil, "added"},
					{3, "three", "four", nil, nil, nil, "added"},
				},
			},
		},
	},
	{
		Name: "schema modification: rename columns",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 int);",
			"call dolt_add('.')",
			"set @Commit1 = '';",
			"call dolt_commit_hash_out(@Commit1, '-am', 'creating table t');",

			"insert into t values(1, 'one', -1), (2, 'two', -2);",
			"set @Commit2 = '';",
			"call dolt_commit_hash_out(@Commit2, '-am', 'inserting into t');",

			"alter table t rename column c2 to c3;",
			"set @Commit3 = '';",
			"call dolt_commit_hash_out(@Commit3, '-am', 'renaming column c2 to c3');",

			"insert into t values (3, 'three', -3);",
			"update t set c3=1 where pk=1;",
			"set @Commit4 = '';",
			"call dolt_commit_hash_out(@Commit4, '-am', 'inserting and updating data');",

			"alter table t rename column c3 to c2;",
			"insert into t values (4, 'four', -4);",
			"set @Commit5 = '';",
			"call dolt_commit_hash_out(@Commit5, '-am', 'renaming column c3 to c2, and inserting data');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit1, @Commit2, 't');",
				Expected: []sql.Row{
					{1, "one", -1, nil, nil, nil, "added"},
					{2, "two", -2, nil, nil, nil, "added"},
				},
			},
			{
				Query:       "SELECT to_c2 from dolt_diff(@Commit2, @Commit3, 't');",
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query:    "SELECT to_pk, to_c1, to_c3, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit2, @Commit3, 't');",
				Expected: []sql.Row{},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c3, from_pk, from_c1, from_c3, diff_type from dolt_diff(@Commit3, @Commit4, 't');",
				Expected: []sql.Row{
					{3, "three", -3, nil, nil, nil, "added"},
					{1, "one", 1, 1, "one", -1, "modified"},
				},
			},
			{
				Query:       "SELECT from_c2 from dolt_diff(@Commit4, @Commit5, 't');",
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query:       "SELECT to_c3 from dolt_diff(@Commit4, @Commit5, 't');",
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c3, diff_type from dolt_diff(@Commit4, @Commit5, 't');",
				Expected: []sql.Row{
					{4, "four", -4, nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit1, @Commit5, 't');",
				Expected: []sql.Row{
					{1, "one", 1, nil, nil, nil, "added"},
					{2, "two", -2, nil, nil, nil, "added"},
					{3, "three", -3, nil, nil, nil, "added"},
					{4, "four", -4, nil, nil, nil, "added"},
				},
			},
		},
	},
	{
		Name: "schema modification: drop and rename columns with different types",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"call dolt_add('.')",
			"set @Commit1 = '';",
			"call dolt_commit_hash_out(@Commit1, '-am', 'creating table t');",

			"insert into t values(1, 'one', 'asdf'), (2, 'two', '2');",
			"set @Commit2 = '';",
			"call dolt_commit_hash_out(@Commit2, '-am', 'inserting into t');",

			"alter table t drop column c2;",
			"set @Commit3 = '';",
			"call dolt_commit_hash_out(@Commit3, '-am', 'dropping column c2');",

			"insert into t values (3, 'three');",
			"update t set c1='fdsa' where pk=1;",
			"set @Commit4 = '';",
			"call dolt_commit_hash_out(@Commit4, '-am', 'inserting and updating data');",

			"alter table t add column c2 int;",
			"insert into t values (4, 'four', -4);",
			"set @Commit5 = '';",
			"call dolt_commit_hash_out(@Commit5, '-am', 'adding column c2, and inserting data');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit1, @Commit2, 't');",
				Expected: []sql.Row{
					{1, "one", "asdf", nil, nil, nil, "added"},
					{2, "two", "2", nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit2, @Commit3, 't');",
				Expected: []sql.Row{
					{1, "one", 1, "one", "asdf", "modified"},
					{2, "two", 2, "two", "2", "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, diff_type from dolt_diff(@Commit3, @Commit4, 't');",
				Expected: []sql.Row{
					{3, "three", nil, nil, "added"},
					{1, "fdsa", 1, "one", "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, diff_type from dolt_diff(@Commit4, @Commit5, 't');",
				Expected: []sql.Row{
					{4, "four", -4, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit1, @Commit5, 't');",
				Expected: []sql.Row{
					{1, "fdsa", nil, nil, nil, nil, "added"},
					{2, "two", nil, nil, nil, nil, "added"},
					{3, "three", nil, nil, nil, nil, "added"},
					{4, "four", -4, nil, nil, nil, "added"},
				},
			},
		},
	},
	{
		Name: "new table",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int)",
			"insert into t1 values (1,2)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select to_a, to_b, from_commit, to_commit, diff_type from dolt_diff('HEAD', 'WORKING', 't1')",
				Expected: []sql.Row{{1, 2, "HEAD", "WORKING", "added"}},
			},
			{
				Query:       "select to_a, from_b, from_commit, to_commit, diff_type from dolt_diff('HEAD', 'WORKING', 't1')",
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query:    "select from_a, from_b, from_commit, to_commit, diff_type from dolt_diff('WORKING', 'HEAD', 't1')",
				Expected: []sql.Row{{1, 2, "WORKING", "HEAD", "removed"}},
			},
		},
	},
	{
		Name: "dropped table",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int)",
			"call dolt_add('.')",
			"insert into t1 values (1,2)",
			"call dolt_commit('-am', 'new table')",
			"drop table t1",
			"call dolt_commit('-am', 'dropped table')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select from_a, from_b, from_commit, to_commit, diff_type from dolt_diff('HEAD~', 'HEAD', 't1')",
				Expected: []sql.Row{{1, 2, "HEAD~", "HEAD", "removed"}},
			},
			{
				Query:    "select from_a, from_b, from_commit, to_commit, diff_type from dolt_diff('HEAD~..HEAD', 't1')",
				Expected: []sql.Row{{1, 2, "HEAD~", "HEAD", "removed"}},
			},
		},
	},
	{
		Name: "renamed table",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int)",
			"call dolt_add('.')",
			"insert into t1 values (1,2)",
			"call dolt_commit('-am', 'new table')",
			"alter table t1 rename to t2",
			"call dolt_add('.')",
			"insert into t2 values (3,4)",
			"call dolt_commit('-am', 'renamed table')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select to_a, to_b, from_commit, to_commit, diff_type from dolt_diff('HEAD~', 'HEAD', 't2')",
				Expected: []sql.Row{{3, 4, "HEAD~", "HEAD", "added"}},
			},
			{
				Query:    "select to_a, to_b, from_commit, to_commit, diff_type from dolt_diff('HEAD~..HEAD', 't2')",
				Expected: []sql.Row{{3, 4, "HEAD~", "HEAD", "added"}},
			},
			{
				// Maybe confusing? We match the old table name as well
				Query:    "select to_a, to_b, from_commit, to_commit, diff_type from dolt_diff('HEAD~', 'HEAD', 't1')",
				Expected: []sql.Row{{3, 4, "HEAD~", "HEAD", "added"}},
			},
		},
	},
	{
		Name: "Renaming a primary key column shows PK values in both the to and from columns",
		SetUpScript: []string{
			"CREATE TABLE t1 (pk int PRIMARY KEY, col1 int);",
			"INSERT INTO t1 VALUES (1, 1);",
			"CREATE TABLE t2 (pk1a int, pk1b int, col1 int, PRIMARY KEY (pk1a, pk1b));",
			"INSERT INTO t2 VALUES (1, 1, 1);",
			"CALL DOLT_ADD('.')",
			"CALL DOLT_COMMIT('-am', 'initial');",

			"ALTER TABLE t1 RENAME COLUMN pk to pk2;",
			"UPDATE t1 set col1 = 100;",
			"ALTER TABLE t2 RENAME COLUMN pk1a to pk2a;",
			"ALTER TABLE t2 RENAME COLUMN pk1b to pk2b;",
			"UPDATE t2 set col1 = 100;",
			"CALL DOLT_COMMIT('-am', 'edit');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select to_pk2, to_col1, from_pk, from_col1, diff_type from dolt_diff('HEAD~', 'HEAD', 't1')",
				Expected: []sql.Row{{1, 100, 1, 1, "modified"}},
			},
			{
				Query:    "select to_pk2, to_col1, from_pk, from_col1, diff_type from dolt_diff('HEAD~..HEAD', 't1')",
				Expected: []sql.Row{{1, 100, 1, 1, "modified"}},
			},
			{
				Query:    "select to_pk2a, to_pk2b, to_col1, from_pk1a, from_pk1b, from_col1, diff_type from dolt_diff('HEAD~', 'HEAD', 't2');",
				Expected: []sql.Row{{1, 1, 100, 1, 1, 1, "modified"}},
			},
			{
				Query:    "select to_pk2a, to_pk2b, to_col1, from_pk1a, from_pk1b, from_col1, diff_type from dolt_diff('HEAD~..HEAD', 't2');",
				Expected: []sql.Row{{1, 1, 100, 1, 1, 1, "modified"}},
			},
		},
	},
}

var DiffStatTableFunctionScriptTests = []queries.ScriptTest{
	{
		Name: "invalid arguments",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"call dolt_add('.')",
			"set @Commit1 = '';",
			"call dolt_commit_hash_out(@Commit1, '-am', 'creating table t');",

			"insert into t values(1, 'one', 'two'), (2, 'two', 'three');",
			"set @Commit2 = '';",
			"call dolt_commit_hash_out(@Commit2, '-am', 'inserting into t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "SELECT * from dolt_diff_stat();",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_diff_stat('t');",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_diff_stat('t', @Commit1, @Commit2, 'extra');",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_diff_stat(null, null, null);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_diff_stat(123, @Commit1, @Commit2);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_diff_stat('t', 123, @Commit2);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_diff_stat('t', @Commit1, 123);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:          "SELECT * from dolt_diff_stat('fake-branch', @Commit2, 't');",
				ExpectedErrStr: "branch not found: fake-branch",
			},
			{
				Query:          "SELECT * from dolt_diff_stat('fake-branch..main', 't');",
				ExpectedErrStr: "branch not found: fake-branch",
			},
			{
				Query:          "SELECT * from dolt_diff_stat(@Commit1, 'fake-branch', 't');",
				ExpectedErrStr: "branch not found: fake-branch",
			},
			{
				Query:          "SELECT * from dolt_diff_stat('main..fake-branch', 't');",
				ExpectedErrStr: "branch not found: fake-branch",
			},
			{
				Query:       "SELECT * from dolt_diff_stat(@Commit1, @Commit2, 'doesnotexist');",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:       "SELECT * from dolt_diff_stat('main^..main', 'doesnotexist');",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:       "SELECT * from dolt_diff_stat(@Commit1, concat('fake', '-', 'branch'), 't');",
				ExpectedErr: sqle.ErrInvalidNonLiteralArgument,
			},
			{
				Query:       "SELECT * from dolt_diff_stat(hashof('main'), @Commit2, 't');",
				ExpectedErr: sqle.ErrInvalidNonLiteralArgument,
			},
			{
				Query:       "SELECT * from dolt_diff_stat(@Commit1, @Commit2, LOWER('T'));",
				ExpectedErr: sqle.ErrInvalidNonLiteralArgument,
			},
			{
				Query:       "SELECT * from dolt_diff_stat('main..main~', LOWER('T'));",
				ExpectedErr: sqle.ErrInvalidNonLiteralArgument,
			},
		},
	},
	{
		Name: "basic case with single table",
		SetUpScript: []string{
			"set @Commit0 = HashOf('HEAD');",
			"set @Commit1 = '';",
			"call dolt_commit_hash_out(@Commit1, '--allow-empty', '-m', 'creating table t');",

			// create table t only
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"call dolt_add('.')",
			"set @Commit2 = '';",
			"call dolt_commit_hash_out(@Commit2, '-am', 'creating table t');",

			// insert 1 row into t
			"insert into t values(1, 'one', 'two');",
			"set @Commit3 = '';",
			"call dolt_commit_hash_out(@Commit3, '-am', 'inserting 1 into table t');",

			// insert 2 rows into t and update two cells
			"insert into t values(2, 'two', 'three'), (3, 'three', 'four');",
			"update t set c1='uno', c2='dos' where pk=1;",
			"set @Commit4 = '';",
			"call dolt_commit_hash_out(@Commit4, '-am', 'inserting 2 into table t');",

			// drop table t only
			"drop table t;",
			"set @Commit5 = '';",
			"call dolt_commit_hash_out(@Commit5, '-am', 'drop table t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// table is added, no data diff, result is empty
				Query:    "SELECT * from dolt_diff_stat(@Commit1, @Commit2, 't');",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit2, @Commit3, 't');",
				Expected: []sql.Row{{"t", 0, 1, 0, 0, 3, 0, 0, 0, 1, 0, 3}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit3, @Commit4, 't');",
				Expected: []sql.Row{{"t", 0, 2, 0, 1, 6, 0, 2, 1, 3, 3, 9}},
			},
			{
				// change from and to commits
				Query:    "SELECT * from dolt_diff_stat(@Commit4, @Commit3, 't');",
				Expected: []sql.Row{{"t", 0, 0, 2, 1, 0, 6, 2, 3, 1, 9, 3}},
			},
			{
				// table is dropped
				Query:    "SELECT * from dolt_diff_stat(@Commit4, @Commit5, 't');",
				Expected: []sql.Row{{"t", 0, 0, 3, 0, 0, 9, 0, 3, 0, 9, 0}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit1, @Commit4, 't');",
				Expected: []sql.Row{{"t", 0, 3, 0, 0, 9, 0, 0, 0, 3, 0, 9}},
			},
			{
				Query:       "SELECT * from dolt_diff_stat(@Commit1, @Commit5, 't');",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query: `
SELECT *
from dolt_diff_stat(@Commit3, @Commit4, 't') 
inner join t as of @Commit3 on rows_unmodified = t.pk;`,
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "basic case with single keyless table",
		SetUpScript: []string{
			"set @Commit0 = HashOf('HEAD');",
			"set @Commit1 = '';",
			"call dolt_commit_hash_out(@Commit1, '--allow-empty', '-m', 'creating table t');",

			// create table t only
			"create table t (id int, c1 varchar(20), c2 varchar(20));",
			"call dolt_add('.')",
			"set @Commit2 = '';",
			"call dolt_commit_hash_out(@Commit2, '-am', 'creating table t');",

			// insert 1 row into t
			"insert into t values(1, 'one', 'two');",
			"set @Commit3 = '';",
			"call dolt_commit_hash_out(@Commit3, '-am', 'inserting 1 into table t');",

			// insert 2 rows into t and update two cells
			"insert into t values(2, 'two', 'three'), (3, 'three', 'four');",
			"update t set c1='uno', c2='dos' where id=1;",
			"set @Commit4 = '';",
			"call dolt_commit_hash_out(@Commit4, '-am', 'inserting 2 into table t');",

			// drop table t only
			"drop table t;",
			"set @Commit5 = '';",
			"call dolt_commit_hash_out(@Commit5, '-am', 'drop table t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// table is added, no data diff, result is empty
				Query:    "SELECT * from dolt_diff_stat(@Commit1, @Commit2, 't');",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit2, @Commit3, 't');",
				Expected: []sql.Row{{"t", nil, 1, 0, nil, nil, nil, nil, nil, nil, nil, nil}},
			},
			{
				// TODO : (correct result is commented out)
				//      update row for keyless table deletes the row and insert the new row
				// 		this causes row added = 3 and row deleted = 1
				Query: "SELECT * from dolt_diff_stat(@Commit3, @Commit4, 't');",
				//Expected:         []sql.Row{{"t", nil, 2, 0, nil, nil, nil, nil, nil, nil, nil, nil}},
				Expected: []sql.Row{{"t", nil, 3, 1, nil, nil, nil, nil, nil, nil, nil, nil}},
			},
			{
				Query: "SELECT * from dolt_diff_stat(@Commit4, @Commit3, 't');",
				//Expected:         []sql.Row{{"t", nil, 0, 2, nil, nil, nil, nil, nil, nil, nil, nil}},
				Expected: []sql.Row{{"t", nil, 1, 3, nil, nil, nil, nil, nil, nil, nil, nil}},
			},
			{
				// table is dropped
				Query:    "SELECT * from dolt_diff_stat(@Commit4, @Commit5, 't');",
				Expected: []sql.Row{{"t", nil, 0, 3, nil, nil, nil, nil, nil, nil, nil, nil}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit1, @Commit4, 't');",
				Expected: []sql.Row{{"t", nil, 3, 0, nil, nil, nil, nil, nil, nil, nil, nil}},
			},
			{
				Query:       "SELECT * from dolt_diff_stat(@Commit1, @Commit5, 't');",
				ExpectedErr: sql.ErrTableNotFound,
			},
		},
	},
	{
		Name: "basic case with multiple tables",
		SetUpScript: []string{
			"set @Commit0 = HashOf('HEAD');",

			// add table t with 1 row
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"insert into t values(1, 'one', 'two');",
			"call dolt_add('.')",
			"set @Commit1 = '';",
			"call dolt_commit_hash_out(@Commit1, '-am', 'inserting into table t');",

			// add table t2 with 1 row
			"create table t2 (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"insert into t2 values(100, 'hundred', 'hundert');",
			"call dolt_add('.')",
			"set @Commit2 = '';",
			"call dolt_commit_hash_out(@Commit2, '-am', 'inserting into table t2');",

			// changes on both tables
			"insert into t values(2, 'two', 'three'), (3, 'three', 'four'), (4, 'four', 'five');",
			"update t set c1='uno', c2='dos' where pk=1;",
			"insert into t2 values(101, 'hundred one', 'one');",
			"set @Commit3 = '';",
			"call dolt_commit_hash_out(@Commit3, '-am', 'inserting into table t');",

			// changes on both tables
			"delete from t where c2 = 'four';",
			"update t2 set c2='zero' where pk=100;",
			"set @Commit4 = '';",
			"call dolt_commit_hash_out(@Commit4, '-am', 'inserting into table t');",

			// create keyless table
			"create table keyless (id int);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit0, @Commit1);",
				Expected: []sql.Row{{"t", 0, 1, 0, 0, 3, 0, 0, 0, 1, 0, 3}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit1, @Commit2);",
				Expected: []sql.Row{{"t2", 0, 1, 0, 0, 3, 0, 0, 0, 1, 0, 3}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit2, @Commit3);",
				Expected: []sql.Row{{"t", 0, 3, 0, 1, 9, 0, 2, 1, 4, 3, 12}, {"t2", 1, 1, 0, 0, 3, 0, 0, 1, 2, 3, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit3, @Commit4);",
				Expected: []sql.Row{{"t", 3, 0, 1, 0, 0, 3, 0, 4, 3, 12, 9}, {"t2", 1, 0, 0, 1, 0, 0, 1, 2, 2, 6, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit4, @Commit2);",
				Expected: []sql.Row{{"t", 0, 0, 2, 1, 0, 6, 2, 3, 1, 9, 3}, {"t2", 0, 0, 1, 1, 0, 3, 1, 2, 1, 6, 3}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit3, 'WORKING');",
				Expected: []sql.Row{{"t", 3, 0, 1, 0, 0, 3, 0, 4, 3, 12, 9}, {"t2", 1, 0, 0, 1, 0, 0, 1, 2, 2, 6, 6}},
			},
		},
	},
	{
		Name: "WORKING and STAGED",
		SetUpScript: []string{
			"set @Commit0 = HashOf('HEAD');",

			"create table t (pk int primary key, c1 text, c2 text);",
			"call dolt_add('.')",
			"insert into t values (1, 'one', 'two'), (2, 'three', 'four');",
			"set @Commit1 = '';",
			"call dolt_commit_hash_out(@Commit1, '-am', 'inserting two rows into table t');",

			"insert into t values (3, 'five', 'six');",
			"delete from t where pk = 2",
			"update t set c2 = '100' where pk = 1",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit1, 'WORKING', 't')",
				Expected: []sql.Row{{"t", 0, 1, 1, 1, 3, 3, 1, 2, 2, 6, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('STAGED', 'WORKING', 't')",
				Expected: []sql.Row{{"t", 0, 1, 1, 1, 3, 3, 1, 2, 2, 6, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('STAGED..WORKING', 't')",
				Expected: []sql.Row{{"t", 0, 1, 1, 1, 3, 3, 1, 2, 2, 6, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('WORKING', 'STAGED', 't')",
				Expected: []sql.Row{{"t", 0, 1, 1, 1, 3, 3, 1, 2, 2, 6, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('WORKING', 'WORKING', 't')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('WORKING..WORKING', 't')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('STAGED', 'STAGED', 't')",
				Expected: []sql.Row{},
			},
			{
				Query:            "call dolt_add('.')",
				SkipResultsCheck: true,
			},
			{
				Query:    "SELECT * from dolt_diff_stat('WORKING', 'STAGED', 't')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('HEAD', 'STAGED', 't')",
				Expected: []sql.Row{{"t", 0, 1, 1, 1, 3, 3, 1, 2, 2, 6, 6}},
			},
		},
	},
	{
		Name: "diff with branch refs",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"call dolt_add('.')",
			"set @Commit1 = '';",
			"call dolt_commit_hash_out(@Commit1, '-am', 'creating table t');",

			"insert into t values(1, 'one', 'two');",
			"set @Commit2 = '';",
			"call dolt_commit_hash_out(@Commit2, '-am', 'inserting row 1 into t in main');",

			"CALL DOLT_checkout('-b', 'branch1');",
			"alter table t drop column c2;",
			"set @Commit3 = '';",
			"call dolt_commit_hash_out(@Commit3, '-am', 'dropping column c2 in branch1');",

			"delete from t where pk=1;",
			"set @Commit4 = '';",
			"call dolt_commit_hash_out(@Commit4, '-am', 'deleting row 1 in branch1');",

			"insert into t values (2, 'two');",
			"set @Commit5 = '';",
			"call dolt_commit_hash_out(@Commit5, '-am', 'inserting row 2 in branch1');",

			"CALL DOLT_checkout('main');",
			"insert into t values (2, 'two', 'three');",
			"set @Commit6 = '';",
			"call dolt_commit_hash_out(@Commit6, '-am', 'inserting row 2 in main');",

			"create table newtable (pk int primary key);",
			"insert into newtable values (1), (2);",
			"set @Commit7 = '';",
			"call dolt_commit_hash_out(@Commit7, '-Am', 'new table newtable');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from dolt_diff_stat('main', 'branch1', 't');",
				Expected: []sql.Row{{"t", 0, 0, 1, 1, 0, 4, 0, 2, 1, 6, 2}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('main..branch1', 't');",
				Expected: []sql.Row{{"t", 0, 0, 1, 1, 0, 4, 0, 2, 1, 6, 2}},
			},
			{
				Query: "SELECT * from dolt_diff_stat('main', 'branch1');",
				Expected: []sql.Row{
					{"t", 0, 0, 1, 1, 0, 4, 0, 2, 1, 6, 2},
					{"newtable", 0, 0, 2, 0, 0, 2, 0, 2, 0, 2, 0},
				},
			},
			{
				Query: "SELECT * from dolt_diff_stat('main..branch1');",
				Expected: []sql.Row{
					{"t", 0, 0, 1, 1, 0, 4, 0, 2, 1, 6, 2},
					{"newtable", 0, 0, 2, 0, 0, 2, 0, 2, 0, 2, 0},
				},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('branch1', 'main', 't');",
				Expected: []sql.Row{{"t", 0, 1, 0, 1, 4, 0, 1, 1, 2, 2, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('branch1..main', 't');",
				Expected: []sql.Row{{"t", 0, 1, 0, 1, 4, 0, 1, 1, 2, 2, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('main~2', 'branch1', 't');",
				Expected: []sql.Row{{"t", 0, 1, 1, 0, 2, 3, 0, 1, 1, 3, 2}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('main~2..branch1', 't');",
				Expected: []sql.Row{{"t", 0, 1, 1, 0, 2, 3, 0, 1, 1, 3, 2}},
			},

			// Three dot
			{
				Query:    "SELECT * from dolt_diff_stat('main...branch1', 't');",
				Expected: []sql.Row{{"t", 0, 1, 1, 0, 2, 3, 0, 1, 1, 3, 2}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('main...branch1');",
				Expected: []sql.Row{{"t", 0, 1, 1, 0, 2, 3, 0, 1, 1, 3, 2}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('branch1...main', 't');",
				Expected: []sql.Row{{"t", 1, 1, 0, 0, 3, 0, 0, 1, 2, 3, 6}},
			},
			{
				Query: "SELECT * from dolt_diff_stat('branch1...main');",
				Expected: []sql.Row{
					{"t", 1, 1, 0, 0, 3, 0, 0, 1, 2, 3, 6},
					{"newtable", 0, 2, 0, 0, 2, 0, 0, 0, 2, 0, 2},
				},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('branch1...main^');",
				Expected: []sql.Row{{"t", 1, 1, 0, 0, 3, 0, 0, 1, 2, 3, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('branch1...main', 'newtable');",
				Expected: []sql.Row{{"newtable", 0, 2, 0, 0, 2, 0, 0, 0, 2, 0, 2}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('main...main', 'newtable');",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "schema modification: drop and add column",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"call dolt_add('.');",
			"insert into t values (1, 'one', 'two'), (2, 'two', 'three');",
			"set @Commit1 = '';",
			"call dolt_commit_hash_out(@Commit1, '-am', 'inserting row 1, 2 into t');",

			// drop 1 column and add 1 row
			"alter table t drop column c2;",
			"set @Commit2 = '';",
			"call dolt_commit_hash_out(@Commit2, '-am', 'dropping column c2');",

			// drop 1 column and add 1 row
			"insert into t values (3, 'three');",
			"set @Commit3 = '';",
			"call dolt_commit_hash_out(@Commit3, '-am', 'inserting row 3');",

			// add 1 column and 1 row and update
			"alter table t add column c2 varchar(20);",
			"insert into t values (4, 'four', 'five');",
			"update t set c2='foo' where pk=1;",
			"set @Commit4 = '';",
			"call dolt_commit_hash_out(@Commit4, '-am', 'adding column c2, inserting, and updating data');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit1, @Commit2, 't');",
				Expected: []sql.Row{{"t", 0, 0, 0, 2, 0, 2, 0, 2, 2, 6, 4}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit2, @Commit3, 't');",
				Expected: []sql.Row{{"t", 2, 1, 0, 0, 2, 0, 0, 2, 3, 4, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit1, @Commit3, 't');",
				Expected: []sql.Row{{"t", 0, 1, 0, 2, 2, 2, 0, 2, 3, 6, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit3, @Commit4, 't');",
				Expected: []sql.Row{{"t", 2, 1, 0, 1, 6, 0, 1, 3, 4, 6, 12}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit1, @Commit4, 't');",
				Expected: []sql.Row{{"t", 0, 2, 0, 2, 6, 0, 2, 2, 4, 6, 12}},
			},
		},
	},
	{
		Name: "schema modification: rename columns",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 int);",
			"call dolt_add('.')",
			"set @Commit1 = '';",
			"call dolt_commit_hash_out(@Commit1, '-am', 'creating table t');",

			"insert into t values(1, 'one', -1), (2, 'two', -2);",
			"set @Commit2 = '';",
			"call dolt_commit_hash_out(@Commit2, '-am', 'inserting into t');",

			"alter table t rename column c2 to c3;",
			"set @Commit3 = '';",
			"call dolt_commit_hash_out(@Commit3, '-am', 'renaming column c2 to c3');",

			"insert into t values (3, 'three', -3);",
			"update t set c3=1 where pk=1;",
			"set @Commit4 = '';",
			"call dolt_commit_hash_out(@Commit4, '-am', 'inserting and updating data');",

			"alter table t rename column c3 to c2;",
			"insert into t values (4, 'four', -4);",
			"set @Commit5 = '';",
			"call dolt_commit_hash_out(@Commit5, '-am', 'renaming column c3 to c2, and inserting data');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit1, @Commit2, 't');",
				Expected: []sql.Row{{"t", 0, 2, 0, 0, 6, 0, 0, 0, 2, 0, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit2, @Commit3, 't');",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit3, @Commit4, 't');",
				Expected: []sql.Row{{"t", 1, 1, 0, 1, 3, 0, 1, 2, 3, 6, 9}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit4, @Commit5, 't');",
				Expected: []sql.Row{{"t", 3, 1, 0, 0, 3, 0, 0, 3, 4, 9, 12}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit1, @Commit5, 't');",
				Expected: []sql.Row{{"t", 0, 4, 0, 0, 12, 0, 0, 0, 4, 0, 12}},
			},
		},
	},
	{
		Name: "new table",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_diff_stat('HEAD', 'WORKING')",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from dolt_diff_stat('WORKING', 'HEAD')",
				Expected: []sql.Row{},
			},
			{
				Query:            "insert into t1 values (1,2)",
				SkipResultsCheck: true,
			},
			{
				Query:    "select * from dolt_diff_stat('HEAD', 'WORKING', 't1')",
				Expected: []sql.Row{{"t1", 0, 1, 0, 0, 2, 0, 0, 0, 1, 0, 2}},
			},
			{
				Query:    "select * from dolt_diff_stat('WORKING', 'HEAD', 't1')",
				Expected: []sql.Row{{"t1", 0, 0, 1, 0, 0, 2, 0, 1, 0, 2, 0}},
			},
		},
	},
	{
		Name: "dropped table",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int)",
			"call dolt_add('.')",
			"insert into t1 values (1,2)",
			"call dolt_commit('-am', 'new table')",
			"drop table t1",
			"call dolt_commit('-am', 'dropped table')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_diff_stat('HEAD~', 'HEAD', 't1')",
				Expected: []sql.Row{{"t1", 0, 0, 1, 0, 0, 2, 0, 1, 0, 2, 0}},
			},
			{
				Query:    "select * from dolt_diff_stat('HEAD', 'HEAD~', 't1')",
				Expected: []sql.Row{{"t1", 0, 1, 0, 0, 2, 0, 0, 0, 1, 0, 2}},
			},
		},
	},
	{
		Name: "renamed table",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int)",
			"call dolt_add('.')",
			"insert into t1 values (1,2)",
			"call dolt_commit('-am', 'new table')",
			"alter table t1 rename to t2",
			"call dolt_add('.')",
			"insert into t2 values (3,4)",
			"call dolt_commit('-am', 'renamed table')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_diff_stat('HEAD~', 'HEAD', 't2')",
				Expected: []sql.Row{{"t2", 1, 1, 0, 0, 2, 0, 0, 1, 2, 2, 4}},
			},
			{
				Query:    "select * from dolt_diff_stat('HEAD~..HEAD', 't2')",
				Expected: []sql.Row{{"t2", 1, 1, 0, 0, 2, 0, 0, 1, 2, 2, 4}},
			},
			{
				// Old table name can be matched as well
				Query:    "select * from dolt_diff_stat('HEAD~', 'HEAD', 't1')",
				Expected: []sql.Row{{"t1", 1, 1, 0, 0, 2, 0, 0, 1, 2, 2, 4}},
			},
			{
				// Old table name can be matched as well
				Query:    "select * from dolt_diff_stat('HEAD~..HEAD', 't1')",
				Expected: []sql.Row{{"t1", 1, 1, 0, 0, 2, 0, 0, 1, 2, 2, 4}},
			},
		},
	},
	{
		Name: "add multiple columns, then set and unset a value. Should not show a diff",
		SetUpScript: []string{
			"CREATE table t (pk int primary key);",
			"Insert into t values (1);",
			"CALL DOLT_ADD('.');",
			"CALL DOLT_COMMIT('-am', 'setup');",
			"alter table t add column col1 int;",
			"alter table t add column col2 int;",
			"CALL DOLT_ADD('.');",
			"CALL DOLT_COMMIT('-am', 'add columns');",
			"UPDATE t set col1 = 1 where pk = 1;",
			"UPDATE t set col1 = null where pk = 1;",
			"CALL DOLT_COMMIT('--allow-empty', '-am', 'fix short tuple');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from dolt_diff_stat('HEAD~2', 'HEAD');",
				Expected: []sql.Row{{"t", 1, 0, 0, 0, 2, 0, 0, 1, 1, 1, 3}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('HEAD~', 'HEAD');",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "pk set change should throw an error for 3 argument dolt_diff_stat",
		SetUpScript: []string{
			"CREATE table t (pk int primary key);",
			"INSERT INTO t values (1);",
			"CALL DOLT_COMMIT('-Am', 'table with row');",
			"ALTER TABLE t ADD col1 int not null default 0;",
			"ALTER TABLE t drop primary key;",
			"ALTER TABLE t add primary key (pk, col1);",
			"CALL DOLT_COMMIT('-am', 'add secondary column with primary key');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "SELECT * from dolt_diff_stat('HEAD~', 'HEAD', 't');",
				ExpectedErrStr: "failed to compute diff stat for table t: primary key set changed",
			},
		},
	},
	{
		Name: "pk set change should report warning for 2 argument dolt_diff_stat",
		SetUpScript: []string{
			"CREATE table t (pk int primary key);",
			"INSERT INTO t values (1);",
			"CREATE table t2 (pk int primary key);",
			"INSERT INTO t2 values (2);",
			"CALL DOLT_COMMIT('-Am', 'multiple tables');",
			"ALTER TABLE t ADD col1 int not null default 0;",
			"ALTER TABLE t drop primary key;",
			"ALTER TABLE t add primary key (pk, col1);",
			"INSERT INTO t2 values (3), (4), (5);",
			"CALL DOLT_COMMIT('-am', 'add secondary column with primary key to t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * from dolt_diff_stat('HEAD~', 'HEAD')",
				Expected: []sql.Row{
					{"t", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					{"t2", 1, 3, 0, 0, 3, 0, 0, 1, 4, 1, 4},
				},
				ExpectedWarning:       dtables.PrimaryKeyChangeWarningCode,
				ExpectedWarningsCount: 1,
			},
		},
	},
}

var UnscopedDiffSystemTableScriptTests = []queries.ScriptTest{
	{
		Name: "working set changes",
		SetUpScript: []string{
			"create table regularTable (a int primary key, b int, c int);",
			"create table droppedTable (a int primary key, b int, c int);",
			"create table renamedEmptyTable (a int primary key, b int, c int);",
			"call dolt_add('.')",
			"insert into regularTable values (1, 2, 3), (2, 3, 4);",
			"insert into droppedTable values (1, 2, 3), (2, 3, 4);",
			"set @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'Creating tables x and y');",

			// changeSet: STAGED; data change: false; schema change: true
			"create table addedTable (a int primary key, b int, c int);",
			"call DOLT_ADD('addedTable');",
			// changeSet: STAGED; data change: true; schema change: true
			"drop table droppedTable;",
			"call DOLT_ADD('droppedTable');",
			// changeSet: WORKING; data change: false; schema change: true
			"rename table renamedEmptyTable to newRenamedEmptyTable",
			// changeSet: WORKING; data change: true; schema change: false
			"insert into regularTable values (3, 4, 5);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF;",
				Expected: []sql.Row{{7}},
			},
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF WHERE commit_hash = @Commit1;",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "SELECT * FROM DOLT_DIFF WHERE commit_hash = @Commit1 AND committer <> 'billy bob';",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT commit_hash, committer FROM DOLT_DIFF WHERE commit_hash <> @Commit1 AND committer = 'billy bob' AND commit_hash NOT IN ('WORKING','STAGED');",
				Expected: []sql.Row{},
			},
			{
				Query: "SELECT commit_hash, table_name FROM DOLT_DIFF WHERE commit_hash <> @Commit1 AND commit_hash NOT IN ('STAGED') ORDER BY table_name;",
				Expected: []sql.Row{
					{"WORKING", "newRenamedEmptyTable"},
					{"WORKING", "regularTable"},
				},
			},
			{
				Query: "SELECT commit_hash, table_name FROM DOLT_DIFF WHERE commit_hash <> @Commit1 OR committer <> 'billy bob' ORDER BY table_name;",
				Expected: []sql.Row{
					{"STAGED", "addedTable"},
					{"STAGED", "droppedTable"},
					{"WORKING", "newRenamedEmptyTable"},
					{"WORKING", "regularTable"},
				},
			},
			{
				Query: "SELECT * FROM DOLT_DIFF WHERE COMMIT_HASH in ('WORKING', 'STAGED') ORDER BY table_name;",
				Expected: []sql.Row{
					{"STAGED", "addedTable", nil, nil, nil, nil, false, true},
					{"STAGED", "droppedTable", nil, nil, nil, nil, true, true},
					{"WORKING", "newRenamedEmptyTable", nil, nil, nil, nil, false, true},
					{"WORKING", "regularTable", nil, nil, nil, nil, true, false},
				},
			},
		},
	},
	{
		Name: "basic case with three tables",
		SetUpScript: []string{
			"create table x (a int primary key, b int, c int);",
			"create table y (a int primary key, b int, c int);",
			"call dolt_add('.')",
			"insert into x values (1, 2, 3), (2, 3, 4);",
			"set @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'Creating tables x and y');",

			"create table z (a int primary key, b int, c int);",
			"call dolt_add('.')",
			"insert into z values (100, 101, 102);",
			"set @Commit2 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit2, '-am', 'Creating tables z');",

			"insert into y values (-1, -2, -3), (-2, -3, -4);",
			"insert into z values (101, 102, 103);",
			"set @Commit3 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit3, '-am', 'Inserting into tables y and z');",

			"alter table y add column d int;",
			"set @Commit4 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit4, '-am', 'Modify schema of table y');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF",
				Expected: []sql.Row{{6}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash = @Commit1",
				Expected: []sql.Row{{"x", true, true}, {"y", true, false}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit2)",
				Expected: []sql.Row{{"z", true, true}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit3)",
				Expected: []sql.Row{{"y", false, true}, {"z", false, true}},
			},
		},
	},
	{
		Name: "renamed table",
		SetUpScript: []string{
			"create table x (a int primary key, b int, c int)",
			"create table y (a int primary key, b int, c int)",
			"call dolt_add('.')",
			"insert into x values (1, 2, 3), (2, 3, 4)",
			"set @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'Creating tables x and y')",

			"create table z (a int primary key, b int, c int)",
			"call dolt_add('.')",
			"insert into z values (100, 101, 102)",
			"set @Commit2 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit2, '-am', 'Creating tables z')",

			"rename table x to x1",
			"call dolt_add('.')",
			"insert into x1 values (1000, 1001, 1002);",
			"set @Commit3 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit3, '-am', 'Renaming table x to x1 and inserting data')",

			"rename table x1 to x2",
			"call dolt_add('.')",
			"set @Commit4 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit4, '-am', 'Renaming table x1 to x2')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF",
				Expected: []sql.Row{{5}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit1)",
				Expected: []sql.Row{{"x", true, true}, {"y", true, false}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit2)",
				Expected: []sql.Row{{"z", true, true}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit3)",
				Expected: []sql.Row{{"x1", true, true}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit4)",
				Expected: []sql.Row{{"x2", true, false}},
			},
		},
	},
	{
		Name: "dropped table",
		SetUpScript: []string{
			"create table x (a int primary key, b int, c int)",
			"create table y (a int primary key, b int, c int)",
			"call dolt_add('.')",
			"insert into x values (1, 2, 3), (2, 3, 4)",
			"set @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'Creating tables x and y')",

			"drop table x",
			"set @Commit2 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit2, '-am', 'Dropping non-empty table x')",

			"drop table y",
			"set @Commit3 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit3, '-am', 'Dropping empty table y')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF",
				Expected: []sql.Row{{4}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit1)",
				Expected: []sql.Row{{"x", true, true}, {"y", true, false}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit2)",
				Expected: []sql.Row{{"x", true, true}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit3)",
				Expected: []sql.Row{{"y", true, false}},
			},
		},
	},
	{
		Name: "empty commit handling",
		SetUpScript: []string{
			"create table x (a int primary key, b int, c int)",
			"create table y (a int primary key, b int, c int)",
			"call dolt_add('.')",
			"insert into x values (1, 2, 3), (2, 3, 4)",
			"set @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'Creating tables x and y')",
			"set @Commit2 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit2, '--allow-empty', '-m', 'Empty!')",
			"insert into y values (-1, -2, -3), (-2, -3, -4)",
			"set @Commit3 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit3, '-am', 'Inserting into table y')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit1)",
				Expected: []sql.Row{{"x", true, true}, {"y", true, false}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit2)",
				Expected: []sql.Row{},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit3)",
				Expected: []sql.Row{{"y", false, true}},
			},
		},
	},
	{
		Name: "includes commits from all branches",
		SetUpScript: []string{
			"CALL DOLT_checkout('-b', 'branch1')",
			"create table x (a int primary key, b int, c int)",
			"create table y (a int primary key, b int, c int)",
			"call dolt_add('.')",
			"insert into x values (1, 2, 3), (2, 3, 4)",
			"set @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'Creating tables x and y')",

			"CALL DOLT_checkout('-b', 'branch2')",
			"create table z (a int primary key, b int, c int)",
			"call dolt_add('.')",
			"insert into z values (100, 101, 102)",
			"set @Commit2 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit2, '-am', 'Creating tables z')",

			"insert into y values (-1, -2, -3), (-2, -3, -4)",
			"insert into z values (101, 102, 103)",
			"set @Commit3 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit3, '-am', 'Inserting into tables y and z')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF",
				Expected: []sql.Row{{5}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit1)",
				Expected: []sql.Row{{"x", true, true}, {"y", true, false}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit2)",
				Expected: []sql.Row{{"z", true, true}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit3)",
				Expected: []sql.Row{{"y", false, true}, {"z", false, true}},
			},
		},
	},
	// The DOLT_DIFF system table doesn't currently show any diff data for a merge commit.
	// When processing a merge commit, diff.GetTableDeltas isn't aware of branch context, so it
	// doesn't detect that any tables have changed.
	{
		Name: "merge history handling",
		SetUpScript: []string{
			"CALL DOLT_checkout('-b', 'branch1')",
			"create table x (a int primary key, b int, c int)",
			"create table y (a int primary key, b int, c int)",
			"call dolt_add('.')",
			"insert into x values (1, 2, 3), (2, 3, 4)",
			"set @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'Creating tables x and y')",

			"CALL DOLT_checkout('-b', 'branch2')",
			"create table z (a int primary key, b int, c int)",
			"call dolt_add('.')",
			"insert into z values (100, 101, 102)",
			"set @Commit2 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit2, '-am', 'Creating tables z')",

			"CALL DOLT_MERGE('branch1', '--no-commit')",
			"set @Commit3 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit3, '-am', 'Merging branch1 into branch2')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit1)",
				Expected: []sql.Row{{"x", true, true}, {"y", true, false}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit2)",
				Expected: []sql.Row{{"z", true, true}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit3)",
				Expected: []sql.Row{},
			},
		},
	},
}

var CommitDiffSystemTableScriptTests = []queries.ScriptTest{
	{
		Name: "error handling",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 int);",
			"call dolt_add('.')",
			"insert into t values (1, 2, 3), (4, 5, 6);",
			"set @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'creating table t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "SELECT * FROM DOLT_COMMIT_DIFF_t;",
				ExpectedErrStr: "error querying table dolt_commit_diff_t: dolt_commit_diff_* tables must be filtered to a single 'to_commit'",
			},
			{
				Query:          "SELECT * FROM DOLT_COMMIT_DIFF_t where to_commit=@Commit1;",
				ExpectedErrStr: "error querying table dolt_commit_diff_t: dolt_commit_diff_* tables must be filtered to a single 'from_commit'",
			},
			{
				Query:          "SELECT * FROM DOLT_COMMIT_DIFF_t where from_commit=@Commit1;",
				ExpectedErrStr: "error querying table dolt_commit_diff_t: dolt_commit_diff_* tables must be filtered to a single 'to_commit'",
			},
		},
	},
	{
		Name: "base case: insert, update, delete",
		SetUpScript: []string{
			"set @Commit0 = HASHOF('HEAD');",
			"create table t (pk int primary key, c1 int, c2 int);",
			"call dolt_add('.')",
			"insert into t values (1, 2, 3), (4, 5, 6);",
			"set @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'creating table t');",

			"update t set c2=0 where pk=1",
			"set @Commit2 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit2, '-am', 'modifying row');",

			"update t set c2=-1 where pk=1",
			"set @Commit3 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit3, '-am', 'modifying row');",

			"update t set c2=-2 where pk=1",
			"set @Commit4 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit4, '-am', 'modifying row');",

			"delete from t where pk=1",
			"set @Commit5 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit5, '-am', 'modifying row');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit1 and FROM_COMMIT=@Commit0;",
				Expected: []sql.Row{
					{1, 2, 3, nil, nil, nil, "added"},
					{4, 5, 6, nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit2 and FROM_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, 2, 0, 1, 2, 3, "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_COMMIT_DIFF_T WHERE TO_COMMIT=@Commit4 and FROM_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, 2, -2, 1, 2, 3, "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_commit_DIFF_t WHERE TO_COMMIT=@Commit5 and FROM_COMMIT=@Commit4 ORDER BY to_pk;",
				Expected: []sql.Row{
					{nil, nil, nil, 1, 2, -2, "removed"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit5 and FROM_COMMIT=@Commit0 ORDER BY to_pk;",
				Expected: []sql.Row{
					{4, 5, 6, nil, nil, nil, "added"},
				},
			},
		},
	},
	{
		// When a column is dropped we should see the column's value set to null in that commit
		Name: "schema modification: column drop",
		SetUpScript: []string{
			"set @Commit0 = HASHOF('HEAD');",
			"create table t (pk int primary key, c1 int, c2 int);",
			"call dolt_add('.')",
			"insert into t values (1, 2, 3), (4, 5, 6);",
			"set @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'creating table t');",

			"alter table t drop column c1;",
			"set @Commit2 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit2, '-am', 'dropping column c');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT to_pk, to_c2, from_pk, from_c2 FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit1 and FROM_COMMIT=@Commit0 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, 3, nil, nil},
					{4, 6, nil, nil},
				},
			},
			{
				Query: "SELECT to_pk, to_c2, from_pk, from_c2 FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit2 and FROM_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, 3, 1, 3},
					{4, 6, 4, 6},
				},
			},
		},
	},
	{
		// When a column is dropped and recreated with the same type, we expect it to be included in dolt_diff output
		Name: "schema modification: column drop, recreate with same type",
		SetUpScript: []string{
			"set @Commit0 = HASHOF('HEAD');",
			"create table t (pk int primary key, c int);",
			"call dolt_add('.')",
			"insert into t values (1, 2), (3, 4);",
			"set @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'creating table t');",

			"alter table t drop column c;",
			"set @Commit2 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit2, '-am', 'dropping column c');",

			"alter table t add column c int;",
			"insert into t values (100, 101);",
			"set @Commit3 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit3, '-am', 'inserting into t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit1 and FROM_COMMIT=@Commit0 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, 2, nil, nil, "added"},
					{3, 4, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, from_pk, from_c, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit2 and FROM_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, 1, 2, "modified"},
					{3, 3, 4, "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit3 and FROM_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.Row{
					{100, 101, nil, nil, "added"},
				},
			},
		},
	},
	{
		// When a column is dropped and another column with the same type is renamed to that name, we expect it to be included in dolt_diff output
		Name: "schema modification: column drop, rename column with same type to same name",
		SetUpScript: []string{
			"set @Commit0 = HASHOF('HEAD');",
			"create table t (pk int primary key, c1 int, c2 int);",
			"call dolt_add('.')",
			"insert into t values (1, 2, 3), (4, 5, 6);",
			"set @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'creating table t');",

			"alter table t drop column c1;",
			"set @Commit2 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit2, '-am', 'dropping column c1');",

			"alter table t rename column c2 to c1;",
			"insert into t values (100, 101);",
			"set @Commit3 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit3, '-am', 'inserting into t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit1 and FROM_COMMIT=@Commit0 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, 2, nil, nil, "added"},
					{4, 5, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit2 and FROM_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, nil, 1, 2, "modified"},
					{4, nil, 4, 5, "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit3 and FROM_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.Row{
					// TODO: Missing rows here see TestDiffSystemTable tests
					{100, 101, nil, nil, "added"},
				},
			},
		},
	},

	{
		// When a column is dropped and recreated with a different type, we expect only the new column
		// to be included in dolt_commit_diff output, with previous values coerced (with any warnings reported) to the new type
		Name: "schema modification: column drop, recreate with different type that can be coerced (int -> string)",
		SetUpScript: []string{
			"set @Commit0 = HASHOF('HEAD');",
			"create table t (pk int primary key, c int);",
			"call dolt_add('.')",
			"insert into t values (1, 2), (3, 4);",
			"set @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'creating table t');",

			"alter table t drop column c;",
			"set @Commit2 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit2, '-am', 'dropping column c');",

			"alter table t add column c varchar(20);",
			"insert into t values (100, '101');",
			"set @Commit3 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit3, '-am', 're-adding column c');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit1 and FROM_COMMIT=@Commit0 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, "2", nil, nil, "added"},
					{3, "4", nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit2 and FROM_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, nil, 1, "2", "modified"},
					{3, nil, 3, "4", "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit3 and FROM_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.Row{
					{100, "101", nil, nil, "added"},
				},
			},
		},
	},
	{
		Name: "schema modification: column drop, recreate with different type that can't be coerced (string -> int)",
		SetUpScript: []string{
			"set @Commit0 = HASHOF('HEAD');",
			"create table t (pk int primary key, c varchar(20));",
			"call dolt_add('.')",
			"insert into t values (1, 'two'), (3, 'four');",
			"set @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'creating table t');",

			"alter table t drop column c;",
			"set @Commit2 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit2, '-am', 'dropping column c');",

			"alter table t add column c int;",
			"insert into t values (100, 101);",
			"set @Commit3 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit3, '-am', 're-adding column c');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit1 and FROM_COMMIT=@Commit0 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, nil, nil, nil, "added"},
					{3, nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit2 and FROM_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, nil, 1, nil, "modified"},
					{3, nil, 3, nil, "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit3 and FROM_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.Row{
					{100, 101, nil, nil, "added"},
				},
			},
			{
				Query:                           "select * from dolt_commit_diff_t where to_commit=@Commit3 and from_commit=@Commit1;",
				ExpectedWarning:                 1105,
				ExpectedWarningsCount:           2,
				ExpectedWarningMessageSubstring: "unable to coerce value from field",
				SkipResultsCheck:                true,
			},
		},
	},
	{
		Name: "schema modification: primary key change",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int);",
			"call dolt_add('.')",
			"insert into t values (1, 2), (3, 4);",
			"set @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'creating table t');",

			"alter table t drop primary key;",
			"insert into t values (5, 6);",
			"set @Commit2 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit2, '-am', 'dropping primary key');",

			"alter table t add primary key (c1);",
			"set @Commit3 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit3, '-am', 'adding primary key');",

			"insert into t values (7, 8);",
			"set @Commit4 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit4, '-am', 'adding more data');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:                           "select * from dolt_commit_diff_t where from_commit=@Commit1 and to_commit=@Commit4;",
				ExpectedWarning:                 1105,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "cannot render full diff between commits",
				SkipResultsCheck:                true,
			},
			{
				Query:    "SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM DOLT_commit_DIFF_t where from_commit=@Commit3 and to_commit=@Commit4;",
				Expected: []sql.Row{{7, 8, nil, nil, "added"}},
			},
		},
	},
}

type systabScript struct {
	name    string
	setup   []string
	queries []systabQuery
}

type systabQuery struct {
	query string
	exp   []sql.Row
	skip  bool
}

var systabSetup = []string{
	"create table xy (x int primary key, y varchar(20));",
	"insert into xy values (0, 'row 0'), (1, 'row 1'), (2, 'row 2'), (3, 'row 3'), (4, 'row 4');",
	"call dolt_add('.');",
	"call dolt_commit('-m', 'commit 0');",
	"update xy set y = y+1 where x < 10",
	"insert into xy values (20, 'row 20'), (21, 'row 21'), (22, 'row 22'), (23, 'row 23'), (24, 'row 24');",
	"call dolt_add('.');",
	"call dolt_commit('-m', 'commit 1');",
	"update xy set y = y+1 where x > 10 and x < 30",
	"insert into xy values (40, 'row 40'), (41, 'row 41'), (42, 'row 42'), (43, 'row 43'), (44, 'row 44');",
	"call dolt_add('.');",
	"call dolt_commit('-m', 'commit 2');",
	"update xy set y = y+1 where x > 30 and x < 50",
	"insert into xy values (60, 'row 60'), (61, 'row 61'), (62, 'row 62'), (63, 'row 63'), (64, 'row 64');",
	"call dolt_add('.');",
	"call dolt_commit('-m', 'commit 3');",
	"update xy set y = y+1 where x > 50 and x < 70",
	"insert into xy values (80, 'row 80'), (81, 'row 81'), (82, 'row 82'), (83, 'row 83'), (84, 'row 84');",
	"call dolt_add('.');",
	"call dolt_commit('-m', 'commit 4');",
}

var SystemTableIndexTests = []systabScript{
	{
		name: "systab benchmarks",
		setup: append(systabSetup,
			"set @commit = (select commit_hash from dolt_log where message = 'commit 2');",
		),
		queries: []systabQuery{
			{
				query: "select from_x, to_x from dolt_diff_xy where to_commit = @commit;",
				exp:   []sql.Row{{20, 20}, {21, 21}, {22, 22}, {23, 23}, {24, 24}, {nil, 40}, {nil, 41}, {nil, 42}, {nil, 43}, {nil, 44}},
			},
			{
				query: "select from_x, to_x from dolt_diff_xy where from_commit = @commit;",
				exp:   []sql.Row{{40, 40}, {41, 41}, {42, 42}, {43, 43}, {44, 44}, {nil, 60}, {nil, 61}, {nil, 62}, {nil, 63}, {nil, 64}},
			},
			{
				query: "select count(*) from dolt_diff where commit_hash = @commit;",
				exp:   []sql.Row{{1}},
			},
			{
				query: "select count(*) from dolt_history_xy where commit_hash = @commit;",
				exp:   []sql.Row{{15}},
			},
			{
				query: "select count(*) from dolt_log where commit_hash = @commit;",
				exp:   []sql.Row{{1}},
			},
			{
				query: "select count(*) from dolt_commits where commit_hash = @commit;",
				exp:   []sql.Row{{1}},
			},
			{
				query: "select count(*) from dolt_commit_ancestors where commit_hash = @commit;",
				exp:   []sql.Row{{1}},
			},
			{
				query: "select count(*) from dolt_diff_xy join dolt_log on commit_hash = to_commit",
				exp:   []sql.Row{{45}},
			},
			{
				query: "select count(*) from dolt_diff_xy join dolt_log on commit_hash = from_commit",
				exp:   []sql.Row{{45}},
			},
			{
				query: "select count(*) from dolt_blame_xy",
				exp:   []sql.Row{{25}},
			},
			{
				query: `SELECT count(*)
           FROM dolt_commits as cm
           JOIN dolt_commit_ancestors as an
           ON cm.commit_hash = an.parent_hash
           ORDER BY cm.date, cm.message asc`,
				exp: []sql.Row{{5}},
			},
		},
	},
	{
		name: "commit indexing edge cases",
		setup: append(systabSetup,
			"call dolt_checkout('-b', 'feat');",
			"call dolt_commit('--allow-empty', '-m', 'feat commit 1');",
			"call dolt_commit('--allow-empty', '-m', 'feat commit 2');",
			"call dolt_checkout('main');",
			"update xy set y = y+1 where x > 70 and x < 90;",
			"set @commit = (select commit_hash from dolt_log where message = 'commit 1');",
			"set @root_commit = (select commit_hash from dolt_log where message = 'Initialize data repository');",
			"set @feat_head = hashof('feat');",
			"set @feat_head1 = hashof('feat~');",
		),
		queries: []systabQuery{
			{
				query: "select from_x, to_x from dolt_diff_xy where to_commit = 'WORKING';",
				exp:   []sql.Row{{80, 80}, {81, 81}, {82, 82}, {83, 83}, {84, 84}},
			},
			{
				query: "select * from dolt_diff_xy where from_commit = @feat_head1;",
				exp:   []sql.Row{},
			},
			{
				query: "select * from dolt_diff_xy where from_commit = 'WORKING';",
				exp:   []sql.Row{},
			},
			{
				query: "select count(*) from dolt_diff where commit_hash = 'WORKING';",
				exp:   []sql.Row{{1}},
			},
			{
				query: "select count(*) from dolt_history_xy where commit_hash = 'WORKING';",
				exp:   []sql.Row{{0}},
			},
			{
				query: "select count(*) from dolt_commit_ancestors where commit_hash = 'WORKING';",
				exp:   []sql.Row{{0}},
			},
			{
				query: "select sum(to_x) from dolt_diff_xy where to_commit in (@commit, 'WORKING');",
				exp:   []sql.Row{{530.0}},
			},
			{
				// TODO from_commit optimization
				query: "select sum(to_x) from dolt_diff_xy where from_commit in (@commit, 'WORKING');",
				exp:   []sql.Row{{320.0}},
			},
			{
				query: "select count(*) from dolt_diff where commit_hash in (@commit, 'WORKING');",
				exp:   []sql.Row{{2}},
			},
			{
				query: "select sum(x) from dolt_history_xy where commit_hash in (@commit, 'WORKING');",
				exp:   []sql.Row{{120.0}},
			},
			{
				// init commit has nil ancestor
				query: "select count(*) from dolt_commit_ancestors where commit_hash in (@commit, @root_commit);",
				exp:   []sql.Row{{2}},
			},
			{
				query: "select count(*) from dolt_log where commit_hash in (@commit, @root_commit);",
				exp:   []sql.Row{{2}},
			},
			{
				// log table cannot access commits is feature branch
				query: "select count(*) from dolt_log where commit_hash = @feat_head;",
				exp:   []sql.Row{{0}},
			},
			{
				// commit table can access all commits
				query: "select count(*) from dolt_commits where commit_hash = @feat_head;",
				exp:   []sql.Row{{1}},
			},
			{
				query: "select count(*) from dolt_commits where commit_hash in (@commit, @root_commit);",
				exp:   []sql.Row{{2}},
			},
			// unknown
			{
				query: "select from_x, to_x from dolt_diff_xy where to_commit = 'unknown';",
				exp:   []sql.Row{},
			},
			{
				query: "select * from dolt_diff_xy where from_commit = 'unknown';",
				exp:   []sql.Row{},
			},
			{
				query: "select * from dolt_diff where commit_hash = 'unknown';",
				exp:   []sql.Row{},
			},
			{
				query: "select * from dolt_history_xy where commit_hash = 'unknown';",
				exp:   []sql.Row{},
			},
			{
				query: "select * from dolt_commit_ancestors where commit_hash = 'unknown';",
				exp:   []sql.Row{},
			},
		},
	},
	{
		name: "empty log table",
		setup: []string{
			"create table xy (x int primary key, y int)",
		},
		queries: []systabQuery{
			{
				query: "select count(*) from dolt_log as dc join dolt_commit_ancestors as dca on dc.commit_hash = dca.commit_hash;",
				exp:   []sql.Row{{1}},
			},
		},
	},
	{
		name: "from_commit at multiple heights, choose highest",
		setup: []string{
			"create table x (x int primary key)",
			"call dolt_add('.');",
			"call dolt_commit_hash_out(@m1h1, '-am', 'main 1');",
			"insert into x values (1),(2);",
			"call dolt_commit_hash_out(@m2h2, '-am', 'main 2');",
			"call dolt_checkout('-b', 'other');",
			"call dolt_reset('--hard', @main1);",
			"insert into x values (3),(4);",
			"call dolt_commit_hash_out(@o1h2, '-am', 'other 1');",
			"insert into x values (5),(6);",
			"call dolt_commit_hash_out(@o2h3, '-am', 'other 2');",
			"call dolt_merge('main');",
			"set @o3h4 = hashof('head~');",
			"call dolt_checkout('main');",
			"insert into x values (7),(8);",
			"call dolt_commit_hash_out(@m32h3, '-am', 'main 3');",
			"call dolt_merge('other');",
			"set @m4h5 = hashof('head~');",
		},
		queries: []systabQuery{
			{
				query: "select count(*) from dolt_diff_x where from_commit = @m2h2",
				exp:   []sql.Row{{4}},
			},
		},
	},
}
