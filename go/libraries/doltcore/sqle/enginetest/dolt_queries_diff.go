// Copyright 2022-2024 Dolthub, Inc.
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
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtablefunctions"
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
				Expected: []sql.UntypedSqlRow{{2}},
			},
			{
				// Test case-insensitive table name
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_T;",
				Expected: []sql.UntypedSqlRow{{2}},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit1 ORDER BY to_pk, to_c2, to_c2, from_pk, from_c1, from_c2, diff_type;",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{{3}},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit2 ORDER BY to_pk, to_c2, to_c2, from_pk, from_c1, from_c2, diff_type;",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{{3}},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{{2}},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit3 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{{4}},
			},
			{
				Query: "SELECT to_pk, to_c2, from_pk, from_c2 FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
					{1, 3, nil, nil},
					{4, 6, nil, nil},
				},
			},
			{
				Query: "SELECT to_pk, to_c2, from_pk, from_c2 FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{{5}},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
					{1, 2, nil, nil, "added"},
					{3, 4, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
					{1, nil, 1, 2, "modified"},
					{3, nil, 3, 4, "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit3 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{{5}},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
					{1, 2, nil, nil, "added"},
					{4, 5, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
					{1, nil, 1, 2, "modified"},
					{4, nil, 4, 5, "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit3 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{{5}},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
					{1, "2", nil, nil, "added"},
					{3, "4", nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
					{1, nil, 1, "2", "modified"},
					{3, nil, 3, "4", "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit3 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{{5}},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
					{1, nil, nil, nil, "added"},
					{3, nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
					{1, nil, 1, nil, "modified"},
					{3, nil, 3, nil, "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit3 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
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
		// https://github.com/dolthub/dolt/issues/6391
		Name: "columns modified to narrower types",
		SetUpScript: []string{
			"create table t (pk int primary key, col1 varchar(20), col2 int);",
			"call dolt_commit('-Am', 'new table t');",
			"insert into t values (1, '123456789012345', 420);",
			"call dolt_commit('-am', 'inserting data');",
			"update t set col1='1234567890', col2=13;",
			"alter table t modify column col1 varchar(10);",
			"alter table t modify column col2 tinyint;",
			"call dolt_commit('-am', 'narrowing types');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select to_pk, to_col1, to_col2, to_commit, from_pk, from_col1, from_col2, from_commit, diff_type from dolt_diff_t order by diff_type ASC;",
				Expected: []sql.UntypedSqlRow{
					{1, nil, nil, doltCommit, nil, nil, nil, doltCommit, "added"},
					{1, "1234567890", 13, doltCommit, 1, nil, nil, doltCommit, "modified"},
				},
				ExpectedWarningsCount: 4,
			},
			{
				Query: "SHOW WARNINGS;",
				Expected: []sql.UntypedSqlRow{
					{"Warning", 1292, "Truncated tinyint value: 420"},
					{"Warning", 1292, "Truncated tinyint value: 420"},
					{"Warning", 1292, "Truncated varchar(10) value: 123456789012345"},
					{"Warning", 1292, "Truncated varchar(10) value: 123456789012345"},
				},
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
				Expected: []sql.UntypedSqlRow{{5}},
			},
			{
				Query: "SELECT to_pk, to_c2, from_pk, from_c2, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
					{1, nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c2, from_pk, from_c2, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
					{3, 4, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c2, from_pk, from_c2, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit3 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
					{1, nil, 1, 2, "modified"},
					{3, nil, 3, 4, "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c2, from_pk, from_c2, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit4 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
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
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t;",
				Expected: []sql.UntypedSqlRow{{1}},
			},
			{
				Query:    "SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM DOLT_DIFF_t where to_commit=@Commit4;",
				Expected: []sql.UntypedSqlRow{{7, 8, nil, nil, "added"}},
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
				Expected: []sql.UntypedSqlRow{{1, 32, nil, 32, "added"}},
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
				Expected: []sql.UntypedSqlRow{{4}},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_DIFF_t WHERE to_pk = 1 ORDER BY to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type;",
				Expected: []sql.UntypedSqlRow{
					{1, 2, 3, nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_DIFF_t WHERE to_pk > 1 ORDER BY to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type;",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{{4}},
			},
			{
				Query: "SELECT to_pk1, to_pk2, to_c1, from_pk1, from_pk2, from_c1, diff_type FROM DOLT_DIFF_t WHERE to_pk1 = 1 ORDER BY to_pk1, to_pk2, to_c1, from_pk1, from_pk2, from_c1, diff_type;",
				Expected: []sql.UntypedSqlRow{
					{1, 2, 3, nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk1, to_pk2, to_c1, from_pk1, from_pk2, from_c1, diff_type FROM DOLT_DIFF_t WHERE to_pk1 = 1 and to_pk2 = 2 ORDER BY to_pk1, to_pk2, to_c1, from_pk1, from_pk2, from_c1, diff_type;",
				Expected: []sql.UntypedSqlRow{
					{1, 2, 3, nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk1, to_pk2, to_c1, from_pk1, from_pk2, from_c1, diff_type FROM DOLT_DIFF_t WHERE to_pk1 > 1 and to_pk2 < 10 ORDER BY to_pk1, to_pk2, to_c1, from_pk1, from_pk2, from_c1, diff_type;",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{{nil, 1, "added"}},
			},
			{
				Query:    "SELECT from_pk2a, from_pk2b, to_pk2a, to_pk2b, diff_type from dolt_diff_t2;",
				Expected: []sql.UntypedSqlRow{{nil, nil, 2, 2, "added"}},
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
				Expected: []sql.UntypedSqlRow{{1, nil, nil, nil, "added"}},
			},
		},
	},
	{
		Name: "duplicate commit_hash",
		SetUpScript: []string{
			"create table t1 (x int primary key)",
			"create table t2 (x int primary key)",
			"call dolt_add('.');",
			"call dolt_commit_hash_out(@commit1, '-Am', 'commit1');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select table_name from dolt_diff where commit_hash = @commit1",
				Expected: []sql.UntypedSqlRow{
					{"t1"},
					{"t2"},
				},
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
				Expected: []sql.UntypedSqlRow{{"2", "2", nil, nil, "added"}},
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
				ExpectedErr: dtablefunctions.ErrInvalidNonLiteralArgument,
			},
			{
				Query:       "SELECT * from dolt_diff(hashof('main'), @Commit2, 't');",
				ExpectedErr: dtablefunctions.ErrInvalidNonLiteralArgument,
			},
			{
				Query:       "SELECT * from dolt_diff(hashof('main'), @Commit2, LOWER('T'));",
				ExpectedErr: dtablefunctions.ErrInvalidNonLiteralArgument,
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
				ExpectedErr: dtablefunctions.ErrInvalidNonLiteralArgument,
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
				Expected: []sql.UntypedSqlRow{{1, "one", "two", nil, nil, nil, "added"}},
			},
			{
				Query:    "SELECT COUNT(*) from dolt_diff(@Commit2, @Commit3, 't');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit3, @Commit4, 't');",
				Expected: []sql.UntypedSqlRow{
					{1, "uno", "dos", 1, "one", "two", "modified"},
					{2, "two", "three", nil, nil, nil, "added"},
					{3, "three", "four", nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit4, @Commit3, 't');",
				Expected: []sql.UntypedSqlRow{
					{1, "one", "two", 1, "uno", "dos", "modified"},
					{nil, nil, nil, 2, "two", "three", "removed"},
					{nil, nil, nil, 3, "three", "four", "removed"},
				},
			},
			{
				// Table t2 had no changes between Commit3 and Commit4, so results should be empty
				Query:    "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type  from dolt_diff(@Commit3, @Commit4, 'T2');",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type  from dolt_diff(@Commit1, @Commit4, 't');",
				Expected: []sql.UntypedSqlRow{
					{1, "uno", "dos", nil, nil, nil, "added"},
					{2, "two", "three", nil, nil, nil, "added"},
					{3, "three", "four", nil, nil, nil, "added"},
				},
			},
			{
				// Reverse the to/from commits to see the diff from the other direction
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type  from dolt_diff(@Commit4, @Commit1, 'T');",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{{1, "one", "two", nil, nil, nil, "added"}},
			},
			{
				Query: `
SELECT to_pk, from_c1, to_c1, from_c1, to_c1, diff_type, diff_type
from dolt_diff(@Commit1, @Commit2, 't') inner join dolt_diff(@Commit1, @Commit3, 't');`,
				ExpectedErr: sql.ErrAmbiguousColumnName,
			},
			{
				Query: `
SELECT a.to_pk, a.from_c1, a.to_c1, b.from_c1, b.to_c1, a.diff_type, b.diff_type
from dolt_diff(@Commit1, @Commit2, 't') a inner join dolt_diff(@Commit1, @Commit3, 't') b
on a.to_pk = b.to_pk;`,
				Expected: []sql.UntypedSqlRow{
					{1, nil, "one", nil, "one", "added", "added"},
				},
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
				Expected: []sql.UntypedSqlRow{
					{1, "one", "two", 1, "one", "100", "modified"},
					{2, "three", "four", nil, nil, nil, "removed"},
					{nil, nil, nil, 3, "five", "six", "added"},
				},
			},
			{
				Query: "SELECT from_pk, from_c1, from_c2, to_pk, to_c1, to_c2, diff_type from dolt_diff('STAGED', 'WORKING', 't') order by coalesce(from_pk, to_pk);",
				Expected: []sql.UntypedSqlRow{
					{1, "one", "two", 1, "one", "100", "modified"},
					{2, "three", "four", nil, nil, nil, "removed"},
					{nil, nil, nil, 3, "five", "six", "added"},
				},
			},
			{
				Query: "SELECT from_pk, from_c1, from_c2, to_pk, to_c1, to_c2, diff_type from dolt_diff('STAGED..WORKING', 't') order by coalesce(from_pk, to_pk);",
				Expected: []sql.UntypedSqlRow{
					{1, "one", "two", 1, "one", "100", "modified"},
					{2, "three", "four", nil, nil, nil, "removed"},
					{nil, nil, nil, 3, "five", "six", "added"},
				},
			},
			{
				Query: "SELECT from_pk, from_c1, from_c2, to_pk, to_c1, to_c2, diff_type from dolt_diff('WORKING', 'STAGED', 't') order by coalesce(from_pk, to_pk);",
				Expected: []sql.UntypedSqlRow{
					{1, "one", "100", 1, "one", "two", "modified"},
					{nil, nil, nil, 2, "three", "four", "added"},
					{3, "five", "six", nil, nil, nil, "removed"},
				},
			},
			{
				Query:    "SELECT from_pk, from_c1, from_c2, to_pk, to_c1, to_c2, diff_type from dolt_diff('WORKING', 'WORKING', 't') order by coalesce(from_pk, to_pk);",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "SELECT from_pk, from_c1, from_c2, to_pk, to_c1, to_c2, diff_type from dolt_diff('WORKING..WORKING', 't') order by coalesce(from_pk, to_pk);",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "SELECT from_pk, from_c1, from_c2, to_pk, to_c1, to_c2, diff_type from dolt_diff('STAGED', 'STAGED', 't') order by coalesce(from_pk, to_pk);",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:            "call dolt_add('.')",
				SkipResultsCheck: true,
			},
			{
				Query:    "SELECT from_pk, from_c1, from_c2, to_pk, to_c1, to_c2, diff_type from dolt_diff('WORKING', 'STAGED', 't') order by coalesce(from_pk, to_pk);",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "SELECT from_pk, from_c1, from_c2, to_pk, to_c1, to_c2, diff_type from dolt_diff('HEAD', 'STAGED', 't') order by coalesce(from_pk, to_pk);",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{
					{nil, nil, 1, "one", "two", "removed"},
					{2, "two", 2, "two", "three", "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff('main..branch1', 't');",
				Expected: []sql.UntypedSqlRow{
					{nil, nil, 1, "one", "two", "removed"},
					{2, "two", 2, "two", "three", "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, diff_type from dolt_diff('branch1', 'main', 't');",
				Expected: []sql.UntypedSqlRow{
					{1, "one", "two", nil, nil, "added"},
					{2, "two", "three", 2, "two", "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, diff_type from dolt_diff('branch1..main', 't');",
				Expected: []sql.UntypedSqlRow{
					{1, "one", "two", nil, nil, "added"},
					{2, "two", "three", 2, "two", "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff('main~', 'branch1', 't');",
				Expected: []sql.UntypedSqlRow{
					{nil, nil, 1, "one", "two", "removed"},
					{2, "two", nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff('main~..branch1', 't');",
				Expected: []sql.UntypedSqlRow{
					{nil, nil, 1, "one", "two", "removed"},
					{2, "two", nil, nil, nil, "added"},
				},
			},

			// Three dot
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff('main...branch1', 't');",
				Expected: []sql.UntypedSqlRow{
					{nil, nil, 1, "one", "two", "removed"},
					{2, "two", nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, diff_type from dolt_diff('branch1...main', 't');",
				Expected: []sql.UntypedSqlRow{
					{2, "two", "three", nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff('main~...branch1', 't');",
				Expected: []sql.UntypedSqlRow{
					{nil, nil, 1, "one", "two", "removed"},
					{2, "two", nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff('main...branch1~', 't');",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{
					{1, "one", "two", nil, nil, nil, "added"},
					{2, "two", "three", nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit2, @Commit3, 't');",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c3, from_pk, from_c1, from_c3, diff_type from dolt_diff(@Commit3, @Commit4, 't');",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{
					{4, "four", -4, nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit1, @Commit5, 't');",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{
					{1, "one", "asdf", nil, nil, nil, "added"},
					{2, "two", "2", nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit2, @Commit3, 't');",
				Expected: []sql.UntypedSqlRow{
					{1, "one", 1, "one", "asdf", "modified"},
					{2, "two", 2, "two", "2", "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, diff_type from dolt_diff(@Commit3, @Commit4, 't');",
				Expected: []sql.UntypedSqlRow{
					{3, "three", nil, nil, "added"},
					{1, "fdsa", 1, "one", "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, diff_type from dolt_diff(@Commit4, @Commit5, 't');",
				Expected: []sql.UntypedSqlRow{
					{4, "four", -4, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit1, @Commit5, 't');",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{{1, 2, "HEAD", "WORKING", "added"}},
			},
			{
				Query:    "select to_a, from_b, from_commit, to_commit, diff_type from dolt_diff('HEAD', 'WORKING', 't1')",
				Expected: []sql.UntypedSqlRow{{1, nil, "HEAD", "WORKING", "added"}},
			},
			{
				Query:    "select from_a, from_b, to_a, from_commit, to_commit, diff_type from dolt_diff('WORKING', 'HEAD', 't1')",
				Expected: []sql.UntypedSqlRow{{1, 2, nil, "WORKING", "HEAD", "removed"}},
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
				Expected: []sql.UntypedSqlRow{{1, 2, "HEAD~", "HEAD", "removed"}},
			},
			{
				Query:    "select from_a, from_b, from_commit, to_commit, diff_type from dolt_diff('HEAD~..HEAD', 't1')",
				Expected: []sql.UntypedSqlRow{{1, 2, "HEAD~", "HEAD", "removed"}},
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
				Expected: []sql.UntypedSqlRow{{3, 4, "HEAD~", "HEAD", "added"}},
			},
			{
				Query:    "select to_a, to_b, from_commit, to_commit, diff_type from dolt_diff('HEAD~..HEAD', 't2')",
				Expected: []sql.UntypedSqlRow{{3, 4, "HEAD~", "HEAD", "added"}},
			},
			{
				// Maybe confusing? We match the old table name as well
				Query:    "select to_a, to_b, from_commit, to_commit, diff_type from dolt_diff('HEAD~', 'HEAD', 't1')",
				Expected: []sql.UntypedSqlRow{{3, 4, "HEAD~", "HEAD", "added"}},
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
				Expected: []sql.UntypedSqlRow{{1, 100, 1, 1, "modified"}},
			},
			{
				Query:    "select to_pk2, to_col1, from_pk, from_col1, diff_type from dolt_diff('HEAD~..HEAD', 't1')",
				Expected: []sql.UntypedSqlRow{{1, 100, 1, 1, "modified"}},
			},
			{
				Query:    "select to_pk2a, to_pk2b, to_col1, from_pk1a, from_pk1b, from_col1, diff_type from dolt_diff('HEAD~', 'HEAD', 't2');",
				Expected: []sql.UntypedSqlRow{{1, 1, 100, 1, 1, 1, "modified"}},
			},
			{
				Query:    "select to_pk2a, to_pk2b, to_col1, from_pk1a, from_pk1b, from_col1, diff_type from dolt_diff('HEAD~..HEAD', 't2');",
				Expected: []sql.UntypedSqlRow{{1, 1, 100, 1, 1, 1, "modified"}},
			},
		},
	},
	{
		Name: "diff on dolt_schemas on events",
		SetUpScript: []string{
			"CREATE TABLE messages (id INT PRIMARY KEY AUTO_INCREMENT, message VARCHAR(255) NOT NULL, created_at DATETIME NOT NULL);",
			"CREATE EVENT IF NOT EXISTS msg_event ON SCHEDULE AT CURRENT_TIMESTAMP + INTERVAL 1 YEAR DISABLE DO INSERT INTO messages(message,created_at) VALUES('Test Dolt Event 1',NOW());",
			"CREATE EVENT my_commit ON SCHEDULE EVERY 1 DAY DISABLE DO CALL DOLT_COMMIT('--allow-empty','-am','my daily commit');",
			"CALL DOLT_ADD('.')",
			"SET @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'Creating table and events')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT type, name FROM dolt_schemas;",
				Expected: []sql.UntypedSqlRow{
					{"event", "msg_event"},
					{"event", "my_commit"},
				},
			},
			{
				Query:       "CREATE EVENT msg_event ON SCHEDULE AT CURRENT_TIMESTAMP + INTERVAL 1 YEAR DISABLE DO INSERT INTO messages(message,created_at) VALUES('Test Dolt Event 2',NOW());",
				ExpectedErr: sql.ErrEventAlreadyExists,
			},
			{
				Query:            "DROP EVENT msg_event;",
				SkipResultsCheck: true,
			},
			{
				Query:            "CREATE EVENT msg_event ON SCHEDULE AT CURRENT_TIMESTAMP + INTERVAL 1 YEAR ON COMPLETION PRESERVE DISABLE DO INSERT INTO messages(message,created_at) VALUES('Test Dolt Event 2',NOW());",
				SkipResultsCheck: true,
			},
			{
				Query:    "SELECT from_type, from_name, to_name, diff_type FROM DOLT_DIFF('HEAD', 'WORKING', 'dolt_schemas')",
				Expected: []sql.UntypedSqlRow{{"event", "msg_event", "msg_event", "modified"}},
			},
			{
				Query: "SELECT type, name FROM dolt_schemas;",
				Expected: []sql.UntypedSqlRow{
					{"event", "msg_event"},
					{"event", "my_commit"},
				},
			},
		},
	},
	{
		Name: "diff table function works with views",
		SetUpScript: []string{
			"create table t (i int primary key);",
			"call dolt_commit('-Am', 'created table')",
			"insert into t values (1), (2), (3);",
			"call dolt_commit('-Am', 'inserted into table')",
			"create view v as select to_i, to_commit, from_i, from_commit, diff_type from dolt_diff('HEAD', 'HEAD~1', 't');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select * from v;",
				Expected: []sql.UntypedSqlRow{
					{nil, "HEAD~1", 1, "HEAD", "removed"},
					{nil, "HEAD~1", 2, "HEAD", "removed"},
					{nil, "HEAD~1", 3, "HEAD", "removed"},
				},
			},
			{
				Query: "insert into t values (4), (5), (6);",
				Expected: []sql.UntypedSqlRow{
					{gmstypes.NewOkResult(3)},
				},
			},
			{
				Query:            "call dolt_commit('-Am', 'inserted into table again');",
				SkipResultsCheck: true,
			},
			{
				Query: "select * from v;",
				Expected: []sql.UntypedSqlRow{
					{nil, "HEAD~1", 4, "HEAD", "removed"},
					{nil, "HEAD~1", 5, "HEAD", "removed"},
					{nil, "HEAD~1", 6, "HEAD", "removed"},
				},
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
				ExpectedErr: dtablefunctions.ErrInvalidNonLiteralArgument,
			},
			{
				Query:       "SELECT * from dolt_diff_stat(hashof('main'), @Commit2, 't');",
				ExpectedErr: dtablefunctions.ErrInvalidNonLiteralArgument,
			},
			{
				Query:       "SELECT * from dolt_diff_stat(@Commit1, @Commit2, LOWER('T'));",
				ExpectedErr: dtablefunctions.ErrInvalidNonLiteralArgument,
			},
			{
				Query:       "SELECT * from dolt_diff_stat('main..main~', LOWER('T'));",
				ExpectedErr: dtablefunctions.ErrInvalidNonLiteralArgument,
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
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit2, @Commit3, 't');",
				Expected: []sql.UntypedSqlRow{{"t", 0, 1, 0, 0, 3, 0, 0, 0, 1, 0, 3}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit3, @Commit4, 't');",
				Expected: []sql.UntypedSqlRow{{"t", 0, 2, 0, 1, 6, 0, 2, 1, 3, 3, 9}},
			},
			{
				// change from and to commits
				Query:    "SELECT * from dolt_diff_stat(@Commit4, @Commit3, 't');",
				Expected: []sql.UntypedSqlRow{{"t", 0, 0, 2, 1, 0, 6, 2, 3, 1, 9, 3}},
			},
			{
				// table is dropped
				Query:    "SELECT * from dolt_diff_stat(@Commit4, @Commit5, 't');",
				Expected: []sql.UntypedSqlRow{{"t", 0, 0, 3, 0, 0, 9, 0, 3, 0, 9, 0}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit1, @Commit4, 't');",
				Expected: []sql.UntypedSqlRow{{"t", 0, 3, 0, 0, 9, 0, 0, 0, 3, 0, 9}},
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
				Expected: []sql.UntypedSqlRow{},
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
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit2, @Commit3, 't');",
				Expected: []sql.UntypedSqlRow{{"t", nil, 1, 0, nil, nil, nil, nil, nil, nil, nil, nil}},
			},
			{
				// TODO : (correct result is commented out)
				//      update row for keyless table deletes the row and insert the new row
				// 		this causes row added = 3 and row deleted = 1
				Query: "SELECT * from dolt_diff_stat(@Commit3, @Commit4, 't');",
				//Expected:         []sql.UntypedSqlRow{{"t", nil, 2, 0, nil, nil, nil, nil, nil, nil, nil, nil}},
				Expected: []sql.UntypedSqlRow{{"t", nil, 3, 1, nil, nil, nil, nil, nil, nil, nil, nil}},
			},
			{
				Query: "SELECT * from dolt_diff_stat(@Commit4, @Commit3, 't');",
				//Expected:         []sql.UntypedSqlRow{{"t", nil, 0, 2, nil, nil, nil, nil, nil, nil, nil, nil}},
				Expected: []sql.UntypedSqlRow{{"t", nil, 1, 3, nil, nil, nil, nil, nil, nil, nil, nil}},
			},
			{
				// table is dropped
				Query:    "SELECT * from dolt_diff_stat(@Commit4, @Commit5, 't');",
				Expected: []sql.UntypedSqlRow{{"t", nil, 0, 3, nil, nil, nil, nil, nil, nil, nil, nil}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit1, @Commit4, 't');",
				Expected: []sql.UntypedSqlRow{{"t", nil, 3, 0, nil, nil, nil, nil, nil, nil, nil, nil}},
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
				Expected: []sql.UntypedSqlRow{{"t", 0, 1, 0, 0, 3, 0, 0, 0, 1, 0, 3}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit1, @Commit2);",
				Expected: []sql.UntypedSqlRow{{"t2", 0, 1, 0, 0, 3, 0, 0, 0, 1, 0, 3}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit2, @Commit3);",
				Expected: []sql.UntypedSqlRow{{"t", 0, 3, 0, 1, 9, 0, 2, 1, 4, 3, 12}, {"t2", 1, 1, 0, 0, 3, 0, 0, 1, 2, 3, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit3, @Commit4);",
				Expected: []sql.UntypedSqlRow{{"t", 3, 0, 1, 0, 0, 3, 0, 4, 3, 12, 9}, {"t2", 1, 0, 0, 1, 0, 0, 1, 2, 2, 6, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit4, @Commit2);",
				Expected: []sql.UntypedSqlRow{{"t", 0, 0, 2, 1, 0, 6, 2, 3, 1, 9, 3}, {"t2", 0, 0, 1, 1, 0, 3, 1, 2, 1, 6, 3}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit3, 'WORKING');",
				Expected: []sql.UntypedSqlRow{{"t", 3, 0, 1, 0, 0, 3, 0, 4, 3, 12, 9}, {"t2", 1, 0, 0, 1, 0, 0, 1, 2, 2, 6, 6}},
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
				Expected: []sql.UntypedSqlRow{{"t", 0, 1, 1, 1, 3, 3, 1, 2, 2, 6, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('STAGED', 'WORKING', 't')",
				Expected: []sql.UntypedSqlRow{{"t", 0, 1, 1, 1, 3, 3, 1, 2, 2, 6, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('STAGED..WORKING', 't')",
				Expected: []sql.UntypedSqlRow{{"t", 0, 1, 1, 1, 3, 3, 1, 2, 2, 6, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('WORKING', 'STAGED', 't')",
				Expected: []sql.UntypedSqlRow{{"t", 0, 1, 1, 1, 3, 3, 1, 2, 2, 6, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('WORKING', 'WORKING', 't')",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('WORKING..WORKING', 't')",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('STAGED', 'STAGED', 't')",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:            "call dolt_add('.')",
				SkipResultsCheck: true,
			},
			{
				Query:    "SELECT * from dolt_diff_stat('WORKING', 'STAGED', 't')",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('HEAD', 'STAGED', 't')",
				Expected: []sql.UntypedSqlRow{{"t", 0, 1, 1, 1, 3, 3, 1, 2, 2, 6, 6}},
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
				Expected: []sql.UntypedSqlRow{{"t", 0, 0, 1, 1, 0, 4, 0, 2, 1, 6, 2}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('main..branch1', 't');",
				Expected: []sql.UntypedSqlRow{{"t", 0, 0, 1, 1, 0, 4, 0, 2, 1, 6, 2}},
			},
			{
				Query: "SELECT * from dolt_diff_stat('main', 'branch1');",
				Expected: []sql.UntypedSqlRow{
					{"t", 0, 0, 1, 1, 0, 4, 0, 2, 1, 6, 2},
					{"newtable", 0, 0, 2, 0, 0, 2, 0, 2, 0, 2, 0},
				},
			},
			{
				Query: "SELECT * from dolt_diff_stat('main..branch1');",
				Expected: []sql.UntypedSqlRow{
					{"t", 0, 0, 1, 1, 0, 4, 0, 2, 1, 6, 2},
					{"newtable", 0, 0, 2, 0, 0, 2, 0, 2, 0, 2, 0},
				},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('branch1', 'main', 't');",
				Expected: []sql.UntypedSqlRow{{"t", 0, 1, 0, 1, 4, 0, 1, 1, 2, 2, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('branch1..main', 't');",
				Expected: []sql.UntypedSqlRow{{"t", 0, 1, 0, 1, 4, 0, 1, 1, 2, 2, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('main~2', 'branch1', 't');",
				Expected: []sql.UntypedSqlRow{{"t", 0, 1, 1, 0, 2, 3, 0, 1, 1, 3, 2}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('main~2..branch1', 't');",
				Expected: []sql.UntypedSqlRow{{"t", 0, 1, 1, 0, 2, 3, 0, 1, 1, 3, 2}},
			},

			// Three dot
			{
				Query:    "SELECT * from dolt_diff_stat('main...branch1', 't');",
				Expected: []sql.UntypedSqlRow{{"t", 0, 1, 1, 0, 2, 3, 0, 1, 1, 3, 2}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('main...branch1');",
				Expected: []sql.UntypedSqlRow{{"t", 0, 1, 1, 0, 2, 3, 0, 1, 1, 3, 2}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('branch1...main', 't');",
				Expected: []sql.UntypedSqlRow{{"t", 1, 1, 0, 0, 3, 0, 0, 1, 2, 3, 6}},
			},
			{
				Query: "SELECT * from dolt_diff_stat('branch1...main');",
				Expected: []sql.UntypedSqlRow{
					{"t", 1, 1, 0, 0, 3, 0, 0, 1, 2, 3, 6},
					{"newtable", 0, 2, 0, 0, 2, 0, 0, 0, 2, 0, 2},
				},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('branch1...main^');",
				Expected: []sql.UntypedSqlRow{{"t", 1, 1, 0, 0, 3, 0, 0, 1, 2, 3, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('branch1...main', 'newtable');",
				Expected: []sql.UntypedSqlRow{{"newtable", 0, 2, 0, 0, 2, 0, 0, 0, 2, 0, 2}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('main...main', 'newtable');",
				Expected: []sql.UntypedSqlRow{},
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
				Expected: []sql.UntypedSqlRow{{"t", 0, 0, 0, 2, 0, 2, 0, 2, 2, 6, 4}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit2, @Commit3, 't');",
				Expected: []sql.UntypedSqlRow{{"t", 2, 1, 0, 0, 2, 0, 0, 2, 3, 4, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit1, @Commit3, 't');",
				Expected: []sql.UntypedSqlRow{{"t", 0, 1, 0, 2, 2, 2, 0, 2, 3, 6, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit3, @Commit4, 't');",
				Expected: []sql.UntypedSqlRow{{"t", 2, 1, 0, 1, 6, 0, 1, 3, 4, 6, 12}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit1, @Commit4, 't');",
				Expected: []sql.UntypedSqlRow{{"t", 0, 2, 0, 2, 6, 0, 2, 2, 4, 6, 12}},
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
				Expected: []sql.UntypedSqlRow{{"t", 0, 2, 0, 0, 6, 0, 0, 0, 2, 0, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit2, @Commit3, 't');",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit3, @Commit4, 't');",
				Expected: []sql.UntypedSqlRow{{"t", 1, 1, 0, 1, 3, 0, 1, 2, 3, 6, 9}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit4, @Commit5, 't');",
				Expected: []sql.UntypedSqlRow{{"t", 3, 1, 0, 0, 3, 0, 0, 3, 4, 9, 12}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat(@Commit1, @Commit5, 't');",
				Expected: []sql.UntypedSqlRow{{"t", 0, 4, 0, 0, 12, 0, 0, 0, 4, 0, 12}},
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
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "select * from dolt_diff_stat('WORKING', 'HEAD')",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:            "insert into t1 values (1,2)",
				SkipResultsCheck: true,
			},
			{
				Query:    "select * from dolt_diff_stat('HEAD', 'WORKING', 't1')",
				Expected: []sql.UntypedSqlRow{{"t1", 0, 1, 0, 0, 2, 0, 0, 0, 1, 0, 2}},
			},
			{
				Query:    "select * from dolt_diff_stat('WORKING', 'HEAD', 't1')",
				Expected: []sql.UntypedSqlRow{{"t1", 0, 0, 1, 0, 0, 2, 0, 1, 0, 2, 0}},
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
				Expected: []sql.UntypedSqlRow{{"t1", 0, 0, 1, 0, 0, 2, 0, 1, 0, 2, 0}},
			},
			{
				Query:    "select * from dolt_diff_stat('HEAD', 'HEAD~', 't1')",
				Expected: []sql.UntypedSqlRow{{"t1", 0, 1, 0, 0, 2, 0, 0, 0, 1, 0, 2}},
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
				Expected: []sql.UntypedSqlRow{{"t2", 1, 1, 0, 0, 2, 0, 0, 1, 2, 2, 4}},
			},
			{
				Query:    "select * from dolt_diff_stat('HEAD~..HEAD', 't2')",
				Expected: []sql.UntypedSqlRow{{"t2", 1, 1, 0, 0, 2, 0, 0, 1, 2, 2, 4}},
			},
			{
				// Old table name can be matched as well
				Query:    "select * from dolt_diff_stat('HEAD~', 'HEAD', 't1')",
				Expected: []sql.UntypedSqlRow{{"t1", 1, 1, 0, 0, 2, 0, 0, 1, 2, 2, 4}},
			},
			{
				// Old table name can be matched as well
				Query:    "select * from dolt_diff_stat('HEAD~..HEAD', 't1')",
				Expected: []sql.UntypedSqlRow{{"t1", 1, 1, 0, 0, 2, 0, 0, 1, 2, 2, 4}},
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
				Expected: []sql.UntypedSqlRow{{"t", 1, 0, 0, 0, 2, 0, 0, 1, 1, 1, 3}},
			},
			{
				Query:    "SELECT * from dolt_diff_stat('HEAD~', 'HEAD');",
				Expected: []sql.UntypedSqlRow{},
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
				Expected: []sql.UntypedSqlRow{
					{"t", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					{"t2", 1, 3, 0, 0, 3, 0, 0, 1, 4, 1, 4},
				},
				ExpectedWarning:       dtables.PrimaryKeyChangeWarningCode,
				ExpectedWarningsCount: 1,
			},
		},
	},
}

var DiffSummaryTableFunctionScriptTests = []queries.ScriptTest{
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
				Query:       "SELECT * from dolt_diff_summary();",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_diff_summary('t');",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_diff_summary('t', @Commit1, @Commit2, 'extra');",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_diff_summary(null, null, null);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_diff_summary(123, @Commit1, @Commit2);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_diff_summary('t', 123, @Commit2);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_diff_summary('t', @Commit1, 123);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:          "SELECT * from dolt_diff_summary('fake-branch', @Commit2, 't');",
				ExpectedErrStr: "branch not found: fake-branch",
			},
			{
				Query:          "SELECT * from dolt_diff_summary('fake-branch..main', 't');",
				ExpectedErrStr: "branch not found: fake-branch",
			},
			{
				Query:          "SELECT * from dolt_diff_summary(@Commit1, 'fake-branch', 't');",
				ExpectedErrStr: "branch not found: fake-branch",
			},
			{
				Query:          "SELECT * from dolt_diff_summary('main..fake-branch', 't');",
				ExpectedErrStr: "branch not found: fake-branch",
			},
			{
				Query:       "SELECT * from dolt_diff_summary(@Commit1, concat('fake', '-', 'branch'), 't');",
				ExpectedErr: dtablefunctions.ErrInvalidNonLiteralArgument,
			},
			{
				Query:       "SELECT * from dolt_diff_summary(hashof('main'), @Commit2, 't');",
				ExpectedErr: dtablefunctions.ErrInvalidNonLiteralArgument,
			},
			{
				Query:       "SELECT * from dolt_diff_summary(@Commit1, @Commit2, LOWER('T'));",
				ExpectedErr: dtablefunctions.ErrInvalidNonLiteralArgument,
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
				// table does not exist, empty result
				Query:    "SELECT * from dolt_diff_summary(@Commit1, @Commit2, 'doesnotexist');",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				// table is added, no data changes
				Query:    "SELECT * from dolt_diff_summary(@Commit1, @Commit2, 't');",
				Expected: []sql.UntypedSqlRow{{"", "t", "added", false, true}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit2, @Commit3, 't');",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, false}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit3, @Commit4, 't');",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, false}},
			},
			{
				// change from and to commits
				Query:    "SELECT * from dolt_diff_summary(@Commit4, @Commit3, 't');",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, false}},
			},
			{
				// table is dropped
				Query:    "SELECT * from dolt_diff_summary(@Commit4, @Commit5, 't');",
				Expected: []sql.UntypedSqlRow{{"t", "", "dropped", true, true}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit1, @Commit4, 't');",
				Expected: []sql.UntypedSqlRow{{"", "t", "added", true, true}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit1, @Commit5, 't');",
				Expected: []sql.UntypedSqlRow{},
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
				Query:    "SELECT * from dolt_diff_summary(@Commit1, @Commit2, 't');",
				Expected: []sql.UntypedSqlRow{{"", "t", "added", false, true}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit2, @Commit3, 't');",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, false}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit3, @Commit4, 't');",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, false}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit4, @Commit3, 't');",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, false}},
			},
			{
				// table is dropped
				Query:    "SELECT * from dolt_diff_summary(@Commit4, @Commit5, 't');",
				Expected: []sql.UntypedSqlRow{{"t", "", "dropped", true, true}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit1, @Commit4, 't');",
				Expected: []sql.UntypedSqlRow{{"", "t", "added", true, true}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit1, @Commit5, 't');",
				Expected: []sql.UntypedSqlRow{},
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
				Query:    "SELECT * from dolt_diff_summary(@Commit0, @Commit1);",
				Expected: []sql.UntypedSqlRow{{"", "t", "added", true, true}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit1, @Commit2);",
				Expected: []sql.UntypedSqlRow{{"", "t2", "added", true, true}},
			},
			{
				Query: "SELECT * from dolt_diff_summary(@Commit2, @Commit3);",
				Expected: []sql.UntypedSqlRow{
					{"t", "t", "modified", true, false},
					{"t2", "t2", "modified", true, false},
				},
			},
			{
				Query: "SELECT * from dolt_diff_summary(@Commit3, @Commit4);",
				Expected: []sql.UntypedSqlRow{
					{"t", "t", "modified", true, false},
					{"t2", "t2", "modified", true, false},
				},
			},
			{
				Query: "SELECT * from dolt_diff_summary(@Commit0, @Commit4);",
				Expected: []sql.UntypedSqlRow{
					{"", "t", "added", true, true},
					{"", "t2", "added", true, true},
				},
			},
			{
				Query: "SELECT * from dolt_diff_summary(@Commit4, @Commit2);",

				Expected: []sql.UntypedSqlRow{
					{"t", "t", "modified", true, false},
					{"t2", "t2", "modified", true, false},
				},
			},
			{
				Query: "SELECT * from dolt_diff_summary(@Commit3, 'WORKING');",
				Expected: []sql.UntypedSqlRow{
					{"t", "t", "modified", true, false},
					{"t2", "t2", "modified", true, false},
					{"", "keyless", "added", false, true}},
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
				Query:    "SELECT * from dolt_diff_summary(@Commit1, 'WORKING', 't')",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, false}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('STAGED', 'WORKING', 't')",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, false}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('STAGED..WORKING', 't')",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, false}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('WORKING', 'STAGED', 't')",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, false}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('WORKING', 'WORKING', 't')",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('WORKING..WORKING', 't')",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('STAGED', 'STAGED', 't')",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:            "call dolt_add('.')",
				SkipResultsCheck: true,
			},
			{
				Query:    "SELECT * from dolt_diff_summary('WORKING', 'STAGED', 't')",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('HEAD', 'STAGED', 't')",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, false}},
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
				Query:    "SELECT * from dolt_diff_summary('main', 'branch1', 't');",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, true}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('main..branch1', 't');",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, true}},
			},
			{
				Query: "SELECT * from dolt_diff_summary('main', 'branch1');",
				Expected: []sql.UntypedSqlRow{
					{"t", "t", "modified", true, true},
					{"newtable", "", "dropped", true, true},
				},
			},
			{
				Query: "SELECT * from dolt_diff_summary('main..branch1');",
				Expected: []sql.UntypedSqlRow{
					{"t", "t", "modified", true, true},
					{"newtable", "", "dropped", true, true},
				},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('branch1', 'main', 't');",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, true}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('branch1..main', 't');",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, true}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('main~2', 'branch1', 't');",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, true}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('main~2..branch1', 't');",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, true}},
			},

			// Three dot
			{
				Query:    "SELECT * from dolt_diff_summary('main...branch1', 't');",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, true}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('main...branch1');",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, true}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('branch1...main', 't');",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, false}},
			},
			{
				Query: "SELECT * from dolt_diff_summary('branch1...main');",
				Expected: []sql.UntypedSqlRow{
					{"t", "t", "modified", true, false},
					{"", "newtable", "added", true, true},
				},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('branch1...main^');",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, false}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('branch1...main', 'newtable');",
				Expected: []sql.UntypedSqlRow{{"", "newtable", "added", true, true}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('main...main', 'newtable');",
				Expected: []sql.UntypedSqlRow{},
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

			// drop 1 column
			"alter table t drop column c2;",
			"set @Commit2 = '';",
			"call dolt_commit_hash_out(@Commit2, '-am', 'dropping column c2');",

			// add 1 row
			"insert into t values (3, 'three');",
			"set @Commit3 = '';",
			"call dolt_commit_hash_out(@Commit3, '-am', 'inserting row 3');",

			// add 1 column
			"alter table t add column c2 varchar(20);",
			"set @Commit4 = '';",
			"call dolt_commit_hash_out(@Commit4, '-am', 'adding column c2');",

			// add 1 row and update
			"insert into t values (4, 'four', 'five');",
			"update t set c2='foo' where pk=1;",
			"set @Commit5 = '';",
			"call dolt_commit_hash_out(@Commit5, '-am', 'inserting and updating data');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit1, @Commit2, 't');",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, true}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit2, @Commit3, 't');",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, false}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit1, @Commit3, 't');",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, true}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit3, @Commit4, 't');",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", false, true}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit3, @Commit5, 't');",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, true}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit1, @Commit5, 't');",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, false}},
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

			// add rows
			"insert into t values(1, 'one', -1), (2, 'two', -2);",
			"set @Commit2 = '';",
			"call dolt_commit_hash_out(@Commit2, '-am', 'inserting into t');",

			// rename column
			"alter table t rename column c2 to c3;",
			"set @Commit3 = '';",
			"call dolt_commit_hash_out(@Commit3, '-am', 'renaming column c2 to c3');",

			// add row and update
			"insert into t values (3, 'three', -3);",
			"update t set c3=1 where pk=1;",
			"set @Commit4 = '';",
			"call dolt_commit_hash_out(@Commit4, '-am', 'inserting and updating data');",

			// rename column and add row
			"alter table t rename column c3 to c2;",
			"insert into t values (4, 'four', -4);",
			"set @Commit5 = '';",
			"call dolt_commit_hash_out(@Commit5, '-am', 'renaming column c3 to c2, and inserting data');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit1, @Commit2, 't');",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, false}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit2, @Commit3, 't');",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", false, true}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit3, @Commit4, 't');",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, false}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit4, @Commit5, 't');",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, true}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit1, @Commit5, 't');",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", true, false}},
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
				Query:    "select * from dolt_diff_summary('HEAD', 'WORKING')",
				Expected: []sql.UntypedSqlRow{{"", "t1", "added", false, true}},
			},
			{
				Query:    "select * from dolt_diff_summary('WORKING', 'HEAD')",
				Expected: []sql.UntypedSqlRow{{"t1", "", "dropped", false, true}},
			},
			{
				Query:            "insert into t1 values (1,2)",
				SkipResultsCheck: true,
			},
			{
				Query:    "select * from dolt_diff_summary('HEAD', 'WORKING', 't1')",
				Expected: []sql.UntypedSqlRow{{"", "t1", "added", true, true}},
			},
			{
				Query:    "select * from dolt_diff_summary('WORKING', 'HEAD', 't1')",
				Expected: []sql.UntypedSqlRow{{"t1", "", "dropped", true, true}},
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
				Query:    "select * from dolt_diff_summary('HEAD~', 'HEAD', 't1')",
				Expected: []sql.UntypedSqlRow{{"t1", "", "dropped", true, true}},
			},
			{
				Query:    "select * from dolt_diff_summary('HEAD', 'HEAD~', 't1')",
				Expected: []sql.UntypedSqlRow{{"", "t1", "added", true, true}},
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
				Query:    "select * from dolt_diff_summary('HEAD~', 'HEAD', 't2')",
				Expected: []sql.UntypedSqlRow{{"t1", "t2", "renamed", true, true}},
			},
			{
				Query:    "select * from dolt_diff_summary('HEAD~..HEAD', 't2')",
				Expected: []sql.UntypedSqlRow{{"t1", "t2", "renamed", true, true}},
			},
			{
				Query:    "select * from dolt_diff_summary('HEAD~', 'HEAD')",
				Expected: []sql.UntypedSqlRow{{"t1", "t2", "renamed", true, true}},
			},
			{
				Query:    "select * from dolt_diff_summary('HEAD~..HEAD')",
				Expected: []sql.UntypedSqlRow{{"t1", "t2", "renamed", true, true}},
			},
			{
				// Old table name can be matched as well
				Query:    "select * from dolt_diff_summary('HEAD~', 'HEAD', 't1')",
				Expected: []sql.UntypedSqlRow{{"t1", "t2", "renamed", true, true}},
			},
			{
				// Old table name can be matched as well
				Query:    "select * from dolt_diff_summary('HEAD~..HEAD', 't1')",
				Expected: []sql.UntypedSqlRow{{"t1", "t2", "renamed", true, true}},
			},
		},
	},
	{
		Name: "foreign key change",
		SetUpScript: []string{
			"create table test (id int primary key);",
			"INSERT INTO test values (1), (2);",
			"set @Commit1 = '';",
			"CALL dolt_commit_hash_out(@Commit1, '-Am', 'create table test');",

			"create table test2 (pk int primary key, test_id int);",
			"alter table test2 add constraint fk_test_id foreign key (test_id) references test(id);",
			"insert into test2 values (1, 1);",
			"set @Commit2 = '';",
			"CALL dolt_commit_hash_out(@Commit2, '-Am', 'table with foreign key and row');",

			"alter table test2 drop foreign key fk_test_id;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit1, @Commit2);",
				Expected: []sql.UntypedSqlRow{{"", "test2", "added", true, true}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('HEAD', 'WORKING');",
				Expected: []sql.UntypedSqlRow{{"test2", "test2", "modified", false, true}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit1, 'WORKING');",
				Expected: []sql.UntypedSqlRow{{"", "test2", "added", true, true}},
			},
		},
	},
	{
		Name: "add multiple columns, then set and unset a value. Should not show a diff",
		SetUpScript: []string{
			"CREATE table t (pk int primary key);",
			"insert into t values (1);",
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
				Query:    "SELECT * from dolt_diff_summary('HEAD~2', 'HEAD');",
				Expected: []sql.UntypedSqlRow{{"t", "t", "modified", false, true}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('HEAD~', 'HEAD');",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name: "pk set change should throw an error for 3 argument dolt_diff_summary",
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
				Query:          "SELECT * from dolt_diff_summary('HEAD~', 'HEAD', 't');",
				ExpectedErrStr: "failed to compute diff summary for table t: primary key set changed",
			},
		},
	},
	{
		Name: "pk set change should report warning for 2 argument dolt_diff_summary",
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
				Query: "SELECT * from dolt_diff_summary('HEAD~', 'HEAD')",
				Expected: []sql.UntypedSqlRow{
					{"t2", "t2", "modified", true, false},
				},
				ExpectedWarning:       dtables.PrimaryKeyChangeWarningCode,
				ExpectedWarningsCount: 1,
			},
		},
	},
}

var PatchTableFunctionScriptTests = []queries.ScriptTest{
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
				Query:       "SELECT * from dolt_patch();",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_patch('t');",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_patch('t', @Commit1, @Commit2, 'extra');",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_patch(null, null, null);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_patch(123, @Commit1, @Commit2);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_patch('t', 123, @Commit2);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_patch('t', @Commit1, 123);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:          "SELECT * from dolt_patch('fake-branch', @Commit2, 't');",
				ExpectedErrStr: "branch not found: fake-branch",
			},
			{
				Query:          "SELECT * from dolt_patch('fake-branch..main', 't');",
				ExpectedErrStr: "branch not found: fake-branch",
			},
			{
				Query:          "SELECT * from dolt_patch(@Commit1, 'fake-branch', 't');",
				ExpectedErrStr: "branch not found: fake-branch",
			},
			{
				Query:          "SELECT * from dolt_patch('main..fake-branch', 't');",
				ExpectedErrStr: "branch not found: fake-branch",
			},
			{
				Query:       "SELECT * from dolt_patch(@Commit1, @Commit2, 'doesnotexist');",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:       "SELECT * from dolt_patch('main^..main', 'doesnotexist');",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:       "SELECT * from dolt_patch(@Commit1, concat('fake', '-', 'branch'), 't');",
				ExpectedErr: dtablefunctions.ErrInvalidNonLiteralArgument,
			},
			{
				Query:       "SELECT * from dolt_patch(hashof('main'), @Commit2, 't');",
				ExpectedErr: dtablefunctions.ErrInvalidNonLiteralArgument,
			},
			{
				Query:       "SELECT * from dolt_patch(@Commit1, @Commit2, LOWER('T'));",
				ExpectedErr: dtablefunctions.ErrInvalidNonLiteralArgument,
			},
			{
				Query:       "SELECT * from dolt_patch('main..main~', LOWER('T'));",
				ExpectedErr: dtablefunctions.ErrInvalidNonLiteralArgument,
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
				Query:    "SELECT * from dolt_patch(@Commit1, @Commit2, 't') WHERE diff_type = 'data';",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "SELECT statement_order, table_name, statement from dolt_patch(@Commit1, @Commit2, 't') WHERE diff_type = 'schema';",
				Expected: []sql.UntypedSqlRow{{1, "t", "CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `c1` varchar(20),\n  `c2` varchar(20),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"}},
			},
			{
				Query:    "SELECT statement_order, table_name, diff_type, statement from dolt_patch(@Commit2, @Commit3, 't');",
				Expected: []sql.UntypedSqlRow{{1, "t", "data", "INSERT INTO `t` (`pk`,`c1`,`c2`) VALUES (1,'one','two');"}},
			},
			{
				Query: "SELECT statement_order, table_name, diff_type, statement from dolt_patch(@Commit3, @Commit4, 't');",
				Expected: []sql.UntypedSqlRow{
					{1, "t", "data", "UPDATE `t` SET `c1`='uno',`c2`='dos' WHERE `pk`=1;"},
					{2, "t", "data", "INSERT INTO `t` (`pk`,`c1`,`c2`) VALUES (2,'two','three');"},
					{3, "t", "data", "INSERT INTO `t` (`pk`,`c1`,`c2`) VALUES (3,'three','four');"},
				},
			},
			{
				// change from and to commits
				Query: "SELECT statement_order, table_name, diff_type, statement from dolt_patch(@Commit4, @Commit3, 't');",
				Expected: []sql.UntypedSqlRow{
					{1, "t", "data", "UPDATE `t` SET `c1`='one',`c2`='two' WHERE `pk`=1;"},
					{2, "t", "data", "DELETE FROM `t` WHERE `pk`=2;"},
					{3, "t", "data", "DELETE FROM `t` WHERE `pk`=3;"},
				},
			},
			{
				// table is dropped
				Query:    "SELECT statement_order, table_name, diff_type, statement from dolt_patch(@Commit4, @Commit5, 't');",
				Expected: []sql.UntypedSqlRow{{1, "t", "schema", "DROP TABLE `t`;"}},
			},
			{
				Query: "SELECT statement_order, table_name, diff_type, statement from dolt_patch(@Commit1, @Commit4, 't');",
				Expected: []sql.UntypedSqlRow{
					{1, "t", "schema", "CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `c1` varchar(20),\n  `c2` varchar(20),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"},
					{2, "t", "data", "INSERT INTO `t` (`pk`,`c1`,`c2`) VALUES (1,'uno','dos');"},
					{3, "t", "data", "INSERT INTO `t` (`pk`,`c1`,`c2`) VALUES (2,'two','three');"},
					{4, "t", "data", "INSERT INTO `t` (`pk`,`c1`,`c2`) VALUES (3,'three','four');"},
				},
			},
			{
				Query:       "SELECT * from dolt_patch(@Commit1, @Commit5, 't');",
				ExpectedErr: sql.ErrTableNotFound,
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/6350
		Name: "binary data in patch statements is hex encoded",
		SetUpScript: []string{
			"create table t (pk varbinary(16) primary key, c1  binary(16));",
			"insert into t values (0x42, NULL);",
			"call dolt_commit('-Am', 'new table with binary pk');",
			"update t set c1 = 0xeeee where pk = 0x42;",
			"insert into t values (0x012345, NULL), (0x054321, binary 'efg_!4');",
			"call dolt_commit('-am', 'more rows');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select statement from dolt_patch('HEAD~', 'HEAD', 't');",
				Expected: []sql.UntypedSqlRow{
					{"INSERT INTO `t` (`pk`,`c1`) VALUES (0x012345,NULL);"},
					{"INSERT INTO `t` (`pk`,`c1`) VALUES (0x054321,0x6566675f213400000000000000000000);"},
					{"UPDATE `t` SET `c1`=0xeeee0000000000000000000000000000 WHERE `pk`=0x42;"},
				},
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
				Query: "SELECT statement_order, table_name, diff_type, statement from dolt_patch(@Commit0, @Commit1);",
				Expected: []sql.UntypedSqlRow{
					{1, "t", "schema", "CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `c1` varchar(20),\n  `c2` varchar(20),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"},
					{2, "t", "data", "INSERT INTO `t` (`pk`,`c1`,`c2`) VALUES (1,'one','two');"},
				},
			},
			{
				Query: "SELECT statement_order, table_name, diff_type, statement from dolt_patch(@Commit1, @Commit2);",
				Expected: []sql.UntypedSqlRow{
					{1, "t2", "schema", "CREATE TABLE `t2` (\n  `pk` int NOT NULL,\n  `c1` varchar(20),\n  `c2` varchar(20),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"},
					{2, "t2", "data", "INSERT INTO `t2` (`pk`,`c1`,`c2`) VALUES (100,'hundred','hundert');"},
				},
			},
			{
				Query: "SELECT statement_order, table_name, diff_type, statement from dolt_patch(@Commit2, @Commit3);",
				Expected: []sql.UntypedSqlRow{
					{1, "t", "data", "UPDATE `t` SET `c1`='uno',`c2`='dos' WHERE `pk`=1;"},
					{2, "t", "data", "INSERT INTO `t` (`pk`,`c1`,`c2`) VALUES (2,'two','three');"},
					{3, "t", "data", "INSERT INTO `t` (`pk`,`c1`,`c2`) VALUES (3,'three','four');"},
					{4, "t", "data", "INSERT INTO `t` (`pk`,`c1`,`c2`) VALUES (4,'four','five');"},
					{5, "t2", "data", "INSERT INTO `t2` (`pk`,`c1`,`c2`) VALUES (101,'hundred one','one');"},
				},
			},
			{
				Query: "SELECT statement_order, table_name, diff_type, statement from dolt_patch(@Commit3, @Commit4);",
				Expected: []sql.UntypedSqlRow{
					{1, "t", "data", "DELETE FROM `t` WHERE `pk`=3;"},
					{2, "t2", "data", "UPDATE `t2` SET `c2`='zero' WHERE `pk`=100;"},
				},
			},
			{
				Query: "SELECT statement_order, table_name, diff_type, statement from dolt_patch(@Commit4, @Commit2);",
				Expected: []sql.UntypedSqlRow{
					{1, "t", "data", "UPDATE `t` SET `c1`='one',`c2`='two' WHERE `pk`=1;"},
					{2, "t", "data", "DELETE FROM `t` WHERE `pk`=2;"},
					{3, "t", "data", "DELETE FROM `t` WHERE `pk`=4;"},
					{4, "t2", "data", "UPDATE `t2` SET `c2`='hundert' WHERE `pk`=100;"},
					{5, "t2", "data", "DELETE FROM `t2` WHERE `pk`=101;"},
				},
			},
			{
				Query: "SELECT statement_order, table_name, diff_type, statement from dolt_patch(@Commit3, 'WORKING');",
				Expected: []sql.UntypedSqlRow{
					{1, "keyless", "schema", "CREATE TABLE `keyless` (\n  `id` int\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"},
					{2, "t", "data", "DELETE FROM `t` WHERE `pk`=3;"},
					{3, "t2", "data", "UPDATE `t2` SET `c2`='zero' WHERE `pk`=100;"},
				},
			},
		},
	},
	{
		Name: "using WORKING and STAGED refs on RENAME, DROP and ADD column",
		SetUpScript: []string{
			"set @Commit0 = HashOf('HEAD');",
			"create table t (pk int primary key, c1 int, c2 int, c3 int, c4 int, c5 int comment 'tag:5');",
			"insert into t values (0,1,2,3,4,5), (1,1,2,3,4,5);",
			"call dolt_commit('-Am', 'inserting two rows into table t');",
			"alter table t rename column c1 to c0;",
			"alter table t drop column c4;",
			"alter table t add c6 bigint;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT statement_order, table_name, diff_type, statement FROM dolt_patch('HEAD', 'WORKING', 't')",
				Expected: []sql.UntypedSqlRow{
					{1, "t", "schema", "ALTER TABLE `t` RENAME COLUMN `c1` TO `c0`;"},
					{2, "t", "schema", "ALTER TABLE `t` DROP `c4`;"},
					{3, "t", "schema", "ALTER TABLE `t` ADD `c6` bigint;"},
					// NOTE: These two update statements aren't technically needed, but we can't tell that from the diff.
					//       Because the rows were altered on disk due to the `drop column` statement above, these rows
					//       really did change on disk and we can't currently safely tell that it was ONLY the column
					//       rename and that there weren't other updates to that column.
					{4, "t", "data", "UPDATE `t` SET `c0`=1 WHERE `pk`=0;"},
					{5, "t", "data", "UPDATE `t` SET `c0`=1 WHERE `pk`=1;"},
				},
			},
			{
				Query: "SELECT * FROM dolt_patch('STAGED', 'WORKING', 't')",
				Expected: []sql.UntypedSqlRow{
					{1, "STAGED", "WORKING", "t", "schema", "ALTER TABLE `t` RENAME COLUMN `c1` TO `c0`;"},
					{2, "STAGED", "WORKING", "t", "schema", "ALTER TABLE `t` DROP `c4`;"},
					{3, "STAGED", "WORKING", "t", "schema", "ALTER TABLE `t` ADD `c6` bigint;"},
					// NOTE: These two update statements aren't technically needed, but we can't tell that from the diff.
					//       Because the rows were altered on disk due to the `drop column` statement above, these rows
					//       really did change on disk and we can't currently safely tell that it was ONLY the column
					//       rename and that there weren't other updates to that column.
					{4, "STAGED", "WORKING", "t", "data", "UPDATE `t` SET `c0`=1 WHERE `pk`=0;"},
					{5, "STAGED", "WORKING", "t", "data", "UPDATE `t` SET `c0`=1 WHERE `pk`=1;"},
				},
			},
			{
				Query: "SELECT * FROM dolt_patch('STAGED..WORKING', 't')",
				Expected: []sql.UntypedSqlRow{
					{1, "STAGED", "WORKING", "t", "schema", "ALTER TABLE `t` RENAME COLUMN `c1` TO `c0`;"},
					{2, "STAGED", "WORKING", "t", "schema", "ALTER TABLE `t` DROP `c4`;"},
					{3, "STAGED", "WORKING", "t", "schema", "ALTER TABLE `t` ADD `c6` bigint;"},
					// NOTE: These two update statements aren't technically needed, but we can't tell that from the diff.
					//       Because the rows were altered on disk due to the `drop column` statement above, these rows
					//       really did change on disk and we can't currently safely tell that it was ONLY the column
					//       rename and that there weren't other updates to that column.
					{4, "STAGED", "WORKING", "t", "data", "UPDATE `t` SET `c0`=1 WHERE `pk`=0;"},
					{5, "STAGED", "WORKING", "t", "data", "UPDATE `t` SET `c0`=1 WHERE `pk`=1;"},
				},
			},
			{
				Query: "SELECT * FROM dolt_patch('WORKING', 'STAGED', 't')",
				Expected: []sql.UntypedSqlRow{
					{1, "WORKING", "STAGED", "t", "schema", "ALTER TABLE `t` RENAME COLUMN `c0` TO `c1`;"},
					{2, "WORKING", "STAGED", "t", "schema", "ALTER TABLE `t` DROP `c6`;"},
					{3, "WORKING", "STAGED", "t", "schema", "ALTER TABLE `t` ADD `c4` int;"},
					// NOTE: Setting c1 in these two update statements isn't technically needed, but we can't tell that
					//       from the diff. Because the rows were altered on disk due to the `drop column` statement above,
					//       these rows really did change on disk and we can't currently safely tell that it was ONLY the
					//       column rename and that there weren't other updates to that column.
					{4, "WORKING", "STAGED", "t", "data", "UPDATE `t` SET `c1`=1,`c4`=4 WHERE `pk`=0;"},
					{5, "WORKING", "STAGED", "t", "data", "UPDATE `t` SET `c1`=1,`c4`=4 WHERE `pk`=1;"},
				},
			},
			{
				Query:    "SELECT statement_order, table_name, diff_type, statement FROM dolt_patch('WORKING', 'WORKING', 't')",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "SELECT statement_order, table_name, diff_type, statement FROM dolt_patch('WORKING..WORKING', 't')",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "SELECT statement_order, table_name, diff_type, statement FROM dolt_patch('STAGED', 'STAGED', 't')",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:            "call dolt_add('.')",
				SkipResultsCheck: true,
			},
			{
				Query:    "SELECT statement_order, table_name, diff_type, statement FROM dolt_patch('WORKING', 'STAGED', 't')",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "SELECT statement_order, table_name, diff_type, statement FROM dolt_patch('HEAD', 'STAGED', 't')",
				Expected: []sql.UntypedSqlRow{
					{1, "t", "schema", "ALTER TABLE `t` RENAME COLUMN `c1` TO `c0`;"},
					{2, "t", "schema", "ALTER TABLE `t` DROP `c4`;"},
					{3, "t", "schema", "ALTER TABLE `t` ADD `c6` bigint;"},
					{4, "t", "data", "UPDATE `t` SET `c0`=1 WHERE `pk`=0;"},
					{5, "t", "data", "UPDATE `t` SET `c0`=1 WHERE `pk`=1;"},
				},
			},
		},
	},
	{
		Name: "using branch refs different ways",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'creating table t');",
			"set @Commit1 = hashof('HEAD');",

			"insert into t values(1, 'one', 'two');",
			"call dolt_commit('-am', 'inserting row 1 into t in main');",
			"set @Commit2 = hashof('HEAD');",

			"CALL DOLT_checkout('-b', 'branch1');",
			"alter table t drop column c2;",
			"call dolt_commit('-am', 'dropping column c2 in branch1');",
			"set @Commit3 = hashof('HEAD');",

			"delete from t where pk=1;",
			"call dolt_commit('-am', 'deleting row 1 in branch1');",
			"set @Commit4 = hashof('HEAD');",

			"insert into t values (2, 'two');",
			"call dolt_commit('-am', 'inserting row 2 in branch1');",
			"set @Commit5 = hashof('HEAD');",

			"CALL DOLT_checkout('main');",
			"insert into t values (2, 'two', 'three');",
			"call dolt_commit('-am', 'inserting row 2 in main');",
			"set @Commit6 = hashof('HEAD');",

			"create table newtable (pk int primary key);",
			"insert into newtable values (1), (2);",
			"call dolt_commit('-Am', 'new table newtable');",
			"set @Commit7 = hashof('HEAD');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT statement_order, table_name, diff_type, statement FROM dolt_patch('main', 'branch1', 't');",
				Expected: []sql.UntypedSqlRow{
					{1, "t", "schema", "ALTER TABLE `t` DROP `c2`;"},
					{2, "t", "data", "DELETE FROM `t` WHERE `pk`=1;"},
				},
			},
			{
				Query: "SELECT statement_order, table_name, diff_type, statement FROM dolt_patch('main..branch1', 't');",
				Expected: []sql.UntypedSqlRow{
					{1, "t", "schema", "ALTER TABLE `t` DROP `c2`;"},
					{2, "t", "data", "DELETE FROM `t` WHERE `pk`=1;"},
				},
			},
			{
				Query: "SELECT statement_order, table_name, diff_type, statement FROM dolt_patch('main', 'branch1');",
				Expected: []sql.UntypedSqlRow{
					{1, "newtable", "schema", "DROP TABLE `newtable`;"},
					{2, "t", "schema", "ALTER TABLE `t` DROP `c2`;"},
					{3, "t", "data", "DELETE FROM `t` WHERE `pk`=1;"},
				},
			},
			{
				Query: "SELECT statement_order, table_name, diff_type, statement FROM dolt_patch('main..branch1');",
				Expected: []sql.UntypedSqlRow{
					{1, "newtable", "schema", "DROP TABLE `newtable`;"},
					{2, "t", "schema", "ALTER TABLE `t` DROP `c2`;"},
					{3, "t", "data", "DELETE FROM `t` WHERE `pk`=1;"},
				},
			},
			{
				Query: "SELECT statement_order, table_name, diff_type, statement FROM dolt_patch('branch1', 'main', 't');",
				Expected: []sql.UntypedSqlRow{
					{1, "t", "schema", "ALTER TABLE `t` ADD `c2` varchar(20);"},
					{2, "t", "data", "INSERT INTO `t` (`pk`,`c1`,`c2`) VALUES (1,'one','two');"},
					{3, "t", "data", "UPDATE `t` SET `c2`='three' WHERE `pk`=2;"},
				},
			},
			{
				Query: "SELECT statement_order, table_name, diff_type, statement FROM dolt_patch('branch1..main', 't');",
				Expected: []sql.UntypedSqlRow{
					{1, "t", "schema", "ALTER TABLE `t` ADD `c2` varchar(20);"},
					{2, "t", "data", "INSERT INTO `t` (`pk`,`c1`,`c2`) VALUES (1,'one','two');"},
					{3, "t", "data", "UPDATE `t` SET `c2`='three' WHERE `pk`=2;"},
				},
			},
			{
				Query: "SELECT statement_order, table_name, diff_type, statement FROM dolt_patch('main~2', 'branch1', 't');",
				Expected: []sql.UntypedSqlRow{
					{1, "t", "schema", "ALTER TABLE `t` DROP `c2`;"},
					{2, "t", "data", "DELETE FROM `t` WHERE `pk`=1;"},
					{3, "t", "data", "INSERT INTO `t` (`pk`,`c1`) VALUES (2,'two');"},
				},
			},
			{
				Query: "SELECT statement_order, table_name, diff_type, statement FROM dolt_patch('main~2..branch1', 't');",
				Expected: []sql.UntypedSqlRow{
					{1, "t", "schema", "ALTER TABLE `t` DROP `c2`;"},
					{2, "t", "data", "DELETE FROM `t` WHERE `pk`=1;"},
					{3, "t", "data", "INSERT INTO `t` (`pk`,`c1`) VALUES (2,'two');"},
				},
			},
			// Three dot
			{
				Query: "SELECT statement_order, table_name, diff_type, statement FROM dolt_patch('main...branch1', 't');",
				Expected: []sql.UntypedSqlRow{
					{1, "t", "schema", "ALTER TABLE `t` DROP `c2`;"},
					{2, "t", "data", "DELETE FROM `t` WHERE `pk`=1;"},
					{3, "t", "data", "INSERT INTO `t` (`pk`,`c1`) VALUES (2,'two');"},
				},
			},
			{
				Query: "SELECT statement_order, table_name, diff_type, statement FROM dolt_patch('main...branch1');",
				Expected: []sql.UntypedSqlRow{
					{1, "t", "schema", "ALTER TABLE `t` DROP `c2`;"},
					{2, "t", "data", "DELETE FROM `t` WHERE `pk`=1;"},
					{3, "t", "data", "INSERT INTO `t` (`pk`,`c1`) VALUES (2,'two');"},
				},
			},
			{
				Query:    "SELECT statement_order, table_name, diff_type, statement FROM dolt_patch('branch1...main', 't');",
				Expected: []sql.UntypedSqlRow{{1, "t", "data", "INSERT INTO `t` (`pk`,`c1`,`c2`) VALUES (2,'two','three');"}},
			},
			{
				Query: "SELECT statement_order, table_name, diff_type, statement FROM dolt_patch('branch1...main');",
				Expected: []sql.UntypedSqlRow{
					{1, "newtable", "schema", "CREATE TABLE `newtable` (\n  `pk` int NOT NULL,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"},
					{2, "newtable", "data", "INSERT INTO `newtable` (`pk`) VALUES (1);"},
					{3, "newtable", "data", "INSERT INTO `newtable` (`pk`) VALUES (2);"},
					{4, "t", "data", "INSERT INTO `t` (`pk`,`c1`,`c2`) VALUES (2,'two','three');"},
				},
			},
			{
				Query:    "SELECT statement_order, table_name, diff_type, statement FROM dolt_patch('branch1...main^');",
				Expected: []sql.UntypedSqlRow{{1, "t", "data", "INSERT INTO `t` (`pk`,`c1`,`c2`) VALUES (2,'two','three');"}},
			},
			{
				Query: "SELECT statement_order, table_name, diff_type, statement FROM dolt_patch('branch1...main', 'newtable');",
				Expected: []sql.UntypedSqlRow{
					{1, "newtable", "schema", "CREATE TABLE `newtable` (\n  `pk` int NOT NULL,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"},
					{2, "newtable", "data", "INSERT INTO `newtable` (`pk`) VALUES (1);"},
					{3, "newtable", "data", "INSERT INTO `newtable` (`pk`) VALUES (2);"},
				},
			},
			{
				Query:    "SELECT statement_order, table_name, diff_type, statement FROM dolt_patch('main...main', 'newtable');",
				Expected: []sql.UntypedSqlRow{},
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
				Query: "select statement_order, table_name, diff_type, statement FROM dolt_patch('HEAD~', 'HEAD', 't2')",
				Expected: []sql.UntypedSqlRow{
					{1, "t2", "schema", "RENAME TABLE `t1` TO `t2`;"},
					{2, "t2", "data", "INSERT INTO `t2` (`a`,`b`) VALUES (3,4);"},
				},
			},
			{
				Query: "select statement_order, table_name, diff_type, statement FROM dolt_patch('HEAD~..HEAD', 't2')",
				Expected: []sql.UntypedSqlRow{
					{1, "t2", "schema", "RENAME TABLE `t1` TO `t2`;"},
					{2, "t2", "data", "INSERT INTO `t2` (`a`,`b`) VALUES (3,4);"},
				},
			},
			{
				// Old table name can be matched as well
				Query: "select statement_order, table_name, diff_type, statement FROM dolt_patch('HEAD~', 'HEAD', 't1')",
				Expected: []sql.UntypedSqlRow{
					{1, "t2", "schema", "RENAME TABLE `t1` TO `t2`;"},
					{2, "t2", "data", "INSERT INTO `t2` (`a`,`b`) VALUES (3,4);"},
				},
			},
			{
				// Old table name can be matched as well
				Query: "select statement_order, table_name, diff_type, statement FROM dolt_patch('HEAD~..HEAD', 't1')",
				Expected: []sql.UntypedSqlRow{
					{1, "t2", "schema", "RENAME TABLE `t1` TO `t2`;"},
					{2, "t2", "data", "INSERT INTO `t2` (`a`,`b`) VALUES (3,4);"},
				},
			},
		},
	},
	{
		Name: "multi PRIMARY KEY and FOREIGN KEY",
		SetUpScript: []string{
			"CREATE TABLE parent (id int PRIMARY KEY, id_ext int, v1 int, v2 text COMMENT 'tag:1', INDEX v1 (v1));",
			"CREATE TABLE child (id int primary key, v1 int);",
			"call dolt_commit('-Am', 'new tables')",
			"ALTER TABLE child ADD CONSTRAINT fk_named FOREIGN KEY (v1) REFERENCES parent(v1);",
			"insert into parent values (0, 1, 2, NULL);",
			"ALTER TABLE parent DROP PRIMARY KEY;",
			"ALTER TABLE parent ADD PRIMARY KEY(id, id_ext);",
			"call dolt_add('.')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT statement_order, table_name, diff_type, statement FROM dolt_patch('HEAD~', 'WORKING')",
				Expected: []sql.UntypedSqlRow{
					{1, "child", "schema", "CREATE TABLE `child` (\n  `id` int NOT NULL,\n  `v1` int,\n  PRIMARY KEY (`id`),\n  KEY `fk_named` (`v1`),\n  CONSTRAINT `fk_named` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"},
					{2, "parent", "schema", "CREATE TABLE `parent` (\n  `id` int NOT NULL,\n  `id_ext` int NOT NULL,\n  `v1` int,\n  `v2` text COMMENT 'tag:1',\n  PRIMARY KEY (`id`,`id_ext`),\n  KEY `v1` (`v1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"},
					{3, "parent", "data", "INSERT INTO `parent` (`id`,`id_ext`,`v1`,`v2`) VALUES (0,1,2,NULL);"},
				},
			},
			{
				Query: "SELECT statement_order, to_commit_hash, table_name, diff_type, statement FROM dolt_patch('HEAD', 'STAGED')",
				Expected: []sql.UntypedSqlRow{
					{1, "STAGED", "child", "schema", "ALTER TABLE `child` ADD INDEX `fk_named`(`v1`);"},
					{2, "STAGED", "child", "schema", "ALTER TABLE `child` ADD CONSTRAINT `fk_named` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`);"},
					{3, "STAGED", "parent", "schema", "ALTER TABLE `parent` DROP PRIMARY KEY;"},
					{4, "STAGED", "parent", "schema", "ALTER TABLE `parent` ADD PRIMARY KEY (id,id_ext);"},
				},
				ExpectedWarningsCount: 1,
			},
			{
				Query: "SHOW WARNINGS;",
				Expected: []sql.UntypedSqlRow{
					{"Warning", 1235, "Primary key sets differ between revisions for table 'parent', skipping data diff"},
				},
			},
		},
	},
	{
		Name: "CHECK CONSTRAINTS",
		SetUpScript: []string{
			"create table foo (pk int, c1 int, CHECK (c1 > 3), PRIMARY KEY (pk));",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT statement_order, table_name, diff_type, statement FROM dolt_patch('HEAD', 'WORKING')",
				Expected: []sql.UntypedSqlRow{{1, "foo", "schema", "CREATE TABLE `foo` (\n  `pk` int NOT NULL,\n  `c1` int,\n  PRIMARY KEY (`pk`),\n  CONSTRAINT `foo_chk_eq3jn5ra` CHECK ((c1 > 3))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"}},
			},
		},
	},
	{
		Name: "charset and collation changes",
		SetUpScript: []string{
			"create table t (pk int primary key) collate='utf8mb4_0900_bin';",
			"call dolt_commit('-Am', 'empty table')",
			"set @commit0=hashof('HEAD');",
			"insert into t values (1)",
			"alter table t collate='utf8mb4_0900_ai_ci';",
			"call dolt_commit('-am', 'inserting a row and altering the collation')",
			"set @commit1=hashof('HEAD');",
			"alter table t CHARACTER SET='utf8mb3';",
			"insert into t values (2)",
			"call dolt_commit('-am', 'inserting a row and altering the collation')",
			"set @commit2=hashof('HEAD');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select * from dolt_patch(@commit1, @commit0);",
				Expected: []sql.UntypedSqlRow{
					{1, doltCommit, doltCommit, "t", "schema", "ALTER TABLE `t` COLLATE='utf8mb4_0900_bin';"},
					{2, doltCommit, doltCommit, "t", "data", "DELETE FROM `t` WHERE `pk`=1;"},
				},
			},
			{
				Query: "select * from dolt_patch(@commit1, @commit2);",
				Expected: []sql.UntypedSqlRow{
					{1, doltCommit, doltCommit, "t", "schema", "ALTER TABLE `t` COLLATE='utf8mb3_general_ci';"},
					{2, doltCommit, doltCommit, "t", "data", "INSERT INTO `t` (`pk`) VALUES (2);"},
				},
			},
		},
	},
	{
		Name: "patch DDL changes",
		SetUpScript: []string{
			"create table t (pk int primary key, a int, b int, c int)",
			"insert into t values (1, null, 1, 1), (2, 2, null, 2), (3, 3, 3, 3)",
			"CALL dolt_commit('-Am', 'new table t')",
			"CALL dolt_checkout('-b', 'other')",
			"alter table t modify column a varchar(100) comment 'foo';",
			"alter table t rename column c to z;",
			"alter table t drop column b",
			"alter table t add column d int",
			"delete from t where pk = 3",
			"update t set a = 9 where a is NULL",
			"insert into t values (7,7,7,7)",
			"CALL dolt_commit('-am', 'modified table t')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT statement FROM dolt_patch('main', 'other', 't') ORDER BY statement_order",
				Expected: []sql.UntypedSqlRow{
					{"ALTER TABLE `t` MODIFY COLUMN `a` varchar(100) COMMENT 'foo';"},
					{"ALTER TABLE `t` DROP `b`;"},
					{"ALTER TABLE `t` RENAME COLUMN `c` TO `z`;"},
					{"ALTER TABLE `t` ADD `d` int;"},
					// TODO: The two updates to z below aren't necessary, since the column
					//       was renamed and those are the old values, but it shows as a diff
					//       because of the column name change, so we output UPDATE statements
					//       for them. This isn't a correctness issue, but it is inefficient.
					{"UPDATE `t` SET `a`='9',`z`=1 WHERE `pk`=1;"},
					{"UPDATE `t` SET `z`=2 WHERE `pk`=2;"},
					{"DELETE FROM `t` WHERE `pk`=3;"},
					{"INSERT INTO `t` (`pk`,`a`,`z`,`d`) VALUES (7,'7',7,7);"},
				},
			},
		},
	},
	{
		Name: "tag collision",
		SetUpScript: []string{
			"CALL dolt_checkout('-b', 'other')",
			"CREATE TABLE `MOTOR_TARIFA_COEFICIENTE_RIESGO_144_075_01` (\n" +
				"  `ID` varchar(8) NOT NULL,\n" +
				"  `PRODUCTO` varchar(255),\n" +
				"  `TARIFA` int,\n" +
				"  `VERSION_INICIO` int,\n" +
				"  `VERSION_FIN` int,\n" +
				"  `COBERTURA` varchar(255),\n" +
				"  `INDICADOR_NM` varchar(255),\n" +
				"  `ANOS_COMPANIA_PROCEDENCIA_MIN` int,\n" +
				"  `ANOS_COMPANIA_PROCEDENCIA_MAX` int,\n" +
				"  `COEFICIENTE` double,\n" +
				"  `DWB_IDENTITY` varchar(64),\n" +
				"  PRIMARY KEY (`ID`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
			"CREATE TABLE `MOTOR_TARIFA_COEFICIENTE_RIESGO_144_075` (\n" +
				"  `ID` varchar(8) NOT NULL,\n" +
				"  `PRODUCTO` varchar(255),\n" +
				"  `TARIFA` int,\n" +
				"  `VERSION_INICIO` int,\n" +
				"  `VERSION_FIN` int,\n" +
				"  `COBERTURA` varchar(255),\n" +
				"  `INDICADOR_NM` varchar(255),\n" +
				"  `ANOS_COMPANIA_PROCEDENCIA_MIN` int,\n" +
				"  `ANOS_COMPANIA_PROCEDENCIA_MAX` int,\n" +
				"  `COEFICIENTE` double,\n" +
				"  `DWB_IDENTITY` varchar(64),\n" +
				"  PRIMARY KEY (`ID`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
			"CALL dolt_commit('-A', '-m', 'create tables on other');",

			"CALL dolt_checkout('main')",
			"CREATE TABLE `MOTOR_TARIFA_COEFICIENTE_RIESGO_060` (\n" +
				"  `ID` varchar(8) NOT NULL,\n" +
				"  `PRODUCTO` varchar(255),\n" +
				"  `TARIFA` int,\n" +
				"  `VERSION_INICIO` int,\n" +
				"  `VERSION_FIN` int,\n" +
				"  `COBERTURA` varchar(255),\n" +
				"  `FRECUENCIA_PAGO` varchar(255),\n" +
				"  `COEFICIENTE` double,\n" +
				"  `DWB_IDENTITY` varchar(64),\n" +
				"  PRIMARY KEY (`ID`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
			"CREATE TABLE `MOTOR_TARIFA_COEFICIENTE_RIESGO_060_01` (\n" +
				"  `ID` varchar(8) NOT NULL,\n" +
				"  `PRODUCTO` varchar(255),\n" +
				"  `TARIFA` int,\n" +
				"  `VERSION_INICIO` int,\n" +
				"  `VERSION_FIN` int,\n" +
				"  `COBERTURA` varchar(255),\n" +
				"  `FRECUENCIA_PAGO` varchar(255),\n" +
				"  `COEFICIENTE` double,\n" +
				"  `DWB_IDENTITY` varchar(64),\n" +
				"  PRIMARY KEY (`ID`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
			"call dolt_commit('-A', '-m', 'create tables on main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT statement FROM dolt_patch('main', 'other') ORDER BY statement_order",
				Expected: []sql.UntypedSqlRow{
					{"DROP TABLE `MOTOR_TARIFA_COEFICIENTE_RIESGO_060`;"},
					{"DROP TABLE `MOTOR_TARIFA_COEFICIENTE_RIESGO_060_01`;"},
					{"CREATE TABLE `MOTOR_TARIFA_COEFICIENTE_RIESGO_144_075` (\n" +
						"  `ID` varchar(8) NOT NULL,\n" +
						"  `PRODUCTO` varchar(255),\n" +
						"  `TARIFA` int,\n" +
						"  `VERSION_INICIO` int,\n" +
						"  `VERSION_FIN` int,\n" +
						"  `COBERTURA` varchar(255),\n" +
						"  `INDICADOR_NM` varchar(255),\n" +
						"  `ANOS_COMPANIA_PROCEDENCIA_MIN` int,\n" +
						"  `ANOS_COMPANIA_PROCEDENCIA_MAX` int,\n" +
						"  `COEFICIENTE` double,\n" +
						"  `DWB_IDENTITY` varchar(64),\n" +
						"  PRIMARY KEY (`ID`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"},
					{"CREATE TABLE `MOTOR_TARIFA_COEFICIENTE_RIESGO_144_075_01` (\n" +
						"  `ID` varchar(8) NOT NULL,\n" +
						"  `PRODUCTO` varchar(255),\n" +
						"  `TARIFA` int,\n" +
						"  `VERSION_INICIO` int,\n" +
						"  `VERSION_FIN` int,\n" +
						"  `COBERTURA` varchar(255),\n" +
						"  `INDICADOR_NM` varchar(255),\n" +
						"  `ANOS_COMPANIA_PROCEDENCIA_MIN` int,\n" +
						"  `ANOS_COMPANIA_PROCEDENCIA_MAX` int,\n" +
						"  `COEFICIENTE` double,\n" +
						"  `DWB_IDENTITY` varchar(64),\n" +
						"  PRIMARY KEY (`ID`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"},
				},
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
				Expected: []sql.UntypedSqlRow{{7}},
			},
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF WHERE commit_hash = @Commit1;",
				Expected: []sql.UntypedSqlRow{{3}},
			},
			{
				Query:    "SELECT * FROM DOLT_DIFF WHERE commit_hash = @Commit1 AND committer <> 'root';",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "SELECT commit_hash, committer FROM DOLT_DIFF WHERE commit_hash <> @Commit1 AND committer = 'root' AND commit_hash NOT IN ('WORKING','STAGED');",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "SELECT commit_hash, table_name FROM DOLT_DIFF WHERE commit_hash = 'WORKING'",
				Expected: []sql.UntypedSqlRow{
					{"WORKING", "newRenamedEmptyTable"},
					{"WORKING", "regularTable"},
				},
			},
			{
				Query: "SELECT commit_hash, table_name FROM DOLT_DIFF WHERE commit_hash = 'STAGED'",
				Expected: []sql.UntypedSqlRow{
					{"STAGED", "addedTable"},
					{"STAGED", "droppedTable"},
				},
			},
			{
				Query: "SELECT commit_hash, table_name FROM DOLT_DIFF WHERE commit_hash <> @Commit1 AND commit_hash NOT IN ('STAGED') ORDER BY table_name;",
				Expected: []sql.UntypedSqlRow{
					{"WORKING", "newRenamedEmptyTable"},
					{"WORKING", "regularTable"},
				},
			},
			{
				Query: "SELECT commit_hash, table_name FROM DOLT_DIFF WHERE commit_hash <> @Commit1 OR committer <> 'root' ORDER BY table_name;",
				Expected: []sql.UntypedSqlRow{
					{"STAGED", "addedTable"},
					{"STAGED", "droppedTable"},
					{"WORKING", "newRenamedEmptyTable"},
					{"WORKING", "regularTable"},
				},
			},
			{
				Query: "SELECT * FROM DOLT_DIFF WHERE COMMIT_HASH in ('WORKING', 'STAGED') ORDER BY table_name;",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{{6}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash = @Commit1",
				Expected: []sql.UntypedSqlRow{{"x", true, true}, {"y", true, false}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit2)",
				Expected: []sql.UntypedSqlRow{{"z", true, true}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit3)",
				Expected: []sql.UntypedSqlRow{{"y", false, true}, {"z", false, true}},
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
				Expected: []sql.UntypedSqlRow{{5}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit1)",
				Expected: []sql.UntypedSqlRow{{"x", true, true}, {"y", true, false}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit2)",
				Expected: []sql.UntypedSqlRow{{"z", true, true}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit3)",
				Expected: []sql.UntypedSqlRow{{"x1", true, true}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit4)",
				Expected: []sql.UntypedSqlRow{{"x2", true, false}},
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
				Expected: []sql.UntypedSqlRow{{4}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit1)",
				Expected: []sql.UntypedSqlRow{{"x", true, true}, {"y", true, false}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit2)",
				Expected: []sql.UntypedSqlRow{{"x", true, true}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit3)",
				Expected: []sql.UntypedSqlRow{{"y", true, false}},
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
				Expected: []sql.UntypedSqlRow{{3}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit1)",
				Expected: []sql.UntypedSqlRow{{"x", true, true}, {"y", true, false}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit2)",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit3)",
				Expected: []sql.UntypedSqlRow{{"y", false, true}},
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
				Expected: []sql.UntypedSqlRow{{5}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit1)",
				Expected: []sql.UntypedSqlRow{{"x", true, true}, {"y", true, false}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit2)",
				Expected: []sql.UntypedSqlRow{{"z", true, true}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit3)",
				Expected: []sql.UntypedSqlRow{{"y", false, true}, {"z", false, true}},
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
			"insert into x values (1, 2, 3), (2, 3, 4)",
			"set @Commit1 = ''",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-Am', 'Creating tables x and y')",

			"CALL DOLT_checkout('-b', 'branch2', 'HEAD~1')",
			"create table z (a int primary key, b int, c int)",
			"insert into z values (100, 101, 102)",
			"set @Commit2 = ''",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit2, '-Am', 'Creating tables z')",

			"CALL DOLT_MERGE('--no-commit', 'branch1')",
			"set @Commit3 = ''",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit3, '-am', 'Merging branch1 into branch2')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF",
				Expected: []sql.UntypedSqlRow{{5}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit1)",
				Expected: []sql.UntypedSqlRow{{"x", true, true}, {"y", true, false}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit2)",
				Expected: []sql.UntypedSqlRow{{"z", true, true}},
			},
			{
				Query: "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit3) order by table_name",
				Expected: []sql.UntypedSqlRow{
					{"x", true, true},
					{"y", true, false},
				},
			},
		},
	},
}

var ColumnDiffSystemTableScriptTests = []queries.ScriptTest{
	{
		Name: "table changes - commit history",
		SetUpScript: []string{
			"create table modifiedTable (a int primary key, b int);",
			"insert into modifiedTable values (1, 2), (2, 3);",
			"create table droppedTable (a int primary key, b int);",
			"insert into droppedTable values (1, 2), (2, 3);",
			"create table renamedTable (a int primary key, b int);",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'creating tables');",

			"update modifiedTable set b = 5 where a = 1;",
			"drop table droppedTable;",
			"rename table renamedTable to newRenamedTable;",
			"create table addedTable (a int primary key, b int);",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'make table changes');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE table_name = 'modifiedTable';",
				Expected: []sql.UntypedSqlRow{
					{"modifiedTable", "a", "added"},
					{"modifiedTable", "b", "added"},
					{"modifiedTable", "b", "modified"},
				},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE table_name = 'droppedTable';",
				Expected: []sql.UntypedSqlRow{
					{"droppedTable", "a", "added"},
					{"droppedTable", "b", "added"},
					{"droppedTable", "a", "removed"},
					{"droppedTable", "b", "removed"},
				},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE table_name = 'renamedTable' OR table_name = 'newRenamedTable';",
				Expected: []sql.UntypedSqlRow{
					{"renamedTable", "a", "added"},
					{"renamedTable", "b", "added"},
				},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE table_name = 'addedTable';",
				Expected: []sql.UntypedSqlRow{
					{"addedTable", "a", "added"},
					{"addedTable", "b", "added"},
				},
			},
		},
	},
	{
		Name: "table changes - working set",
		SetUpScript: []string{
			"create table modifiedTable (a int primary key, b int);",
			"insert into modifiedTable values (1, 2), (2, 3);",
			"create table droppedTable (a int primary key, b int);",
			"insert into droppedTable values (1, 2), (2, 3);",
			"create table renamedTable (a int primary key, b int);",
			"call dolt_add('.')",

			"update modifiedTable set b = 5 where a = 1;",
			"drop table droppedTable;",
			"rename table renamedTable to newRenamedTable;",
			"create table addedTable (a int primary key, b int);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT commit_hash, table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE table_name = 'modifiedTable' ORDER BY commit_hash, table_name, column_name;",
				Expected: []sql.UntypedSqlRow{
					{"STAGED", "modifiedTable", "a", "added"},
					{"STAGED", "modifiedTable", "b", "added"},
					{"WORKING", "modifiedTable", "b", "modified"},
				},
			},
			{
				Query: "SELECT commit_hash, table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE table_name = 'droppedTable' ORDER BY commit_hash, table_name, column_name;",
				Expected: []sql.UntypedSqlRow{
					{"STAGED", "droppedTable", "a", "added"},
					{"STAGED", "droppedTable", "b", "added"},
					{"WORKING", "droppedTable", "a", "removed"},
					{"WORKING", "droppedTable", "b", "removed"},
				},
			},
			{
				Query: "SELECT commit_hash, table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE table_name = 'renamedTable' OR table_name = 'newRenamedTable' ORDER BY commit_hash, table_name, column_name;",
				Expected: []sql.UntypedSqlRow{
					{"STAGED", "renamedTable", "a", "added"},
					{"STAGED", "renamedTable", "b", "added"},
				},
			},
			{
				Query: "SELECT commit_hash, table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE table_name = 'addedTable' ORDER BY commit_hash, table_name, column_name;",
				Expected: []sql.UntypedSqlRow{
					{"WORKING", "addedTable", "a", "added"},
					{"WORKING", "addedTable", "b", "added"},
				},
			},
		},
	},
	{
		Name: "add column - commit history",
		SetUpScript: []string{
			"create table t (pk int primary key, c int);",
			"call dolt_add('.')",
			"insert into t values (1, 2), (3, 4);",
			"set @Commit1 = '';",
			"call dolt_commit_hash_out(@Commit1, '-am', 'creating table t');",

			"alter table t add column d int;",
			"set @Commit2 = '';",
			"call dolt_add('.')",
			"call dolt_commit_hash_out(@Commit2, '-m', 'updating d in t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select count(*) from dolt_column_diff where commit_hash = @Commit1;",
				Expected: []sql.UntypedSqlRow{{2}},
			},
			{
				Query:    "select table_name, column_name, diff_type from dolt_column_diff where commit_hash = @Commit2;",
				Expected: []sql.UntypedSqlRow{{"t", "d", "added"}},
			},
		},
	},
	{
		Name: "add column - working set",
		SetUpScript: []string{
			"create table t (pk int primary key, c int);",
			"insert into t values (1, 2), (3, 4);",
			"call dolt_add('.')",

			"alter table t add column d int;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select count(*) from dolt_column_diff where commit_hash = 'STAGED';",
				Expected: []sql.UntypedSqlRow{{2}},
			},
			{
				Query:    "select table_name, column_name, diff_type from dolt_column_diff where commit_hash = 'WORKING';",
				Expected: []sql.UntypedSqlRow{{"t", "d", "added"}},
			},
		},
	},
	{
		Name: "modify column - commit history",
		SetUpScript: []string{
			"create table t (pk int primary key, c int);",
			"call dolt_add('.')",
			"insert into t values (1, 2), (3, 4);",
			"set @Commit1 = '';",
			"call dolt_commit_hash_out(@Commit1, '-am', 'creating table t');",

			"update t set c = 5 where pk = 3;",
			"call dolt_add('.')",
			"set @Commit2 = '';",
			"call dolt_commit_hash_out(@Commit2, '-am', 'updating value in t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select count(*) from dolt_column_diff where commit_hash = @Commit1;",
				Expected: []sql.UntypedSqlRow{{2}},
			},
			{
				Query:    "select table_name, column_name, diff_type from dolt_column_diff where commit_hash = @Commit2;",
				Expected: []sql.UntypedSqlRow{{"t", "c", "modified"}},
			},
		},
	},
	{
		Name: "modify column - working set",
		SetUpScript: []string{
			"create table t (pk int primary key, c int);",
			"insert into t values (1, 2), (3, 4);",
			"call dolt_add('.')",

			"update t set c = 5 where pk = 3;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select count(*) from dolt_column_diff where commit_hash = 'STAGED';",
				Expected: []sql.UntypedSqlRow{{2}},
			},
			{
				Query:    "select table_name, column_name, diff_type from dolt_column_diff where commit_hash = 'WORKING';",
				Expected: []sql.UntypedSqlRow{{"t", "c", "modified"}},
			},
		},
	},
	{
		Name: "drop column - commit history",
		SetUpScript: []string{
			"create table t (pk int primary key, c int);",
			"call dolt_add('.')",
			"insert into t values (1, 2), (3, 4);",
			"set @Commit1 = '';",
			"call dolt_commit_hash_out(@Commit1, '-am', 'creating table t');",

			"alter table t drop column c;",
			"call dolt_add('.')",
			"set @Commit2 = '';",
			"call dolt_commit_hash_out(@Commit2, '-am', 'dropping column c in t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select count(*) from dolt_column_diff where commit_hash = @Commit1;",
				Expected: []sql.UntypedSqlRow{{2}},
			},
			{
				Query:    "select table_name, column_name, diff_type from dolt_column_diff where commit_hash = @Commit2;",
				Expected: []sql.UntypedSqlRow{{"t", "c", "removed"}},
			},
		},
	},
	{
		Name: "drop column - working set",
		SetUpScript: []string{
			"create table t (pk int primary key, c int);",
			"insert into t values (1, 2), (3, 4);",
			"call dolt_add('.')",

			"alter table t drop column c;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select count(*) from dolt_column_diff where commit_hash = 'STAGED';",
				Expected: []sql.UntypedSqlRow{{2}},
			},
			{
				Query:    "select table_name, column_name, diff_type from dolt_column_diff where commit_hash = 'WORKING';",
				Expected: []sql.UntypedSqlRow{{"t", "c", "removed"}},
			},
		},
	},
	{
		Name: "drop column and recreate with same type - commit history",
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
				Query:    "SELECT COUNT(*) FROM DOLT_COLUMN_DIFF;",
				Expected: []sql.UntypedSqlRow{{5}},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash=@Commit1;",
				Expected: []sql.UntypedSqlRow{
					{"t", "pk", "added"},
					{"t", "c", "added"},
				},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash=@Commit2;",
				Expected: []sql.UntypedSqlRow{
					{"t", "c", "removed"},
				},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash=@Commit3",
				Expected: []sql.UntypedSqlRow{
					{"t", "pk", "modified"},
					{"t", "c", "added"},
				},
			},
		},
	},
	{
		Name: "drop column and recreate with same type - working set",
		SetUpScript: []string{
			"create table t (pk int primary key, c int);",
			"insert into t values (1, 2), (3, 4);",
			"call dolt_add('.')",

			"alter table t drop column c;",
			"alter table t add column c int;",
			"insert into t values (100, 101);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_COLUMN_DIFF;",
				Expected: []sql.UntypedSqlRow{{4}},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash='STAGED';",
				Expected: []sql.UntypedSqlRow{
					{"t", "pk", "added"},
					{"t", "c", "added"},
				},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash='WORKING';",
				Expected: []sql.UntypedSqlRow{
					{"t", "pk", "modified"},
					{"t", "c", "modified"},
				},
			},
		},
	},
	{
		Name: "drop column, then rename column with same type to same name - commit history",
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
				Query:    "SELECT COUNT(*) FROM DOLT_COLUMN_DIFF;",
				Expected: []sql.UntypedSqlRow{{6}},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash=@Commit1;",
				Expected: []sql.UntypedSqlRow{
					{"t", "pk", "added"},
					{"t", "c1", "added"},
					{"t", "c2", "added"},
				},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash=@Commit2;",
				Expected: []sql.UntypedSqlRow{
					{"t", "c1", "removed"},
				},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash=@Commit3;",
				Expected: []sql.UntypedSqlRow{
					{"t", "pk", "modified"},
					{"t", "c1", "modified"},
				},
			},
		},
	},
	{
		Name: "drop column, then rename column with same type to same name - working set",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 int);",
			"insert into t values (1, 2, 3), (4, 5, 6);",
			"call dolt_add('.')",

			"alter table t drop column c1;",
			"alter table t rename column c2 to c1;",
			"insert into t values (100, 101);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_COLUMN_DIFF;",
				Expected: []sql.UntypedSqlRow{{6}},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash='STAGED';",
				Expected: []sql.UntypedSqlRow{
					{"t", "pk", "added"},
					{"t", "c1", "added"},
					{"t", "c2", "added"},
				},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash='WORKING';",
				Expected: []sql.UntypedSqlRow{
					{"t", "pk", "modified"},
					{"t", "c1", "removed"},
					{"t", "c1", "modified"},
				},
			},
		},
	},
	{
		Name: "column drop and recreate with different type that can be coerced (int -> string) - commit history",
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
				Query:    "SELECT COUNT(*) FROM DOLT_COLUMN_DIFF;",
				Expected: []sql.UntypedSqlRow{{5}},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash=@Commit1;",
				Expected: []sql.UntypedSqlRow{
					{"t", "pk", "added"},
					{"t", "c", "added"},
				},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash=@Commit2;",
				Expected: []sql.UntypedSqlRow{
					{"t", "c", "removed"},
				},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash=@Commit3;",
				Expected: []sql.UntypedSqlRow{
					{"t", "pk", "modified"},
					{"t", "c", "added"},
				},
			},
		},
	},
	{
		Name: "column drop and recreate with different type that can be coerced (int -> string) - working set",
		SetUpScript: []string{
			"create table t (pk int primary key, c int);",
			"insert into t values (1, 2), (3, 4);",
			"call dolt_add('.')",

			"alter table t drop column c;",
			"alter table t add column c varchar(20);",
			"insert into t values (100, '101');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_COLUMN_DIFF;",
				Expected: []sql.UntypedSqlRow{{5}},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash='STAGED';",
				Expected: []sql.UntypedSqlRow{
					{"t", "pk", "added"},
					{"t", "c", "added"},
				},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash='WORKING';",
				Expected: []sql.UntypedSqlRow{
					{"t", "pk", "modified"},
					{"t", "c", "removed"},
					{"t", "c", "added"},
				},
			},
		},
	},
	{
		Name: "column drop and recreate with different type that can NOT be coerced (string -> int) - commit history",
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
				Query:    "SELECT COUNT(*) FROM DOLT_COLUMN_DIFF;",
				Expected: []sql.UntypedSqlRow{{5}},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash=@Commit1;",
				Expected: []sql.UntypedSqlRow{
					{"t", "pk", "added"},
					{"t", "c", "added"},
				},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash=@Commit2;",
				Expected: []sql.UntypedSqlRow{
					{"t", "c", "removed"},
				},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash=@Commit3;",
				Expected: []sql.UntypedSqlRow{
					{"t", "pk", "modified"},
					{"t", "c", "added"},
				},
			},
		},
	},
	{
		Name: "column drop and recreate with different type that can NOT be coerced (string -> int) - working set",
		SetUpScript: []string{
			"create table t (pk int primary key, c varchar(20));",
			"insert into t values (1, 'two'), (3, 'four');",
			"call dolt_add('.')",

			"alter table t drop column c;",
			"alter table t add column c int;",
			"insert into t values (100, 101);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_COLUMN_DIFF;",
				Expected: []sql.UntypedSqlRow{{5}},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash='STAGED';",
				Expected: []sql.UntypedSqlRow{
					{"t", "pk", "added"},
					{"t", "c", "added"},
				},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash='WORKING';",
				Expected: []sql.UntypedSqlRow{
					{"t", "pk", "modified"},
					{"t", "c", "removed"},
					{"t", "c", "added"},
				},
			},
		},
	},
	{
		Name: "multiple column renames - commit history",
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
				Query:    "SELECT COUNT(*) FROM DOLT_COLUMN_DIFF;",
				Expected: []sql.UntypedSqlRow{{7}},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash=@Commit1;",
				Expected: []sql.UntypedSqlRow{
					{"t", "pk", "added"},
					{"t", "c1", "added"},
				},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash=@Commit2;",
				Expected: []sql.UntypedSqlRow{
					{"t", "pk", "modified"},
					{"t", "c2", "modified"},
				},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash=@Commit3;",
				Expected: []sql.UntypedSqlRow{
					{"t", "c2", "removed"},
				},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash=@Commit4;",
				Expected: []sql.UntypedSqlRow{
					{"t", "pk", "modified"},
					{"t", "c2", "added"},
				},
			},
		},
	},
	{
		Name: "multiple column renames - working set",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int);",
			"insert into t values (1, 2);",
			"call dolt_add('.')",

			"alter table t rename column c1 to c2;",
			"insert into t values (3, 4);",

			"alter table t drop column c2;",
			"alter table t add column c2 int;",
			"insert into t values (100, '101');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_COLUMN_DIFF;",
				Expected: []sql.UntypedSqlRow{{5}},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash='STAGED';",
				Expected: []sql.UntypedSqlRow{
					{"t", "pk", "added"},
					{"t", "c1", "added"},
				},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash='WORKING';",
				Expected: []sql.UntypedSqlRow{
					{"t", "pk", "modified"},
					{"t", "c1", "removed"},
					{"t", "c2", "added"},
				},
			},
		},
	},
	{
		Name: "primary key change - commit history",
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
				Query:    "SELECT COUNT(*) FROM DOLT_COLUMN_DIFF;",
				Expected: []sql.UntypedSqlRow{{8}},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash=@Commit1;",
				Expected: []sql.UntypedSqlRow{
					{"t", "pk", "added"},
					{"t", "c1", "added"},
				},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash=@Commit2;",
				Expected: []sql.UntypedSqlRow{
					{"t", "pk", "modified"},
					{"t", "c1", "modified"},
				},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash=@Commit3;",
				Expected: []sql.UntypedSqlRow{
					{"t", "pk", "modified"},
					{"t", "c1", "modified"},
				},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash=@Commit4;",
				Expected: []sql.UntypedSqlRow{
					{"t", "pk", "modified"},
					{"t", "c1", "modified"},
				},
			},
		},
	},
	{
		Name: "primary key change - working set",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int);",
			"insert into t values (1, 2), (3, 4);",
			"call dolt_add('.')",

			"alter table t drop primary key;",
			"alter table t add primary key (c1);",
			"insert into t values (7, 8);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_COLUMN_DIFF;",
				Expected: []sql.UntypedSqlRow{{4}},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash='STAGED';",
				Expected: []sql.UntypedSqlRow{
					{"t", "pk", "added"},
					{"t", "c1", "added"},
				},
			},
			{
				Query: "SELECT table_name, column_name, diff_type FROM DOLT_COLUMN_DIFF WHERE commit_hash='WORKING';",
				Expected: []sql.UntypedSqlRow{
					{"t", "pk", "modified"},
					{"t", "c1", "modified"},
				},
			},
		},
	},
	{
		Name: "json column change",
		SetUpScript: []string{
			"create table t (pk int primary key, j json);",
			`insert into t values (1, '{"test": 123}');`,
			"call dolt_add('.')",
			"call dolt_commit('-m', 'commit1');",

			`update t set j = '{"nottest": 321}'`,
			"call dolt_add('.')",
			"call dolt_commit('-m', 'commit2');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select column_name, diff_type from dolt_column_diff;",
				Expected: []sql.UntypedSqlRow{
					{"j", "modified"},
					{"pk", "added"},
					{"j", "added"},
				},
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
				ExpectedErrStr: dtables.ErrInvalidCommitDiffTableArgs.Error(),
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
				Expected: []sql.UntypedSqlRow{
					{1, 2, 3, nil, nil, nil, "added"},
					{4, 5, 6, nil, nil, nil, "added"},
				},
			},
			{
				// Test case-insensitive table name
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_COMMIT_DIFF_T WHERE TO_COMMIT=@Commit1 and FROM_COMMIT=@Commit0;",
				Expected: []sql.UntypedSqlRow{
					{1, 2, 3, nil, nil, nil, "added"},
					{4, 5, 6, nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit2 and FROM_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
					{1, 2, 0, 1, 2, 3, "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_COMMIT_DIFF_T WHERE TO_COMMIT=@Commit4 and FROM_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
					{1, 2, -2, 1, 2, 3, "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_commit_DIFF_t WHERE TO_COMMIT=@Commit5 and FROM_COMMIT=@Commit4 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
					{nil, nil, nil, 1, 2, -2, "removed"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit5 and FROM_COMMIT=@Commit0 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{
					{1, 3, nil, nil},
					{4, 6, nil, nil},
				},
			},
			{
				Query: "SELECT to_pk, to_c2, from_pk, from_c2 FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit2 and FROM_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{
					{1, 2, nil, nil, "added"},
					{3, 4, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, from_pk, from_c, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit2 and FROM_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
					{1, 1, 2, "modified"},
					{3, 3, 4, "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit3 and FROM_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{
					{1, 2, nil, nil, "added"},
					{4, 5, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit2 and FROM_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
					{1, nil, 1, 2, "modified"},
					{4, nil, 4, 5, "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit3 and FROM_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
					// TODO: Missing rows here see TestDiffSystemTable tests
					{100, 101, nil, nil, "added"},
				},
			},
		},
	},
	{
		Name: "working and staged commits",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 int);",
			"call dolt_commit('-Am', 'created table');",
			"set @Commit0 = HASHOF('HEAD');",
			"insert into t values (1, 2, 3);",
			"call dolt_add('.');",
			"insert into t values (4, 5, 6);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT='WORKING' and FROM_COMMIT=@Commit0;",
				Expected: []sql.UntypedSqlRow{
					{1, 2, 3, nil, nil, nil, "added"},
					{4, 5, 6, nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT='STAGED' and FROM_COMMIT=@Commit0;",
				Expected: []sql.UntypedSqlRow{
					{1, 2, 3, nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT='WORKING' and FROM_COMMIT='STAGED';",
				Expected: []sql.UntypedSqlRow{
					{4, 5, 6, nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT='STAGED' and FROM_COMMIT='WORKING';",
				Expected: []sql.UntypedSqlRow{
					{nil, nil, nil, 4, 5, 6, "removed"},
				},
			},
		},
	},
	{
		// When in a detached head mode, dolt_commit_diff should still work, even though it doesn't have a staged root
		Name: "detached head",
		SetUpScript: []string{
			"CREATE TABLE t (pk int primary key, c1 varchar(100));",
			"CALL dolt_commit('-Am', 'create table t');",
			"SET @commit1 = hashof('HEAD');",
			"INSERT INTO t VALUES (1, 'one');",
			"CALL dolt_commit('-Am', 'insert 1');",
			"SET @commit2 = hashof('HEAD');",
			"CALL dolt_tag('v1', @commit2);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "use mydb/v1;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				// With no working set, this query should still compute the diff between two commits
				Query:    "SELECT COUNT(*) AS table_diff_num FROM dolt_commit_diff_t WHERE from_commit=@commit1 AND to_commit=@commit2;",
				Expected: []sql.UntypedSqlRow{{1}},
			},
			{
				// With no working set, STAGED should reference the current root of the checked out tag
				Query:    "SELECT COUNT(*) AS table_diff_num FROM dolt_commit_diff_t WHERE from_commit=@commit1 AND to_commit='STAGED';",
				Expected: []sql.UntypedSqlRow{{1}},
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
				Expected: []sql.UntypedSqlRow{
					{1, "2", nil, nil, "added"},
					{3, "4", nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit2 and FROM_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
					{1, nil, 1, "2", "modified"},
					{3, nil, 3, "4", "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit3 and FROM_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{
					{1, nil, nil, nil, "added"},
					{3, nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit2 and FROM_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
					{1, nil, 1, nil, "modified"},
					{3, nil, 3, nil, "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit3 and FROM_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{{7, 8, nil, nil, "added"}},
			},
		},
	},
	{
		Name: "added and dropped table",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int);",
			"call dolt_add('.')",
			"insert into t values (1, 2), (3, 4);",
			"set @Commit1 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit1, '-am', 'creating table t');",

			"drop table t",
			"set @Commit2 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit2, '-am', 'dropping table');",

			"create table unrelated (a int primary key);",
			"set @Commit3 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit3, '-Am', 'created unrelated table');",

			"create table t (pk int primary key, c1 int);",
			"call dolt_add('.')",
			"insert into t values (1, 2);",
			"set @Commit4 = '';",
			"CALL DOLT_COMMIT_HASH_OUT(@Commit4, '-Am', 'recreating table t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:                           "select * from dolt_commit_diff_t where from_commit=@Commit2 and to_commit=@Commit3;",
				ExpectedWarning:                 1105,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "cannot render full diff between commits",
				Expected:                        []sql.UntypedSqlRow{},
			},
			{
				Query:                           "select * from dolt_commit_diff_t where from_commit=@Commit3 and to_commit=@Commit3;",
				ExpectedWarning:                 1105,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "cannot render full diff between commits",
				Expected:                        []sql.UntypedSqlRow{},
			},
			{
				Query:    "SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM DOLT_commit_DIFF_t where from_commit=@Commit3 and to_commit=@Commit4;",
				Expected: []sql.UntypedSqlRow{{1, 2, nil, nil, "added"}},
			},
			{
				Query:    "SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM DOLT_commit_DIFF_t where from_commit=@Commit1 and to_commit=@Commit4;",
				Expected: []sql.UntypedSqlRow{{nil, nil, 3, 4, "removed"}},
			},
		},
	},
}

var SchemaDiffTableFunctionScriptTests = []queries.ScriptTest{
	{
		Name: "basic schema changes",
		SetUpScript: []string{
			"create table employees (pk int primary key, name varchar(50));",
			"create table vacations (pk int primary key, name varchar(50));",
			"call dolt_add('.');",
			"set @Commit0 = '';",
			"call dolt_commit_hash_out(@Commit0, '-am', 'commit 0');",

			"call dolt_checkout('-b', 'branch1');",
			"create table inventory (pk int primary key, name varchar(50), quantity int);",
			"drop table employees;",
			"rename table vacations to trips;",
			"call dolt_add('.');",
			"set @Commit1 = '';",
			"call dolt_commit_hash_out(@Commit1, '-am', 'commit 1');",
			"call dolt_tag('tag1');",

			"call dolt_checkout('-b', 'branch2');",
			"alter table inventory drop column quantity, add column color varchar(10);",
			"call dolt_add('.');",
			"set @Commit2 = '';",
			"call dolt_commit_hash_out(@Commit2, '-m', 'commit 2');",
			"call dolt_tag('tag2');",

			"call dolt_checkout('-b', 'branch3');",
			"insert into inventory values (1, 2, 3);",
			"call dolt_add('.');",
			"set @Commit3 = '';",
			"call dolt_commit_hash_out(@Commit3, '-m', 'commit 3');",
			"call dolt_tag('tag3');",
		},
		Assertions: []queries.ScriptTestAssertion{
			// Error cases
			{
				Query:          "select * from dolt_schema_diff();",
				ExpectedErrStr: "function 'dolt_schema_diff' expected 1 to 3 arguments, 0 received",
			},
			{
				Query:          "select * from dolt_schema_diff('HEAD');",
				ExpectedErrStr: "Invalid argument to dolt_schema_diff: There are less than 2 arguments present, and the first does not contain '..'",
			},
			{
				Query:          "select * from dolt_schema_diff(@Commit1);",
				ExpectedErrStr: "Invalid argument to dolt_schema_diff: There are less than 2 arguments present, and the first does not contain '..'",
			},
			{
				Query:          "select * from dolt_schema_diff('branc1');",
				ExpectedErrStr: "Invalid argument to dolt_schema_diff: There are less than 2 arguments present, and the first does not contain '..'",
			},
			{
				Query:          "select * from dolt_schema_diff('tag1');",
				ExpectedErrStr: "Invalid argument to dolt_schema_diff: There are less than 2 arguments present, and the first does not contain '..'",
			},
			{
				Query:          "select * from dolt_schema_diff('HEAD', '');",
				ExpectedErrStr: "expected strings for from and to revisions, got: HEAD, ",
			},
			{
				Query:          "select * from dolt_schema_diff('tag1', '');",
				ExpectedErrStr: "expected strings for from and to revisions, got: tag1, ",
			},
			{
				Query:          "select * from dolt_schema_diff('HEAD', 'inventory');",
				ExpectedErrStr: "branch not found: inventory",
			},
			{
				Query:          "select * from dolt_schema_diff('inventory');",
				ExpectedErrStr: "Invalid argument to dolt_schema_diff: There are less than 2 arguments present, and the first does not contain '..'",
			},
			{
				Query:          "select * from dolt_schema_diff('tag3', 'tag4');",
				ExpectedErrStr: "branch not found: tag4",
			},
			{
				Query:          "select * from dolt_schema_diff('tag3', 'tag4', 'inventory');",
				ExpectedErrStr: "branch not found: tag4",
			},
			// Empty diffs due to same refs
			{
				Query:    "select * from dolt_schema_diff('HEAD', 'HEAD');",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "select * from dolt_schema_diff(@Commit1, @Commit1);",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "select * from dolt_schema_diff('branch1', 'branch1');",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "select * from dolt_schema_diff('tag1', 'tag1');",
				Expected: []sql.UntypedSqlRow{},
			},
			// Empty diffs due to fake table
			{
				Query:    "select * from dolt_schema_diff(@Commit1, @Commit2, 'fake-table');",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "select * from dolt_schema_diff('tag1', 'tag2', 'fake-table');",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "select * from dolt_schema_diff('branch1', 'branch2', 'fake-table');",
				Expected: []sql.UntypedSqlRow{},
			},
			// Empty diffs due to no changes between different commits
			{
				Query:    "select * from dolt_schema_diff(@Commit2, @Commit3);",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "select * from dolt_schema_diff(@Commit2, @Commit3, 'inventory');",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "select * from dolt_schema_diff('tag2', 'tag3');",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "select * from dolt_schema_diff('tag2', 'tag3', 'inventory');",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "select * from dolt_schema_diff('branch2', 'branch3');",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "select * from dolt_schema_diff('branch2', 'branch3', 'inventory');",
				Expected: []sql.UntypedSqlRow{},
			},
			// Compare diffs where between from and to where: tables are added, tables are dropped, tables are renamed
			{
				Query: "select * from dolt_schema_diff(@Commit0, @Commit1);",
				Expected: []sql.UntypedSqlRow{
					{"employees", "", "CREATE TABLE `employees` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;", ""},
					{"", "inventory", "", "CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `quantity` int,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"},
					{"vacations", "trips", "CREATE TABLE `vacations` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;", "CREATE TABLE `trips` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"},
				},
			},
			{
				Query: "select * from dolt_schema_diff(@Commit1, @Commit0);",
				Expected: []sql.UntypedSqlRow{
					{"inventory", "", "CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `quantity` int,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;", ""},
					{"", "employees", "", "CREATE TABLE `employees` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"},
					{"trips", "vacations", "CREATE TABLE `trips` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;", "CREATE TABLE `vacations` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"},
				},
			},
			// Compare diffs with explicit table names
			{
				Query: "select * from dolt_schema_diff(@Commit0, @Commit1, 'employees');",
				Expected: []sql.UntypedSqlRow{
					{"employees", "", "CREATE TABLE `employees` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;", ""},
				},
			},
			{
				Query: "select * from dolt_schema_diff(@Commit1, @Commit0, 'employees');",
				Expected: []sql.UntypedSqlRow{
					{"", "employees", "", "CREATE TABLE `employees` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"},
				},
			},
			{
				Query: "select * from dolt_schema_diff(@Commit0, @Commit1, 'inventory');",
				Expected: []sql.UntypedSqlRow{
					{"", "inventory", "", "CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `quantity` int,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"},
				},
			},
			{
				Query: "select * from dolt_schema_diff(@Commit1, @Commit0, 'inventory');",
				Expected: []sql.UntypedSqlRow{
					{"inventory", "", "CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `quantity` int,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;", ""},
				},
			},
			{
				Query: "select * from dolt_schema_diff(@Commit0, @Commit1, 'trips');",
				Expected: []sql.UntypedSqlRow{
					{"vacations", "trips", "CREATE TABLE `vacations` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;", "CREATE TABLE `trips` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"},
				},
			},
			{
				Query: "select * from dolt_schema_diff(@Commit1, @Commit0, 'trips');",
				Expected: []sql.UntypedSqlRow{
					{"trips", "vacations", "CREATE TABLE `trips` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;", "CREATE TABLE `vacations` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"},
				},
			},
			{
				Query: "select * from dolt_schema_diff(@Commit0, @Commit1, 'vacations');",
				Expected: []sql.UntypedSqlRow{
					{"vacations", "trips", "CREATE TABLE `vacations` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;", "CREATE TABLE `trips` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"},
				},
			},
			{
				Query: "select * from dolt_schema_diff(@Commit1, @Commit0, 'vacations');",
				Expected: []sql.UntypedSqlRow{
					{"trips", "vacations", "CREATE TABLE `trips` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;", "CREATE TABLE `vacations` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"},
				},
			},
			// Compare two different commits, get expected results
			{
				Query: "select * from dolt_schema_diff(@Commit1, @Commit2);",
				Expected: []sql.UntypedSqlRow{
					{
						"inventory",
						"inventory",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `quantity` int,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `color` varchar(10),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					},
				},
			},
			{
				Query: "select * from dolt_schema_diff(@Commit1, @Commit2, 'inventory');",
				Expected: []sql.UntypedSqlRow{
					{
						"inventory",
						"inventory",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `quantity` int,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `color` varchar(10),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					},
				},
			},
			{
				Query: "select * from dolt_schema_diff('branch1', 'branch2');",
				Expected: []sql.UntypedSqlRow{
					{
						"inventory",
						"inventory",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `quantity` int,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `color` varchar(10),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					},
				},
			},
			{
				Query: "select * from dolt_schema_diff('branch1..branch2');",
				Expected: []sql.UntypedSqlRow{
					{
						"inventory",
						"inventory",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `quantity` int,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `color` varchar(10),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					},
				},
			},
			{
				Query: "select * from dolt_schema_diff('branch1...branch2');",
				Expected: []sql.UntypedSqlRow{
					{
						"inventory",
						"inventory",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `quantity` int,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `color` varchar(10),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					},
				},
			},
			{
				Query: "select * from dolt_schema_diff('branch1', 'branch2', 'inventory');",
				Expected: []sql.UntypedSqlRow{
					{
						"inventory",
						"inventory",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `quantity` int,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `color` varchar(10),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					},
				},
			},
			{
				Query: "select * from dolt_schema_diff('tag1', 'tag2');",
				Expected: []sql.UntypedSqlRow{
					{
						"inventory",
						"inventory",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `quantity` int,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `color` varchar(10),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					},
				},
			},
			{
				Query: "select * from dolt_schema_diff('tag1..tag2');",
				Expected: []sql.UntypedSqlRow{
					{
						"inventory",
						"inventory",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `quantity` int,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `color` varchar(10),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					},
				},
			},
			{
				Query: "select * from dolt_schema_diff('tag1...tag2');",
				Expected: []sql.UntypedSqlRow{
					{
						"inventory",
						"inventory",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `quantity` int,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `color` varchar(10),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					},
				},
			},
			{
				Query: "select * from dolt_schema_diff('tag1', 'tag2', 'inventory');",
				Expected: []sql.UntypedSqlRow{
					{
						"inventory",
						"inventory",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `quantity` int,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `color` varchar(10),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					},
				},
			},
			// Swap the order of the refs, get opposite diff
			{
				Query: "select * from dolt_schema_diff(@Commit2, @Commit1);",
				Expected: []sql.UntypedSqlRow{
					{
						"inventory",
						"inventory",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `color` varchar(10),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `quantity` int,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					},
				},
			},
			{
				Query: "select * from dolt_schema_diff(@Commit2, @Commit1, 'inventory');",
				Expected: []sql.UntypedSqlRow{
					{
						"inventory",
						"inventory",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `color` varchar(10),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `quantity` int,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					},
				},
			},
			{
				Query: "select * from dolt_schema_diff('branch2', 'branch1');",
				Expected: []sql.UntypedSqlRow{
					{
						"inventory",
						"inventory",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `color` varchar(10),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `quantity` int,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					},
				},
			},
			{
				Query: "select * from dolt_schema_diff('branch2', 'branch1', 'inventory');",
				Expected: []sql.UntypedSqlRow{
					{
						"inventory",
						"inventory",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `color` varchar(10),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `quantity` int,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					},
				},
			},
			{
				Query: "select * from dolt_schema_diff('tag2', 'tag1');",
				Expected: []sql.UntypedSqlRow{
					{
						"inventory",
						"inventory",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `color` varchar(10),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `quantity` int,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					},
				},
			},
			{
				Query: "select * from dolt_schema_diff('tag2', 'tag1', 'inventory');",
				Expected: []sql.UntypedSqlRow{
					{
						"inventory",
						"inventory",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `color` varchar(10),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
						"CREATE TABLE `inventory` (\n  `pk` int NOT NULL,\n  `name` varchar(50),\n  `quantity` int,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					},
				},
			},
		},
	},
	{
		Name: "prepared table functions",
		SetUpScript: []string{
			"create table t1 (a int primary key)",
			"insert into t1 values (0), (1)",
			"call dolt_add('.');",
			"set @Commit0 = '';",
			"call dolt_commit_hash_out(@Commit0, '-am', 'commit 0');",
			//
			"alter table t1 add column b int default 1",
			"call dolt_add('.');",
			"set @Commit1 = '';",
			"call dolt_commit_hash_out(@Commit1, '-am', 'commit 1');",
			//
			"create table t2 (a int primary key)",
			"insert into t2 values (0), (1)",
			"insert into t1 values (2,2), (3,2)",
			"call dolt_add('.');",
			"set @Commit2 = '';",
			"call dolt_commit_hash_out(@Commit2, '-am', 'commit 2');",
			//
			"prepare sch_diff from 'select count(*) from dolt_schema_diff(?,?,?)'",
			"prepare diff_stat from 'select count(*) from dolt_diff_stat(?,?,?)'",
			"prepare diff_sum from 'select count(*) from dolt_diff_summary(?,?,?)'",
			//"prepare table_diff from 'select * from dolt_diff(?,?,?)'",
			"prepare patch from 'select count(*) from dolt_schema_diff(?,?,?)'",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "set @t1_name = 't1';",
			},
			{
				Query:    "execute sch_diff using @Commit0, @Commit1, @t1_name",
				Expected: []sql.UntypedSqlRow{{1}},
			},
			{
				Query:    "execute diff_stat using @Commit1, @Commit2, @t1_name",
				Expected: []sql.UntypedSqlRow{{1}},
			},
			{
				Query:    "execute diff_sum using @Commit1, @Commit2, @t1_name",
				Expected: []sql.UntypedSqlRow{{1}},
			},
			//{
			//	Query:    "execute table_diff using @Commit2, @Commit2, @t1_name",
			//	Expected: []sql.UntypedSqlRow{},
			//},
			{
				Query:    "execute patch using @Commit0, @Commit1, @t1_name",
				Expected: []sql.UntypedSqlRow{{1}},
			},
		},
	},
}

var DoltDatabaseCollationScriptTests = []queries.ScriptTest{
	{
		Name:        "can't use __DATABASE__ prefix in table names",
		SetUpScript: []string{},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "create table __DATABASE__t(i int);",
				ExpectedErrStr: "Invalid table name __DATABASE__t. Table names beginning with `__DATABASE__` are reserved for internal use",
			},
		},
	},
	{
		Name:        "db collation change with dolt_add('.')",
		SetUpScript: []string{},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_status",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "select commit_hash, table_name, data_change, schema_change from dolt_diff",
				Expected: []sql.UntypedSqlRow{},
			},

			{
				Query: "alter database mydb collate utf8mb4_spanish_ci",
				Expected: []sql.UntypedSqlRow{
					{gmstypes.NewOkResult(1)},
				},
			},
			{
				Query: "select * from dolt_status",
				Expected: []sql.UntypedSqlRow{
					{"__DATABASE__mydb", false, "modified"},
				},
			},
			{
				Query: "select commit_hash, table_name, data_change, schema_change from dolt_diff",
				Expected: []sql.UntypedSqlRow{
					{"WORKING", "__DATABASE__mydb", false, true},
				},
			},

			{
				Query: "call dolt_add('.')",
				Expected: []sql.UntypedSqlRow{
					{0},
				},
			},
			{
				Query: "select * from dolt_status",
				Expected: []sql.UntypedSqlRow{
					{"__DATABASE__mydb", true, "modified"},
				},
			},
			{
				Query: "select commit_hash, table_name, data_change, schema_change from dolt_diff",
				Expected: []sql.UntypedSqlRow{
					{"STAGED", "__DATABASE__mydb", false, true},
				},
			},
			{
				Query: "select message from dolt_log",
				Expected: []sql.UntypedSqlRow{
					{"checkpoint enginetest database mydb"},
					{"Initialize data repository"},
				},
			},

			{
				Query:            "call dolt_commit('-m', 'db collation changed')",
				SkipResultsCheck: true,
			},
			{
				Query:    "select * from dolt_status",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "select table_name, data_change, schema_change from dolt_diff",
				Expected: []sql.UntypedSqlRow{
					{"__DATABASE__mydb", false, true},
				},
			},
			{
				Query: "select message from dolt_log",
				Expected: []sql.UntypedSqlRow{
					{"db collation changed"},
					{"checkpoint enginetest database mydb"},
					{"Initialize data repository"},
				},
			},
		},
	},
	{
		Name:        "db collation change with dolt_add('__DATABASE__mydb')",
		SetUpScript: []string{},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_status",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "select commit_hash, table_name, data_change, schema_change from dolt_diff",
				Expected: []sql.UntypedSqlRow{},
			},

			{
				Query: "alter database mydb collate utf8mb4_spanish_ci",
				Expected: []sql.UntypedSqlRow{
					{gmstypes.NewOkResult(1)},
				},
			},
			{
				Query: "select * from dolt_status",
				Expected: []sql.UntypedSqlRow{
					{"__DATABASE__mydb", false, "modified"},
				},
			},
			{
				Query: "select commit_hash, table_name, data_change, schema_change from dolt_diff",
				Expected: []sql.UntypedSqlRow{
					{"WORKING", "__DATABASE__mydb", false, true},
				},
			},

			{
				Query: "call dolt_add('__DATABASE__mydb')",
				Expected: []sql.UntypedSqlRow{
					{0},
				},
			},
			{
				Query: "select * from dolt_status",
				Expected: []sql.UntypedSqlRow{
					{"__DATABASE__mydb", true, "modified"},
				},
			},
			{
				Query: "select commit_hash, table_name, data_change, schema_change from dolt_diff",
				Expected: []sql.UntypedSqlRow{
					{"STAGED", "__DATABASE__mydb", false, true},
				},
			},
			{
				Query: "select message from dolt_log",
				Expected: []sql.UntypedSqlRow{
					{"checkpoint enginetest database mydb"},
					{"Initialize data repository"},
				},
			},

			{
				Query:            "call dolt_commit('-m', 'db collation changed')",
				SkipResultsCheck: true,
			},
			{
				Query:    "select * from dolt_status",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "select table_name, data_change, schema_change from dolt_diff",
				Expected: []sql.UntypedSqlRow{
					{"__DATABASE__mydb", false, true},
				},
			},
			{
				Query: "select message from dolt_log",
				Expected: []sql.UntypedSqlRow{
					{"db collation changed"},
					{"checkpoint enginetest database mydb"},
					{"Initialize data repository"},
				},
			},
		},
	},
	{
		Name:        "db collation change with dolt_commit('-Am', '')",
		SetUpScript: []string{},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_status",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "select commit_hash, table_name, data_change, schema_change from dolt_diff",
				Expected: []sql.UntypedSqlRow{},
			},

			{
				Query: "alter database mydb collate utf8mb4_spanish_ci",
				Expected: []sql.UntypedSqlRow{
					{gmstypes.NewOkResult(1)},
				},
			},
			{
				Query: "select * from dolt_status",
				Expected: []sql.UntypedSqlRow{
					{"__DATABASE__mydb", false, "modified"},
				},
			},
			{
				Query: "select commit_hash, table_name, data_change, schema_change from dolt_diff",
				Expected: []sql.UntypedSqlRow{
					{"WORKING", "__DATABASE__mydb", false, true},
				},
			},
			{
				Query: "select message from dolt_log",
				Expected: []sql.UntypedSqlRow{
					{"checkpoint enginetest database mydb"},
					{"Initialize data repository"},
				},
			},

			{
				Query:            "call dolt_commit('-Am', 'db collation changed')",
				SkipResultsCheck: true,
			},
			{
				Query:    "select * from dolt_status",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "select table_name, data_change, schema_change from dolt_diff",
				Expected: []sql.UntypedSqlRow{
					{"__DATABASE__mydb", false, true},
				},
			},
			{
				Query: "select message from dolt_log",
				Expected: []sql.UntypedSqlRow{
					{"db collation changed"},
					{"checkpoint enginetest database mydb"},
					{"Initialize data repository"},
				},
			},
		},
	},
	{
		Name: "db collation hard reset",
		SetUpScript: []string{
			"alter database mydb collate utf8mb4_spanish_ci",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select * from dolt_status;",
				Expected: []sql.UntypedSqlRow{
					{"__DATABASE__mydb", false, "modified"},
				},
			},
			{
				Query: "call dolt_reset('--hard');",
				Expected: []sql.UntypedSqlRow{
					{0},
				},
			},
			{
				Query:    "select * from dolt_status;",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name: "db collation with branch",
		SetUpScript: []string{
			"call dolt_checkout('-b', 'other');",
			"alter database mydb collate utf8mb4_spanish_ci;",
			"call dolt_commit('-Am', 'db collation');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "show create database mydb;",
				Expected: []sql.UntypedSqlRow{
					{"mydb", "CREATE DATABASE `mydb` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_spanish_ci */"},
				},
			},
			{
				Query:            "call dolt_checkout('main');",
				SkipResultsCheck: true,
			},
			{
				Query: "show create database mydb;",
				Expected: []sql.UntypedSqlRow{
					{"mydb", "CREATE DATABASE `mydb` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin */"},
				},
			},
		},
	},
	{
		Name: "db collation with ff merge",
		SetUpScript: []string{
			"call dolt_checkout('-b', 'other');",
			"alter database mydb collate utf8mb4_spanish_ci;",
			"call dolt_commit('-Am', 'db collation');",
			"call dolt_checkout('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "show create database mydb;",
				Expected: []sql.UntypedSqlRow{
					{"mydb", "CREATE DATABASE `mydb` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin */"},
				},
			},
			{
				Query:            "call dolt_merge('other');",
				SkipResultsCheck: true,
			},
			{
				Query: "show create database mydb;",
				Expected: []sql.UntypedSqlRow{
					{"mydb", "CREATE DATABASE `mydb` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_spanish_ci */"},
				},
			},
		},
	},
	{
		Name: "db collation merge conflict",
		SetUpScript: []string{
			"call dolt_branch('other');",
			"alter database mydb collate utf8mb4_spanish_ci;",
			"call dolt_commit('-Am', 'main collation');",
			"call dolt_checkout('other');",
			"alter database mydb collate utf8mb4_danish_ci;",
			"call dolt_commit('-Am', 'main collation');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "call dolt_merge('main');",
				ExpectedErrStr: "database collation conflict, please resolve manually. ours: utf8mb4_danish_ci, theirs: utf8mb4_spanish_ci",
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
	exp   []sql.UntypedSqlRow
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
				exp:   []sql.UntypedSqlRow{{20, 20}, {21, 21}, {22, 22}, {23, 23}, {24, 24}, {nil, 40}, {nil, 41}, {nil, 42}, {nil, 43}, {nil, 44}},
			},
			{
				query: "select from_x, to_x from dolt_diff_xy where from_commit = @commit;",
				exp:   []sql.UntypedSqlRow{{40, 40}, {41, 41}, {42, 42}, {43, 43}, {44, 44}, {nil, 60}, {nil, 61}, {nil, 62}, {nil, 63}, {nil, 64}},
			},
			{
				query: "select count(*) from dolt_diff where commit_hash = @commit;",
				exp:   []sql.UntypedSqlRow{{1}},
			},
			{
				query: "select count(*) from dolt_history_xy where commit_hash = @commit;",
				exp:   []sql.UntypedSqlRow{{15}},
			},
			{
				query: "select count(*) from dolt_log where commit_hash = @commit;",
				exp:   []sql.UntypedSqlRow{{1}},
			},
			{
				query: "select count(*) from dolt_commits where commit_hash = @commit;",
				exp:   []sql.UntypedSqlRow{{1}},
			},
			{
				query: "select count(*) from dolt_commit_ancestors where commit_hash = @commit;",
				exp:   []sql.UntypedSqlRow{{1}},
			},
			{
				query: "select count(*) from dolt_diff_xy join dolt_log on commit_hash = to_commit",
				exp:   []sql.UntypedSqlRow{{45}},
			},
			{
				query: "select count(*) from dolt_diff_xy join dolt_log on commit_hash = from_commit",
				exp:   []sql.UntypedSqlRow{{45}},
			},
			{
				query: "select count(*) from dolt_blame_xy",
				exp:   []sql.UntypedSqlRow{{25}},
			},
			{
				query: `SELECT count(*)
           FROM dolt_commits as cm
           JOIN dolt_commit_ancestors as an
           ON cm.commit_hash = an.parent_hash
           ORDER BY cm.date, cm.message asc`,
				exp: []sql.UntypedSqlRow{{5}},
			},
		},
	},
	{
		name: "required index lookup in join",
		setup: append(systabSetup,
			"set @tag_head = hashof('main^');",
			"call dolt_tag('t1', concat(@tag_head, '^'));",
		),
		queries: []systabQuery{
			{
				query: `
select /*+ HASH_JOIN(t,cd) */ distinct t.tag_name
from dolt_tags t
left join dolt_commit_diff_xy cd
    on cd.to_commit = t.tag_name and
       cd.from_commit = concat(t.tag_name, '^')`,
				exp: []sql.UntypedSqlRow{{"t1"}},
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
				exp:   []sql.UntypedSqlRow{{80, 80}, {81, 81}, {82, 82}, {83, 83}, {84, 84}},
			},
			{
				query: "select * from dolt_diff_xy where from_commit = @feat_head1;",
				exp:   []sql.UntypedSqlRow{},
			},
			{
				query: "select * from dolt_diff_xy where from_commit = 'WORKING';",
				exp:   []sql.UntypedSqlRow{},
			},
			{
				query: "select count(*) from dolt_diff where commit_hash = 'WORKING';",
				exp:   []sql.UntypedSqlRow{{1}},
			},
			{
				query: "select count(*) from dolt_history_xy where commit_hash = 'WORKING';",
				exp:   []sql.UntypedSqlRow{{0}},
			},
			{
				query: "select count(*) from dolt_commit_ancestors where commit_hash = 'WORKING';",
				exp:   []sql.UntypedSqlRow{{0}},
			},
			{
				query: "select sum(to_x) from dolt_diff_xy where to_commit in (@commit, 'WORKING');",
				exp:   []sql.UntypedSqlRow{{530.0}},
			},
			{
				// TODO from_commit optimization
				query: "select sum(to_x) from dolt_diff_xy where from_commit in (@commit, 'WORKING');",
				exp:   []sql.UntypedSqlRow{{320.0}},
			},
			{
				query: "select count(*) from dolt_diff where commit_hash in (@commit, 'WORKING');",
				exp:   []sql.UntypedSqlRow{{2}},
			},
			{
				query: "select sum(x) from dolt_history_xy where commit_hash in (@commit, 'WORKING');",
				exp:   []sql.UntypedSqlRow{{120.0}},
			},
			{
				// init commit has nil ancestor
				query: "select count(*) from dolt_commit_ancestors where commit_hash in (@commit, @root_commit);",
				exp:   []sql.UntypedSqlRow{{2}},
			},
			{
				query: "select count(*) from dolt_log where commit_hash in (@commit, @root_commit);",
				exp:   []sql.UntypedSqlRow{{2}},
			},
			{
				// log table cannot access commits is feature branch
				query: "select count(*) from dolt_log where commit_hash = @feat_head;",
				exp:   []sql.UntypedSqlRow{{0}},
			},
			{
				// commit table can access all commits
				query: "select count(*) from dolt_commits where commit_hash = @feat_head;",
				exp:   []sql.UntypedSqlRow{{1}},
			},
			{
				query: "select count(*) from dolt_commits where commit_hash in (@commit, @root_commit);",
				exp:   []sql.UntypedSqlRow{{2}},
			},
			// unknown
			{
				query: "select from_x, to_x from dolt_diff_xy where to_commit = 'unknown';",
				exp:   []sql.UntypedSqlRow{},
			},
			{
				query: "select * from dolt_diff_xy where from_commit = 'unknown';",
				exp:   []sql.UntypedSqlRow{},
			},
			{
				query: "select * from dolt_diff where commit_hash = 'unknown';",
				exp:   []sql.UntypedSqlRow{},
			},
			{
				query: "select * from dolt_history_xy where commit_hash = 'unknown';",
				exp:   []sql.UntypedSqlRow{},
			},
			{
				query: "select * from dolt_commit_ancestors where commit_hash = 'unknown';",
				exp:   []sql.UntypedSqlRow{},
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
				exp:   []sql.UntypedSqlRow{{1}},
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
			"call dolt_reset('--hard', @m1h1);",
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
				exp:   []sql.UntypedSqlRow{{4}},
			},
		},
	},
}

var QueryDiffTableScriptTests = []queries.ScriptTest{
	{
		Name: "basic query diff tests",
		SetUpScript: []string{
			"create table t (i int primary key, j int);",
			"insert into t values (1, 1), (2, 2), (3, 3);",
			"create table tt (i int primary key, j int);",
			"insert into tt values (10, 10), (20, 20), (30, 30);",
			"call dolt_add('.');",
			"call dolt_commit('-m', 'first');",
			"call dolt_branch('other');",
			"update t set j = 10 where i = 2;",
			"delete from t where i = 3;",
			"insert into t values (4, 4);",
			"call dolt_add('.');",
			"call dolt_commit('-m', 'second');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "select * from dolt_query_diff();",
				ExpectedErrStr: "function 'dolt_query_diff' expected 2 arguments, 0 received",
			},
			{
				Query:          "select * from dolt_query_diff('selectsyntaxerror', 'selectsyntaxerror');",
				ExpectedErrStr: "syntax error at position 18 near 'selectsyntaxerror'",
			},
			{
				Query:          "select * from dolt_query_diff('', '');",
				ExpectedErrStr: "query must be a SELECT statement",
			},
			{
				Query:          "select * from dolt_query_diff('create table tt (i int)', 'create table ttt (j int)');",
				ExpectedErrStr: "query must be a SELECT statement",
			},
			{
				Query:          "select * from dolt_query_diff('select * from missingtable', '');",
				ExpectedErrStr: "table not found: missingtable",
			},
			{
				Query: "select * from dolt_query_diff('select * from t as of other', 'select * from t as of head');",
				Expected: []sql.UntypedSqlRow{
					{2, 2, 2, 10, "modified"},
					{3, 3, nil, nil, "deleted"},
					{nil, nil, 4, 4, "added"},
				},
			},
			{
				Query: "select * from dolt_query_diff('select * from t as of head', 'select * from t as of other');",
				Expected: []sql.UntypedSqlRow{
					{2, 10, 2, 2, "modified"},
					{nil, nil, 3, 3, "added"},
					{4, 4, nil, nil, "deleted"},
				},
			},
			{
				Query: "select * from dolt_query_diff('select * from t as of other where i = 2', 'select * from t as of head where i = 2');",
				Expected: []sql.UntypedSqlRow{
					{2, 2, 2, 10, "modified"},
				},
			},
			{
				Query: "select * from dolt_query_diff('select * from t as of other where i < 2', 'select * from t as of head where i > 2');",
				Expected: []sql.UntypedSqlRow{
					{1, 1, nil, nil, "deleted"},
					{nil, nil, 4, 4, "added"},
				},
			},
			{
				Query: "select * from dolt_query_diff('select * from t', 'select * from tt');",
				Expected: []sql.UntypedSqlRow{
					{1, 1, nil, nil, "deleted"},
					{2, 10, nil, nil, "deleted"},
					{4, 4, nil, nil, "deleted"},
					{nil, nil, 10, 10, "added"},
					{nil, nil, 20, 20, "added"},
					{nil, nil, 30, 30, "added"},
				},
			},
		},
	},
}
