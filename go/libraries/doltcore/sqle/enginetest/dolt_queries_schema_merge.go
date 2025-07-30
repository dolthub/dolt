// Copyright 2023 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
)

var SchemaChangeTestsForDataConflicts = []MergeScriptTest{
	{
		Name: "data conflict",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col1 int, col2 varchar(100), col3 varchar(50), " +
				"col4 varchar(20), UNIQUE KEY unique1 (col2, pk));",
			"INSERT into t values (1, 10, '100', '1', '11'), (2, 20, '200', '2', '22');",
			"alter table t add index idx1 (col4, col1);",
		},
		RightSetUpScript: []string{
			"alter table t drop column col3;",
			"update t set col1=-100, col2='-100' where pk = 1;",
		},
		LeftSetUpScript: []string{
			"update t set col1=-1000 where t.pk = 1;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{{"t", uint64(1), uint64(0)}},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select * from dolt_conflicts;",
				Expected: []sql.Row{{"t", uint(1)}},
			},
			{
				Query: "select base_pk, base_col1, base_col2, base_col3, base_col4, " +
					"our_pk, our_col1, our_col2, our_col4, " +
					"their_pk, their_col1, their_col2, their_col4 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{
						1, 10, "100", "1", "11",
						1, -1000, "100", "11",
						1, -100, "-100", "11",
					},
				},
			},
			{
				Query:    "update dolt_conflicts_t set our_col1 = their_col1, their_col2 = our_col2;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 0, Info: plan.UpdateInfo{Matched: 1, Updated: 1, Warnings: 0}}}},
			},
			{
				Query: "select * from t;",
				Expected: []sql.Row{
					{1, -100, "100", "11"},
					{2, 20, "200", "22"},
				},
			},
		},
	},
}

var SchemaChangeTestsBasicCases = []MergeScriptTest{
	{
		Name: "dropping columns",
		AncSetUpScript: []string{
			"CREATE table t (pk int primary key, col1 int, col2 varchar(100), UNIQUE KEY unique1 (col2, pk));",
			"INSERT into t values (1, 10, '100'), (2, 20, '200');",
			"alter table t add index idx1 (pk, col1);",
			"alter table t add index idx2 (pk, col1, col2);",
			"alter table t add index idx3 (col1, col2);",
			"alter table t add index idx4 (pk, col2);",
			"CREATE INDEX idx5 ON t(col2(2));",
		},
		RightSetUpScript: []string{
			"alter table t drop column col1;",
			"insert into t values (3, '300'), (4, '400');",
		},
		LeftSetUpScript: []string{
			"insert into t values (5, 50, '500'), (6, 60, '600');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "select pk, col2 from t;",
				Expected: []sql.Row{{1, "100"}, {2, "200"}, {3, "300"}, {4, "400"}, {5, "500"}, {6, "600"}},
			},
		},
	},
	{
		Name: "renaming a column",
		AncSetUpScript: []string{
			"CREATE table t (pk int primary key, col1 int, col2 varchar(100));",
			"INSERT into t values (1, 10, '100'), (2, 20, '200');",
			"alter table t add index idx1 (pk, col1);",
			"alter table t add index idx2 (col1, pk);",
			"alter table t add index idx3 (pk, col1, col2);",
			"alter table t add index idx4 (col1, col2);",
		},
		RightSetUpScript: []string{
			"alter table t rename column col1 to col11;",
			"insert into t values (3, 30, '300'), (4, 40, '400');",
		},
		LeftSetUpScript: []string{
			"insert into t values (5, 50, '500'), (6, 60, '600');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select * from t;",
				Expected: []sql.Row{
					{1, 10, "100"}, {2, 20, "200"},
					{3, 30, "300"}, {4, 40, "400"},
					{5, 50, "500"}, {6, 60, "600"},
				},
			},
		},
	},
	{
		Name: "renaming and reordering a column",
		AncSetUpScript: []string{
			"CREATE table t (pk int primary key, col1 int, col2 varchar(100));",
			"INSERT into t values (1, 10, '100'), (2, 20, '200');",
			"alter table t add index idx1 (pk, col1);",
			"alter table t add index idx2 (col2);",
			"alter table t add index idx3 (pk, col1, col2);",
			"alter table t add index idx4 (col1, col2);",
			"alter table t add index idx5 (col2, col1);",
			"alter table t add index idx6 (col2, pk, col1);",
		},
		RightSetUpScript: []string{
			"alter table t rename column col1 to col11;",
			"alter table t modify col11 int after col2;",
			"insert into t values (3, '300', 30), (4, '400', 40);",
		},
		LeftSetUpScript: []string{
			"insert into t values (5, 50, '500'), (6, 60, '600');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select pk, col11, col2 from t;",
				Expected: []sql.Row{
					{1, 10, "100"}, {2, 20, "200"},
					{3, 30, "300"}, {4, 40, "400"},
					{5, 50, "500"}, {6, 60, "600"},
				},
			},
		},
	},
	{
		Name: "reordering a column",
		AncSetUpScript: []string{
			"CREATE table t (pk int primary key, col1 int, col2 varchar(100));",
			"INSERT into t values (1, 10, '100'), (2, 20, '200');",
			"alter table t add index idx1 (pk, col1);",
			"alter table t add index idx2 (col2);",
			"alter table t add index idx3 (pk, col1, col2);",
			"alter table t add index idx4 (col1, col2);",
			"alter table t add index idx5 (col2, col1);",
			"alter table t add index idx6 (col2, pk, col1);",
		},
		RightSetUpScript: []string{
			"alter table t modify col1 int after col2;",
			"insert into t (pk, col1, col2) values (3, 30, '300'), (4, 40, '400');",
		},
		LeftSetUpScript: []string{
			"insert into t values (5, 50, '500'), (6, 60, '600');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select pk, col1, col2 from t;",
				Expected: []sql.Row{
					{1, 10, "100"}, {2, 20, "200"},
					{3, 30, "300"}, {4, 40, "400"},
					{5, 50, "500"}, {6, 60, "600"}},
			},
		},
	},
	{
		Name: "adding nullable columns to one side",
		AncSetUpScript: []string{
			"create table t (pk int primary key, col1 int);",
			"insert into t values (1, 1);",
			"alter table t add index idx1 (col1, pk);",
			"alter table t add index idx2 (col1);",
		},
		RightSetUpScript: []string{
			"alter table t add column col2 int;",
			"alter table t add column col3 int;",
			"insert into t values (2, 2, 2, 2);",
		},
		LeftSetUpScript: []string{
			"insert into t values (3, 3);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, 1, nil, nil}, {2, 2, 2, 2}, {3, 3, nil, nil}},
			},
		},
	},
	{
		Name: "adding a column with a literal default value",
		AncSetUpScript: []string{
			"CREATE table t (pk int primary key);",
			"INSERT into t values (1);",
		},
		RightSetUpScript: []string{
			"alter table t add column c1 varchar(100) default ('hello');",
			"insert into t values (2, 'hi');",
			"alter table t add index idx1 (c1, pk);",
		},
		LeftSetUpScript: []string{
			"insert into t values (3);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, "hello"}, {2, "hi"}, {3, "hello"}},
			},
		},
	},
	{
		Name: "altering a column to add a literal default value",
		AncSetUpScript: []string{
			"CREATE table t (pk int primary key, c1 varchar(100));",
			"INSERT into t values (1, NULL);",
			"alter table t add index idx1 (c1, pk);",
		},
		RightSetUpScript: []string{
			"alter table t modify column c1 varchar(100) default ('hello');",
			"insert into t values (2, DEFAULT);",
		},
		LeftSetUpScript: []string{
			"insert into t values (3, NULL);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, nil}, {2, "hello"}, {3, nil}},
			},
		},
	},
	{
		Name: "adding a column with a non-literal default value",
		AncSetUpScript: []string{
			"CREATE table t (pk varchar(100) primary key);",
			"INSERT into t values ('1');",
		},
		RightSetUpScript: []string{
			"alter table t add column c1 varchar(100) default (CONCAT(pk, 'h','e','l','l','o'));",
			"insert into t values ('2', 'hi');",
			"alter table t add index idx1 (c1, pk);",
		},
		LeftSetUpScript: []string{
			"insert into t values ('3');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{"1", "1hello"}, {"2", "hi"}, {"3", "3hello"}},
			},
		},
	},
	{
		// Tests that column default expressions are correctly evaluated when the left-side schema
		// has changed and the right row needs to be mapped to the new left-side schema
		Name: "right-side adds a column with a default value, left-side drops a column",
		AncSetUpScript: []string{
			"CREATE table t (pk int primary key, c1 varchar(100), c2 varchar(100));",
			"INSERT into t values ('1', 'BAD', 'hello');",
		},
		RightSetUpScript: []string{
			"alter table t add column c3 varchar(100) default (CONCAT(c2, 'h','e','l','l','o'));",
			"insert into t values ('2', 'BAD', 'hello', 'hi');",
			"alter table t add index idx1 (c1, pk);",
		},
		LeftSetUpScript: []string{
			"insert into t values ('3', 'BAD', 'hello');",
			"alter table t drop column c1;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, "hello", "hellohello"}, {2, "hello", "hi"}, {3, "hello", "hellohello"}},
			},
		},
	},
	{
		Name: "right-side adds a column with a default value",
		AncSetUpScript: []string{
			"CREATE table t (pk int primary key, c1 varchar(100), c2 varchar(100));",
			"INSERT into t values ('1', 'BAD', 'hello');",
		},
		RightSetUpScript: []string{
			"alter table t add column c3 varchar(100) default (CONCAT(c2, c1, 'default'));",
			"insert into t values ('2', 'BAD', 'hello', 'hi');",
		},
		LeftSetUpScript: []string{
			"insert into t values ('3', 'BAD', 'hi');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "select * from t order by pk;",
				Expected: []sql.Row{{1, "BAD", "hello", "helloBADdefault"}, {2, "BAD", "hello", "hi"}, {3, "BAD", "hi", "hiBADdefault"}},
			},
		},
	},
	{
		Name: "adding different columns to both sides",
		AncSetUpScript: []string{
			"create table t (pk int primary key);",
			"insert into t values (1), (2);",
			"alter table t add index idx1 (pk);",
		},
		RightSetUpScript: []string{
			"alter table t add column col2 varchar(100);",
			"insert into t values (3, '300'), (4, '400');",
		},
		LeftSetUpScript: []string{
			"alter table t add column col1 int;",
			"insert into t values (5, 50), (6, 60);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select pk, col1, col2 from t;",
				Expected: []sql.Row{
					{1, nil, nil},
					{2, nil, nil},
					{3, nil, "300"},
					{4, nil, "400"},
					{5, 50, nil},
					{6, 60, nil},
				},
			},
		},
	},
	{
		Name: "adding columns with default values to both sides",
		AncSetUpScript: []string{
			"create table t (pk int primary key);",
			"insert into t values (1), (2);",
			"alter table t add index idx1 (pk);",
		},
		RightSetUpScript: []string{
			"alter table t add column col2 varchar(100) default 'abc'",
			"insert into t values (3, '300'), (4, '400');",
		},
		LeftSetUpScript: []string{
			"alter table t add column col1 int default 101;",
			"insert into t values (5, 50), (6, 60);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select pk, col1, col2 from t;",
				Expected: []sql.Row{
					{1, 101, "abc"},
					{2, 101, "abc"},
					{3, 101, "300"},
					{4, 101, "400"},
					{5, 50, "abc"},
					{6, 60, "abc"},
				},
			},
		},
	},
	{
		Name: "adding indexed columns to both sides",
		AncSetUpScript: []string{
			"create table t (pk int primary key);",
			"insert into t values (1), (2);",
		},
		RightSetUpScript: []string{
			"alter table t add column col2 varchar(100);",
			"insert into t (pk, col2) values (3, '3hello'), (4, '4hello');",
			"alter table t add index (col2);",
		},
		LeftSetUpScript: []string{
			"alter table t add column col1 int default (pk + 100);",
			"insert into t (pk) values (5), (6);",
			"alter table t add index (col1);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select pk, col1, col2 from t;",
				Expected: []sql.Row{
					{1, 101, nil},
					{2, 102, nil},
					{3, 103, "3hello"},
					{4, 104, "4hello"},
					{5, 105, nil},
					{6, 106, nil},
				},
			},
		},
	},
	{
		// TODO: Need another test with a different type for the same column name, and verify it's an error?
		Name: "dropping and adding a column with the same name",
		AncSetUpScript: []string{
			"create table t (pk int primary key, col1 int, col2 varchar(100));",
			"insert into t values (1, 10, '100'), (2, 20, '200');",
			"alter table t add index idx1 (col1, pk);",
			"alter table t add index idx2 (col2, pk);",
			"alter table t add index idx3 (col2, col1);",
		},
		RightSetUpScript: []string{
			"alter table t drop column col1;",
			"alter table t add column col1 int;",
			"insert into t values (3, '300', 30), (4, '400', 40);",
		},
		LeftSetUpScript: []string{
			"insert into t values (5, 50, '500'), (6, 60, '600');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				// NOTE: If we can't find an exact tag mapping, then we fall back to
				//       matching by name and exact type.
				Query: "select pk, col1, col2 from t order by pk;",
				Expected: []sql.Row{
					{1, nil, "100"},
					{2, nil, "200"},
					{3, 30, "300"},
					{4, 40, "400"},
					{5, 50, "500"},
					{6, 60, "600"},
				},
			},
		},
	},
	{
		// Repro for issue in: https://github.com/dolthub/dolt/pull/6496
		Name: "drop a column, schema contains BLOB/AddrEnc columns",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col1 int, col2 text, col3 varchar(10));",
			"INSERT into t values (1, 10, 'a', 'b'), (2, 20, 'c', 'd');",
		},
		RightSetUpScript: []string{
			"INSERT into t values (300, 30, 'e', 'f');",
		},
		LeftSetUpScript: []string{
			"alter table t drop column col1;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select * from t;",
				Expected: []sql.Row{
					{1, "a", "b"},
					{2, "c", "d"},
					{300, "e", "f"}},
			},
		},
	},
	{
		Name: "convergent schema changes",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE TABLE parent(id int primary key);",
			"insert into parent values (1), (2), (3);",
			"CREATE table t (pk int primary key, col1 int);",
			"INSERT into t values (1, 10);",
		},
		RightSetUpScript: []string{
			"alter table t modify column col1 int not null;",
			"alter table t add column col3 int ;",
			"alter table t add index idx1 (col3, col1);",
			"alter table t add constraint fk1 foreign key (col3) references parent(id);",
		},
		LeftSetUpScript: []string{
			"alter table t modify column col1 int not null;",
			"alter table t add column col3 int;",
			"alter table t add index idx1 (col3, col1);",
			"update t set col1=-1000 where t.pk = 1;",
			"alter table t add constraint fk1 foreign key (col3) references parent(id);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "show create table t;",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` int NOT NULL,\n  `col3` int,\n  PRIMARY KEY (`pk`),\n  KEY `idx1` (`col3`,`col1`),\n  CONSTRAINT `fk1` FOREIGN KEY (`col3`) REFERENCES `parent` (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, -1000, nil}},
			},
		},
	},
	{
		// Currently skipped bc of https://github.com/dolthub/dolt/issues/7767
		Name: "ambiguous choice of ancestor column",
		AncSetUpScript: []string{
			"CREATE table t (pk int primary key, col1 int, col2 int);",
			"INSERT into t values (1, 10, 100), (2, 20, 200);",
		},
		RightSetUpScript: []string{
			"alter table t drop column col2;",
			"insert into t values (3, 30), (4, 40);",
		},
		LeftSetUpScript: []string{
			"alter table t drop column col1;",
			"alter table t rename column col2 to col1;",
			"insert into t values (5, 50), (6, 60);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Skip:     true,
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Skip:           true,
				Query:          "call dolt_merge('right');",
				ExpectedErrStr: "Merge conflict detected, @autocommit transaction rolled back. @autocommit must be disabled so that merge conflicts can be resolved using the dolt_conflicts and dolt_schema_conflicts tables before manually committing the transaction. Alternatively, to commit transactions with merge conflicts, set @@dolt_allow_commit_conflicts = 1",
			},
		},
	},
	{
		// One branch makes a new column, deletes the old one, and renames.
		// The other just has a data change.
		Name: "creating new column to replace ancestor column",
		AncSetUpScript: []string{
			"CREATE table t (pk int primary key, col1 int);",
			"INSERT into t values (1, 10), (2, 20);",
		},
		RightSetUpScript: []string{
			"insert into t values (3, 30), (4, 40);",
		},
		LeftSetUpScript: []string{
			"alter table t add column col2 int",
			"update t set col2 = 10*col1",
			"alter table t drop column col1;",
			"alter table t rename column col2 to col1;",
			"insert into t values (5, 50), (6, 60);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select * from t;",
				Expected: []sql.Row{
					{1, 100}, {2, 200},
					{3, 30}, {4, 40},
					{5, 50}, {6, 60},
				},
			},
		},
	},
	{
		// Ensure duplicate indexes on same columns merge. https://github.com/dolthub/dolt/issues/8975
		Name: "",
		AncSetUpScript: []string{
			`CREATE TABLE t (
          id CHAR(36) PRIMARY KEY,
          time DATETIME,
          INDEX i1 (time DESC),
          INDEX i2 (time))`,
			"INSERT INTO t VALUES (UUID(), NOW())",
		},
		RightSetUpScript: []string{
			"INSERT INTO t VALUES (UUID(), NOW())",
		},
		LeftSetUpScript: []string{
			"INSERT INTO t VALUES (UUID(), NOW())",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
		},
	},
	{
		// One branch adds a new column with NULL as default
		// Other branch has new rows which need to be migrated.
		// Created values are NULL, not "NULL".
		Name: "creating new column to replace ancestor column",
		AncSetUpScript: []string{
			"CREATE table t (pk int primary key);",
			"INSERT into t values (1), (2);",
		},
		RightSetUpScript: []string{
			"ALTER TABLE t ADD new_col LONGTEXT DEFAULT NULL",
			"INSERT INTO t VALUES (3, 'three'), (4, 'four');",
		},
		LeftSetUpScript: []string{
			// Put rows on main which are not in the right branch.
			"INSERT INTO t VALUES (5), (6)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select * from t;",
				Expected: []sql.Row{
					{1, nil},
					{2, nil},
					{3, "three"},
					{4, "four"},
					{5, nil},
					{6, nil},
				},
			},
		},
	},
}

var SchemaChangeTestsCollations = []MergeScriptTest{
	{
		Name: "Changing a table's default collation on one side",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col1 varchar(100)) collate utf8mb3_unicode_ci;",
			"INSERT into t values (1, '10');",
		},
		RightSetUpScript: []string{
			"alter table t collate utf8mb4_0900_bin;",
			"insert into t values (2, '20');",
		},
		LeftSetUpScript: []string{
			"insert into t values (3, '30');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, "10"}, {2, "20"}, {3, "30"}},
			},
			{
				Query:    "show create table t;",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "Changing a table's default collation on both sides to different values",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col1 varchar(100)) collate utf8mb3_unicode_ci;",
			"INSERT into t values (1, '10');",
		},
		RightSetUpScript: []string{
			"alter table t collate utf8mb4_0900_bin;",
			"insert into t values (2, '20');",
		},
		LeftSetUpScript: []string{
			"alter table t collate ascii_general_ci;",
			"insert into t values (3, '30');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				ExpectedErr: merge.ErrDefaultCollationConflict,
			},
			{
				Query:       "call dolt_merge('right');",
				ExpectedErr: merge.ErrDefaultCollationConflict,
			},
		},
	},
	{
		Name: "Changing a table's default collation on both sides to the same value",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col1 varchar(100)) collate utf8mb3_unicode_ci;",
			"INSERT into t values (1, '10');",
		},
		RightSetUpScript: []string{
			"alter table t collate utf8mb4_0900_bin;",
			"insert into t values (2, '20');",
		},
		LeftSetUpScript: []string{
			"alter table t collate utf8mb4_0900_bin;",
			"insert into t values (3, '30');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, "10"}, {2, "20"}, {3, "30"}},
			},
			{
				Query:    "show create table t;",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
}

var SchemaChangeTestsConstraints = []MergeScriptTest{
	{
		// Regression test for a bug where rows weren't being deleted in
		// a secondary index because the incorrect/non-matching tuple was
		// used to update the index, and foreign key violations were
		// incorrectly identified.
		Name: "updating fk index when ancestor schema has changed",
		AncSetUpScript: []string{
			"CREATE TABLE parent(pk int primary key, c1 varchar(100));",
			"CREATE TABLE child(pk int primary key, remove_me int, parent_id int, KEY `fk_idx1` (`parent_id`), foreign key fk1 (parent_id) references parent(pk));",
			"INSERT INTO parent VALUES (100, 'one hundred'), (200, 'two hundred');",
			"INSERT INTO child VALUES (1, -1, 100), (2, -1, 200);",
		},
		RightSetUpScript: []string{
			"DELETE FROM child;",
			"DELETE FROM parent;",
			"ALTER TABLE child drop column remove_me;",
		},
		LeftSetUpScript: []string{
			"ALTER TABLE child drop column remove_me;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
		},
	},
	{
		Name: "removing a not-null constraint",
		AncSetUpScript: []string{
			"create table t (pk int primary key, col1 int not null);",
			"insert into t values (1, 1), (2, 2);",
			"alter table t add index idx1 (col1, pk);",
			"alter table t add index idx2 (col1);",
		},
		RightSetUpScript: []string{
			"alter table t modify col1 int;",
			"insert into t values (3, null);",
		},
		LeftSetUpScript: []string{
			"insert into t values (4, 4);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select * from t;",
				Expected: []sql.Row{
					{1, 1},
					{2, 2},
					{3, nil},
					{4, 4},
				},
			},
		},
	},
	{
		Name: "adding a foreign key to one side, with fk constraint violation",
		AncSetUpScript: []string{
			"create table parent (pk int primary key);",
			"create table child (pk int primary key, p_fk int);",
			"insert into parent values (1);",
			"insert into child values (1, 1);",
			"set DOLT_FORCE_TRANSACTION_COMMIT = true;",
			"alter table child add index idx1 (p_fk, pk);",
		},
		RightSetUpScript: []string{
			"alter table child add constraint fk_parent foreign key (p_fk) references parent(pk);",
			"alter table child add column col1 int after pk;",
		},
		LeftSetUpScript: []string{
			"insert into child values (2, 2);",
			"update child set p_fk = 3 where pk = 1;",
			"alter table child add column col2 varchar(100) after pk;",
			"update child set col2 = '1col2' where pk = 1;",
			"update child set col2 = '2col2' where pk = 2;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select pk, p_fk, col1, col2 from child order by pk;",
				Expected: []sql.Row{{1, 3, nil, "1col2"}, {2, 2, nil, "2col2"}},
			},
			{
				Query:    "select pk, p_fk, col1, col2 from dolt_constraint_violations_child order by pk;",
				Expected: []sql.Row{{1, 3, nil, "1col2"}, {2, 2, nil, "2col2"}},
			},
		},
	},
	{
		Name: "altering a check constraint on one side",
		AncSetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(100));",
			"insert into t values (1, 'one');",
			"alter table t ADD CONSTRAINT check1 CHECK (c1 in ('one', 'two'));",
		},
		RightSetUpScript: []string{
			"alter table t drop constraint check1;",
			"alter table t ADD CONSTRAINT check1 CHECK (c1 in ('one', 'two', 'three'));",
			"insert into t values (3, 'three');",
		},
		LeftSetUpScript: []string{
			"insert into t values (2, 'two');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, "one"}, {2, "two"}, {3, "three"}},
			},
		},
	},
	{
		Name: "dropping a foreign key",
		AncSetUpScript: []string{
			"create table parent (pk int primary key);",
			"create table child (pk int primary key, p_fk int, CONSTRAINT parent_fk FOREIGN KEY (p_fk) REFERENCES parent (pk));",
			"insert into parent values (1);",
			"insert into child values (1, 1);",
		},
		RightSetUpScript: []string{
			"alter table child drop constraint parent_fk;",
			"delete from parent;",
		},
		LeftSetUpScript: []string{
			"insert into child values (2, 1);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "select * from parent;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from child;",
				Expected: []sql.Row{{1, 1}, {2, 1}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/9014
		Name: "foreign keys – drop tables with FK reference between them",
		AncSetUpScript: []string{
			"CREATE TABLE a (id int PRIMARY KEY, name varchar(256) NOT NULL);",
			"CREATE TABLE b (id int PRIMARY KEY, assetId int, KEY file_assetid_foreign (assetId), CONSTRAINT file_assetid_foreign FOREIGN KEY (assetId) REFERENCES a(id));",
		},
		RightSetUpScript: []string{
			"CREATE TABLE c (id int PRIMARY KEY);",
		},
		LeftSetUpScript: []string{
			"DROP TABLE a, b;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
		},
	},
	{
		Name: "adding a unique key, with unique key violation",
		AncSetUpScript: []string{
			"create table t (pk int, col1 int);",
			"insert into t values (1, 1);",
			"set DOLT_FORCE_TRANSACTION_COMMIT = 1;",
		},
		RightSetUpScript: []string{
			"alter table t add unique (col1);",
		},
		LeftSetUpScript: []string{
			"insert into t values (2, 1);",
			"insert into t values (3, 1);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select pk, col1 from t;",
				Expected: []sql.Row{{1, 1}, {2, 1}, {3, 1}},
			},
			{
				Query:    "select pk, col1 from dolt_constraint_violations_t;",
				Expected: []sql.Row{{1, 1}, {2, 1}, {3, 1}},
			},
		},
	},
	{
		Name: "unique constraint violation",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk varchar(100) primary key, col1 int, col2 varchar(100), UNIQUE KEY unique1 (col2));",
			"INSERT into t values ('0', 0, '');",
			"alter table t add index idx1 (pk, col2);",
		},
		RightSetUpScript: []string{
			"alter table t drop column col1;",
			"INSERT into t (pk, col2) values ('10', 'same');",
		},
		LeftSetUpScript: []string{
			"INSERT into t values ('1', 10, 'same');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select * from dolt_conflicts;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.Row{{"t", uint(2)}},
			},
			{
				Query: "select violation_type, pk, col2, violation_info from dolt_constraint_violations_t;",
				Expected: []sql.Row{
					{"unique index", "1", "same", merge.UniqCVMeta{Columns: []string{"col2"}, Name: "unique1"}},
					{"unique index", "10", "same", merge.UniqCVMeta{Columns: []string{"col2"}, Name: "unique1"}},
				},
			},
			{
				Query: "select pk, col2 from t;",
				Expected: []sql.Row{
					{"0", ""},
					{"1", "same"},
					{"10", "same"},
				},
			},
		},
	},
	{
		Name: "dropping a unique key",
		AncSetUpScript: []string{
			"create table t (pk int primary key, col1 int UNIQUE);",
			"insert into t values (1, 1);",
		},
		RightSetUpScript: []string{
			"alter table t drop col1;",
			"alter table t add col1 int;",
			"update t set col1 = 1 where pk = 1;",
			"insert into t values (2, 1);",
		},
		LeftSetUpScript: []string{
			"insert into t values (3, 3);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, 1}, {2, 1}, {3, 3}},
			},
		},
	},
	{
		// Tests that we correctly build the row to pass into the check constraint expression when
		// the primary key fields are not all positioned at the start of the schema.
		Name: "check constraint - non-contiguous primary key",
		AncSetUpScript: []string{
			"CREATE table t (pk1 int, col1 int, pk2 varchar(100), CHECK (col1 in (0, 1)), primary key (pk1, pk2));",
			"INSERT into t values (1, 0, 1);",
		},
		RightSetUpScript: []string{
			"alter table t add column col2 varchar(100);",
			"insert into t values (2, 1, 2, 'hello');",
		},
		LeftSetUpScript: []string{
			"insert into t values (3, 0, 3);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select * from t;",
				Expected: []sql.Row{
					{1, 0, "1", nil},
					{2, 1, "2", "hello"},
					{3, 0, "3", nil},
				},
			},
		},
	},
	{
		Name: "check constraint violation - simple case, no schema changes",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col1 int, col2 int, CHECK (col1 != col2));",
			"INSERT into t values (1, 2, 3);",
			"alter table t add index idx1 (pk, col2);",
		},
		RightSetUpScript: []string{
			"update t set col2=4;",
		},
		LeftSetUpScript: []string{
			"update t set col1=4;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.Row{{"t", uint64(1)}},
			},
			{
				Query:    "select violation_type, pk, col1, col2, violation_info like '\\%NOT((`col1` = `col2`))\\%' from dolt_constraint_violations_t;",
				Expected: []sql.Row{{"check constraint", 1, 4, 4, true}},
			},
		},
	},
	{
		// Check Constraint Coercion:
		// MySQL doesn't allow creating non-boolean check constraint
		// expressions, but we currently allow it. Eventually we should
		// close this gap and then we wouldn't need to coerce return values.
		Name: "check constraint - coercion to bool",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col1 int, col2 int, CHECK (col1+col2));",
			"INSERT into t values (1, 1, 1);",
			"alter table t add index idx1 (pk, col2);",
		},
		RightSetUpScript: []string{
			"update t set col2=0;",
		},
		LeftSetUpScript: []string{
			"update t set col1=2;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, 2, 0}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/6411
		Name: "check constraint violation - coercion to bool",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col1 int, col2 int, CHECK (col1+col2));",
			"INSERT into t values (1, 1, 1);",
			"alter table t add index idx1 (pk, col2);",
		},
		RightSetUpScript: []string{
			"update t set col2=0;",
		},
		LeftSetUpScript: []string{
			"update t set col1=0;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.Row{{"t", uint64(1)}},
			},
			{
				Query:    "select violation_type, pk, col1, col2, violation_info like '%(`col1` + `col2`)%' from dolt_constraint_violations_t;",
				Expected: []sql.Row{{"check constraint", 1, 0, 0, true}},
			},
		},
	},
	{
		Name: "check constraint violation - schema change",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col1 int, col2 int, col3 int, CHECK (col2 != col3));",
			"INSERT into t values (1, 2, 3, -3);",
			"alter table t add index idx1 (pk, col2);",
		},
		RightSetUpScript: []string{
			"update t set col2=100;",
		},
		LeftSetUpScript: []string{
			"alter table t drop column col1;",
			"update t set col3=100;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.Row{{"t", uint64(1)}},
			},
			{
				Query:    "select violation_type, pk, col2, col3, violation_info like '\\%NOT((`col2` = `col3`))\\%' from dolt_constraint_violations_t;",
				Expected: []sql.Row{{"check constraint", 1, 100, 100, true}},
			},
		},
	},
	{
		Name: "check constraint violation - deleting rows",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col1 int, col2 int, col3 int, CHECK (col2 != col3));",
			"INSERT into t values (1, 2, 3, -3);",
			"alter table t add index idx1 (pk, col2);",
		},
		RightSetUpScript: []string{
			"delete from t where pk=1;",
		},
		LeftSetUpScript: []string{
			"insert into t values (4, 3, 2, 1);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
		},
	},
	{
		Name: "check constraint violation - divergent edits",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col1 varchar(100) default ('hello'));",
			"INSERT into t values (1, 'hi');",
			"alter table t add index idx1 (col1);",
		},
		RightSetUpScript: []string{
			"alter table t add constraint CHECK (col1 != concat('he', 'llo'))",
			"update t set col1 = 'bye' where pk=1;",
		},
		LeftSetUpScript: []string{
			"update t set col1 = 'adios' where pk=1;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
		},
	},
	{
		Name: "check constraint violation - check is always NULL",
		AncSetUpScript: []string{
			"CREATE table t (pk int primary key, col1 varchar(100) default ('hello'));",
			"INSERT into t values (1, 'hi');",
			"alter table t add index idx1 (col1);",
		},
		RightSetUpScript: []string{
			"alter table t add constraint CHECK (NULL = NULL)",
		},
		LeftSetUpScript: []string{
			"insert into t values (2, DEFAULT);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
		},
	},
	{
		Name: "check constraint violation - check is always false",
		AncSetUpScript: []string{
			"SET @@dolt_force_transaction_commit=1;",
			"CREATE table t (pk int primary key, col1 varchar(100) default ('hello'));",
			"alter table t add index idx1 (col1);",
		},
		RightSetUpScript: []string{
			"alter table t add constraint CHECK (1 = 2)",
		},
		LeftSetUpScript: []string{
			"insert into t values (1, DEFAULT);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
		},
	},
	{
		Name: "check constraint violation - right side violates new check constraint",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col00 int, col01 int, col1 varchar(100) default ('hello'));",
			"INSERT into t values (1, 0, 0, 'hi');",
			"alter table t add index idx1 (col1);",
		},
		RightSetUpScript: []string{
			"insert into t values (2, 0, 0, DEFAULT);",
		},
		LeftSetUpScript: []string{
			"alter table t drop column col00;",
			"alter table t drop column col01;",
			"alter table t add constraint CHECK (col1 != concat('he', 'llo'))",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.Row{{"t", uint64(1)}},
			},
			{
				Query:    "select violation_type, pk, col1, violation_info like \"%NOT((`col1` = concat('he','llo')))%\" from dolt_constraint_violations_t;",
				Expected: []sql.Row{{"check constraint", 2, "hello", true}},
			},
		},
	},
	{
		Name: "check constraint violation - keyless table, right side violates new check constraint",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (c0 int, col0 varchar(100), col1 varchar(100) default ('hello'));",
			"INSERT into t values (1, 'adios', 'hi');",
			"alter table t add index idx1 (col1);",
		},
		RightSetUpScript: []string{
			"insert into t values (2, 'hola', DEFAULT);",
		},
		LeftSetUpScript: []string{
			"alter table t add constraint CHECK (col1 != concat('he', 'llo'))",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.Row{{"t", uint64(1)}},
			},
			{
				Query:    "select violation_type, c0, col0, col1, violation_info like \"%NOT((`col1` = concat('he','llo')))%\" from dolt_constraint_violations_t;",
				Expected: []sql.Row{{"check constraint", 2, "hola", "hello", true}},
			},
		},
	},
}

// SchemaChangeTestsTypeChanges holds test scripts for schema merge where column types have changed. Note that
// unlike other schema change tests, these tests are NOT symmetric, so they do not get automatically run in both
// directions.
var SchemaChangeTestsTypeChanges = []MergeScriptTest{
	{
		Name: "varchar widening",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col1 varchar(10));",
			"INSERT into t values (1, '123');",
			"alter table t add index idx1 (col1);",
		},
		RightSetUpScript: []string{
			"alter table t modify column col1 varchar(100);",
			"INSERT into t values (2, '12345678901234567890');",
		},
		LeftSetUpScript: []string{
			"INSERT into t values (3, '321');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, "123"}, {2, "12345678901234567890"}, {3, "321"}},
			},
			{
				Query: "show create table t;",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n" +
					"  `pk` int NOT NULL,\n" +
					"  `col1` varchar(100),\n" +
					"  PRIMARY KEY (`pk`),\n" +
					"  KEY `idx1` (`col1`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "insert into t values (4, 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJJKLMNOPQRSTUVWXYZ!@#$%^&*()_+');",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
		},
	},
	{
		Name: "enums and sets widening",
		AncSetUpScript: []string{
			"CREATE table t (pk int primary key, col1 enum('blue', 'green'), col2 set('blue', 'green'));",
			"INSERT into t values (1, 'blue', 'blue,green');",
			"alter table t add index idx1 (col1);",
		},
		RightSetUpScript: []string{
			"alter table t modify column col1 enum('blue', 'green', 'red');",
			"alter table t modify column col2 set('blue', 'green', 'red');",
			"INSERT into t values (3, 'red', 'red,blue');",
		},
		LeftSetUpScript: []string{
			"INSERT into t values (2, 'green', 'green,blue');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select * from t order by pk;",
				Expected: []sql.Row{
					{1, "blue", "blue,green"},
					{2, "green", "blue,green"},
					{3, "red", "blue,red"},
				},
			},
		},
	},
	{
		Name: "VARCHAR to TEXT widening with right side modification",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col1 varchar(10), col2 int);",
			"INSERT into t values (1, '123', 10);",
			"alter table t add index idx1 (col1(10));",
		},
		RightSetUpScript: []string{
			"alter table t modify column col1 TEXT;",
			"UPDATE t SET col2 = 40 WHERE col2 = 10",
			"INSERT into t values (2, '12345678901234567890', 20);",
		},
		LeftSetUpScript: []string{
			"INSERT into t values (3, '321', 30);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select * from t order by pk;",
				Expected: []sql.Row{
					{1, "123", 40},
					{2, "12345678901234567890", 20},
					{3, "321", 30},
				},
			},
		},
	},
	{
		Name: "VARCHAR to TEXT widening",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col1 varchar(10));",
			"INSERT into t values (1, '123');",
			"alter table t add index idx1 (col1(10));",
		},
		RightSetUpScript: []string{
			"alter table t modify column col1 TEXT;",
			"INSERT into t values (2, '12345678901234567890');",
		},
		LeftSetUpScript: []string{
			"INSERT into t values (3, '321');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select * from t order by pk;",
				Expected: []sql.Row{
					{1, "123"},
					{2, "12345678901234567890"},
					{3, "321"},
				},
			},
		},
	},
	{
		Name: "VARBINARY to BLOB widening",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col1 varbinary(10));",
			"INSERT into t values (1, '123');",
			"alter table t add index idx1 (col1(10));",
		},
		RightSetUpScript: []string{
			"alter table t modify column col1 BLOB;",
			"INSERT into t values (2, '12345678901234567890');",
		},
		LeftSetUpScript: []string{
			"INSERT into t values (3, '321');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select * from t order by pk;",
				Expected: []sql.Row{
					{1, []uint8{0x31, 0x32, 0x33}},
					{2, []uint8{0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x30}},
					{3, []uint8{0x33, 0x32, 0x31}},
				},
			},
		},
	},
	{
		Name: "varchar(300) to TINYTEXT(255) narrowing",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col1 varchar(300));",
			"INSERT into t values (1, '123');",
			"alter table t add index idx1 (col1(10));",
		},
		RightSetUpScript: []string{
			"alter table t modify column col1 TINYTEXT;",
			"INSERT into t values (2, '12345678901234567890');",
		},
		LeftSetUpScript: []string{
			"INSERT into t values (3, '321');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{{"t", nil, uint64(1)}},
			},
			{
				Query:          "select * from dolt_preview_merge_conflicts('main', 'right', 't');",
				ExpectedErrStr: "schema conflicts found: 1",
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select * from dolt_conflicts;",
				Expected: []sql.Row{{"t", uint64(0)}},
			},
			{
				Query:    "select count(*) from dolt_schema_conflicts where description like 'incompatible column types for column ''col1''%';",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "schema conflict: VARBINARY(300) to TINYBLOB(255)",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col1 VARBINARY(300));",
			"INSERT into t values (1, '123');",
			"alter table t add index idx1 (col1(10));",
		},
		RightSetUpScript: []string{
			"alter table t modify column col1 TINYBLOB;",
			"INSERT into t values (2, '12345678901234567890');",
		},
		LeftSetUpScript: []string{
			"INSERT into t values (3, '321');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{{"t", nil, uint64(1)}},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select * from dolt_conflicts;",
				Expected: []sql.Row{{"t", uint64(0)}},
			},
			{
				Query:    "select count(*) from dolt_schema_conflicts where description like 'incompatible column types for column ''col1''%';",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "CHAR(5) to TINYTEXT widening",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col1 char(5));",
			"INSERT into t values (1, '123');",
			"alter table t add index idx1 (col1(5));",
		},
		RightSetUpScript: []string{
			"alter table t modify column col1 TINYTEXT;",
			"INSERT into t values (2, '12345678901234567890');",
		},
		LeftSetUpScript: []string{
			"INSERT into t values (3, '321');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select * from t order by pk;",
				Expected: []sql.Row{
					{1, "123"},
					{2, "12345678901234567890"},
					{3, "321"},
				},
			},
		},
	},
	{
		Name: "CHAR(5) to TINYTEXT, different charsets",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col1 char(5) COLLATE utf8mb3_esperanto_ci);",
			"INSERT into t values (1, '123');",
			"alter table t add index idx1 (col1(3));",
		},
		RightSetUpScript: []string{
			"alter table t modify column col1 TINYTEXT COLLATE utf32_unicode_ci;",
			"INSERT into t values (2, '12345678901234567890');",
		},
		LeftSetUpScript: []string{
			"INSERT into t values (3, '321');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{{"t", nil, uint64(1)}},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select count(*) from dolt_schema_conflicts where description like 'incompatible column types for column ''col1''%';",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "varchar narrowing",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col1 varchar(10));",
			"INSERT into t values (1, '123');",
			"alter table t add index idx1 (col1(10));",
		},
		RightSetUpScript: []string{
			"alter table t modify column col1 varchar(9);",
			"INSERT into t values (2, '12345');",
		},
		LeftSetUpScript: []string{
			"INSERT into t values (3, '321');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{{"t", nil, uint64(1)}},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select table_name, description like 'incompatible column types for column ''col1''%' from dolt_schema_conflicts;",
				Expected: []sql.Row{{"t", true}},
			},
		},
	},
	{
		Name: "TEXT to VARCHAR narrowing",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col1 TEXT);",
			"INSERT into t values (1, '123');",
			"alter table t add index idx1 (col1(10));",
		},
		RightSetUpScript: []string{
			"alter table t modify column col1 varchar(9);",
			"INSERT into t values (2, '12345');",
		},
		LeftSetUpScript: []string{
			"INSERT into t values (3, '321');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{{"t", nil, uint64(1)}},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select table_name, description like 'incompatible column types for column ''col1''%' from dolt_schema_conflicts;",
				Expected: []sql.Row{{"t", true}},
			},
		},
	},
	{
		Name: "VARCHAR to CHAR widening",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col1 VARCHAR(10));",
			"INSERT into t values (1, '123');",
			"alter table t add unique index idx1 (col1);",
		},
		RightSetUpScript: []string{
			"alter table t modify column col1 CHAR(11);",
			"INSERT into t values (2, '12345');",
		},
		LeftSetUpScript: []string{
			"INSERT into t values (3, '321');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select * from t order by pk;",
				Expected: []sql.Row{
					{1, "123"},
					{2, "12345"},
					{3, "321"},
				},
			},
		},
	},
	{
		Name: "BINARY to VARBINARY widening",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col1 BINARY(5));",
			"INSERT into t values (1, 0x01);",
			"alter table t add unique index idx1 (col1);",
		},
		RightSetUpScript: []string{
			"alter table t modify column col1 VARBINARY(10);",
			"INSERT into t values (2, 0x0102);",
		},
		LeftSetUpScript: []string{
			"INSERT into t values (3, 0x010203);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select pk, col1 from t order by pk;",
				Expected: []sql.Row{
					// NOTE: When MySQL converts from BINARY(N) to VARBINARY(N), it does not change any values. But...
					//       when converting from VARBINARY(N) to BINARY(N), MySQL *DOES* right-pad any values up to
					//       N bytes.

					// Written to BINARY(N), so right padded
					{1, []byte{0x01, 0x00, 0x00, 0x00, 0x00}},
					// Written to VARBINARY(N), so no padding
					{2, []byte{0x01, 0x02}},
					// Written to BINARY(N), so right padded
					{3, []byte{0x01, 0x02, 0x03, 0x00, 0x00}},
				},
			},
		},
	},
	{
		Name: "VARBINARY to BINARY widening",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col1 VARBINARY(3));",
			"INSERT into t values (1, 0x01);",
			"alter table t add unique index idx1 (col1);",
		},
		RightSetUpScript: []string{
			"alter table t modify column col1 BINARY(5);",
			"INSERT into t values (2, 0x0102);",
		},
		LeftSetUpScript: []string{
			"INSERT into t values (3, 0x010203);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select pk, col1 from t order by pk;",
				Expected: []sql.Row{
					// NOTE: When MySQL converts from BINARY(N) to VARBINARY(N), it does not change any values. But...
					//       when converting from VARBINARY(N) to BINARY(N), MySQL *DOES* right-pad any values up to
					//       N bytes, so all values here are right padded, matching MySQL's behavior.
					{1, []byte{0x01, 0x00, 0x00, 0x00, 0x00}},
					{2, []byte{0x01, 0x02, 0x00, 0x00, 0x00}},
					{3, []byte{0x01, 0x02, 0x03, 0x00, 0x00}},
				},
			},
		},
	},
	{
		Name: "TINYBLOB to BINARY(300) widening",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col1 TINYBLOB);",
			"INSERT into t values (1, 0x01);",
			"alter table t add unique index idx1 (col1(10));",
		},
		RightSetUpScript: []string{
			"alter table t modify column col1 BINARY(255);",
			"INSERT into t values (2, 0x0102);",
		},
		LeftSetUpScript: []string{
			"INSERT into t values (3, 0x010203);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				// When MySQL converts from TINYTEXT to BINARY(255), MySQL right-pads each existing value
				// with null bytes, to expand the value up to 255 bytes.
				Query:    "select pk, length(col1) from t order by pk;",
				Expected: []sql.Row{{1, 255}, {2, 255}, {3, 255}},
			},
		},
	},
	{
		Name: "TINYTEXT to VARCHAR(300) widening",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col1 TINYTEXT);",
			"INSERT into t values (1, 'tiny tiny text');",
			"alter table t add unique index idx1 (col1(10));",
		},
		RightSetUpScript: []string{
			"alter table t modify column col1 VARCHAR(300);",
			"INSERT into t values (2, 'more teeny tiny text');",
		},
		LeftSetUpScript: []string{
			"INSERT into t values (3, 'the teeniest of tiny text');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "select pk, col1 from t order by pk;",
				Expected: []sql.Row{{1, "tiny tiny text"}, {2, "more teeny tiny text"}, {3, "the teeniest of tiny text"}},
			},
		},
	},
}

var SchemaChangeTestsSchemaConflicts = []MergeScriptTest{
	{
		// Type widening - these changes move from smaller types to bigger types, so they are guaranteed to be safe.
		// TODO: We don't support automatically converting all types in merges yet, so currently these won't
		//       automatically merge and instead return schema conflicts.
		Name: "type widening",
		AncSetUpScript: []string{
			"set @@autocommit=0;",
			"CREATE table t (pk int primary key, col1 enum('blue', 'green'), col2 float, col3 smallint, " +
				"col4 decimal(4,2), col5 varchar(10), col6 set('a', 'b'), col7 bit(1));",
			"INSERT into t values (1, 'blue', 1.0, 1, 0.1, 'one', 'a,b', 1);",
			"alter table t add index idx1 (col1);",
		},
		RightSetUpScript: []string{
			"alter table t modify column col1 enum('blue', 'green', 'red');",
			"alter table t modify column col2 double;",
			"alter table t modify column col3 bigint;",
			"alter table t modify column col4 decimal(8,4);",
			"alter table t modify column col5 varchar(20);",
			"alter table t modify column col6 set('a', 'b', 'c');",
			"alter table t modify column col7 bit(2);",
			"INSERT into t values (3, 'red', 3.0, 420, 0.001, 'three', 'a,b,c', 3);",
		},
		LeftSetUpScript: []string{
			"INSERT into t values (2, 'green', 2.0, 2, 0.2, 'two', 'a,b', 1);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{{"t", nil, uint64(4)}},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query: "select table_name, our_schema, their_schema, base_schema from dolt_schema_conflicts;",
				Expected: []sql.Row{{"t",
					"CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` enum('blue','green'),\n  `col2` float,\n  `col3` smallint,\n  `col4` decimal(4,2),\n  `col5` varchar(10),\n  `col6` set('a','b'),\n  `col7` bit(1),\n  PRIMARY KEY (`pk`),\n  KEY `idx1` (`col1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					"CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` enum('blue','green','red'),\n  `col2` double,\n  `col3` bigint,\n  `col4` decimal(8,4),\n  `col5` varchar(20),\n  `col6` set('a','b','c'),\n  `col7` bit(2),\n  PRIMARY KEY (`pk`),\n  KEY `idx1` (`col1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					"CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` enum('blue','green'),\n  `col2` float,\n  `col3` smallint,\n  `col4` decimal(4,2),\n  `col5` varchar(10),\n  `col6` set('a','b'),\n  `col7` bit(1),\n  PRIMARY KEY (`pk`),\n  KEY `idx1` (`col1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"}},
			},
		},
	},
	{
		// Type shortening – these changes move from a larger type to a smaller type and are not always safe.
		// For now, we automatically fail all of these with a schema conflict that the user must resolve, but in
		// theory, we could try to apply these changes and see if the data in the tables is compatible or not, but
		// that's an optimization left for the future. Until then, customers can manually alter their schema to
		// get merges to work, based on the schema conflict information.
		Name: "type shortening",
		AncSetUpScript: []string{
			"set @@autocommit=0;",
			"CREATE TABLE t (pk int primary key, col1 enum('blue','green','red'), col2 double, col3 bigint, col4 decimal(8,4), " +
				"col5 varchar(20), col6 set('a','b','c'), col7 bit(2));",
			"INSERT into t values (3, 'green', 3.0, 420, 0.001, 'three', 'a,b', 1);",
			"alter table t add index idx1 (col1);",
		},
		RightSetUpScript: []string{
			"alter table t modify column col1 enum('blue', 'green');",
			"alter table t modify column col2 float;",
			"alter table t modify column col3 smallint;",
			"alter table t modify column col4 decimal(4,2);",
			"alter table t modify column col5 varchar(10);",
			"alter table t modify column col6 set('a', 'b');",
			"alter table t modify column col7 bit(1);",
			"INSERT into t values (1, 'blue', 1.0, 1, 0.1, 'one', 'a,b', 1);",
		},
		LeftSetUpScript: []string{
			"INSERT into t values (2, 'green', 2.0, 2, 0.2, 'two', 'a,b', 1);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{{"t", nil, uint64(7)}},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query: "select table_name, our_schema, their_schema, base_schema from dolt_schema_conflicts;",
				Expected: []sql.Row{{"t",
					"CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` enum('blue','green','red'),\n  `col2` double,\n  `col3` bigint,\n  `col4` decimal(8,4),\n  `col5` varchar(20),\n  `col6` set('a','b','c'),\n  `col7` bit(2),\n  PRIMARY KEY (`pk`),\n  KEY `idx1` (`col1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					"CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` enum('blue','green'),\n  `col2` float,\n  `col3` smallint,\n  `col4` decimal(4,2),\n  `col5` varchar(10),\n  `col6` set('a','b'),\n  `col7` bit(1),\n  PRIMARY KEY (`pk`),\n  KEY `idx1` (`col1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					"CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` enum('blue','green','red'),\n  `col2` double,\n  `col3` bigint,\n  `col4` decimal(8,4),\n  `col5` varchar(20),\n  `col6` set('a','b','c'),\n  `col7` bit(2),\n  PRIMARY KEY (`pk`),\n  KEY `idx1` (`col1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"}},
			},
		},
	},
	{
		// Dolt indexes currently use the set of columns covered by the index, as a unique identifier for matching
		// indexes on either side of a merge. As Dolt's index support has grown, this isn't guaranteed to be a unique
		// id anymore, so instead of allowing a race condition in the merge logic, if we detect that multiple indexes
		// cover the same set of columns, we return a schema conflict and let the user decide how to resolve it.
		Name: "duplicate index tag set",
		AncSetUpScript: []string{
			"set @@autocommit=0;",
			"CREATE table t (pk int primary key, col1 varchar(100));",
			"INSERT into t values (1, '100'), (2, '200');",
			"alter table t add unique index idx1 (col1);",
		},
		RightSetUpScript: []string{
			"alter table t add index idx2 (col1(10));",
		},
		LeftSetUpScript: []string{
			"INSERT into t values (3, '300');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{{"t", nil, uint64(1)}},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query: "select table_name, our_schema, their_schema, base_schema, description from dolt_schema_conflicts;",
				Expected: []sql.Row{{"t",
					"CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` varchar(100),\n  PRIMARY KEY (`pk`),\n  UNIQUE KEY `idx1` (`col1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					"CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` varchar(100),\n  PRIMARY KEY (`pk`),\n  UNIQUE KEY `idx1` (`col1`),\n  KEY `idx2` (`col1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					"CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` varchar(100),\n  PRIMARY KEY (`pk`),\n  UNIQUE KEY `idx1` (`col1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					"multiple indexes covering the same column set cannot be merged: 'idx1' and 'idx2'"}},
			},
		},
	},
	{
		Name: "index conflicts: both sides add an index with the same name, same columns, but different type",
		AncSetUpScript: []string{
			"set @@autocommit=0;",
			"CREATE table t (pk int primary key, col1 int, col2 varchar(100));",
		},
		RightSetUpScript: []string{
			"alter table t add index idx1 (col2(2));",
			"INSERT into t values (1, 10, '100');",
		},
		LeftSetUpScript: []string{
			"alter table t add index idx1 (col2);",
			"INSERT into t values (2, 20, '200');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{{"t", nil, uint64(2)}},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query: "select table_name, base_schema, our_schema, their_schema from dolt_schema_conflicts;",
				Expected: []sql.Row{{"t",
					"CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` int,\n  `col2` varchar(100),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					"CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` int,\n  `col2` varchar(100),\n  PRIMARY KEY (`pk`),\n  KEY `idx1` (`col2`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					"CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` int,\n  `col2` varchar(100),\n  PRIMARY KEY (`pk`),\n  KEY `idx1` (`col2`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
				}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/2973
		Name: "modifying a column on one side of a merge, and deleting it on the other",
		AncSetUpScript: []string{
			"set @@autocommit=0;",
			"create table t(i int primary key, j int);",
		},
		RightSetUpScript: []string{
			"alter table t drop column j;",
		},
		LeftSetUpScript: []string{
			"alter table t modify column j varchar(24);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{{"t", nil, uint64(1)}},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select table_name from dolt_schema_conflicts",
				Expected: []sql.Row{{"t"}},
			},
		},
	},
	{
		Name: "type changes to a column on both sides of a merge",
		AncSetUpScript: []string{
			"set @@autocommit=0;",
			"create table t(i int primary key, j int);",
		},
		RightSetUpScript: []string{
			"alter table t modify column j varchar(100);",
		},
		LeftSetUpScript: []string{
			"alter table t modify column j float;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{{"t", nil, uint64(1)}},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select table_name from dolt_schema_conflicts",
				Expected: []sql.Row{{"t"}},
			},
		},
	},
	{
		Name: "changing the type of a column",
		AncSetUpScript: []string{
			"set @@autocommit=0;",
			"create table t (pk int primary key, col1 int);",
			"insert into t values (1, 10), (2, 20);",
		},
		RightSetUpScript: []string{
			"alter table t modify column col1 varchar(100)",
			"insert into t values (3, 'thirty'), (4, 'forty')",
		},
		LeftSetUpScript: []string{
			"insert into t values (5, 50), (6, 60);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{{"t", nil, uint64(1)}},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query: "select table_name, our_schema, their_schema, base_schema from dolt_schema_conflicts;",
				Expected: []sql.Row{{"t",
					"CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` int,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					"CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` varchar(100),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					"CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` int,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"}},
			},
		},
	},
	{
		Name: "changing the type of a column with an index",
		AncSetUpScript: []string{
			"set @@autocommit=0;",
			"create table t (pk int primary key, col1 int, INDEX col1_idx (col1));",
			"insert into t values (1, 100), (2, 20);",
		},
		RightSetUpScript: []string{
			"alter table t modify column col1 varchar(100);",
			"insert into t values (3, 'thirty'), (4, 'forty')",
		},
		LeftSetUpScript: []string{
			"insert into t values (5, 50), (6, 60);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{{"t", nil, uint64(1)}},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query: "select table_name, our_schema, their_schema, base_schema from dolt_schema_conflicts;",
				Expected: []sql.Row{{"t",
					"CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` int,\n  PRIMARY KEY (`pk`),\n  KEY `col1_idx` (`col1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					"CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` varchar(100),\n  PRIMARY KEY (`pk`),\n  KEY `col1_idx` (`col1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					"CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` int,\n  PRIMARY KEY (`pk`),\n  KEY `col1_idx` (`col1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"}},
			},
		},
	},
	{
		Name: "dropping column on right shifts column index between compatible types",
		AncSetUpScript: []string{
			"set @@autocommit=0;",
			"CREATE TABLE t (id int PRIMARY KEY, a int, b char(20), c char(20), INDEX(c));",
			`INSERT INTO t VALUES (1, 1, "2", "3")`,
		},
		RightSetUpScript: []string{
			"ALTER TABLE t DROP COLUMN b;",
			`UPDATE t SET c = "4";`,
		},
		LeftSetUpScript: []string{
			"UPDATE t SET a = 2;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
		},
	},

	// Unsupported automatic merge cases
	{
		// In this case, we can't auto merge a new column, because we don't know what value to plug in for existing rows,
		// since it can't be NULL and there's no default value specified. The only resolution option we could apply
		// automatically is `dolt conflicts resolve --ours`, which would ignore the new column. Since we have limited
		// resolution options, instead of reporting this through the schema conflict interface, we throw an error.
		Name: "add a non-nullable column, with no default value",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col1 int);",
			"INSERT into t values (1, 10);",
		},
		RightSetUpScript: []string{
			"alter table t add column col3 int not null;",
			"alter table t add index idx1 (col3, col1);",
		},
		LeftSetUpScript: []string{
			"insert into t values (2, 20);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				ExpectedErr: merge.ErrUnmergeableNewColumn,
			},
			{
				Query:       "call dolt_merge('right');",
				ExpectedErr: merge.ErrUnmergeableNewColumn,
			},
		},
	},
	{
		// This merge test reports a conflict on pk=1, because the tuple value is different on the left side, right
		// side, and base. The value is the base is (10, '100'), on the right is nil, and on the left is ('100'),
		// because the data migration for the schema change happens before the diff iterator is invoked.
		// This should NOT be a conflict for a user – Dolt should not conflate the schema merge data migration with
		// a real data conflict created by a user. Allowing this is still better than completely blocking all schema
		// merges though, so we can live with this while we continue iterating and fine-tuning schema merge logic.
		Name: "schema change combined with drop row",
		AncSetUpScript: []string{
			"SET autocommit = 0",
			"CREATE table t (pk int primary key, col1 int, col2 varchar(100), UNIQUE KEY unique1 (col2, pk));",
			"INSERT into t values (1, 10, '100'), (2, 20, '200');",
			"alter table t add index idx1 (pk, col1);",
			"alter table t add index idx2 (pk, col1, col2);",
			"alter table t add index idx3 (col1, col2);",
			"alter table t add index idx4 (pk, col2);",
			"CREATE INDEX idx5 ON t(col2(2));",
		},
		RightSetUpScript: []string{
			"alter table t drop column col1;",
			"insert into t values (3, '300'), (4, '400');",
			"delete from t where pk = 1;",
		},
		LeftSetUpScript: []string{
			"insert into t values (5, 50, '500'), (6, 60, '600');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				// See the comment above about why this should NOT report a conflict and why this is skipped
				Skip:     true,
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Skip:     true,
				Query:    "select pk, col2 from t;",
				Expected: []sql.Row{{2, "200"}, {3, "300"}, {4, "400"}, {5, "500"}, {6, "600"}},
			},
		},
	},
	{
		Name: "adding a non-null column with a default value to one side",
		AncSetUpScript: []string{
			"set dolt_force_transaction_commit = on;",
			"create table t (pk int primary key, col1 int);",
			"insert into t values (1, 1);",
		},
		RightSetUpScript: []string{
			"alter table t add column col2 int not null default 0",
			"alter table t add column col3 int;",
			"insert into t values (2, 2, 2, null);",
		},
		LeftSetUpScript: []string{
			"insert into t values (3, 3);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, 1, 0, nil}, {2, 2, 2, nil}, {3, 3, 0, nil}},
			},
			{
				Query:    "select pk, violation_type from dolt_constraint_violations_t",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "adding a non-null column with a default value to one side (with update to existing row)",
		AncSetUpScript: []string{
			"set dolt_force_transaction_commit = on;",
			"create table t (pk int primary key, col1 int);",
			"insert into t values (1, 1);",
		},
		RightSetUpScript: []string{
			"alter table t add column col2 int not null default 0",
			"alter table t add column col3 int;",
			"update t set col2 = 1 where pk = 1;",
			"insert into t values (2, 2, 2, null);",
		},
		LeftSetUpScript: []string{
			"insert into t values (3, 3);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				SkipResultsCheck: true,
				Query:            "call dolt_merge('right');",
				Expected:         []sql.Row{{"", 0, 0, "merge successful"}}, // non-symmetric result
			},
			{
				Skip:     true,
				Query:    "select * from t;", // fails with row(1,1,0,NULL)
				Expected: []sql.Row{{1, 1, 1, nil}, {2, 2, 2, nil}, {3, 3, 0, nil}},
			},
			{
				Query:    "select pk, violation_type from dolt_constraint_violations_t",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "adding a not-null constraint and default value to a column",
		AncSetUpScript: []string{
			"set dolt_force_transaction_commit = on;",
			"create table t (pk int primary key, col1 int);",
			"insert into t values (1, null), (2, null);",
		},
		RightSetUpScript: []string{
			"update t set col1 = 9999 where col1 is null;",
			"alter table t modify column col1 int not null default 9999;",
			"insert into t values (3, 30), (4, 40);",
		},
		LeftSetUpScript: []string{
			"insert into t values (5, null), (6, null);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query: "select pk, col1 from t;",
				Expected: []sql.Row{
					{1, 9999},
					{2, 9999},
					{3, 30},
					{4, 40},
				},
			},
			{
				Query: "select pk, violation_type from dolt_constraint_violations_t",
				Expected: []sql.Row{
					{5, "not null"},
					{6, "not null"},
				},
			},
		},
	},
	{
		Name: "adding a not-null constraint and default value to a column, alongside table rewrite",
		AncSetUpScript: []string{
			"set dolt_force_transaction_commit = on;",
			"create table t (pk int primary key, col1 int);",
			"insert into t values (1, null), (2, null);",
		},
		RightSetUpScript: []string{
			"update t set col1 = 9999 where col1 is null;",
			"alter table t modify column col1 int not null default 9999;",
			"alter table t add column col2 int default 100",
			"insert into t values (3, 30, 200), (4, 40, 300);",
		},
		LeftSetUpScript: []string{
			"insert into t values (5, null), (6, null);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query: "select * from t;",
				Expected: []sql.Row{
					{1, 9999, 100},
					{2, 9999, 100},
					{3, 30, 200},
					{4, 40, 300},
				},
			},
			{
				Query: "select pk, violation_type from dolt_constraint_violations_t",
				Expected: []sql.Row{
					{5, "not null"},
					{6, "not null"},
				},
			},
		},
	},
	{
		Name: "adding a not-null constraint to one side",
		AncSetUpScript: []string{
			"set dolt_force_transaction_commit = on;",
			"create table t (pk int primary key, col1 int);",
			"insert into t values (1, null), (2, null);",
		},
		RightSetUpScript: []string{
			"update t set col1 = 0 where col1 is null;",
			"alter table t modify col1 int not null;",
		},
		LeftSetUpScript: []string{
			"insert into t values (3, null);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query: "select pk, col1 from t;",
				Expected: []sql.Row{
					{1, 0},
					{2, 0},
				},
			},
			{
				Query: "select violation_type, pk, violation_info from dolt_constraint_violations_t",
				Expected: []sql.Row{
					{"not null", 3, merge.NullViolationMeta{Columns: []string{"col1"}}},
				},
			},
		},
	},
}

var SchemaChangeTestsGeneratedColumns = []MergeScriptTest{
	{
		Name: "reordering a column",
		AncSetUpScript: []string{
			"CREATE table t (pk int primary key, col1 int, col2 varchar(100) as (concat(col1, 'hello')) stored);",
			"INSERT into t (pk, col1) values (1, 10), (2, 20);",
			"alter table t add index idx1 (pk, col1);",
			"alter table t add index idx2 (col2);",
			"alter table t add index idx3 (pk, col1, col2);",
			"alter table t add index idx4 (col1, col2);",
			"alter table t add index idx5 (col2, col1);",
			"alter table t add index idx6 (col2, pk, col1);",
		},
		RightSetUpScript: []string{
			"alter table t modify col1 int after col2;",
			"insert into t (pk, col1) values (3, 30), (4, 40);",
		},
		LeftSetUpScript: []string{
			"insert into t (pk, col1) values (5, 50), (6, 60);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select pk, col1, col2 from t;",
				Expected: []sql.Row{
					{1, 10, "10hello"}, {2, 20, "20hello"},
					{3, 30, "30hello"}, {4, 40, "40hello"},
					{5, 50, "50hello"}, {6, 60, "60hello"}},
			},
		},
	},
	{
		Name: "adding columns to a table with a virtual column",
		AncSetUpScript: []string{
			"create table t (pk int primary key, col1 int as (pk + 1));",
			"insert into t (pk) values (1);",
			"alter table t add index idx1 (col1, pk);",
			"alter table t add index idx2 (col1);",
		},
		RightSetUpScript: []string{
			"alter table t add column col2 int;",
			"alter table t add column col3 int;",
			"insert into t (pk, col2, col3) values (2, 4, 5);",
		},
		LeftSetUpScript: []string{
			"insert into t (pk) values (3);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select pk, col1, col2, col3 from t order by pk",
				Expected: []sql.Row{
					{1, 2, nil, nil},
					{2, 3, 4, 5},
					{3, 4, nil, nil}},
			},
		},
	},
	{
		Name: "adding a virtual column to one side, regular columns to other side",
		AncSetUpScript: []string{
			"create table t (pk int primary key);",
			"insert into t (pk) values (1);",
		},
		RightSetUpScript: []string{
			"alter table t add column col1 int as (pk + 1)",
			"insert into t (pk) values (3);",
			"alter table t add index idx1 (col1, pk);",
			"alter table t add index idx2 (col1);",
		},
		LeftSetUpScript: []string{
			"alter table t add column col2 int;",
			"alter table t add column col3 int;",
			"insert into t (pk, col2, col3) values (2, 4, 5);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select pk, col1, col2, col3 from t;",
				Expected: []sql.Row{
					{1, 2, nil, nil},
					{2, 3, 4, 5},
					{3, 4, nil, nil},
				},
			},
		},
	},
	{
		Name: "adding a virtual column to one side",
		AncSetUpScript: []string{
			"create table t (pk int primary key);",
			"insert into t (pk) values (1);",
		},
		RightSetUpScript: []string{
			"alter table t add column col1 int as (pk + 1)",
			"insert into t (pk) values (3);",
			"alter table t add index idx1 (col1, pk);",
			"alter table t add index idx2 (col1);",
		},
		LeftSetUpScript: []string{
			"insert into t (pk) values (2);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select pk, col1 from t;",
				Expected: []sql.Row{
					{1, 2},
					{2, 3},
					{3, 4},
				},
			},
		},
	},
	{
		Name: "adding a stored generated column to one side",
		AncSetUpScript: []string{
			"create table t (pk int primary key);",
			"insert into t (pk) values (1);",
		},
		RightSetUpScript: []string{
			"alter table t add column col1 int as (pk + 1) stored",
			"insert into t (pk) values (3);",
			"alter table t add index idx1 (col1, pk);",
			"alter table t add index idx2 (col1);",
		},
		LeftSetUpScript: []string{
			"insert into t (pk) values (2);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select pk, col1 from t;",
				Expected: []sql.Row{
					{1, 2},
					{2, 3},
					{3, 4},
				},
			},
		},
	},
	{
		Name: "adding generated columns to both sides",
		AncSetUpScript: []string{
			"create table t (pk int primary key);",
			"insert into t values (1), (2);",
		},
		RightSetUpScript: []string{
			"alter table t add column col2 varchar(100) as (concat(pk, 'hello'));",
			"insert into t (pk) values (3), (4);",
			"alter table t add index (col2);",
		},
		LeftSetUpScript: []string{
			"alter table t add column col1 int as (pk + 100) stored;",
			"insert into t (pk) values (5), (6);",
			"alter table t add index (col1);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
				Skip:     true, // this fails merging right into left
			},
			{
				Query: "select pk, col1, col2 from t;",
				Expected: []sql.Row{
					{1, 101, "1hello"},
					{2, 102, "2hello"},
					{3, 103, "3hello"},
					{4, 104, "4hello"},
					{5, 105, "5hello"},
					{6, 106, "6hello"},
				},
				Skip: true, // this fails merging right into left
			},
		},
	},
	{
		Name: "adding virtual columns to both sides",
		AncSetUpScript: []string{
			"create table t (pk int primary key);",
			"insert into t values (1), (2);",
		},
		RightSetUpScript: []string{
			"alter table t add column col2 varchar(100) as (concat(pk, 'hello'));",
			"insert into t (pk) values (3), (4);",
			"alter table t add index (col2);",
		},
		LeftSetUpScript: []string{
			"alter table t add column col1 int as (pk + 100);",
			"insert into t (pk) values (5), (6);",
			"alter table t add index (col1);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
				Skip:     true, // this fails merging right into left
			},
			{
				Query: "select pk, col1, col2 from t;",
				Expected: []sql.Row{
					{1, 101, "1hello"},
					{2, 102, "2hello"},
					{3, 103, "3hello"},
					{4, 104, "4hello"},
					{5, 105, "5hello"},
					{6, 106, "6hello"},
				},
				Skip: true, // this fails merging right into left
			},
		},
	},
	{
		Name: "convergent schema changes with virtual columns",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col1 int);",
			"INSERT into t values (1, 10);",
		},
		RightSetUpScript: []string{
			"alter table t modify column col1 int not null;",
			"alter table t add column col3 int as (pk + 1);",
			"alter table t add index idx1 (col3, col1);",
		},
		LeftSetUpScript: []string{
			"alter table t modify column col1 int not null;",
			"alter table t add column col3 int as (pk + 1);",
			"alter table t add index idx1 (col3, col1);",
			"update t set col1=-1000 where t.pk = 1;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "show create table t;",
				Skip:  true, // there should be an index on col3, but there isn't
				Expected: []sql.Row{{"t",
					"CREATE TABLE `t` (\n" +
						"  `pk` int NOT NULL,\n" +
						"  `col1` int NOT NULL,\n" +
						"  `col3` int GENERATED ALWAYS AS ((pk + 1)),\n" +
						"  PRIMARY KEY (`pk`)\n" +
						"  KEY `idx1` (`col3`,`col1`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, -1000, 2}},
			},
		},
	},
}

var SchemaChangeTestsForJsonConflicts = []MergeScriptTest{
	{
		Name: "json merge succeeds without @@dolt_dont_merge_json",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, j json);",
			"INSERT into t values (1, '{}');",
		},
		RightSetUpScript: []string{
			`update t set j = '{"a": 1}';`,
		},
		LeftSetUpScript: []string{
			`update t set j = '{"b": 2}';`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select * from t;",
				Expected: []sql.Row{
					{
						1, `{"a": 1, "b": 2}`,
					},
				},
			},
		},
	},
	{
		Name: "json merge fails with @@dolt_dont_merge_json",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"set @@dolt_dont_merge_json = 1;",
			"CREATE table t (pk int primary key, j json);",
			"INSERT into t values (1, '{}');",
		},
		RightSetUpScript: []string{
			`update t set j = '{"a": 1}';`,
		},
		LeftSetUpScript: []string{
			`update t set j = '{"b": 2}';`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_preview_merge_conflicts_summary('main', 'right');",
				Expected: []sql.Row{{"t", uint64(1), uint64(0)}},
			},
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select * from dolt_conflicts;",
				Expected: []sql.Row{{"t", uint(1)}},
			},
			{
				Query: "select base_j, our_j, their_j from dolt_conflicts_t;",
				Expected: []sql.Row{
					{
						`{}`, `{"b": 2}`, `{"a": 1}`,
					},
				},
			},
		},
	},
}

// These tests are not run because they cause panics during set-up.
// Each one is labeled with a GitHub issue.
var DisabledSchemaChangeTests = []MergeScriptTest{}
