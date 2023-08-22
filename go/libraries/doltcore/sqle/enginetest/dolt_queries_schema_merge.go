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
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1}},
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
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0}},
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
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0}},
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
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0}},
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
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0}},
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
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0}},
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
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0}},
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
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0}},
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
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0}},
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
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, "hello", "hellohello"}, {2, "hello", "hi"}, {3, "hello", "hellohello"}},
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
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0}},
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
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0}},
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
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0}},
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
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0}},
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
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0}},
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
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0}},
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
		// TODO: Changing a column's collation may require rewriting the table and any indexes on that column.
		//       For now, we just detect the schema incompatibility and return schema conflict metadata, but we could
		//       go further here and automatically convert the data to the new collation.
		Name: "changing the collation of a column",
		AncSetUpScript: []string{
			"set @@autocommit=0;",
			"create table t (pk int primary key, col1 varchar(32) character set utf8mb4 collate utf8mb4_bin, index col1_idx (col1));",
			"insert into t values (1, 'ab'), (2, 'Ab');",
		},
		RightSetUpScript: []string{
			"alter table t modify col1 varchar(32) character set utf8mb4 collate utf8mb4_general_ci;",
		},
		LeftSetUpScript: []string{
			"insert into t values (3, 'c');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1}},
			},
			{
				Query: "select table_name, our_schema, their_schema, base_schema from dolt_schema_conflicts;",
				Expected: []sql.Row{{"t",
					"CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` varchar(32) COLLATE utf8mb4_bin,\n  PRIMARY KEY (`pk`),\n  KEY `col1_idx` (`col1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					"CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` varchar(32) COLLATE utf8mb4_general_ci,\n  PRIMARY KEY (`pk`),\n  KEY `col1_idx` (`col1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					"CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` varchar(32) COLLATE utf8mb4_bin,\n  PRIMARY KEY (`pk`),\n  KEY `col1_idx` (`col1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"}},
			},
		},
	},
}

var SchemaChangeTestsConstraints = []MergeScriptTest{
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
				Expected: []sql.Row{{doltCommit, 0, 0}},
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
				Expected: []sql.Row{{"", 0, 1}},
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
				Expected: []sql.Row{{doltCommit, 0, 0}},
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
				Expected: []sql.Row{{"", 0, 1}},
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
				Expected: []sql.Row{{"", 0, 1}},
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
					{uint(2), "1", "same", types.JSONDocument{Val: merge.UniqCVMeta{Columns: []string{"col2"}, Name: "unique1"}}},
					{uint(2), "10", "same", types.JSONDocument{Val: merge.UniqCVMeta{Columns: []string{"col2"}, Name: "unique1"}}},
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
				Expected: []sql.Row{{doltCommit, 0, 0}},
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
				Expected: []sql.Row{{doltCommit, 0, 0}},
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
				Expected: []sql.Row{{"", 0, 1}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.Row{{"t", uint64(1)}},
			},
			{
				Query:    "select violation_type, pk, col1, col2, violation_info like '\\%NOT((col1 = col2))\\%' from dolt_constraint_violations_t;",
				Expected: []sql.Row{{uint64(3), 1, 4, 4, true}},
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
				Expected: []sql.Row{{doltCommit, 0, 0}},
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
				Expected: []sql.Row{{"", 0, 1}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.Row{{"t", uint64(1)}},
			},
			{
				Query:    "select violation_type, pk, col1, col2, violation_info like '%(col1 + col2)%' from dolt_constraint_violations_t;",
				Expected: []sql.Row{{uint64(3), 1, 0, 0, true}},
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
				Expected: []sql.Row{{"", 0, 1}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.Row{{"t", uint64(1)}},
			},
			{
				Query:    "select violation_type, pk, col2, col3, violation_info like '\\%NOT((col2 = col3))\\%' from dolt_constraint_violations_t;",
				Expected: []sql.Row{{uint64(3), 1, 100, 100, true}},
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
				Expected: []sql.Row{{doltCommit, 0, 0}},
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
				Expected: []sql.Row{{"", 0, 1}},
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
				Expected: []sql.Row{{doltCommit, 0, 0}},
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
				Expected: []sql.Row{{"", 0, 1}},
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
				Expected: []sql.Row{{"", 0, 1}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.Row{{"t", uint64(1)}},
			},
			{
				Query:    `select violation_type, pk, col1, violation_info like "\%NOT((col1 = concat('he','llo')))\%" from dolt_constraint_violations_t;`,
				Expected: []sql.Row{{uint64(3), 2, "hello", true}},
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
				Expected: []sql.Row{{"", 0, 1}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.Row{{"t", uint64(1)}},
			},
			{
				Query:    `select violation_type, c0, col0, col1, violation_info like "\%NOT((col1 = concat('he','llo')))\%" from dolt_constraint_violations_t;`,
				Expected: []sql.Row{{uint64(3), 2, "hola", "hello", true}},
			},
		},
	},
}

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
				Expected: []sql.Row{{doltCommit, 0, 0}},
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
		Name: "type widening - enums and sets",
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
				Expected: []sql.Row{{doltCommit, 0, 0}},
			},
			{
				Query: "select * from t order by pk;",
				Expected: []sql.Row{
					{1, uint64(1), uint64(3)},
					{2, uint64(2), uint64(3)},
					{3, uint64(3), uint64(5)},
				},
			},
		},
	},
}

var SchemaChangeTestsSchemaConflicts = []MergeScriptTest{
	{
		// Type widening - these changes move from smaller types to bigger types, so they are guaranteed to be safe.
		// TODO: We don't support automatically converting column types in merges yet, so currently these won't
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
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1}},
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
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1}},
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
		Name: "varchar shortening",
		AncSetUpScript: []string{
			"set autocommit = 0;",
			"CREATE table t (pk int primary key, col1 varchar(10));",
			"INSERT into t values (1, '123');",
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
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1}},
			},
			{
				Query:    "select table_name, description like 'incompatible column types for column ''col1''%' from dolt_schema_conflicts;",
				Expected: []sql.Row{{"t", true}},
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
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1}},
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
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1}},
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
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1}},
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
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1}},
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
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1}},
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
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1}},
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
				// See the comment above about why this should NOT report a conflict and why this is skipped
				Skip:     true,
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0}},
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
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0}},
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
				SkipResultsCheck: true,
				Query:            "call dolt_merge('right');",
				Expected:         []sql.Row{{"", 0, 0}}, // non-symmetric result
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
				Expected: []sql.Row{{"", 0, 1}},
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
					{5, uint16(4)},
					{6, uint16(4)},
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
				Expected: []sql.Row{{"", 0, 1}},
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
					{uint16(4), 3, types.JSONDocument{Val: merge.NullViolationMeta{Columns: []string{"col1"}}}},
				},
			},
		},
	},
}
