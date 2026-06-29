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
)

var SystemTableFunctionIndexTests = []queries.ScriptTest{
	{
		Name: "DOLT_DIFF() with secondary index basic use case",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20), index covering (c1, c2), index non_covering (c2));",
			"insert into t values(1, 'foo', 'a'), (2, 'bar', 'b'), (3, 'foo', 'c'), (4, 'bar', 'd');",
			"set @Commit0 = HashOf('HEAD');",
			"call dolt_add('.');",
			"call dolt_commit('-m', 'create table');",
			"set @Commit1 = HashOf('HEAD');",
			"insert into t values (5, 'foo', 'e'), (6, 'bar', 'f');",
			"delete from t where pk <= 2;",
			"update t set c1 = 'bar' where pk = 3;",
			"update t set c1 = 'foo' where pk = 4;",
			"call dolt_add('.');",
			"call dolt_commit('-m', 'insert rows');",
			"set @Commit2 = HashOf('HEAD');",
			"create table lookup_vals (val varchar(20));",
			"insert into lookup_vals values ('foo'), ('bar');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit1, @Commit2, 't');",
				Expected: []sql.Row{
					{nil, nil, nil, 1, "foo", "a", "removed"},
					{nil, nil, nil, 2, "bar", "b", "removed"},
					{3, "bar", "c", 3, "foo", "c", "modified"},
					{4, "foo", "d", 4, "bar", "d", "modified"},
					{5, "foo", "e", nil, nil, nil, "added"},
					{6, "bar", "f", nil, nil, nil, "added"},
				},
				ExpectedIndexes: []string{},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit1, @Commit2, 't') WHERE to_c1 = 'foo' ORDER BY to_pk;",
				Expected: []sql.Row{
					{4, "foo", "d", 4, "bar", "d", "modified"},
					{5, "foo", "e", nil, nil, nil, "added"},
				},
				ExpectedIndexes: []string{"to_covering"},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit1, @Commit2, 't') WHERE to_c1 = 'bar';",
				Expected: []sql.Row{
					{3, "bar", "c", 3, "foo", "c", "modified"},
					{6, "bar", "f", nil, nil, nil, "added"},
				},
				ExpectedIndexes: []string{"to_covering"},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit1, @Commit2, 't') WHERE from_c1 = 'foo' ORDER BY from_pk;",
				Expected: []sql.Row{
					{nil, nil, nil, 1, "foo", "a", "removed"},
					{3, "bar", "c", 3, "foo", "c", "modified"},
				},
				ExpectedIndexes: []string{"from_covering"},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit1, @Commit2, 't') WHERE from_c1 = 'bar';",
				Expected: []sql.Row{
					{nil, nil, nil, 2, "bar", "b", "removed"},
					{4, "foo", "d", 4, "bar", "d", "modified"},
				},
				ExpectedIndexes: []string{"from_covering"},
			},
			{
				// Only covering indexes can be used.
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit1, @Commit2, 't') WHERE to_c2 = 'c';",
				Expected: []sql.Row{
					{3, "bar", "c", 3, "foo", "c", "modified"},
				},
				ExpectedIndexes: []string{},
			},
			{
				Query: "SELECT /*+ JOIN_ORDER(lookup_vals,dolt_diff) LOOKUP_JOIN(lookup_vals,dolt_diff) */ val, to_pk, to_c1, diff_type from lookup_vals join dolt_diff(@Commit1, @Commit2, 't') on lookup_vals.val = to_c1;",
				Expected: []sql.Row{
					{"bar", 3, "bar", "modified"},
					{"bar", 6, "bar", "added"},
					{"foo", 4, "foo", "modified"},
					{"foo", 5, "foo", "added"},
				},
				ExpectedIndexes: []string{"to_covering"},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit0, @Commit1, 't') WHERE to_c1 = 'foo' ORDER BY to_pk;",
				Expected: []sql.Row{
					{4, "foo", "d", 4, "bar", "d", "modified"},
					{5, "foo", "e", nil, nil, nil, "added"},
				},
				ExpectedIndexes: []string{"to_covering"},
			},
		},
	},
	{
		// There are two considerations with how virtual columns might affect DOLT_DIFF() with secondary indexes:
		// 1. A secondary index does not need to cover virtual columns in order to be usable.
		// 2. If a secondary index *does* cover a virtual column, it should not affect the output.
		Name: "test DOLT_DIFF() secondary index with generated columns",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 int, c3 int as (pk+c1+c2) virtual, index covering (c2, c1), index covering_with_virtual (c1, c2, c3) );",
			"set @Commit0 = HashOf('HEAD');",
			"call dolt_add('.');",
			"call dolt_commit('-m', 'create table');",
			"set @Commit1 = HashOf('HEAD');",
			"insert into t (pk, c1, c2) values(1, 2, 3), (4, 5, 6);",
			"call dolt_add('.');",
			"call dolt_commit('-m', 'insert rows');",
			"set @Commit2 = HashOf('HEAD');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT to_pk, to_c1, to_c2, to_c3, from_pk, from_c1, from_c2, from_c3, diff_type from dolt_diff(@Commit1, @Commit2, 't') WHERE to_c2 = 3;",
				Expected: []sql.Row{
					{1, 2, 3, nil, nil, nil, nil, nil, "added"},
				},
				ExpectedIndexes: []string{"to_covering"},
			},
			{
				// TODO: When using a secondary index that covers a virtual column, DOLT_DIFF produces values for that column.
				// When not using a secondary index, those values are always nil. We should decide which behavior is correct.
				Query: "SELECT to_pk, to_c1, to_c2, to_c3, from_pk, from_c1, from_c2, from_c3, diff_type from dolt_diff(@Commit1, @Commit2, 't') WHERE to_c1 = 5;",
				Expected: []sql.Row{
					{4, 5, 6, 15, nil, nil, nil, nil, "added"},
				},
				ExpectedIndexes: []string{"to_covering_with_virtual"},
			},
		},
	},
	{
		Name: "test DOLT_DIFF() with expression index",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 int);",
			"create index expression_index on t ((c1+c2));",
			"set @Commit0 = HashOf('HEAD');",
			"call dolt_add('.');",
			"call dolt_commit('-m', 'create table');",
			"set @Commit1 = HashOf('HEAD');",
			"insert into t (pk, c1, c2) values(1, 2, 3), (4, 5, 6);",
			"call dolt_add('.');",
			"call dolt_commit('-m', 'insert rows');",
			"set @Commit2 = HashOf('HEAD');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit1, @Commit2, 't') WHERE to_c1 + to_c2 = 5;",
				Expected: []sql.Row{
					{1, 2, 3, nil, nil, nil, "added"},
				},
				ExpectedIndexes: []string{"to_covering"},
			},
		},
	},
	{
		Name: "test DOLT_DIFF() with unusual primary index ordinals",
		SetUpScript: []string{
			"create table t (c0 int, c1 int, c2 int, c3 int, c4 int, primary key (c4, c2, c0), index sec_idx (c3, c1));",
			"set @Commit0 = HashOf('HEAD');",
			"call dolt_add('.');",
			"call dolt_commit('-m', 'create table');",
			"set @Commit1 = HashOf('HEAD');",
			"insert into t (c0, c1, c2, c3, c4) values(0, 1, 2, 3, 4), (5, 6, 7, 8, 9);",
			"call dolt_add('.');",
			"call dolt_commit('-m', 'insert rows');",
			"set @Commit2 = HashOf('HEAD');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT to_c0, to_c1, to_c2, to_c3, to_c4, diff_type from dolt_diff(@Commit1, @Commit2, 't') WHERE to_c3 = 8;",
				Expected: []sql.Row{
					{5, 6, 7, 8, 9, "added"},
				},
				ExpectedIndexes: []string{"to_sec_idx"},
			},
		},
	},
	{
		Name: "test DOLT_PATCH() indexes",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"insert into t values(1, 'one', 'two');",
			"set @Commit0 = HashOf('HEAD');",
			"call dolt_add('.');",
			"call dolt_commit('-m', 'commit one');",
			"set @Commit1 = HashOf('HEAD');",
			"create table diff_type_name(name varchar(20), t varchar(20));",
			"insert into diff_type_name values ('s', 'schema'), ('d', 'data');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT table_name, diff_type, statement from dolt_patch(@Commit0, @Commit1, 't') WHERE diff_type = 'schema';",
				Expected: []sql.Row{
					{"t", "schema", "CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `c1` varchar(20),\n  `c2` varchar(20),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"},
				},
				ExpectedIndexes: []string{"diff_type"},
			},
			{
				Query: "SELECT table_name, diff_type, statement from dolt_patch(@Commit0, @Commit1, 't') WHERE diff_type = 'data';",
				Expected: []sql.Row{
					{"t", "data", "INSERT INTO `t` (`pk`,`c1`,`c2`) VALUES (1,'one','two');"},
				},
				ExpectedIndexes: []string{"diff_type"},
			},
			{
				Query: "SELECT /*+ JOIN_ORDER(diff_type_name,dolt_patch) LOOKUP_JOIN(diff_type_name,dolt_patch) */ name, t, table_name, statement from diff_type_name join dolt_patch(@Commit0, @Commit1, 't') on diff_type_name.t = diff_type;",
				Expected: []sql.Row{
					{"s", "schema", "t", "CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `c1` varchar(20),\n  `c2` varchar(20),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"},
					{"d", "data", "t", "INSERT INTO `t` (`pk`,`c1`,`c2`) VALUES (1,'one','two');"},
				},
				ExpectedIndexes: []string{"diff_type"},
			},
			{
				Query: "SELECT ( SELECT statement FROM (SELECT * FROM dolt_patch(@Commit0, @Commit1, 't') where diff_type = diff_type_name.t) as rhs) from diff_type_name;",
				Expected: []sql.Row{
					{"CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `c1` varchar(20),\n  `c2` varchar(20),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"},
					{"INSERT INTO `t` (`pk`,`c1`,`c2`) VALUES (1,'one','two');"},
				},
				ExpectedIndexes: []string{"diff_type"},
			},
		},
	},
}
