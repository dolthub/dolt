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
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// DoltTempTableScripts tests temporary tables.
// Temporary tables are not supported in GMS, eventually should move those tests there.
var DoltTempTableScripts = []queries.ScriptTest{
	{
		Name: "temporary and non-temporary table name collisions",
		SetUpScript: []string{
			"create table t1 (id varchar(100))",
			"insert into t1 values ('this is a non-temporary table')",
			"create temporary table t1 (id varchar(100))", // okay to create temporary table with the same name as a non-temporary table
			"insert into t1 values ('this is a temporary table')",
			"create temporary table t2 (id int)",
			"create table t2 (id int)", // okay to create a non-temporary table with the same name as a temporary table
			"create temporary table t3 (id int)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// should show temporary table t1
				Query:    "select * from t1",
				Expected: []sql.Row{{"this is a temporary table"}},
			},
			{
				// cannot create a temporary table with the same name as another temporary table
				Query:       "create temporary table t3 (id int)",
				ExpectedErr: sql.ErrTableAlreadyExists,
			},
			{
				// should show temporary table
				Query:    "show create table t1",
				Expected: []sql.Row{{"t1", "CREATE TEMPORARY TABLE `t1` (\n  `id` varchar(100)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				// should drop temporary table t1
				Query:    "drop table t1",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// should show non-temporary table that was created before
				Query:    "show create table t1",
				Expected: []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `id` varchar(100)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "temporary table supports auto increment",
		SetUpScript: []string{
			"create temporary table t (i int primary key auto_increment)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "show create table t;",
				Expected: []sql.Row{
					{"t", "CREATE TEMPORARY TABLE `t` (\n" +
						"  `i` int NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "insert into t values (), (), ()",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 3, InsertID: 1}},
				},
			},
			{
				Query: "show create table t;",
				Expected: []sql.Row{
					{"t", "CREATE TEMPORARY TABLE `t` (\n" +
						"  `i` int NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "select * from t",
				Expected: []sql.Row{
					{1},
					{2},
					{3},
				},
			},
			{
				Query: "insert into t values (100), (1000)",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 2, InsertID: 100}},
				},
			},
			{
				Query: "show create table t;",
				Expected: []sql.Row{
					{"t", "CREATE TEMPORARY TABLE `t` (\n" +
						"  `i` int NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=1001 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "insert into t values (), (), ()",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 3, InsertID: 0x3e9}},
				},
			},
			{
				Query: "select * from t",
				Expected: []sql.Row{
					{1},
					{2},
					{3},
					{100},
					{1000},
					{1001},
					{1002},
					{1003},
				},
			},
		},
	},
	{
		Name: "temporary table inline indexes",
		// PostgreSQL does not support inline INDEX/UNIQUE KEY declarations, SHOW CREATE TABLE, or FORCE INDEX.
		Dialect: "mysql",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "CREATE TEMPORARY TABLE inline_indexes (id VARCHAR(512), n INT, INDEX idx (id(255)), UNIQUE KEY uniq (n))",
			},
			{
				Query: "INSERT INTO inline_indexes VALUES ('abc', 1), ('def', 2)",
			},
			{
				Query:    "SHOW CREATE TABLE inline_indexes",
				Expected: []sql.Row{{"inline_indexes", "CREATE TEMPORARY TABLE `inline_indexes` (\n  `id` varchar(512),\n  `n` int,\n  KEY `idx` (`id`(255)),\n  UNIQUE KEY `uniq` (`n`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "SELECT id, n FROM inline_indexes FORCE INDEX (idx) WHERE id = 'abc'",
				Expected: []sql.Row{{"abc", 1}},
			},
			{
				Query:       "INSERT INTO inline_indexes VALUES ('other', 1)",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:    "SELECT id, n FROM inline_indexes ORDER BY n",
				Expected: []sql.Row{{"abc", 1}, {"def", 2}},
			},
		},
	},
	{
		Name: "temporary table inline index with AS SELECT",
		// PostgreSQL does not support inline INDEX declarations in CTAS, SHOW CREATE TABLE, or FORCE INDEX.
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE source_entries (id VARCHAR(512))",
			"INSERT INTO source_entries VALUES ('abc'), ('def')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "CREATE TEMPORARY TABLE mat_entries (INDEX mat_entries_id (id(255))) AS SELECT id FROM source_entries",
			},
			{
				Query:    "SHOW CREATE TABLE mat_entries",
				Expected: []sql.Row{{"mat_entries", "CREATE TEMPORARY TABLE `mat_entries` (\n  `id` varchar(512),\n  KEY `mat_entries_id` (`id`(255))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "SELECT id FROM mat_entries FORCE INDEX (mat_entries_id) WHERE id = 'abc'",
				Expected: []sql.Row{{"abc"}},
			},
			{
				Query: "CREATE INDEX by_id ON mat_entries (id)",
			},
			{
				Query: "INSERT INTO mat_entries VALUES ('ghi')",
			},
			{
				Query:    "SELECT id FROM mat_entries FORCE INDEX (mat_entries_id) WHERE id = 'ghi'",
				Expected: []sql.Row{{"ghi"}},
			},
			{
				Query:    "SELECT id FROM mat_entries FORCE INDEX (by_id) WHERE id = 'abc'",
				Expected: []sql.Row{{"abc"}},
			},
			{
				Query:    "SELECT id FROM mat_entries FORCE INDEX (by_id) WHERE id = 'ghi'",
				Expected: []sql.Row{{"ghi"}},
			},
		},
	},
	{
		Name: "temporary table tag collision",
		SetUpScript: []string{
			"CREATE TABLE note(a int, b int, userid int);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "CREATE TEMPORARY TABLE tmp_tbl(a int, b int, c int, d int, e int, f int, g int);",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
		},
	},
	{
		Name:    "drop temporary table behavior",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (i int);",
			"create temporary table tmp (i int);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "show create table tmp;",
				Expected: []sql.Row{
					{"tmp", "CREATE TEMPORARY TABLE `tmp` (\n" +
						"  `i` int\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "drop temporary table tmp;",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},

			{
				Query: "create temporary table t (i int, j int);",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t;",
				Expected: []sql.Row{
					{"t", "CREATE TEMPORARY TABLE `t` (\n" +
						"  `i` int,\n" +
						"  `j` int\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "drop temporary table t;",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t;",
				Expected: []sql.Row{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query:       "drop temporary table t;",
				ExpectedErr: sql.ErrUnknownTable,
			},
		},
	},
	{
		Name:    "temporary tables in stored procedures",
		Dialect: "mysql",
		SetUpScript: []string{
			`
create procedure proc()
begin
  create temporary table tmp (i int primary key);
  insert into tmp values (1), (2), (3);
  select * from tmp;
end;
`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "call proc();",
				Expected: []sql.Row{
					{1},
					{2},
					{3},
				},
			},
			{
				Query: "select * from tmp;",
				Expected: []sql.Row{
					{1},
					{2},
					{3},
				},
			},
		},
	},
	{
		Name: "index lookups on temporary tables with zero or many ranges",
		SetUpScript: []string{
			"create temporary table t (x int primary key, y int, key(y))",
			"insert into t values (1, 10), (2, 20), (3, 30), (4, 40)",
			"create temporary table empty_t (x int primary key)",
			"create temporary table big_t (x int primary key)",
			"insert into big_t with recursive r(n) as (select 1 union all select n + 1 from r where n < 1000) select n from r",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select x from t where x = 1 and x = 2",
				Expected: []sql.Row{},
			},
			{
				Query:    "select x from empty_t where x = 1 and x = 2",
				Expected: []sql.Row{},
			},
			{
				Query:    "select x from t where x > 5 and x < 3",
				Expected: []sql.Row{},
			},
			{
				Query:    "select x from t where x in (1) and x not in (1)",
				Expected: []sql.Row{},
			},
			{
				Query:    "select x from t where y = 10 and y = 20",
				Expected: []sql.Row{},
			},
			{
				Query:    "select x from t where x = 1 or x = 3 order by x",
				Expected: []sql.Row{{1}, {3}},
			},
			{
				Query:    "select x from t where y = 20 or y = 40 order by x",
				Expected: []sql.Row{{2}, {4}},
			},
			{
				Query:    "select x from t where x < 2 or x > 3 order by x desc",
				Expected: []sql.Row{{4}, {1}},
			},
			{
				Query:    "select x from big_t where x = 500",
				Expected: []sql.Row{{500}},
			},
			{
				Query:    "select x from big_t where x = 500 or x = 7 order by x",
				Expected: []sql.Row{{7}, {500}},
			},
			{
				Query:    "update t set y = y + 1 where x = 1 and x = 2",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0, Info: plan.UpdateInfo{Matched: 0, Updated: 0}}}},
			},
			{
				Query:    "delete from t where x = 1 and x = 2",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "delete from t where x = 1 or x = 4",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				Query:    "select x, y from t order by x",
				Expected: []sql.Row{{2, 20}, {3, 30}},
			},
		},
	},
	{
		Name: "temporary table scans after index lookups",
		SetUpScript: []string{
			"create temporary table t (x int primary key, y int, key(y))",
			"insert into t values (1, 10), (2, 20), (3, 30), (4, 40)",
			"create temporary table tt (id int primary key, v varchar(32))",
			"insert into tt values (1, md5(1)), (2, md5(2)), (3, md5(3))",
			"alter table tt add index vidx (v)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select count(distinct id) from tt where v is not null",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "select count(distinct id) from tt where v = md5(id)",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "select x from t where y = 20",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "select x from t where y = x * 10 order by x",
				Expected: []sql.Row{{1}, {2}, {3}, {4}},
			},
			{
				Query:    "select a.x, b.x from t a join t b on b.y = a.x * 10 where a.x > 2 order by a.x",
				Expected: []sql.Row{{3, 3}, {4, 4}},
			},
		},
	},
}
