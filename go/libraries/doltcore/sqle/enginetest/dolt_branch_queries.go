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
	"fmt"
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"
)

var ForeignKeyBranchTests = []queries.ScriptTest{
	{
		Name: "create fk on branch",
		SetUpScript: []string{
			"call dolt_branch('b1')",
			"use mydb/b1",
			"ALTER TABLE child ADD CONSTRAINT fk_named FOREIGN KEY (v1) REFERENCES parent(v1);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "use mydb/b1",
				SkipResultsCheck: true,
			},
			{
				Query: "SHOW CREATE TABLE child;",
				Expected: []sql.Row{{"child", "CREATE TABLE `child` (\n" +
					"  `id` int NOT NULL,\n" +
					"  `v1` int,\n" +
					"  `v2` int,\n" +
					"  PRIMARY KEY (`id`),\n" +
					"  KEY `fk_named` (`v1`),\n" +
					"  CONSTRAINT `fk_named` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:       "insert into child values (1, 1, 1)",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query:            "use mydb/main",
				SkipResultsCheck: true,
			},
			{
				Query: "SHOW CREATE TABLE child;",
				Expected: []sql.Row{{"child", "CREATE TABLE `child` (\n" +
					"  `id` int NOT NULL,\n" +
					"  `v1` int,\n" +
					"  `v2` int,\n" +
					"  PRIMARY KEY (`id`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "insert into child values (1, 1, 1)",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:       "insert into `mydb/b1`.child values (1, 1, 1)",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
		},
	},
	{
		Name: "create fk with branch checkout",
		SetUpScript: []string{
			"call dolt_branch('b1')",
			"call dolt_checkout('b1')",
			"ALTER TABLE child ADD CONSTRAINT fk_named FOREIGN KEY (v1) REFERENCES parent(v1);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "call dolt_checkout('b1')",
				SkipResultsCheck: true,
			},
			{
				Query: "SHOW CREATE TABLE child;",
				Expected: []sql.Row{{"child", "CREATE TABLE `child` (\n" +
					"  `id` int NOT NULL,\n" +
					"  `v1` int,\n" +
					"  `v2` int,\n" +
					"  PRIMARY KEY (`id`),\n" +
					"  KEY `fk_named` (`v1`),\n" +
					"  CONSTRAINT `fk_named` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:       "insert into child values (1, 1, 1)",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query:            "call dolt_checkout('main')",
				SkipResultsCheck: true,
			},
			{
				Query: "SHOW CREATE TABLE child;",
				Expected: []sql.Row{{"child", "CREATE TABLE `child` (\n" +
					"  `id` int NOT NULL,\n" +
					"  `v1` int,\n" +
					"  `v2` int,\n" +
					"  PRIMARY KEY (`id`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "insert into child values (1, 1, 1)",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
		},
	},
	{
		Name: "create fk on branch not being used",
		SetUpScript: []string{
			"call dolt_branch('b1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "ALTER TABLE `mydb/b1`.child ADD CONSTRAINT fk_named FOREIGN KEY (v1) REFERENCES parent(v1);",
				Skip:  true, // Incorrectly flagged as a cross-DB foreign key relation
			},
			{
				Query: "SHOW CREATE TABLE `mydb/b1`.child;",
				Skip:  true,
				Expected: []sql.Row{{"child", "CREATE TABLE `child` (\n" +
					"  `id` int NOT NULL,\n" +
					"  `v1` int,\n" +
					"  `v2` int,\n" +
					"  PRIMARY KEY (`id`),\n" +
					"  KEY `v1` (`v1`),\n" +
					"  CONSTRAINT `fk_named` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:       "insert into `mydb/b1`.child values (1, 1, 1)",
				Skip:        true,
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query: "SHOW CREATE TABLE child;",
				Skip:  true,
				Expected: []sql.Row{{"child", "CREATE TABLE `child` (\n" +
					"  `id` int NOT NULL,\n" +
					"  `v1` int,\n" +
					"  `v2` int,\n" +
					"  PRIMARY KEY (`id`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "insert into child values (1, 1, 1)",
				Skip:     true,
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
		},
	},
}

var ViewBranchTests = []queries.ScriptTest{
	{
		Name: "create view on branch",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int)",
			"insert into t1 values (1, 1), (2, 2), (3, 3)",
			"call dolt_commit('-Am', 'first commit')",
			"call dolt_branch('b1')",
			"use mydb/b1",
			"create view v1 as select * from t1 where a > 2",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "use mydb/b1",
				SkipResultsCheck: true,
			},
			{
				Query:    "select * from v1",
				Expected: []sql.Row{{3, 3}},
			},
			{
				Query:            "use mydb/main",
				SkipResultsCheck: true,
			},
			{
				Query:       "select * from v1",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:    "select * from `mydb/b1`.v1",
				Expected: []sql.Row{{3, 3}},
			},
		},
	},
	{
		Name: "create view on different branch",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int)",
			"insert into t1 values (1, 1), (2, 2), (3, 3)",
			"call dolt_commit('-Am', 'first commit')",
			"call dolt_branch('b1')",
			"create view `mydb/b1`.v1 as select * from t1 where a > 2",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "use mydb/b1",
				SkipResultsCheck: true,
			},
			{
				Query:    "select * from v1",
				Expected: []sql.Row{{3, 3}},
				Skip:     true, // https://github.com/dolthub/dolt/issues/6078
			},
			{
				Query:            "use mydb/main",
				SkipResultsCheck: true,
			},
			{
				Query:       "select * from v1",
				ExpectedErr: sql.ErrTableNotFound,
				Skip:        true, // https://github.com/dolthub/dolt/issues/6078
			},
			{
				Query:    "select * from `mydb/b1`.v1",
				Expected: []sql.Row{{3, 3}},
				Skip:     true, // https://github.com/dolthub/dolt/issues/6078
			},
		},
	},
}

var DdlBranchTests = []queries.ScriptTest{
	{
		Name: "create table on branch",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int)",
			"insert into t1 values (1, 1), (2, 2), (3, 3)",
			"call dolt_commit('-Am', 'first commit')",
			"call dolt_branch('b1')",
			"use mydb/b1",
			"create table t2 (a int primary key, b int)",
			"insert into t2 values (4, 4)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "use mydb/b1",
				SkipResultsCheck: true,
			},
			{
				Query:    "select * from t2",
				Expected: []sql.Row{{4, 4}},
			},
			{
				Query:            "use mydb/main",
				SkipResultsCheck: true,
			},
			{
				Query:       "select * from t2",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:    "select * from `mydb/b1`.t2",
				Expected: []sql.Row{{4, 4}},
			},
		},
	},
	{
		Name: "create table on different branch",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int)",
			"insert into t1 values (1, 1), (2, 2), (3, 3)",
			"call dolt_commit('-Am', 'first commit')",
			"call dolt_branch('b1')",
			"create table `mydb/b1`.t2 (a int primary key, b int)",
			"insert into `mydb/b1`.t2 values (4,4)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "use mydb/b1",
				SkipResultsCheck: true,
			},
			{
				Query:    "select * from t2",
				Expected: []sql.Row{{4, 4}},
			},
			{
				Query:            "use mydb/main",
				SkipResultsCheck: true,
			},
			{
				Query:       "select * from t2",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:    "select * from `mydb/b1`.t2",
				Expected: []sql.Row{{4, 4}},
			},
		},
	},
	{
		Name: "create table on different branch, autocommit off",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int)",
			"insert into t1 values (1, 1), (2, 2), (3, 3)",
			"call dolt_commit('-Am', 'first commit')",
			"call dolt_branch('b1')",
			"set autocommit = off",
			"create table `mydb/b1`.t2 (a int primary key, b int)",
			"insert into `mydb/b1`.t2 values (4,4)",
			"commit",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "use mydb/b1",
				SkipResultsCheck: true,
			},
			{
				Query:    "select * from t2",
				Expected: []sql.Row{{4, 4}},
			},
			{
				Query:            "use mydb/main",
				SkipResultsCheck: true,
			},
			{
				Query:       "select * from t2",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:    "select * from `mydb/b1`.t2",
				Expected: []sql.Row{{4, 4}},
			},
		},
	},
	{
		Name: "alter table on different branch, add column",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int)",
			"insert into t1 values (1, 1), (2, 2), (3, 3)",
			"call dolt_commit('-Am', 'first commit')",
			"call dolt_branch('b1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "alter table `mydb/b1`.t1 add column c int",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0}}},
			},
			{
				Query:    "select * from `mydb/b1`.t1",
				Expected: []sql.Row{{1, 1, nil}, {2, 2, nil}, {3, 3, nil}},
			},
			{
				Query:    "select * from t1",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
		},
	},
	{
		Name: "alter table on different branch, drop column",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int)",
			"insert into t1 values (1, 1), (2, 2), (3, 3)",
			"call dolt_commit('-Am', 'first commit')",
			"call dolt_branch('b1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "alter table `mydb/b1`.t1 drop column b",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0}}},
			},
			{
				Query:    "select * from `mydb/b1`.t1",
				Expected: []sql.Row{{1}, {2}, {3}},
			},
			{
				Query:    "select * from t1",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
		},
	},
	{
		Name: "alter table on different branch, modify column",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int)",
			"insert into t1 values (1, 1), (2, 2), (3, 3)",
			"call dolt_commit('-Am', 'first commit')",
			"call dolt_branch('b1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "alter table `mydb/b1`.t1 modify column b varchar(1) first",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0}}},
			},
			{
				Query:    "select * from `mydb/b1`.t1",
				Expected: []sql.Row{{"1", 1}, {"2", 2}, {"3", 3}},
			},
			{
				Query:    "select * from t1",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
		},
	},
	{
		Name: "alter table on different branch, create and drop index",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int)",
			"insert into t1 values (1, 1), (2, 2), (3, 3)",
			"call dolt_commit('-Am', 'first commit')",
			"call dolt_branch('b1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "create index idx on `mydb/b1`.t1 (b)",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0}}},
			},
			{
				Query: "show create table `mydb/b1`.t1",
				Expected: []sql.Row{{"t1", "CREATE TABLE `t1` (\n" +
					"  `a` int NOT NULL,\n" +
					"  `b` int,\n" +
					"  PRIMARY KEY (`a`),\n" +
					"  KEY `idx` (`b`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.Row{{"t1", "CREATE TABLE `t1` (\n" +
					"  `a` int NOT NULL,\n" +
					"  `b` int,\n" +
					"  PRIMARY KEY (`a`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "alter table `mydb/b1`.t1 drop index idx",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0}}},
			},
			{
				Query: "show create table `mydb/b1`.t1",
				Expected: []sql.Row{{"t1", "CREATE TABLE `t1` (\n" +
					"  `a` int NOT NULL,\n" +
					"  `b` int,\n" +
					"  PRIMARY KEY (`a`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "alter table on different branch, add and drop constraint",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int)",
			"insert into t1 values (1, 1), (2, 2), (3, 3)",
			"call dolt_commit('-Am', 'first commit')",
			"call dolt_branch('b1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "alter table `mydb/b1`.t1 add constraint chk1 check (b < 4)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table `mydb/b1`.t1",
				Expected: []sql.Row{{"t1", "CREATE TABLE `t1` (\n" +
					"  `a` int NOT NULL,\n" +
					"  `b` int,\n" +
					"  PRIMARY KEY (`a`),\n" +
					"  CONSTRAINT `chk1` CHECK ((`b` < 4))\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:       "insert into `mydb/b1`.t1 values (4, 4)",
				ExpectedErr: sql.ErrCheckConstraintViolated,
			},
			{
				Query: "show create table t1",
				Expected: []sql.Row{{"t1", "CREATE TABLE `t1` (\n" +
					"  `a` int NOT NULL,\n" +
					"  `b` int,\n" +
					"  PRIMARY KEY (`a`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "insert into t1 values (4, 4)",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "alter table `mydb/b1`.t1 drop constraint chk1",
				Expected: []sql.Row{},
			},
			{
				Query: "show create table `mydb/b1`.t1",
				Expected: []sql.Row{{"t1", "CREATE TABLE `t1` (\n" +
					"  `a` int NOT NULL,\n" +
					"  `b` int,\n" +
					"  PRIMARY KEY (`a`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
}

type indexQuery struct {
	Query string
	Index bool
}

var BranchPlanTests = []struct {
	Name        string
	SetUpScript []string
	Queries     []indexQuery
}{
	{
		Name: "use index on branch database",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int)",
			"insert into t1 values (1, 1), (2, 2), (3, 3)",
			"call dolt_commit('-Am', 'first commit')",
			"call dolt_branch('b1')",
			"use mydb/b1",
			"create index idx on t1 (b)",
		},
		Queries: []indexQuery{
			{
				Query: "select * from t1 where b = 1",
				Index: true,
			},
			{
				Query: "use mydb/main",
			},
			{
				Query: "select * from `mydb/b1`.t1 where b = 1",
				Index: true,
			},
		},
	},
	{
		Name: "use index on branch database join",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int)",
			"insert into t1 values (1, 1), (2, 2), (3, 3)",
			"call dolt_commit('-Am', 'first commit')",
			"call dolt_branch('b1')",
			"use mydb/b1",
			"create index idx on t1 (b)",
		},
		Queries: []indexQuery{
			{
				Query: "select /*+ LOOKUP_JOIN(t1a,t1b) */ * from t1 t1a join t1 t1b on t1a.b = t1b.b order by 1",
				Index: true,
			},
			{
				Query: "select * from `mydb/main`.t1 t1a join `mydb/main`.t1 t1b on t1a.b = t1b.b order by 1",
				Index: false,
			},
			{
				Query: "use mydb/main",
			},
			{
				Query: "select /*+ LOOKUP_JOIN(t1a,t1b) */ * from t1 t1a join t1 t1b on t1a.b = t1b.b order by 1",
				Index: true,
			},
			{
				Query: "select /*+ LOOKUP_JOIN(t1a,t1b) */ * from `mydb/b1`.t1 t1a join `mydb/b1`.t1 t1b on t1a.b = t1b.b order by 1",
				Index: true,
			},
		},
	},
}

func TestIndexedAccess(t *testing.T, e enginetest.QueryEngine, harness enginetest.Harness, query string, index bool) {
	ctx := enginetest.NewContext(harness)
	ctx = ctx.WithQuery(query)
	a, err := analyzeQuery(ctx, e, query)
	require.NoError(t, err)
	var hasIndex bool
	transform.Inspect(a, func(n sql.Node) bool {
		if n == nil {
			return false
		}
		if _, ok := n.(*plan.IndexedTableAccess); ok {
			hasIndex = true
		}
		return true
	})

	if index != hasIndex {
		fmt.Println(a.String())
	}
	require.Equal(t, index, hasIndex)
}

func analyzeQuery(ctx *sql.Context, e enginetest.QueryEngine, query string) (sql.Node, error) {
	binder := planbuilder.New(ctx, e.EngineAnalyzer().Catalog, sql.NewMysqlParser())
	parsed, _, _, qFlags, err := binder.Parse(query, false)
	if err != nil {
		return nil, err
	}
	return e.EngineAnalyzer().Analyze(ctx, parsed, nil, qFlags)
}
