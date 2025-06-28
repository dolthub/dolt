// Copyright 2021 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

var DoltTransactionTests = []queries.TransactionTest{
	{
		Name: "duplicate inserts, autocommit on",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ insert into t values (2, 2)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:       "/* client b */ insert into t values (2, 2)",
				ExpectedErr: sql.ErrPrimaryKeyViolation,
			},
		},
	},
	{
		Name: "duplicate inserts, autocommit off",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into t values (2, 2)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into t values (2, 2)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
		},
	},
	{
		Name: "conflicting inserts",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into t values (2, 2)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into t values (2, 3)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:          "/* client b */ commit",
				ExpectedErrStr: sql.ErrLockDeadlock.New(dsess.ErrRetryTransaction.Error()).Error(),
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{ // client b gets a rollback after failed commit, so gets a new tx
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:          "/* client b */ insert into t values (2, 3)",
				ExpectedErrStr: "duplicate primary key given: [2]",
			},
		},
	},
	{
		Name: "duplicate updates, autocommit on",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1), (2, 2)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "/* client a */ update t set y = 2",
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: uint64(1),
					Info: plan.UpdateInfo{
						Matched: 2,
						Updated: 1,
					},
				}}},
			},
			{
				Query: "/* client b */ update t set y = 2",
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: uint64(0),
					Info: plan.UpdateInfo{
						Matched: 2,
						Updated: 0,
					},
				}}},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 2}, {2, 2}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 2}, {2, 2}},
			},
		},
	},
	{
		Name: "duplicate updates, autocommit off",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1), (2, 2)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query: "/* client a */ update t set y = 2",
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: uint64(1),
					Info: plan.UpdateInfo{
						Matched: 2,
						Updated: 1,
					},
				}}},
			},
			{
				Query: "/* client b */ update t set y = 2",
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: uint64(1),
					Info: plan.UpdateInfo{
						Matched: 2,
						Updated: 1,
					},
				}}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 2}, {2, 2}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 2}, {2, 2}},
			},
		},
	},
	{
		Name: "conflicting updates",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1), (2, 2)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query: "/* client a */ update t set y = 3",
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: uint64(2),
					Info: plan.UpdateInfo{
						Matched: 2,
						Updated: 2,
					},
				}}},
			},
			{
				Query: "/* client b */ update t set y = 4",
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: uint64(2),
					Info: plan.UpdateInfo{
						Matched: 2,
						Updated: 2,
					},
				}}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:          "/* client b */ commit",
				ExpectedErrStr: sql.ErrLockDeadlock.New(dsess.ErrRetryTransaction.Error()).Error(),
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 3}, {2, 3}},
			},
			{ // client b got rolled back when its commit failed, so it sees the same values as client a
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 3}, {2, 3}},
			},
			{
				Query:    "/* client b */ rollback",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 3}, {2, 3}},
			},
		},
	},
	{
		Name: "non overlapping updates (diff rows)",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1), (2, 2)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query: "/* client a */ update t set y = 3 where x = 1",
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: uint64(1),
					Info: plan.UpdateInfo{
						Matched: 1,
						Updated: 1,
					},
				}}},
			},
			{
				Query: "/* client b */ update t set y = 4 where x = 2",
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: uint64(1),
					Info: plan.UpdateInfo{
						Matched: 1,
						Updated: 1,
					},
				}}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 3}, {2, 4}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 3}, {2, 4}},
			},
		},
	},
	{
		Name: "non overlapping updates (diff cols)",
		SetUpScript: []string{
			"create table t (x int primary key, y int, z int)",
			"insert into t values (1, 1, 1), (2, 2, 2)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query: "/* client a */ update t set y = 2",
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: uint64(1),
					Info: plan.UpdateInfo{
						Matched: 2,
						Updated: 1,
					},
				}}},
			},
			{
				Query: "/* client b */ update t set z = 3",
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: uint64(2),
					Info: plan.UpdateInfo{
						Matched: 2,
						Updated: 2,
					},
				}}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 2, 3}, {2, 2, 3}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 2, 3}, {2, 2, 3}},
			},
		},
	},
	{
		Name: "duplicate deletes, autocommit on",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1), (2, 2)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ delete from t where y = 2",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ delete from t where y = 2",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}},
			},
		},
	},
	{
		Name: "duplicate deletes, autocommit off",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1), (2, 2)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ delete from t where y = 2",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ delete from t where y = 2",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}},
			},
		},
	},
	{
		Name: "non overlapping deletes",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1), (2, 2), (3, 3)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ delete from t where y = 2",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ delete from t where y = 3",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}},
			},
		},
	},
	{
		Name: "conflicting delete and update",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1), (2, 2)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query: "/* client a */ update t set y = 3 where y = 2",
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: uint64(1),
					Info: plan.UpdateInfo{
						Matched: 1,
						Updated: 1,
					},
				}}},
			},
			{
				Query:    "/* client b */ delete from t where y = 2",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:          "/* client b */ commit",
				ExpectedErrStr: sql.ErrLockDeadlock.New(dsess.ErrRetryTransaction.Error()).Error(),
			},
			{
				Query:    "/* client b */ rollback",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 3}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 3}},
			},
		},
	},
	{
		Name: "delete in one client, insert into another",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1), (2, 2)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ delete from t where y = 1",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ delete from t",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				Query:    "/* client b */ insert into t values (1,1)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "multiple client edit session",
		SetUpScript: []string{
			"create table t (x int primary key, y int, z int)",
			"insert into t values (1, 1, 1), (2, 2, 2)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client c */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query: "/* client a */ update t set y = 3 where y = 2",
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: uint64(1),
					Info: plan.UpdateInfo{
						Matched: 1,
						Updated: 1,
					},
				}}},
			},
			{
				Query:    "/* client b */ delete from t where y = 1",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query: "/* client c */ update t set z = 4 where y = 2",
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: uint64(1),
					Info: plan.UpdateInfo{
						Matched: 1,
						Updated: 1,
					},
				}}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{2, 3, 2}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{2, 3, 2}},
			},
			{
				Query:    "/* client c */ select * from t order by x",
				Expected: []sql.Row{{1, 1, 1}, {2, 2, 4}},
			},
			{
				Query:    "/* client c */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{2, 3, 4}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{2, 3, 4}},
			},
			{
				Query:    "/* client c */ select * from t order by x",
				Expected: []sql.Row{{2, 3, 4}},
			},
		},
	},
	{
		Name: "edits from different clients to table with out of order primary key set",
		SetUpScript: []string{
			"create table test (x int, y int, z int, primary key(z, y))",
			"insert into test values (1, 1, 1), (2, 2, 2)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query: "/* client a */ update test set y = 3 where y = 2",
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: uint64(1),
					Info: plan.UpdateInfo{
						Matched: 1,
						Updated: 1,
					},
				}}},
			},
			{
				Query: "/* client b */ update test set y = 5 where y = 2",
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: uint64(1),
					Info: plan.UpdateInfo{
						Matched: 1,
						Updated: 1,
					},
				}}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from test order by x",
				Expected: []sql.Row{{1, 1, 1}, {2, 3, 2}, {2, 5, 2}},
			},
			{
				Query:    "/* client b */ select * from test order by x",
				Expected: []sql.Row{{1, 1, 1}, {2, 3, 2}, {2, 5, 2}},
			},
			{
				Query:       "/* client b */ insert into test values (4,3,2)",
				ExpectedErr: sql.ErrPrimaryKeyViolation,
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/7956
		Name: "Merge unresolved FKs after resolved FKs were committed",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ SET @@foreign_key_checks=0;",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ SET @@autocommit=0;",
				SkipResultsCheck: true,
			},
			{
				// Create a table for the FK to reference
				Query:    "/* client a */ CREATE TABLE ref (id varchar(100) PRIMARY KEY, status int);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// Create a table with an FK
				Query:    "/* client a */ CREATE TABLE t (id int, ref_id varchar(100), FOREIGN KEY (ref_id) REFERENCES ref(id));",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client a */ COMMIT;",
				Expected: []sql.Row{},
			},
			{
				// Turn @@foreign_key_checks back on in client a
				Query:    "/* client a */ SET @@foreign_key_checks=1;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// Reference the table with an unresolved FK, so that it gets loaded and resolved
				Query:    "/* client a */ UPDATE t SET ref_id = 42 where ref_id > 100000;",
				Expected: []sql.Row{{types.OkResult{Info: plan.UpdateInfo{}}}},
			},
			{
				// Make any change in client b's session
				Query:    "/* client b */ create table foo (i int);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0}}},
			},
			{
				// Client a still has an unresolved FK at this point
				Query:    "/* client a */ COMMIT;",
				Expected: []sql.Row{},
			},
			{
				// Assert that client a can see the schema with the foreign key constraints still present
				Query:    "/* client a */ show create table t;",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `id` int,\n  `ref_id` varchar(100),\n  KEY `ref_id` (`ref_id`),\n  CONSTRAINT `t_ibfk_1` FOREIGN KEY (`ref_id`) REFERENCES `ref` (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				// Assert that client b can see the schema with the foreign key constraints still present
				Query:    "/* client b */ show create table t;",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `id` int,\n  `ref_id` varchar(100),\n  KEY `ref_id` (`ref_id`),\n  CONSTRAINT `t_ibfk_1` FOREIGN KEY (`ref_id`) REFERENCES `ref` (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name:        "TRANSACTION ISOLATION READ-COMMITTED does not break AUTOCOMMIT=OFF",
		SetUpScript: []string{},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ set session transaction isolation level read committed",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ set autocommit = off",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client a */ select @@transaction_isolation, @@autocommit",
				Expected: []sql.Row{{"READ-COMMITTED", 0}},
			},
			{
				Query:            "/* client a */ savepoint abc",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client a */ release savepoint abc",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "non-ff commit merge with multiple indexes on a column",
		SetUpScript: []string{
			"create table t1 (pk int primary key, val int)",
			"create index i1 on t1 (val)",
			"alter table t1 add unique key u1 (val)",
			"insert into t1 values (1, 1)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ set autocommit = off",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set autocommit = off",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client a */ insert into t1 values (2, 2)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into t1 values (3, 3)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:            "/* client a */ commit",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ commit",
				Skip:             true, // multiple indexes covering the same column set cannot be merged: 'i1' and 'u1'
				SkipResultsCheck: true,
			},
		},
	},
}

var DoltConflictHandlingTests = []queries.TransactionTest{
	{
		Name: "default behavior (rollback on commit conflict)",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key, val int)",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0, 0)",
			"CALL DOLT_COMMIT('-a', '-m', 'initial table');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ set autocommit = off",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ set autocommit = off",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into test values (1, 1)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into test values (1, 2)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:          "/* client b */ commit",
				ExpectedErrStr: sql.ErrLockDeadlock.New(dsess.ErrRetryTransaction.Error()).Error(),
			},
			{ // no conflicts, transaction got rolled back
				Query:    "/* client b */ select count(*) from dolt_conflicts",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "/* client b */ select * from test order by 1",
				Expected: []sql.Row{{0, 0}, {1, 1}},
			},
		},
	},
	{
		Name: "allow commit conflicts on, conflict on transaction commit",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key, val int)",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0, 0)",
			"CALL DOLT_COMMIT('-a', '-m', 'initial table');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ set autocommit = off, dolt_allow_commit_conflicts = on",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ set autocommit = off, dolt_allow_commit_conflicts = on",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into test values (1, 1)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into test values (1, 2)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:          "/* client b */ commit",
				ExpectedErrStr: sql.ErrLockDeadlock.New(dsess.ErrRetryTransaction.Error()).Error(),
			},
			{ // We see the merge value from a's commit here because we were rolled back and a new transaction begun
				Query:    "/* client b */ select * from test order by 1",
				Expected: []sql.Row{{0, 0}, {1, 1}},
			},
			{ // no conflicts, transaction got rolled back
				Query:    "/* client b */ select count(*) from dolt_conflicts",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "force commit on, conflict on transaction commit (same as dolt_allow_commit_conflicts)",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key, val int)",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0, 0)",
			"CALL DOLT_COMMIT('-a', '-m', 'initial table');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ set autocommit = off, dolt_force_transaction_commit = on",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ set autocommit = off, dolt_force_transaction_commit = on",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into test values (1, 1)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into test values (1, 2)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:          "/* client b */ commit",
				ExpectedErrStr: sql.ErrLockDeadlock.New(dsess.ErrRetryTransaction.Error()).Error(),
			},
			{ // We see the merge value from a's commit here because we were rolled back and a new transaction begun
				Query:    "/* client b */ select * from test order by 1",
				Expected: []sql.Row{{0, 0}, {1, 1}},
			},
			{ // no conflicts, transaction got rolled back
				Query:    "/* client b */ select count(*) from dolt_conflicts",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "allow commit conflicts on, conflict on dolt_merge",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key, val int)",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0, 0)",
			"CALL DOLT_COMMIT('-a', '-m', 'initial table');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ set autocommit = off, dolt_allow_commit_conflicts = on",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ set autocommit = off, dolt_allow_commit_conflicts = on",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into test values (1, 1)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:            "/* client b */ call dolt_checkout('-b', 'new-branch')",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ call dolt_commit('-am', 'commit on main')",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ insert into test values (1, 2)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:            "/* client b */ call dolt_commit('-am', 'commit on new-branch')",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ select * from dolt_preview_merge_conflicts_summary('new-branch', 'main')",
				Expected: []sql.Row{{"test", uint64(1), uint64(0)}},
			},
			{
				Query:    "/* client b */ call dolt_merge('main')",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "/* client b */ select count(*) from dolt_conflicts",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "/* client b */ select * from test order by 1",
				Expected: []sql.Row{{0, 0}, {1, 2}},
			},
			{ // no error because of our session settings
				// TODO: we should also be able to commit this if the other client made a compatible change
				//  (has the same merge conflicts we do), but that's an error right now
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{ // TODO: it should be possible to do this without specifying a literal in the subselect, but it's not working
				Query: "/* client b */ update test t set val = (select their_val from dolt_conflicts_test where our_pk = 1) where pk = 1",
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: 1,
					Info: plan.UpdateInfo{
						Matched: 1,
						Updated: 1,
					},
				}}},
			},
			{
				Query:    "/* client b */ delete from dolt_conflicts_test",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ select * from test order by 1",
				Expected: []sql.Row{{0, 0}, {1, 1}},
			},
			{
				Query:    "/* client b */ select count(*) from dolt_conflicts",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "force commit on, conflict on dolt_merge (same as dolt_allow_commit_conflicts)",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key, val int)",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0, 0)",
			"CALL DOLT_COMMIT('-a', '-m', 'initial table');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ set autocommit = off, dolt_force_transaction_commit = on",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ set autocommit = off, dolt_force_transaction_commit = on",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into test values (1, 1)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:            "/* client b */ call dolt_checkout('-b', 'new-branch')",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ call dolt_commit('-am', 'commit on main')",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ insert into test values (1, 2)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:            "/* client b */ call dolt_commit('-am', 'commit on new-branch')",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ select * from dolt_preview_merge_conflicts_summary('new-branch', 'main')",
				Expected: []sql.Row{{"test", uint64(1), uint64(0)}},
			},
			{
				Query:    "/* client b */ call dolt_merge('main')",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "/* client b */ select count(*) from dolt_conflicts",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "/* client b */ select * from test order by 1",
				Expected: []sql.Row{{0, 0}, {1, 2}},
			},
			{ // no error because of our session settings
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{ // TODO: it should be possible to do this without specifying a literal in the subselect, but it's not working
				Query: "/* client b */ update test t set val = (select their_val from dolt_conflicts_test where our_pk = 1) where pk = 1",
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: 1,
					Info: plan.UpdateInfo{
						Matched: 1,
						Updated: 1,
					},
				}}},
			},
			{
				Query:    "/* client b */ delete from dolt_conflicts_test",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ select * from test order by 1",
				Expected: []sql.Row{{0, 0}, {1, 1}},
			},
			{
				Query:    "/* client b */ select count(*) from dolt_conflicts",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "allow commit conflicts off, conflict on dolt_merge",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key, val int)",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0, 0)",
			"CALL DOLT_COMMIT('-a', '-m', 'initial table');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ set autocommit = off",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ set autocommit = off",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into test values (1, 1)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:            "/* client b */ call dolt_checkout('-b', 'new-branch')",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ call dolt_commit('-am', 'commit on main')",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ insert into test values (1, 2)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:            "/* client b */ call dolt_commit('-am', 'commit on new-branch')",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ select * from dolt_preview_merge_conflicts_summary('new-branch', 'main')",
				Expected: []sql.Row{{"test", uint64(1), uint64(0)}},
			},
			{
				Query:    "/* client b */ call dolt_merge('main')",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "/* client b */ select count(*) from dolt_conflicts",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "/* client b */ select * from test order by 1",
				Expected: []sql.Row{{0, 0}, {1, 2}},
			},
			{
				Query:    "/* client b */ insert into test values (2, 2)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:          "/* client b */ commit",
				ExpectedErrStr: dsess.ErrUnresolvedConflictsCommit.Error(),
			},
			{ // our transaction got rolled back, so we lose the above insert
				Query:    "/* client b */ select * from test order by 1",
				Expected: []sql.Row{{0, 0}, {1, 2}},
			},
		},
	},
	{
		Name: "conflicts from a DOLT_MERGE return an initially unhelpful error in a concurrent write scenario",
		SetUpScript: []string{
			"CREATE TABLE t (pk int PRIMARY KEY, col1 int);",
			"CALL DOLT_ADD('.')",
			"CALL DOLT_COMMIT('-am', 'create table');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"INSERT INTO t values (1, 100);",
			"CALL DOLT_COMMIT('-am', 'right edit');",

			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO t VALUES (1, 200);",
			"CALL DOLT_COMMIT('-am', 'left edit');",

			"SET dolt_allow_commit_conflicts = on;",
			"CALL DOLT_MERGE('right');",
			"SET dolt_allow_commit_conflicts = off;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ SET @@autocommit=0;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client b */ SET @@autocommit=0;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client a */ SELECT base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{{nil, nil, 1, 200, 1, 100}},
			},
			{
				Query:    "/* client b */ SELECT base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{{nil, nil, 1, 200, 1, 100}},
			},
			// nominal inserts that will not result in a conflict or constraint violation
			// They are needed to trigger a three-way transaction merge
			{
				Query:    "/* client a */ INSERT into t VALUES (2, 2);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ INSERT into t VALUES (3, 3);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ SET dolt_allow_commit_conflicts = on;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client a */ COMMIT;",
				Expected: []sql.Row{},
			},
			{
				Query: "/* client b */ COMMIT;",
				// TODO: No it didn't! Client b contains conflicts from an internal merge! Retrying will not help.
				ExpectedErrStr: sql.ErrLockDeadlock.New(dsess.ErrRetryTransaction.Error()).Error(),
			},
			{
				Query:    "/* client b */ INSERT into t VALUES (3, 3);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query: "/* client b */ COMMIT;",
				// Retrying did not help. But at-least the error makes sense.
				ExpectedErrStr: dsess.ErrUnresolvedConflictsCommit.Error(),
			},
		},
	},
	{
		Name: "transaction conflicts follows first-write-wins (a commits first)",
		SetUpScript: []string{
			"CREATE table t (pk int PRIMARY KEY, col1 int, INDEX col1_idx (col1));",
			"CREATE table keyless (col1 int);",
			"INSERT INTO t VALUES (1, 1);",
			"INSERT INTO keyless VALUES (1);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ START TRANSACTION",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ START TRANSACTION",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ UPDATE t SET col1 = -100 where pk = 1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "/* client b */ UPDATE t SET col1 = 100 where pk = 1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "/* client a */ INSERT into KEYLESS VALUES (1);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ INSERT into KEYLESS VALUES (1), (1);",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				Query:    "/* client a */ COMMIT;",
				Expected: []sql.Row{},
			},
			{
				Query:       "/* client b */ COMMIT;",
				ExpectedErr: sql.ErrLockDeadlock,
			},
			{
				Query:    "/* client b */ SELECT * from t;",
				Expected: []sql.Row{{1, -100}},
			},
			{
				Query:    "/* client b */ SELECT * from keyless;",
				Expected: []sql.Row{{1}, {1}},
			},
			{
				Query:    "/* client b */ SELECT * from t where col1 = -100;",
				Expected: []sql.Row{{1, -100}},
			},
		},
	},
	{
		Name: "transaction conflicts follows first-write-wins (b commits first)",
		SetUpScript: []string{
			"CREATE table t (pk int PRIMARY KEY, col1 int, INDEX col1_idx (col1));",
			"CREATE table keyless (col1 int);",
			"INSERT INTO t VALUES (1, 1);",
			"INSERT INTO keyless VALUES (1);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ START TRANSACTION",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ START TRANSACTION",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ UPDATE t SET col1 = -100 where pk = 1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "/* client b */ UPDATE t SET col1 = 100 where pk = 1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "/* client a */ INSERT into KEYLESS VALUES (1);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ INSERT into KEYLESS VALUES (1), (1);",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				Query:    "/* client b */ COMMIT;",
				Expected: []sql.Row{},
			},
			{
				Query:       "/* client a */ COMMIT;",
				ExpectedErr: sql.ErrLockDeadlock,
			},
			{
				Query:    "/* client b */ SELECT * from t;",
				Expected: []sql.Row{{1, 100}},
			},
			{
				Query:    "/* client b */ SELECT * from keyless;",
				Expected: []sql.Row{{1}, {1}, {1}},
			},
			{
				Query:    "/* client b */ SELECT * from t where col1 = 100;",
				Expected: []sql.Row{{1, 100}},
			},
		},
	},
}

var DoltStoredProcedureTransactionTests = []queries.TransactionTest{
	{
		Name: "committed conflicts are seen by other sessions",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key, val int)",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0, 0)",
			"CALL DOLT_COMMIT('-a', '-m', 'Step 1');",
			"CALL DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (1, 1);",
			"UPDATE test SET val=1000 WHERE pk=0;",
			"CALL DOLT_COMMIT('-a', '-m', 'this is a normal commit');",
			"CALL DOLT_CHECKOUT('main');",
			"UPDATE test SET val=1001 WHERE pk=0;",
			"CALL DOLT_COMMIT('-a', '-m', 'update a value');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ select * from dolt_preview_merge_conflicts_summary('main', 'feature-branch')",
				Expected: []sql.Row{{"test", uint64(1), uint64(0)}},
			},
			{
				Query:    "/* client a */ select * from dolt_preview_merge_conflicts_summary('main', 'feature-branch')",
				Expected: []sql.Row{{"test", uint64(1), uint64(0)}},
			},
			{
				Query:    "/* client a */ CALL DOLT_MERGE('feature-branch')",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "/* client a */ SELECT count(*) from dolt_conflicts_test",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "/* client b */ SELECT count(*) from dolt_conflicts_test",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "/* client a */ set dolt_allow_commit_conflicts = 1",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ SELECT count(*) from dolt_conflicts_test",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ CALL DOLT_MERGE('--abort')",
				Expected: []sql.Row{{"", 0, 0, "merge aborted"}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ SET @@dolt_allow_commit_conflicts = 0",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client a */ select * from dolt_preview_merge_conflicts_summary('main', 'feature-branch')",
				Expected: []sql.Row{{"test", uint64(1), uint64(0)}},
			},
			{
				Query:          "/* client a */ CALL DOLT_MERGE('feature-branch')",
				ExpectedErrStr: dsess.ErrUnresolvedConflictsAutoCommit.Error(),
			},
			{ // client rolled back on merge with conflicts
				Query:    "/* client a */ SELECT count(*) from dolt_conflicts_test",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ SELECT count(*) from dolt_conflicts_test",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "dolt_commit with one table, no merge conflict, no unstaged changes",
		SetUpScript: []string{
			"create table users (id int primary key, name varchar(32))",
			"insert into users values (1, 'tim'), (2, 'jim')",
			"call dolt_commit('-A', '-m', 'initial commit')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update users set name = 'tim2' where name = 'tim'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update users set name = 'jim2' where name = 'jim'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ call dolt_commit('-A', '-m', 'update tim')",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ call dolt_commit('-A', '-m', 'update jim')",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client a */ select count(*) from dolt_status", // clean working set
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "/* client b */ select count(*) from dolt_status", // clean working set
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "/* client a */ select * from users order by id",
				Expected: []sql.Row{{1, "tim2"}, {2, "jim2"}},
			},
			{
				Query:    "/* client b */ select * from users order by id",
				Expected: []sql.Row{{1, "tim2"}, {2, "jim2"}},
			},
		},
	},
	{
		Name: "mix of dolt_commit and normal commit",
		SetUpScript: []string{
			"create table users (id int primary key, name varchar(32))",
			"insert into users values (1, 'tim'), (2, 'jim')",
			"call dolt_commit('-A', '-m', 'initial commit')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update users set name = 'tim2' where name = 'tim'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update users set name = 'jim2' where name = 'jim'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ commit",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ call dolt_commit('-A', '-m', 'update jim')",
				SkipResultsCheck: true,
			},
			{
				// dirty working set: client a's changes were not committed to head
				Query:    "/* client a */ select * from dolt_status",
				Expected: []sql.Row{{"users", false, "modified"}},
			},
			{
				// dirty working set: client a's changes were not committed to head, but are visible to client b
				Query:    "/* client b */ select * from dolt_status",
				Expected: []sql.Row{{"users", false, "modified"}},
			},
			{
				Query:    "/* client a */ select * from users order by id",
				Expected: []sql.Row{{1, "tim2"}, {2, "jim2"}},
			},
			{
				Query:    "/* client b */ select * from users order by id",
				Expected: []sql.Row{{1, "tim2"}, {2, "jim2"}},
			},
			{
				// changes from client a are in the working set, but not in HEAD
				Query: "/* client a */ select from_id, to_id, from_name, to_name from dolt_diff('HEAD', 'WORKING', 'users') order by from_id, to_id",
				Expected: []sql.Row{
					{1, 1, "tim", "tim2"},
				},
			},
			{
				Query: "/* client b */ select from_id, to_id, from_name, to_name from dolt_diff('HEAD', 'WORKING', 'users') order by from_id, to_id",
				Expected: []sql.Row{
					{1, 1, "tim", "tim2"},
				},
			},
		},
	},
	{
		Name: "staged change in working set",
		SetUpScript: []string{
			"create table users (id int primary key, name varchar(32))",
			"insert into users values (1, 'tim'), (2, 'jim')",
			"call dolt_commit('-A', '-m', 'initial commit')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update users set name = 'tim2' where name = 'tim'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update users set name = 'jim2' where name = 'jim'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ call dolt_add('users')",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ call dolt_add('users')",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ commit",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ commit",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client a */ select * from users order by id",
				Expected: []sql.Row{{1, "tim2"}, {2, "jim2"}},
			},
			{
				Query:    "/* client b */ select * from users order by id",
				Expected: []sql.Row{{1, "tim2"}, {2, "jim2"}},
			},
			{
				// dirty working set: modifications are staged
				Query: "/* client a */ select * from dolt_status",
				Expected: []sql.Row{
					{"users", true, "modified"},
				},
			},
			{
				// dirty working set: modifications are staged
				Query: "/* client b */ select * from dolt_status",
				Expected: []sql.Row{
					{"users", true, "modified"},
				},
			},
			{
				// staged changes include changes from both A and B
				Query: "/* client a */ select from_id, to_id, from_name, to_name from dolt_diff('HEAD', 'STAGED', 'users') order by from_id, to_id",
				Expected: []sql.Row{
					{1, 1, "tim", "tim2"},
					{2, 2, "jim", "jim2"},
				},
			},
			{
				// staged changes include changes from both A and B
				Query: "/* client a */ select from_id, to_id, from_name, to_name from dolt_diff('HEAD', 'STAGED', 'users') order by from_id, to_id",
				Expected: []sql.Row{
					{1, 1, "tim", "tim2"},
					{2, 2, "jim", "jim2"},
				},
			},
		},
	},
	{
		Name: "staged and unstaged changes in working set",
		SetUpScript: []string{
			"create table users (id int primary key, name varchar(32))",
			"insert into users values (1, 'tim'), (2, 'jim')",
			"call dolt_commit('-A', '-m', 'initial commit')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update users set name = 'tim2' where name = 'tim'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update users set name = 'jim2' where name = 'jim'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ call dolt_add('users')",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ call dolt_add('users')",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update users set name = 'tim3' where name = 'tim2'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update users set name = 'jim3' where name = 'jim2'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ commit",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ commit",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client a */ select * from users order by id",
				Expected: []sql.Row{{1, "tim3"}, {2, "jim3"}},
			},
			{
				Query:    "/* client b */ select * from users order by id",
				Expected: []sql.Row{{1, "tim3"}, {2, "jim3"}},
			},
			{
				// dirty working set: modifications are staged and unstaged
				Query: "/* client a */ select * from dolt_status",
				Expected: []sql.Row{
					{"users", true, "modified"},
					{"users", false, "modified"},
				},
			},
			{
				// dirty working set: modifications are staged and unstaged
				Query: "/* client b */ select * from dolt_status",
				Expected: []sql.Row{
					{"users", true, "modified"},
					{"users", false, "modified"},
				},
			},
			{
				// staged changes include changes from both A and B at staged revision of data
				Query: "/* client a */ select from_id, to_id, from_name, to_name from dolt_diff('HEAD', 'STAGED', 'users') order by from_id, to_id",
				Expected: []sql.Row{
					{1, 1, "tim", "tim2"},
					{2, 2, "jim", "jim2"},
				},
			},
			{
				// staged changes include changes from both A and B at staged revision of data
				Query: "/* client a */ select from_id, to_id, from_name, to_name from dolt_diff('HEAD', 'STAGED', 'users') order by from_id, to_id",
				Expected: []sql.Row{
					{1, 1, "tim", "tim2"},
					{2, 2, "jim", "jim2"},
				},
			},
			{
				// working changes include changes from both A and B at working revision of data
				Query: "/* client a */ select from_id, to_id, from_name, to_name from dolt_diff('HEAD', 'WORKING', 'users') order by from_id, to_id",
				Expected: []sql.Row{
					{1, 1, "tim", "tim3"},
					{2, 2, "jim", "jim3"},
				},
			},
			{
				// working changes include changes from both A and B at working revision of data
				Query: "/* client a */ select from_id, to_id, from_name, to_name from dolt_diff('HEAD', 'WORKING', 'users') order by from_id, to_id",
				Expected: []sql.Row{
					{1, 1, "tim", "tim3"},
					{2, 2, "jim", "jim3"},
				},
			},
			{
				Query: "/* client a */ select from_id, to_id, from_name, to_name from dolt_diff('STAGED', 'WORKING', 'users') order by from_id, to_id",
				Expected: []sql.Row{
					{1, 1, "tim2", "tim3"},
					{2, 2, "jim2", "jim3"},
				},
			},
			{
				Query: "/* client a */ select from_id, to_id, from_name, to_name from dolt_diff('STAGED', 'WORKING', 'users') order by from_id, to_id",
				Expected: []sql.Row{
					{1, 1, "tim2", "tim3"},
					{2, 2, "jim2", "jim3"},
				},
			},
		},
	},
	{
		Name: "staged changes in working set, dolt_add and dolt_commit on top of it",
		SetUpScript: []string{
			"create table users (id int primary key, name varchar(32))",
			"insert into users values (1, 'tim'), (2, 'jim')",
			"call dolt_commit('-A', '-m', 'initial commit')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update users set name = 'tim2' where name = 'tim'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update users set name = 'jim2' where name = 'jim'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ call dolt_add('users')",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ commit",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ call dolt_add('users')",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ select * from users order by id",
				Expected: []sql.Row{{1, "tim"}, {2, "jim2"}},
			},
			{
				Query:            "/* client b */ call dolt_commit('-m', 'jim2 commit')",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ select * from users order by id",
				Expected: []sql.Row{{1, "tim2"}, {2, "jim2"}},
			},
			{
				Query:    "/* client b */ select * from users as of 'HEAD' order by id",
				Expected: []sql.Row{{1, "tim2"}, {2, "jim2"}},
			},
			{
				Query:    "/* client b */ select * from dolt_status",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ select from_id, to_id, from_name, to_name from dolt_diff('HEAD', 'STAGED', 'users') order by from_id, to_id",
				Expected: []sql.Row{},
			},
			{
				// staged changes include changes from both A and B at staged revision of data
				Query:    "/* client b */ select from_id, to_id, from_name, to_name from dolt_diff('HEAD', 'WORKING', 'users') order by from_id, to_id",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "call dolt_commit commits staged stuff, merges with working set and branch head",
		SetUpScript: []string{
			"create table t1 (id int primary key, val int)",
			"create table t2 (id int primary key, val int)",
			"insert into t1 values (1, 1), (2, 2)",
			"insert into t2 values (1, 1), (2, 2)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ set autocommit = off",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client a */ call dolt_add('t1')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:            "/* client a */ call dolt_commit('-m', 'initial commit of t1')",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ set autocommit = off",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into t1 values (3, 3)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into t1 values (4, 4)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ insert into t2 values (3, 3)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into t2 values (4, 4)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ call dolt_add('t1')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "/* client b */ call dolt_add('t1')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "/* client a */ insert into t1 values (5, 5)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into t1 values (6, 6)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client c */ insert into t2 values (6, 6)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:            "/* client a */ call dolt_commit('-m', 'add 3 to t1')",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client a */ select * from t2 order by id asc",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {6, 6}},
			},
			{
				Query:    "/* client a */ select * from t1 order by id asc",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {5, 5}},
			},
			{
				Query:    "/* client a */ select * from t1 as of 'HEAD' order by id asc",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
			{
				Query:            "/* client b */ call dolt_commit('-m', 'add 4 to t1')",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ select * from t2 order by id asc",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {6, 6}},
			},
			{
				Query:    "/* client b */ select * from t1 order by id asc",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}},
			},
			{
				Query:    "/* client b */ select * from t1 as of 'HEAD' order by id asc",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {4, 4}},
			},
			{
				// working set has t2 new, t1 modified, nothing staged
				Query: "/* client a */ select * from dolt_status",
				Expected: []sql.Row{
					{"t2", false, "new table"},
					{"t1", false, "modified"},
				},
			},
			{
				// working set has t2 new, t1 modified, nothing staged
				Query: "/* client b */ select * from dolt_status",
				Expected: []sql.Row{
					{"t2", false, "new table"},
					{"t1", false, "modified"},
				},
			},
			{
				// client a has a stale view of t1 from before the commit, so it's missing the row with 4 in its session's working set
				Query: "/* client a */ select from_id, to_id, from_val, to_val from dolt_diff('HEAD', 'WORKING', 't1') order by from_id",
				Expected: []sql.Row{
					{nil, 5, nil, 5},
					{4, nil, 4, nil},
				},
			},
			{
				Query: "/* client b */ select from_id, to_id, from_val, to_val from dolt_diff('HEAD', 'WORKING', 't1') order by from_id",
				Expected: []sql.Row{
					{nil, 5, nil, 5},
					{nil, 6, nil, 6},
				},
			},
		},
	},
}

var DoltConstraintViolationTransactionTests = []queries.TransactionTest{
	{
		Name: "Constraint violations created by concurrent writes should cause a rollback",
		SetUpScript: []string{
			"CREATE table parent (pk int PRIMARY KEY);",
			"CREATE table child (pk int PRIMARY KEY, parent_fk int, FOREIGN KEY (parent_fk) REFERENCES parent (pk));",
			"INSERT into parent VALUES (1);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ SET @@autocommit=0;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client b */ SET @@autocommit=0;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client a */ START TRANSACTION;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ START TRANSACTION;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ DELETE FROM parent where pk = 1;",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ INSERT INTO child VALUES (1, 1);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ COMMIT;",
				Expected: []sql.Row{},
			},
			{
				Query: "/* client b */ COMMIT;",
				ExpectedErrStr: "Committing this transaction resulted in a working set with constraint violations, transaction rolled back. " +
					"This constraint violation may be the result of a previous merge or the result of transaction sequencing. " +
					"Constraint violations from a merge can be resolved using the dolt_constraint_violations table before committing the transaction. " +
					"To allow transactions to be committed with constraint violations from a merge or transaction sequencing set @@dolt_force_transaction_commit=1.\n" +
					"Constraint violations: \n" +
					"Type: Foreign Key Constraint Violation\n" +
					"\tForeignKey: child_ibfk_1,\n" +
					"\tTable: child,\n" +
					"\tReferencedTable: ,\n" +
					"\tIndex: parent_fk,\n" +
					"\tReferencedIndex: ",
			},
			{
				Query:          "/* client b */ INSERT INTO child VALUES (1, 1);",
				ExpectedErrStr: "cannot add or update a child row - Foreign key violation on fk: `child_ibfk_1`, table: `child`, referenced table: `parent`, key: `[1]`",
			},
		},
	},
	{
		Name: "Constraint violations created by DOLT_MERGE should cause a roll back",
		SetUpScript: []string{
			"CREATE TABLE t (pk int PRIMARY KEY, col1 int UNIQUE);",
			"CALL DOLT_ADD('.')",
			"CALL DOLT_COMMIT('-am', 'create table');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"INSERT INTO t values (2, 1);",
			"CALL DOLT_COMMIT('-am', 'right edit');",

			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO t VALUES (1, 1);",
			"CALL DOLT_COMMIT('-am', 'left edit');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "/* client a */ CALL DOLT_MERGE('right');",
				ExpectedErrStr: "Committing this transaction resulted in a working set with constraint violations, transaction rolled back. " +
					"This constraint violation may be the result of a previous merge or the result of transaction sequencing. " +
					"Constraint violations from a merge can be resolved using the dolt_constraint_violations table before committing the transaction. " +
					"To allow transactions to be committed with constraint violations from a merge or transaction sequencing set @@dolt_force_transaction_commit=1.\n" +
					"Constraint violations: \n" +
					"Type: Unique Key Constraint Violation,\n" +
					"\tName: col1,\n" +
					"\tColumns: [col1]",
			},
			{
				Query:    "/* client a */ SELECT * from DOLT_CONSTRAINT_VIOLATIONS;",
				Expected: []sql.Row{},
			},
			{
				Query:          "/* client a */ CALL DOLT_MERGE('--abort');",
				ExpectedErrStr: "fatal: There is no merge to abort",
			},
		},
	},
	{
		Name: "a transaction commit that is a fast-forward produces no constraint violations",
		SetUpScript: []string{
			"CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));",
			"CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);",
			"CALL DOLT_ADD('.')",
			"INSERT INTO parent VALUES (10, 1), (20, 2);",
			"INSERT INTO child VALUES (1, 1), (2, 2);",
			"ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1);",
			"CALL DOLT_COMMIT('-am', 'MC1');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ SET FOREIGN_KEY_CHECKS = 0;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client a */ START TRANSACTION;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ DELETE FROM PARENT where v1 = 2;",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ COMMIT;",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "a transaction commit that is a three-way merge produces constraint violations",
		SetUpScript: []string{
			"CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));",
			"CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);",
			"CALL DOLT_ADD('.')",
			"INSERT INTO parent VALUES (10, 1), (20, 2);",
			"INSERT INTO child VALUES (1, 1), (2, 2);",
			"ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1);",
			"CALL DOLT_COMMIT('-am', 'MC1');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ SET FOREIGN_KEY_CHECKS = 0;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client a */ START TRANSACTION;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ START TRANSACTION;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ DELETE FROM PARENT where v1 = 2;",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ INSERT INTO parent VALUES (30, 3);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ COMMIT;",
				Expected: []sql.Row{},
			},
			{
				Query: "/* client b */ COMMIT;",
				ExpectedErrStr: "Committing this transaction resulted in a working set with constraint violations, transaction rolled back. " +
					"This constraint violation may be the result of a previous merge or the result of transaction sequencing. " +
					"Constraint violations from a merge can be resolved using the dolt_constraint_violations table before committing the transaction. " +
					"To allow transactions to be committed with constraint violations from a merge or transaction sequencing set @@dolt_force_transaction_commit=1.\n" +
					"Constraint violations: \n" +
					"Type: Foreign Key Constraint Violation\n" +
					"\tForeignKey: fk_name,\n" +
					"\tTable: child,\n" +
					"\tReferencedTable: v1,\n" +
					"\tIndex: fk_name,\n" +
					"\tReferencedIndex: v1",
			},
		},
	},
	//	{
	//		Name:        "Run GC concurrently with other transactions",
	//		SetUpScript: gcSetup(),
	//		Assertions: []queries.ScriptTestAssertion{
	//			{
	//				Query:    "/* client a */ SELECT count(*) FROM t;",
	//				Expected: []sql.Row{{250}},
	//			},
	//			{
	//				Query:    "/* client a */ START TRANSACTION",
	//				Expected: []sql.Row{},
	//			},
	//			{
	//				Query:    "/* client b */ START TRANSACTION",
	//				Expected: []sql.Row{},
	//			},
	//			{
	//				Query:    "/* client a */ CALL DOLT_GC();",
	//				Expected: []sql.Row{{1}},
	//			},
	//			{
	//				Query:    "/* client b */ INSERT into t VALUES (300);",
	//				Expected: []sql.Row{{types.NewOkResult(1)}},
	//			},
	//			{
	//				Query:    "/* client a */ COMMIT;",
	//				Expected: []sql.Row{},
	//			},
	//			{
	//				Query:    "/* client b */ COMMIT;",
	//				Expected: []sql.Row{},
	//			},
	//			{
	//				Query:    "/* client a */ SELECT count(*) FROM t;",
	//				Expected: []sql.Row{{251}},
	//			},
	//			{
	//				Query:    "/* client b */ SELECT count(*) FROM t;",
	//				Expected: []sql.Row{{251}},
	//			},
	//		},
	//	},
}

var BranchIsolationTests = []queries.TransactionTest{
	{
		Name: "clients can't see changes on other branch working sets made since transaction start",
		SetUpScript: []string{
			"create table t1 (a int)",
			"insert into t1 values (1)",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'new table')",
			"call dolt_branch('b1')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ call dolt_checkout('b1')",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into t1 values (2)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ commit",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client a */ select * from t1 order by a",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "/* client a */ select * from t1 as of 'b1' order by a",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "/* client a */ select * from `mydb/b1`.t1 order by a",
				Expected: []sql.Row{{1}},
			},
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client a */ select * from t1 order by a",
				Expected: []sql.Row{{1}},
			},
			{
				// This query specifies the branch HEAD commit, which hasn't changed
				Query:    "/* client a */ select * from t1 as of 'b1' order by a",
				Expected: []sql.Row{{1}},
			},
			{
				// This query specifies the working set of that branch, which has changed
				Query:    "/* client a */ select * from `mydb/b1`.t1 order by a",
				Expected: []sql.Row{{1}, {2}},
			},
		},
	},
	{
		Name: "clients can't see changes on other branch heads made since transaction start",
		SetUpScript: []string{
			"create table t1 (a int)",
			"insert into t1 values (1)",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'new table')",
			"call dolt_branch('b1')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ call dolt_checkout('b1')",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into t1 values (2)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ call dolt_commit('-am', 'new row')",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client a */ select * from t1 order by a",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "/* client a */ select * from t1 as of 'b1' order by a",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "/* client a */ select * from `mydb/b1`.t1 order by a",
				Expected: []sql.Row{{1}},
			},
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client a */ select * from t1 order by a",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "/* client a */ select * from t1 as of 'b1' order by a",
				Expected: []sql.Row{{1}, {2}},
			},
			{
				Query:    "/* client a */ select * from `mydb/b1`.t1 order by a",
				Expected: []sql.Row{{1}, {2}},
			},
		},
	},
	{
		Name: "dolt_branches table has consistent view",
		SetUpScript: []string{
			"create table t1 (a int)",
			"insert into t1 values (1)",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'new table')",
			"call dolt_branch('b1')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ call dolt_branch('-d', 'b1')",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ call dolt_branch('b2')",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ select name from dolt_branches order by 1",
				Expected: []sql.Row{{"b2"}, {"main"}},
			},
			{
				Query:            "/* client b */ commit",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client a */ select name from dolt_branches order by 1",
				Expected: []sql.Row{{"b1"}, {"main"}},
			},
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client a */ select name from dolt_branches order by 1",
				Expected: []sql.Row{{"b2"}, {"main"}},
			},
		},
	},
}

var MultiDbTransactionTests = []queries.ScriptTest{
	{
		Name: "committing to another branch",
		SetUpScript: []string{
			"create table t1 (a int)",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'new table')",
			"call dolt_branch('b1')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "insert into `mydb/b1`.t1 values (1)",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query: "insert into `mydb/b1`.t1 values (2)",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from `mydb/b1`.t1 order by a",
				Expected: []sql.Row{
					{1}, {2},
				},
			},
			{
				Query:    "commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{},
			},
			{
				Query:            "call dolt_checkout('b1')",
				SkipResultsCheck: true,
			},
			{
				Query: "select * from t1 order by a",
				Expected: []sql.Row{
					{1}, {2},
				},
			},
		},
	},
	{
		Name: "committing to another branch with autocommit",
		SetUpScript: []string{
			"create table t1 (a int)",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'new table')",
			"call dolt_branch('b1')",
			"set autocommit = on", // unnecessary but make it explicit
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "insert into `mydb/b1`.t1 values (1)",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{},
			},
			{
				Query:            "call dolt_checkout('b1')",
				SkipResultsCheck: true,
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "committing to another branch with dolt_transaction_commit",
		SetUpScript: []string{
			"create table t1 (a int)",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'new table')",
			"call dolt_branch('b1')",
			"set autocommit = 0",
			"set dolt_transaction_commit = on",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "insert into `mydb/b1`.t1 values (1)",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query: "insert into `mydb/b1`.t1 values (2)",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from `mydb/b1`.t1 order by a",
				Expected: []sql.Row{
					{1}, {2},
				},
			},
			{
				Query:          "commit",
				ExpectedErrStr: "no changes to dolt_commit on branch main",
			},
			{
				Query:    "select * from `mydb/main`.t1 order by a",
				Expected: []sql.Row{},
			},
			{
				Query:    "use mydb/b1",
				Expected: []sql.Row{},
			},
			{
				Query:    "commit",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from `mydb/b1`.t1 order by a",
				Expected: []sql.Row{
					{1}, {2},
				},
			},
			{
				Query:    "select * from `mydb/main`.t1 order by a",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from t1 order by a",
				Expected: []sql.Row{
					{1}, {2},
				},
			},
		},
	},
	{
		Name: "committing to another branch with dolt_commit",
		SetUpScript: []string{
			"create table t1 (a int)",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'new table')",
			"call dolt_branch('b1')",
			"set autocommit = off",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "insert into `mydb/b1`.t1 values (1)",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{},
			},
			{
				Query:          "call dolt_commit('-am', 'changes on b1')",
				ExpectedErrStr: "nothing to commit", // this error is different from what you get with @@dolt_transaction_commit
			},
			{
				Query:    "use mydb/b1",
				Expected: []sql.Row{},
			},
			{
				Query:            "call dolt_commit('-am', 'other changes on b1')",
				SkipResultsCheck: true,
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "select message from dolt_log order by date desc limit 1",
				Expected: []sql.Row{{"other changes on b1"}},
			},
		},
	},
	{
		Name: "committing to another branch with autocommit and dolt_transaction_commit",
		SetUpScript: []string{
			"create table t1 (a int)",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'new table')",
			"call dolt_branch('b1')",
			"set autocommit = on", // unnecessary but make it explicit
			"set dolt_transaction_commit = on",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "insert into `mydb/b1`.t1 values (1)",
				ExpectedErrStr: "no changes to dolt_commit on branch main",
			},
			{
				Query:    "use mydb/b1",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from t1 order by a",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query:    "commit",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from t1 order by a",
				Expected: []sql.Row{
					{1},
				},
			},
		},
	},
	{
		Name: "active_branch with dolt_checkout and use",
		SetUpScript: []string{
			"create table t1 (a int)",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'new table')",
			"call dolt_branch('b1')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "insert into `mydb/b1`.t1 values (1)",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query: "insert into `mydb/b1`.t1 values (2)",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{},
			},
			{
				Query:            "call dolt_checkout('b1')",
				SkipResultsCheck: true,
			},
			{
				Query:    "select active_branch()",
				Expected: []sql.Row{{"b1"}},
			},
			{
				Query: "select * from t1 order by a",
				Expected: []sql.Row{
					{1}, {2},
				},
			},
			{
				Query:            "call dolt_checkout('main')",
				SkipResultsCheck: true,
			},
			{
				Query:    "select active_branch()",
				Expected: []sql.Row{{"main"}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{},
			},
			{
				Query:    "use `mydb/b1`",
				Expected: []sql.Row{},
			},
			{
				Query:    "select active_branch()",
				Expected: []sql.Row{{"b1"}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{{1}, {2}},
			},
			{
				Query:    "use mydb",
				Expected: []sql.Row{},
			},
			{
				Query:    "select active_branch()",
				Expected: []sql.Row{{"main"}},
			},
			{
				Query:    "commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{},
			},
			{
				Query:            "call dolt_checkout('b1')",
				SkipResultsCheck: true,
			},
			{
				Query: "select * from t1 order by a",
				Expected: []sql.Row{
					{1}, {2},
				},
			},
		},
	},
	{
		Name: "committing to another database",
		SetUpScript: []string{
			"create table t1 (a int)",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'new table')",
			"create database db1",
			"use db1",
			"create table t1 (a int)",
			"use mydb",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "insert into db1.t1 values (1)",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query: "insert into db1.t1 values (2)",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from db1.t1 order by a",
				Expected: []sql.Row{{1}, {2}},
			},
			{
				Query:    "commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from db1.t1 order by a",
				Expected: []sql.Row{{1}, {2}},
			},
		},
	},
	{
		Name: "committing to another database with dolt_commit",
		SetUpScript: []string{
			"create table t1 (a int)",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'new table')",
			"call dolt_branch('b1')",
			"create database db1",
			"use db1",
			"create table t1 (a int)",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'new table')",
			"call dolt_branch('b1')",
			"use mydb/b1",
			"set autocommit = off",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "insert into `db1/b1`.t1 values (1)",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:          "call dolt_commit('-am', 'changes on b1')",
				ExpectedErrStr: "nothing to commit", // this error is different from what you get with @@dolt_transaction_commit
			},
			{
				Query:    "use db1/b1",
				Expected: []sql.Row{},
			},
			{
				Query:            "call dolt_commit('-am', 'other changes on b1')",
				SkipResultsCheck: true,
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "select message from dolt_log order by date desc limit 1",
				Expected: []sql.Row{{"other changes on b1"}},
			},
		},
	},
	{
		Name: "committing to another branch on another database",
		SetUpScript: []string{
			"create table t1 (a int)",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'new table')",
			"create database db1",
			"use db1",
			"create table t1 (a int)",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'new table')",
			"call dolt_branch('b1')",
			"use mydb",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "insert into `db1/b1`.t1 values (1)",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query: "insert into `db1/b1`.t1 values (2)",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from db1.t1 order by a",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from `db1/b1`.t1 order by a",
				Expected: []sql.Row{{1}, {2}},
			},
			{
				Query:    "commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from db1.t1 order by a",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from `db1/b1`.t1 order by a",
				Expected: []sql.Row{{1}, {2}},
			},
		},
	},
	{
		Name: "committing to another branch on another database with dolt_transaction_commit and autocommit",
		SetUpScript: []string{
			"create table t1 (a int)",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'new table')",
			"call dolt_branch('b1')",
			"create database db1",
			"use db1",
			"create table t1 (a int)",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'new table')",
			"call dolt_branch('b1')",
			"use mydb/b1",
			"set autocommit = 1",
			"set dolt_transaction_commit = 1",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "insert into `db1/b1`.t1 values (1)",
				ExpectedErrStr: "no changes to dolt_commit on database mydb",
			},
		},
	},
	{
		Name: "committing to another branch on another database with dolt_transaction_commit, no autocommit",
		SetUpScript: []string{
			"create table t1 (a int)",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'new table')",
			"call dolt_branch('b1')",
			"create database db1",
			"use db1",
			"create table t1 (a int)",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'new table')",
			"call dolt_branch('b1')",
			"commit",
			"use mydb/b1",
			"set autocommit = off",
			"set dolt_transaction_commit = 1",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "insert into `db1/b1`.t1 values (1)",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query:          "commit",
				ExpectedErrStr: "no changes to dolt_commit on database mydb",
			},
		},
	},
	{
		Name: "committing to more than one branch at a time",
		SetUpScript: []string{
			"create table t1 (a int)",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'new table')",
			"call dolt_branch('b1')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "insert into t1 values (1)",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query: "insert into `mydb/b1`.t1 values (2)",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query:          "commit",
				ExpectedErrStr: "Cannot commit changes on more than one branch / database",
			},
		},
	},
	{
		Name: "committing to more than one branch at a time with checkout",
		SetUpScript: []string{
			"create table t1 (a int)",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'new table')",
			"call dolt_branch('b1')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "insert into t1 values (1)",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query:            "call dolt_checkout('b1')",
				SkipResultsCheck: true,
			},
			{
				Query: "insert into t1 values (2)",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query:          "commit",
				ExpectedErrStr: "Cannot commit changes on more than one branch / database",
			},
		},
	},
	{
		Name: "committing to more than one database at a time",
		SetUpScript: []string{
			"create table t1 (a int)",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'new table')",
			"create database db2",
			"set autocommit = 0",
			"create table db2.t1 (a int)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "insert into t1 values (1)",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query: "insert into db2.t1 values (2)",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query:          "commit",
				ExpectedErrStr: "Cannot commit changes on more than one branch / database",
			},
		},
	},
}

var MultiDbSavepointTests = []queries.TransactionTest{
	{
		Name: "rollback to savepoint with multiple databases edited",
		SetUpScript: []string{
			"create database db1",
			"create database db2",
			"create table db1.t (x int primary key, y int)",
			"insert into db1.t values (1, 1)",
			"create table db2.t (x int primary key, y int)",
			"insert into db2.t values (2, 2)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ set autocommit = off",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client b */ set autocommit = off",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into db1.t values (3, 3)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ insert into db2.t values (4, 4)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into db1.t values (5, 5)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into db2.t values (6, 6)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ savepoint spa1",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ savepoint spb1",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into db1.t values (5, 5)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ insert into db2.t values (6, 6)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into db1.t values (7, 7)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into db2.t values (8, 8)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ savepoint spa2",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ savepoint spb2",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into db1.t values (7, 7)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ insert into db2.t values (8, 8)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into db1.t values (9, 9)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into db2.t values (10, 10)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ select * from db1.t order by x",
				Expected: []sql.Row{{1, 1}, {3, 3}, {5, 5}, {7, 7}},
			},
			{
				Query:    "/* client a */ select * from db2.t order by x",
				Expected: []sql.Row{{2, 2}, {4, 4}, {6, 6}, {8, 8}},
			},
			{
				Query:    "/* client b */ select * from db1.t order by x",
				Expected: []sql.Row{{1, 1}, {5, 5}, {7, 7}, {9, 9}},
			},
			{
				Query:    "/* client b */ select * from db2.t order by x",
				Expected: []sql.Row{{2, 2}, {6, 6}, {8, 8}, {10, 10}},
			},
			{
				Query:    "/* client a */ rollback to SPA2",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ rollback to spB2",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from db1.t order by x",
				Expected: []sql.Row{{1, 1}, {3, 3}, {5, 5}},
			},
			{
				Query:    "/* client a */ select * from db2.t order by x",
				Expected: []sql.Row{{2, 2}, {4, 4}, {6, 6}},
			},
			{
				Query:    "/* client b */ select * from db1.t order by x",
				Expected: []sql.Row{{1, 1}, {5, 5}, {7, 7}},
			},
			{
				Query:    "/* client b */ select * from db2.t order by x",
				Expected: []sql.Row{{2, 2}, {6, 6}, {8, 8}},
			},
			{
				Query:    "/* client a */ rollback to sPa2",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ rollback to Spb2",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from db1.t order by x",
				Expected: []sql.Row{{1, 1}, {3, 3}, {5, 5}},
			},
			{
				Query:    "/* client a */ select * from db2.t order by x",
				Expected: []sql.Row{{2, 2}, {4, 4}, {6, 6}},
			},
			{
				Query:    "/* client b */ select * from db1.t order by x",
				Expected: []sql.Row{{1, 1}, {5, 5}, {7, 7}},
			},
			{
				Query:    "/* client b */ select * from db2.t order by x",
				Expected: []sql.Row{{2, 2}, {6, 6}, {8, 8}},
			},
			{
				Query:    "/* client a */ rollback to spA1",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ rollback to SPb1",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from db1.t order by x",
				Expected: []sql.Row{{1, 1}, {3, 3}},
			},
			{
				Query:    "/* client a */ select * from db2.t order by x",
				Expected: []sql.Row{{2, 2}, {4, 4}},
			},
			{
				Query:    "/* client b */ select * from db1.t order by x",
				Expected: []sql.Row{{1, 1}, {5, 5}},
			},
			{
				Query:    "/* client b */ select * from db2.t order by x",
				Expected: []sql.Row{{2, 2}, {6, 6}},
			},
			{
				Query:       "/* client a */ rollback to spa2",
				ExpectedErr: sql.ErrSavepointDoesNotExist,
			},
			{
				Query:       "/* client b */ rollback to spb2",
				ExpectedErr: sql.ErrSavepointDoesNotExist,
			},
			{
				Query:    "/* client a */ rollback to Spa1",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ rollback to spB1",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from db1.t order by x",
				Expected: []sql.Row{{1, 1}, {3, 3}},
			},
			{
				Query:    "/* client a */ select * from db2.t order by x",
				Expected: []sql.Row{{2, 2}, {4, 4}},
			},
			{
				Query:    "/* client b */ select * from db1.t order by x",
				Expected: []sql.Row{{1, 1}, {5, 5}},
			},
			{
				Query:    "/* client b */ select * from db2.t order by x",
				Expected: []sql.Row{{2, 2}, {6, 6}},
			},
			{
				Query:    "/* client a */ rollback",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ rollback",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from db1.t order by x",
				Expected: []sql.Row{{1, 1}},
			},
			{
				Query:    "/* client a */ select * from db2.t order by x",
				Expected: []sql.Row{{2, 2}},
			},
			{
				Query:    "/* client b */ select * from db1.t order by x",
				Expected: []sql.Row{{1, 1}},
			},
			{
				Query:    "/* client b */ select * from db2.t order by x",
				Expected: []sql.Row{{2, 2}},
			},
			{
				Query:       "/* client a */ rollback to spa1",
				ExpectedErr: sql.ErrSavepointDoesNotExist,
			},
			{
				Query:       "/* client b */ rollback to spb1",
				ExpectedErr: sql.ErrSavepointDoesNotExist,
			},
		},
	},
	{
		Name: "overwrite savepoint with multiple dbs edited",
		SetUpScript: []string{
			"create database db1",
			"create database db2",
			"create table db1.t (x int primary key, y int)",
			"insert into db1.t values (1, 1)",
			"create table db2.t (x int primary key, y int)",
			"insert into db2.t values (2, 2)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into db1.t values (3, 3)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ insert into db2.t values (4, 4)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ savepoint spa1",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into db1.t values (5, 5)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ insert into db2.t values (6, 6)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ savepoint spa2",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into db1.t values (7, 7)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ insert into db2.t values (8, 8)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ savepoint SPA1",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into db1.t values (9, 9)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ insert into db2.t values (10, 10)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ select * from db1.t order by x",
				Expected: []sql.Row{{1, 1}, {3, 3}, {5, 5}, {7, 7}, {9, 9}},
			},
			{
				Query:    "/* client a */ select * from db2.t order by x",
				Expected: []sql.Row{{2, 2}, {4, 4}, {6, 6}, {8, 8}, {10, 10}},
			},
			{
				Query:    "/* client a */ rollback to Spa1",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from db1.t order by x",
				Expected: []sql.Row{{1, 1}, {3, 3}, {5, 5}, {7, 7}},
			},
			{
				Query:    "/* client a */ select * from db2.t order by x",
				Expected: []sql.Row{{2, 2}, {4, 4}, {6, 6}, {8, 8}},
			},
			{
				Query:    "/* client a */ rollback to spa2",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from db1.t order by x",
				Expected: []sql.Row{{1, 1}, {3, 3}, {5, 5}},
			},
			{
				Query:    "/* client a */ select * from db2.t order by x",
				Expected: []sql.Row{{2, 2}, {4, 4}, {6, 6}},
			},
			{
				Query:       "/* client a */ rollback to spa1",
				ExpectedErr: sql.ErrSavepointDoesNotExist,
			},
			{
				Query:       "/* client a */ release savepoint spa1",
				ExpectedErr: sql.ErrSavepointDoesNotExist,
			},
		},
	},
}
