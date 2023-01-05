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

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

var DoltTransactionTests = []queries.TransactionTest{
	{
		// Repro for https://github.com/dolthub/dolt/issues/3402
		Name: "DDL changes from transactions are available before analyzing statements in other sessions (autocommit on)",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ select @@autocommit;",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "/* client b */ select @@autocommit;",
				Expected: []sql.Row{{1}},
			},
			{
				Query:       "/* client a */ select * from t;",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:       "/* client b */ select * from t;",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:    "/* client a */ create table t(pk int primary key);",
				Expected: []sql.Row{{sql.OkResult{}}},
			},
			{
				Query:    "/* client b */ select count(*) from t;",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "duplicate inserts, autocommit on",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ insert into t values (2, 2)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
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
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into t values (2, 2)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
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
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into t values (2, 3)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
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
				Expected: []sql.Row{{sql.OkResult{
					RowsAffected: uint64(1),
					Info: plan.UpdateInfo{
						Matched: 2,
						Updated: 1,
					},
				}}},
			},
			{
				Query: "/* client b */ update t set y = 2",
				Expected: []sql.Row{{sql.OkResult{
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
				Expected: []sql.Row{{sql.OkResult{
					RowsAffected: uint64(1),
					Info: plan.UpdateInfo{
						Matched: 2,
						Updated: 1,
					},
				}}},
			},
			{
				Query: "/* client b */ update t set y = 2",
				Expected: []sql.Row{{sql.OkResult{
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
				Expected: []sql.Row{{sql.OkResult{
					RowsAffected: uint64(2),
					Info: plan.UpdateInfo{
						Matched: 2,
						Updated: 2,
					},
				}}},
			},
			{
				Query: "/* client b */ update t set y = 4",
				Expected: []sql.Row{{sql.OkResult{
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
				Expected: []sql.Row{{sql.OkResult{
					RowsAffected: uint64(1),
					Info: plan.UpdateInfo{
						Matched: 1,
						Updated: 1,
					},
				}}},
			},
			{
				Query: "/* client b */ update t set y = 4 where x = 2",
				Expected: []sql.Row{{sql.OkResult{
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
				Expected: []sql.Row{{sql.OkResult{
					RowsAffected: uint64(1),
					Info: plan.UpdateInfo{
						Matched: 2,
						Updated: 1,
					},
				}}},
			},
			{
				Query: "/* client b */ update t set z = 3",
				Expected: []sql.Row{{sql.OkResult{
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
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ delete from t where y = 2",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ delete from t where y = 2",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
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
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ delete from t where y = 3",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
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
				Expected: []sql.Row{{sql.OkResult{
					RowsAffected: uint64(1),
					Info: plan.UpdateInfo{
						Matched: 1,
						Updated: 1,
					},
				}}},
			},
			{
				Query:    "/* client b */ delete from t where y = 2",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
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
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ delete from t",
				Expected: []sql.Row{{sql.NewOkResult(2)}},
			},
			{
				Query:    "/* client b */ insert into t values (1,1)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
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
				Expected: []sql.Row{{sql.OkResult{
					RowsAffected: uint64(1),
					Info: plan.UpdateInfo{
						Matched: 1,
						Updated: 1,
					},
				}}},
			},
			{
				Query:    "/* client b */ delete from t where y = 1",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query: "/* client c */ update t set z = 4 where y = 2",
				Expected: []sql.Row{{sql.OkResult{
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
				Expected: []sql.Row{{sql.OkResult{
					RowsAffected: uint64(1),
					Info: plan.UpdateInfo{
						Matched: 1,
						Updated: 1,
					},
				}}},
			},
			{
				Query: "/* client b */ update test set y = 5 where y = 2",
				Expected: []sql.Row{{sql.OkResult{
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
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ set autocommit = off",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into test values (1, 1)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into test values (1, 2)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
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
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ set autocommit = off, dolt_allow_commit_conflicts = on",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into test values (1, 1)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into test values (1, 2)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
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
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ set autocommit = off, dolt_force_transaction_commit = on",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into test values (1, 1)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into test values (1, 2)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
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
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ set autocommit = off, dolt_allow_commit_conflicts = on",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into test values (1, 1)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
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
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:            "/* client b */ call dolt_commit('-am', 'commit on new-branch')",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ call dolt_merge('main')",
				Expected: []sql.Row{{0, 1}},
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
				Expected: []sql.Row{{sql.OkResult{
					RowsAffected: 1,
					Info: plan.UpdateInfo{
						Matched: 1,
						Updated: 1,
					},
				}}},
			},
			{
				Query:    "/* client b */ delete from dolt_conflicts_test",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
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
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ set autocommit = off, dolt_force_transaction_commit = on",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into test values (1, 1)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
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
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:            "/* client b */ call dolt_commit('-am', 'commit on new-branch')",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ call dolt_merge('main')",
				Expected: []sql.Row{{0, 1}},
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
				Expected: []sql.Row{{sql.OkResult{
					RowsAffected: 1,
					Info: plan.UpdateInfo{
						Matched: 1,
						Updated: 1,
					},
				}}},
			},
			{
				Query:    "/* client b */ delete from dolt_conflicts_test",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
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
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ set autocommit = off",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into test values (1, 1)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
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
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:            "/* client b */ call dolt_commit('-am', 'commit on new-branch')",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ call dolt_merge('main')",
				Expected: []sql.Row{{0, 1}},
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
				Expected: []sql.Row{{sql.NewOkResult(1)}},
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
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client b */ SET @@autocommit=0;",
				Expected: []sql.Row{{}},
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
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ INSERT into t VALUES (3, 3);",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ SET dolt_allow_commit_conflicts = on;",
				Expected: []sql.Row{{}},
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
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query: "/* client b */ COMMIT;",
				// Retrying did not help. But at-least the error makes sense.
				ExpectedErrStr: "Merge conflict detected, transaction rolled back. Merge conflicts must be resolved using the dolt_conflicts tables before committing a transaction. To commit transactions with merge conflicts, set @@dolt_allow_commit_conflicts = 1",
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
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "/* client b */ UPDATE t SET col1 = 100 where pk = 1;",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "/* client a */ INSERT into KEYLESS VALUES (1);",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ INSERT into KEYLESS VALUES (1), (1);",
				Expected: []sql.Row{{sql.NewOkResult(2)}},
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
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "/* client b */ UPDATE t SET col1 = 100 where pk = 1;",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "/* client a */ INSERT into KEYLESS VALUES (1);",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ INSERT into KEYLESS VALUES (1), (1);",
				Expected: []sql.Row{{sql.NewOkResult(2)}},
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

var DoltSqlFuncTransactionTests = []queries.TransactionTest{
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
				Query:    "/* client a */ CALL DOLT_MERGE('feature-branch')",
				Expected: []sql.Row{{0, 1}},
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
				Expected: []sql.Row{{}},
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
				Expected: []sql.Row{{0, 0}},
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
				Expected: []sql.Row{{}},
			},
			{
				Query:          "/* client a */ CALL DOLT_MERGE('feature-branch')",
				ExpectedErrStr: dsess.ErrUnresolvedConflictsCommit.Error(),
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
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client b */ SET @@autocommit=0;",
				Expected: []sql.Row{{}},
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
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ INSERT INTO child VALUES (1, 1);",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
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
					"To allow transactions to be committed with constraint violations from a merge or transaction sequencing set @@dolt_force_transaction_commit=1.",
			},
			{
				Query:          "/* client b */ INSERT INTO child VALUES (1, 1);",
				ExpectedErrStr: "cannot add or update a child row - Foreign key violation on fk: `0050p5ek`, table: `child`, referenced table: `parent`, key: `[1]`",
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
					"To allow transactions to be committed with constraint violations from a merge or transaction sequencing set @@dolt_force_transaction_commit=1.",
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
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client a */ START TRANSACTION;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ DELETE FROM PARENT where v1 = 2;",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
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
				Expected: []sql.Row{{}},
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
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ INSERT INTO parent VALUES (30, 3);",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
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
					"To allow transactions to be committed with constraint violations from a merge or transaction sequencing set @@dolt_force_transaction_commit=1.",
			},
		},
	},
}
