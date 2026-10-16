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
)

var DoltMultiBranchCommitTests = []queries.ScriptTest{
	{
		Name: "multi branch commit defaults off but SQL working set commits still work",
		SetUpScript: []string{
			"create table t (pk int primary key)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"set autocommit=0",
			"insert into t values (1)",
			"insert into `mydb/b1`.t values (2)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select @@session.dolt_multi_branch_commit, @@global.dolt_multi_branch_commit",
				Expected: []sql.Row{
					{0, 0},
				},
			},
			{
				Query:          "call dolt_commit('-am', 'rejected')",
				ExpectedErrStr: "Cannot commit changes on more than one branch / database",
			},
			{
				Query:    "commit",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from t",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select * from `mydb/b1`.t",
				Expected: []sql.Row{
					{2},
				},
			},
			{
				Query:    "select * from t as of 'HEAD'",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from `mydb/b1`.t as of 'HEAD'",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "transaction commits reject multiple branches until enabled",
		SetUpScript: []string{
			"create table t (pk int primary key)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"set autocommit=0",
			"set @@dolt_transaction_commit=1",
			"set @@dolt_transaction_commit_message='multi transaction'",
			"insert into t values (1)",
			"insert into `mydb/b1`.t values (2)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "commit",
				ExpectedErrStr: "Cannot commit changes on more than one branch / database",
			},
			{
				Query:    "select * from t as of 'HEAD'",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from `mydb/b1`.t as of 'HEAD'",
				Expected: []sql.Row{},
			},
			{
				Query:            "set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:    "commit",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from t as of 'HEAD'",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select * from `mydb/b1`.t as of 'HEAD'",
				Expected: []sql.Row{
					{2},
				},
			},
			{
				Query: "select message from dolt_log limit 1",
				Expected: []sql.Row{
					{"multi transaction"},
				},
			},
			{
				Query: "select message from `mydb/b1`.dolt_log limit 1",
				Expected: []sql.Row{
					{"multi transaction"},
				},
			},
		},
	},
	{
		Name: "multi branch dolt_commit returns one hash per branch and can be disabled again",
		SetUpScript: []string{
			"create table t (pk int primary key)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"set autocommit=0",
			"set @@dolt_multi_branch_commit=1",
			"insert into t values (1)",
			"insert into `mydb/b1`.t values (2)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "call dolt_commit('-am', 'multi')",
				Expected: []sql.Row{
					{doltCommit},
					{doltCommit},
				},
			},
			{
				Query: "select * from t as of 'HEAD'",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select * from `mydb/b1`.t as of 'HEAD'",
				Expected: []sql.Row{
					{2},
				},
			},
			{
				Query:            "set @@dolt_multi_branch_commit=0",
				SkipResultsCheck: true,
			},
			{
				Query:            "insert into t values (3)",
				SkipResultsCheck: true,
			},
			{
				Query:            "insert into `mydb/b1`.t values (4)",
				SkipResultsCheck: true,
			},
			{
				Query:          "call dolt_commit('-am', 'rejected')",
				ExpectedErrStr: "Cannot commit changes on more than one branch / database",
			},
			{
				Query: "call dolt_commit_all('-am', 'explicit')",
				Expected: []sql.Row{
					{"b1", doltCommit},
					{"main", doltCommit},
				},
			},
		},
	},
	{
		Name: "automatic multi branch commit stages new tables with a clean selected branch",
		SetUpScript: []string{
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"create table `mydb/b1`.created (pk int primary key)",
			"create table `mydb/b2`.created (pk int primary key)",
			"set autocommit=0",
			"set @@dolt_multi_branch_commit=1",
			"set @@dolt_transaction_commit=1",
			"insert into `mydb/b1`.created values (1)",
			"insert into `mydb/b2`.created values (2)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "commit",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from `mydb/b1`.created as of 'HEAD'",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select * from `mydb/b2`.created as of 'HEAD'",
				Expected: []sql.Row{
					{2},
				},
			},
			{
				Query: "select message from `mydb/b1`.dolt_log limit 1",
				Expected: []sql.Row{
					{"Transaction commit"},
				},
			},
			{
				Query: "select database(), active_branch()",
				Expected: []sql.Row{
					{"mydb", "main"},
				},
			},
		},
	},
	{
		Name: "enabled multi branch commits still reject multiple physical databases",
		SetUpScript: []string{
			"create table t (pk int primary key)",
			"call dolt_commit('-Am','setup')",
			"create database other",
			"create table other.t (pk int primary key)",
			"set autocommit=0",
			"set @@dolt_multi_branch_commit=1",
			"insert into t values (1)",
			"insert into other.t values (2)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "call dolt_commit('-am','rejected')",
				ExpectedErrStr: "Cannot atomically commit changes to more than one database",
			},
			{
				Query:            "set @@dolt_transaction_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:          "commit",
				ExpectedErrStr: "Cannot atomically commit changes to more than one database",
			},
			{
				Query:    "rollback",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from t",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from other.t",
				Expected: []sql.Row{},
			},
		},
	},
}

var MultiBranchExplicitCommitTransactions = []queries.TransactionTest{
	{
		Name: "three clients merge partially overlapping branch sets",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ use `mydb/b3`",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/main`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b1`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b1`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b2`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ insert into `mydb/b2`.t values (6,60,600)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ insert into `mydb/b3`.t values (6,60,600)",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client b */ call dolt_commit('-am', 'batch')",
				Expected: []sql.Row{
					{doltCommit},
					{doltCommit},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client c */ select * from `mydb/b2`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{6, 60, 600},
				},
			},
			{
				Query: "/* client a */ call dolt_commit('-am', 'batch')",
				Expected: []sql.Row{
					{doltCommit},
					{doltCommit},
				},
			},
			{
				Query: "/* client c */ call dolt_commit('-am', 'batch')",
				Expected: []sql.Row{
					{doltCommit},
					{doltCommit},
				},
			},
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b2`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{5, 50, 500},
					{6, 60, 600},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b3`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{6, 60, 600},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b2`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{5, 50, 500},
					{6, 60, 600},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b3`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{6, 60, 600},
				},
			},
		},
	},
	{
		Name: "disjoint updates and deletes merge on overlapping branches",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ use `mydb/b3`",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update `mydb/b1`.t set x=11 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ delete from `mydb/main`.t where pk=3",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ delete from `mydb/b1`.t where pk=3",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update `mydb/b2`.t set y=222 where pk=2",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client b */ call dolt_commit('-am', 'batch')",
				Expected: []sql.Row{
					{doltCommit},
					{doltCommit},
				},
			},
			{
				Query: "/* client a */ call dolt_commit('-am', 'batch')",
				Expected: []sql.Row{
					{doltCommit},
					{doltCommit},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 11, 100},
					{2, 20, 200},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b2`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 222},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 11, 100},
					{2, 20, 200},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b2`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 222},
					{3, 30, 300},
				},
			},
		},
	},
	{
		Name: "different columns of the same row merge on two branches",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ use `mydb/b3`",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update `mydb/b1`.t set x=11 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update `mydb/main`.t set x=11 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update `mydb/b1`.t set y=111 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update `mydb/main`.t set y=111 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client a */ call dolt_commit('-am', 'batch')",
				Expected: []sql.Row{
					{doltCommit},
					{doltCommit},
				},
			},
			{
				Query: "/* client b */ call dolt_commit('-am', 'batch')",
				Expected: []sql.Row{
					{doltCommit},
					{doltCommit},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 11, 111},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 11, 111},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 11, 111},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/main`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 11, 111},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
		},
	},
	{
		Name: "late branch conflict rolls back the complete batch and permits retry",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ use `mydb/b3`",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b1`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b2`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update `mydb/main`.t set x=11 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update `mydb/main`.t set x=12 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b3`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client b */ call dolt_commit('-am', 'batch')",
				Expected: []sql.Row{
					{doltCommit},
					{doltCommit},
				},
			},
			{
				Query:       "/* client a */ call dolt_commit('-am', 'batch')",
				ExpectedErr: sql.ErrLockDeadlock,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client b */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b2`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 12, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b1`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update `mydb/main`.t set x=13 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client a */ call dolt_commit('-am', 'batch')",
				Expected: []sql.Row{
					{doltCommit},
					{doltCommit},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 13, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 13, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
		},
	},
	{
		Name: "delete update conflict does not publish nonconflicting branches",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ use `mydb/b3`",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ delete from `mydb/b1`.t where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b2`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update `mydb/b1`.t set x=11 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b3`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client b */ call dolt_commit('-am', 'batch')",
				Expected: []sql.Row{
					{doltCommit},
					{doltCommit},
				},
			},
			{
				Query:       "/* client a */ call dolt_commit('-am', 'batch')",
				ExpectedErr: sql.ErrLockDeadlock,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client b */ select * from `mydb/b2`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 11, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b2`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 11, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
		},
	},
	{
		Name: "identical edits merge while preserving independent branch writes",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ use `mydb/b3`",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update `mydb/b1`.t set x=11 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update `mydb/b1`.t set x=11 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/main`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b2`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client a */ call dolt_commit('-am', 'batch')",
				Expected: []sql.Row{
					{doltCommit},
					{doltCommit},
				},
			},
			{
				Query: "/* client b */ call dolt_commit('-am', 'batch')",
				Expected: []sql.Row{
					{doltCommit},
					{doltCommit},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 11, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b2`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 11, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/main`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b2`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{5, 50, 500},
				},
			},
		},
	},
	{
		Name: "savepoint restores edits across branches before merging another client",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ use `mydb/b3`",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b1`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/main`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ savepoint keep",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b3`.t values (6,60,600)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ delete from `mydb/b1`.t where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update `mydb/main`.t set x=99 where pk=2",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b1`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b2`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client b */ call dolt_commit('-am', 'batch')",
				Expected: []sql.Row{
					{doltCommit},
					{doltCommit},
				},
			},
			{
				Query:            "/* client a */ rollback to savepoint keep",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client a */ call dolt_commit('-am', 'batch')",
				Expected: []sql.Row{
					{doltCommit},
					{doltCommit},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b3`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b3`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
		},
	},
	{
		Name: "rollback and no-op commit leave other clients branch writes intact",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ use `mydb/b3`",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b1`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/main`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b1`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b2`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client b */ call dolt_commit('-am', 'batch')",
				Expected: []sql.Row{
					{doltCommit},
					{doltCommit},
				},
			},
			{
				Query:            "/* client a */ rollback",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ commit",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b2`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b2`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{5, 50, 500},
				},
			},
		},
	},
	{
		Name: "multi branch opt in is session local",
		SetUpScript: []string{
			"create table t (pk int primary key)",
			"call dolt_commit('-Am','setup')",
			"call dolt_branch('b1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client b */ select @@dolt_multi_branch_commit",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query:            "/* client b */ set @@dolt_transaction_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into t values (1)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b1`.t values (2)",
				SkipResultsCheck: true,
			},
			{
				Query:          "/* client b */ call dolt_commit('-am', 'session setting')",
				ExpectedErrStr: "Cannot commit changes on more than one branch / database",
			},
			{
				Query:    "/* client b */ rollback",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from `mydb/b1`.t",
				Expected: []sql.Row{},
			},
		},
	},
}

var MultiBranchAutomaticCommitTransactions = []queries.TransactionTest{
	{
		Name: "three clients merge partially overlapping branch sets",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ set @@dolt_transaction_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ set @@dolt_transaction_commit_message='automatic batch'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_transaction_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_transaction_commit_message='automatic batch'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_transaction_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_transaction_commit_message='automatic batch'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/main`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b1`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b1`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b2`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ insert into `mydb/b2`.t values (6,60,600)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ insert into `mydb/b3`.t values (6,60,600)",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client c */ select * from `mydb/b2`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{6, 60, 600},
				},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client c */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b2`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{5, 50, 500},
					{6, 60, 600},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b3`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{6, 60, 600},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b2`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{5, 50, 500},
					{6, 60, 600},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b3`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{6, 60, 600},
				},
			},
			{
				Query: "/* client a */ select message from `mydb/b1`.dolt_log limit 1",
				Expected: []sql.Row{
					{"automatic batch"},
				},
			},
		},
	},
	{
		Name: "disjoint updates and deletes merge on overlapping branches",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ set @@dolt_transaction_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ set @@dolt_transaction_commit_message='automatic batch'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_transaction_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_transaction_commit_message='automatic batch'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_transaction_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_transaction_commit_message='automatic batch'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update `mydb/b1`.t set x=11 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ delete from `mydb/main`.t where pk=3",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ delete from `mydb/b1`.t where pk=3",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update `mydb/b2`.t set y=222 where pk=2",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 11, 100},
					{2, 20, 200},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b2`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 222},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 11, 100},
					{2, 20, 200},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b2`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 222},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select message from `mydb/b1`.dolt_log limit 1",
				Expected: []sql.Row{
					{"automatic batch"},
				},
			},
		},
	},
	{
		Name: "different columns of the same row merge on two branches",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ set @@dolt_transaction_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ set @@dolt_transaction_commit_message='automatic batch'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_transaction_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_transaction_commit_message='automatic batch'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_transaction_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_transaction_commit_message='automatic batch'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update `mydb/b1`.t set x=11 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update `mydb/main`.t set x=11 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update `mydb/b1`.t set y=111 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update `mydb/main`.t set y=111 where pk=1",
				SkipResultsCheck: true,
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
				Query: "/* client b */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 11, 111},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 11, 111},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 11, 111},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/main`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 11, 111},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select message from `mydb/b1`.dolt_log limit 1",
				Expected: []sql.Row{
					{"automatic batch"},
				},
			},
		},
	},
	{
		Name: "late branch conflict rolls back the complete batch and permits retry",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ set @@dolt_transaction_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ set @@dolt_transaction_commit_message='automatic batch'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_transaction_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_transaction_commit_message='automatic batch'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_transaction_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_transaction_commit_message='automatic batch'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b1`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b2`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update `mydb/main`.t set x=11 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update `mydb/main`.t set x=12 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b3`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:       "/* client a */ commit",
				ExpectedErr: sql.ErrLockDeadlock,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client b */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b2`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 12, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b1`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update `mydb/main`.t set x=13 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 13, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 13, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select message from `mydb/b1`.dolt_log limit 1",
				Expected: []sql.Row{
					{"automatic batch"},
				},
			},
		},
	},
	{
		Name: "delete update conflict does not publish nonconflicting branches",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ set @@dolt_transaction_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ set @@dolt_transaction_commit_message='automatic batch'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_transaction_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_transaction_commit_message='automatic batch'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_transaction_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_transaction_commit_message='automatic batch'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ delete from `mydb/b1`.t where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b2`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update `mydb/b1`.t set x=11 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b3`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:       "/* client a */ commit",
				ExpectedErr: sql.ErrLockDeadlock,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client b */ select * from `mydb/b2`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 11, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b2`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 11, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select message from `mydb/b1`.dolt_log limit 1",
				Expected: []sql.Row{
					{"automatic batch"},
				},
			},
		},
	},
	{
		Name: "identical edits merge while preserving independent branch writes",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ set @@dolt_transaction_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ set @@dolt_transaction_commit_message='automatic batch'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_transaction_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_transaction_commit_message='automatic batch'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_transaction_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_transaction_commit_message='automatic batch'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update `mydb/b1`.t set x=11 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update `mydb/b1`.t set x=11 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/main`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b2`.t values (5,50,500)",
				SkipResultsCheck: true,
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
				Query: "/* client b */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 11, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b2`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 11, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/main`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b2`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client a */ select message from `mydb/b1`.dolt_log limit 1",
				Expected: []sql.Row{
					{"automatic batch"},
				},
			},
		},
	},
	{
		Name: "savepoint restores edits across branches before merging another client",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ set @@dolt_transaction_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ set @@dolt_transaction_commit_message='automatic batch'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_transaction_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_transaction_commit_message='automatic batch'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_transaction_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_transaction_commit_message='automatic batch'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b1`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/main`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ savepoint keep",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b3`.t values (6,60,600)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ delete from `mydb/b1`.t where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update `mydb/main`.t set x=99 where pk=2",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b1`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b2`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:            "/* client a */ rollback to savepoint keep",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b3`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b3`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select message from `mydb/b1`.dolt_log limit 1",
				Expected: []sql.Row{
					{"automatic batch"},
				},
			},
		},
	},
	{
		Name: "rollback and no-op commit leave other clients branch writes intact",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ set @@dolt_transaction_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ set @@dolt_transaction_commit_message='automatic batch'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_transaction_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set @@dolt_transaction_commit_message='automatic batch'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_transaction_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ set @@dolt_transaction_commit_message='automatic batch'",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b1`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/main`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b1`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b2`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:            "/* client a */ rollback",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ commit",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b2`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b2`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client a */ select message from `mydb/b1`.dolt_log limit 1",
				Expected: []sql.Row{
					{"automatic batch"},
				},
			},
		},
	},
	{
		Name: "multi branch opt in is session local",
		SetUpScript: []string{
			"create table t (pk int primary key)",
			"call dolt_commit('-Am','setup')",
			"call dolt_branch('b1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ set @@dolt_multi_branch_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client b */ select @@dolt_multi_branch_commit",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query:            "/* client b */ set @@dolt_transaction_commit=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into t values (1)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b1`.t values (2)",
				SkipResultsCheck: true,
			},
			{
				Query:          "/* client b */ commit",
				ExpectedErrStr: "Cannot commit changes on more than one branch / database",
			},
			{
				Query:    "/* client b */ rollback",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from `mydb/b1`.t",
				Expected: []sql.Row{},
			},
		},
	},
}
