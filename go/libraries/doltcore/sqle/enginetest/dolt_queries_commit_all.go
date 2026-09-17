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

var DoltCommitAllTransactionTests = []queries.TransactionTest{
	{Name: "concurrent dolt_commit_all merges every head",
		SetUpScript: []string{"create table t (pk int primary key, v int)", "insert into t values (1, 0)", "call dolt_commit('-Am', 'setup')", "call dolt_branch('b1')", "call dolt_branch('b2')", "set autocommit = 0"},
		Assertions: []queries.ScriptTestAssertion{
			{Query: "/* client a */ start transaction", SkipResultsCheck: true},
			{Query: "/* client b */ start transaction", SkipResultsCheck: true},
			{Query: "/* client a */ insert into `mydb/main`.t values (2, 20)", SkipResultsCheck: true},
			{Query: "/* client a */ insert into `mydb/b1`.t values (2, 20)", SkipResultsCheck: true},
			{Query: "/* client a */ insert into `mydb/b2`.t values (2, 20)", SkipResultsCheck: true},
			{Query: "/* client b */ insert into `mydb/main`.t values (3, 30)", SkipResultsCheck: true},
			{Query: "/* client b */ insert into `mydb/b1`.t values (3, 30)", SkipResultsCheck: true},
			{Query: "/* client b */ insert into `mydb/b2`.t values (3, 30)", SkipResultsCheck: true},
			{Query: "/* client b */ call dolt_commit_all('-am', 'b')", Expected: []sql.Row{{"b1", doltCommit}, {"b2", doltCommit}, {"main", doltCommit}}},
			{Query: "/* client a */ select * from `mydb/b1`.t as of 'HEAD'", Expected: []sql.Row{{1, 0}}},
			{Query: "/* client a */ call dolt_commit_all('-am', 'a')", Expected: []sql.Row{{"b1", doltCommit}, {"b2", doltCommit}, {"main", doltCommit}}},
			{Query: "/* client a */ select * from `mydb/main`.t as of 'HEAD' order by pk", Expected: []sql.Row{{1, 0}, {2, 20}, {3, 30}}},
			{Query: "/* client a */ select * from `mydb/b1`.t as of 'HEAD' order by pk", Expected: []sql.Row{{1, 0}, {2, 20}, {3, 30}}},
			{Query: "/* client a */ select * from `mydb/b2`.t as of 'HEAD' order by pk", Expected: []sql.Row{{1, 0}, {2, 20}, {3, 30}}},
		}},
	{Name: "conflict in dolt_commit_all leaves all other heads unchanged",
		SetUpScript: []string{"create table t (pk int primary key, v int)", "insert into t values (1, 0)", "call dolt_commit('-Am', 'setup')", "call dolt_branch('b1')", "call dolt_branch('b2')", "set autocommit = 0"},
		Assertions: []queries.ScriptTestAssertion{
			{Query: "/* client a */ start transaction", SkipResultsCheck: true},
			{Query: "/* client b */ start transaction", SkipResultsCheck: true},
			{Query: "/* client a */ insert into `mydb/b1`.t values (2, 20)", SkipResultsCheck: true},
			{Query: "/* client a */ insert into `mydb/b2`.t values (2, 20)", SkipResultsCheck: true},
			{Query: "/* client a */ update t set v = 10", SkipResultsCheck: true},
			{Query: "/* client b */ update t set v = 20", SkipResultsCheck: true},
			{Query: "/* client b */ call dolt_commit('-am', 'b')", SkipResultsCheck: true},
			{Query: "/* client a */ call dolt_commit_all('-am', 'a')", ExpectedErr: sql.ErrLockDeadlock},
			{Query: "/* client a */ select * from `mydb/b1`.t as of 'HEAD'", Expected: []sql.Row{{1, 0}}},
			{Query: "/* client a */ select * from `mydb/b1`.t", Expected: []sql.Row{{1, 0}}},
			{Query: "/* client a */ select * from `mydb/b2`.t as of 'HEAD'", Expected: []sql.Row{{1, 0}}},
			{Query: "/* client a */ select * from `mydb/b2`.t", Expected: []sql.Row{{1, 0}}},
		}},
	{Name: "stale amend aborts every head in dolt_commit_all",
		SetUpScript: []string{"create table t (pk int primary key, v int)", "insert into t values (1, 0)", "call dolt_commit('-Am', 'setup')", "call dolt_branch('b1')", "call dolt_branch('b2')", "set autocommit = 0"},
		Assertions: []queries.ScriptTestAssertion{
			{Query: "/* client a */ start transaction", SkipResultsCheck: true},
			{Query: "/* client b */ start transaction", SkipResultsCheck: true},
			{Query: "/* client a */ insert into `mydb/main`.t values (2, 20)", SkipResultsCheck: true},
			{Query: "/* client a */ insert into `mydb/b1`.t values (2, 20)", SkipResultsCheck: true},
			{Query: "/* client a */ insert into `mydb/b2`.t values (2, 20)", SkipResultsCheck: true},
			{Query: "/* client b */ insert into t values (3, 30)", SkipResultsCheck: true},
			{Query: "/* client b */ call dolt_commit('-am', 'b')", SkipResultsCheck: true},
			{Query: "/* client a */ call dolt_commit_all('--amend', '-am', 'a')", ExpectedErr: sql.ErrLockDeadlock},
			{Query: "/* client a */ select * from `mydb/b1`.t as of 'HEAD'", Expected: []sql.Row{{1, 0}}},
			{Query: "/* client a */ select * from `mydb/b2`.t as of 'HEAD'", Expected: []sql.Row{{1, 0}}},
		}},
}

var DoltCommitAllTests = []queries.ScriptTest{
	{Name: "dolt_commit_all publishes three heads and preserves selected database",
		SetUpScript: []string{"create table t (pk int primary key, v int)", "insert into t values (1, 0)", "call dolt_commit('-Am', 'setup')", "call dolt_branch('b1')", "call dolt_branch('b2')", "set autocommit = 0", "insert into `mydb/main`.t values (2, 20)", "insert into `mydb/b1`.t values (2, 20)", "insert into `mydb/b2`.t values (2, 20)"},
		Assertions: []queries.ScriptTestAssertion{
			{Query: "call dolt_commit_all('-am', 'all branches')", Expected: []sql.Row{{"b1", doltCommit}, {"b2", doltCommit}, {"main", doltCommit}}},
			{Query: "select * from `mydb/main`.t as of 'HEAD' order by pk", Expected: []sql.Row{{1, 0}, {2, 20}}},
			{Query: "select message from `mydb/main`.dolt_log limit 1", Expected: []sql.Row{{"all branches"}}},
			{Query: "select * from `mydb/b1`.t as of 'HEAD' order by pk", Expected: []sql.Row{{1, 0}, {2, 20}}},
			{Query: "select message from `mydb/b1`.dolt_log limit 1", Expected: []sql.Row{{"all branches"}}},
			{Query: "select * from `mydb/b2`.t as of 'HEAD' order by pk", Expected: []sql.Row{{1, 0}, {2, 20}}},
			{Query: "select message from `mydb/b2`.dolt_log limit 1", Expected: []sql.Row{{"all branches"}}},
			{Query: "select database(), active_branch()", Expected: []sql.Row{{"mydb", "main"}}},
			{Query: "insert into t values (3, 30)", SkipResultsCheck: true},
			{Query: "commit", SkipResultsCheck: true},
			{Query: "select count(*) from t", Expected: []sql.Row{{3}}},
		}},
	{Name: "dolt_commit retains its single dirty branch restriction",
		SetUpScript: []string{"create table t (pk int primary key, v int)", "insert into t values (1, 0)", "call dolt_commit('-Am', 'setup')", "call dolt_branch('b1')", "call dolt_branch('b2')", "set autocommit = 0", "insert into `mydb/main`.t values (2, 20)", "insert into `mydb/b1`.t values (2, 20)", "insert into `mydb/b2`.t values (2, 20)"},
		Assertions: []queries.ScriptTestAssertion{
			{Query: "call dolt_commit('-am', 'single')", ExpectedErrStr: "Cannot commit changes on more than one branch / database"},
			{Query: "select * from `mydb/main`.t as of 'HEAD'", Expected: []sql.Row{{1, 0}}},
			{Query: "select * from `mydb/b1`.t as of 'HEAD'", Expected: []sql.Row{{1, 0}}},
			{Query: "select * from `mydb/b2`.t as of 'HEAD'", Expected: []sql.Row{{1, 0}}},
			{Query: "call dolt_commit_all('-am', 'all branches')", Expected: []sql.Row{{"b1", doltCommit}, {"b2", doltCommit}, {"main", doltCommit}}},
		}},
	{Name: "dolt_commit_all keeps unstaged changes on every branch",
		SetUpScript: []string{"create table t (pk int primary key, v int)", "insert into t values (1, 0)", "call dolt_commit('-Am', 'setup')", "call dolt_branch('b1')", "call dolt_branch('b2')", "set autocommit = 0", "insert into t values (2, 20)", "call dolt_add('.')", "insert into t values (3, 30)", "use mydb/b1", "insert into t values (2, 20)", "call dolt_add('.')", "insert into t values (3, 30)"},
		Assertions: []queries.ScriptTestAssertion{
			{Query: "call dolt_commit_all('-m', 'staged only')", Expected: []sql.Row{{"b1", doltCommit}, {"main", doltCommit}}},
			{Query: "select * from `mydb/main`.t as of 'HEAD' order by pk", Expected: []sql.Row{{1, 0}, {2, 20}}},
			{Query: "select * from `mydb/main`.t order by pk", Expected: []sql.Row{{1, 0}, {2, 20}, {3, 30}}},
			{Query: "select * from `mydb/b1`.t as of 'HEAD' order by pk", Expected: []sql.Row{{1, 0}, {2, 20}}},
			{Query: "select * from `mydb/b1`.t order by pk", Expected: []sql.Row{{1, 0}, {2, 20}, {3, 30}}},
		}},
	{Name: "dolt_commit_all preparation failure leaves every head unchanged",
		SetUpScript: []string{"create table t (pk int primary key, v int)", "insert into t values (1, 0)", "call dolt_commit('-Am', 'setup')", "call dolt_branch('b1')", "call dolt_branch('b2')", "set autocommit = 0", "insert into `mydb/b1`.t values (2, 20)", "use mydb/b1", "call dolt_add('.')", "use mydb/main", "insert into t values (3, 30)"},
		Assertions: []queries.ScriptTestAssertion{
			{Query: "call dolt_commit_all('-m', 'fails')", ExpectedErrStr: "nothing to commit on mydb/main"},
			{Query: "select * from `mydb/main`.t as of 'HEAD'", Expected: []sql.Row{{1, 0}}},
			{Query: "select * from `mydb/b1`.t as of 'HEAD'", Expected: []sql.Row{{1, 0}}},
			{Query: "select database()", Expected: []sql.Row{{"mydb/main"}}},
			{Query: "call dolt_commit_all('-am', 'retry')", Expected: []sql.Row{{"b1", doltCommit}, {"main", doltCommit}}},
		}},
	{Name: "dolt_commit_all skip-empty still publishes skipped working sets",
		SetUpScript: []string{"create table t (pk int primary key, v int)", "insert into t values (1, 0)", "call dolt_commit('-Am', 'setup')", "call dolt_branch('b1')", "call dolt_branch('b2')", "set autocommit = 0", "insert into `mydb/b1`.t values (2, 20)", "use mydb/b1", "call dolt_add('.')", "use mydb", "insert into t values (3, 30)"},
		Assertions: []queries.ScriptTestAssertion{
			{Query: "call dolt_commit_all('--skip-empty', '-m', 'staged branch')", Expected: []sql.Row{{"b1", doltCommit}}},
			{Query: "select * from t as of 'HEAD'", Expected: []sql.Row{{1, 0}}},
			{Query: "select * from t order by pk", Expected: []sql.Row{{1, 0}, {3, 30}}},
			{Query: "select * from `mydb/b1`.t as of 'HEAD' order by pk", Expected: []sql.Row{{1, 0}, {2, 20}}},
		}},
	{Name: "dolt_commit_all amends each branch independently",
		SetUpScript: []string{"create table t (pk int primary key, v int)", "insert into t values (1, 0)", "call dolt_commit('-Am', 'setup')", "call dolt_branch('b1')", "call dolt_branch('b2')", "set autocommit = 0", "insert into `mydb/main`.t values (2, 20)", "insert into `mydb/b1`.t values (2, 20)", "insert into `mydb/b2`.t values (2, 20)"},
		Assertions: []queries.ScriptTestAssertion{
			{Query: "set @before_count = (select count(*) from dolt_log)", SkipResultsCheck: true},
			{Query: "call dolt_commit_all('--amend', '-am', 'amended')", Expected: []sql.Row{{"b1", doltCommit}, {"b2", doltCommit}, {"main", doltCommit}}},
			{Query: "select count(*) = @before_count from `mydb/main`.dolt_log", Expected: []sql.Row{{true}}},
			{Query: "select count(*) = @before_count from `mydb/b1`.dolt_log", Expected: []sql.Row{{true}}},
			{Query: "select count(*) = @before_count from `mydb/b2`.dolt_log", Expected: []sql.Row{{true}}},
		}},
	{Name: "dolt_commit_all with no dirty branches uses the selected branch",
		SetUpScript: []string{"create table t (pk int primary key, v int)", "insert into t values (1, 0)", "call dolt_commit('-Am', 'setup')", "call dolt_branch('b1')", "call dolt_branch('b2')", "set autocommit = 0"},
		Assertions: []queries.ScriptTestAssertion{
			{Query: "call dolt_commit_all('--allow-empty', '-m', 'empty')", Expected: []sql.Row{{"main", doltCommit}}},
			{Query: "call dolt_commit_all('--skip-empty', '-m', 'skip')", Expected: []sql.Row{}},
			{Query: "call dolt_commit_all('--branch', 'b1', '-am', 'unsupported')", ExpectedErrStr: "--branch is not supported by dolt_commit_all"},
		}},
}
