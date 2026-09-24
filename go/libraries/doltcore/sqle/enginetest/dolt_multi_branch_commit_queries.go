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
	"strings"

	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
)

var DoltMultiBranchCommitTests = []queries.ScriptTest{
	{
		Name: "multi branch commit defaults off but SQL working set commits still work",
		SetUpScript: []string{
			"create table t (pk int primary key)",
			"call dolt_commit('-Am', 'setup')", "call dolt_branch('b1')", "set autocommit=0",
			"insert into t values (1)", "insert into `mydb/b1`.t values (2)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{Query: "select @@session.dolt_multi_branch_commit, @@global.dolt_multi_branch_commit", Expected: []sql.Row{{0, 0}}},
			{Query: "call dolt_commit('-am', 'rejected')", ExpectedErrStr: "Cannot commit changes on more than one branch / database"},
			{Query: "commit", Expected: []sql.Row{}},
			{Query: "select * from t", Expected: []sql.Row{{1}}},
			{Query: "select * from `mydb/b1`.t", Expected: []sql.Row{{2}}},
			{Query: "select * from t as of 'HEAD'", Expected: []sql.Row{}},
			{Query: "select * from `mydb/b1`.t as of 'HEAD'", Expected: []sql.Row{}},
		},
	},
	{
		Name: "transaction commits reject multiple branches until enabled",
		SetUpScript: []string{
			"create table t (pk int primary key)",
			"call dolt_commit('-Am', 'setup')", "call dolt_branch('b1')", "set autocommit=0",
			"set @@dolt_transaction_commit=1", "set @@dolt_transaction_commit_message='multi transaction'",
			"insert into t values (1)", "insert into `mydb/b1`.t values (2)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{Query: "commit", ExpectedErrStr: "Cannot commit changes on more than one branch / database"},
			{Query: "select * from t as of 'HEAD'", Expected: []sql.Row{}},
			{Query: "select * from `mydb/b1`.t as of 'HEAD'", Expected: []sql.Row{}},
			{Query: "set @@dolt_multi_branch_commit=1", SkipResultsCheck: true},
			{Query: "commit", Expected: []sql.Row{}},
			{Query: "select * from t as of 'HEAD'", Expected: []sql.Row{{1}}},
			{Query: "select * from `mydb/b1`.t as of 'HEAD'", Expected: []sql.Row{{2}}},
			{Query: "select message from dolt_log limit 1", Expected: []sql.Row{{"multi transaction"}}},
			{Query: "select message from `mydb/b1`.dolt_log limit 1", Expected: []sql.Row{{"multi transaction"}}},
		},
	},
	{
		Name: "multi branch dolt_commit returns one hash per branch and can be disabled again",
		SetUpScript: []string{
			"create table t (pk int primary key)", "call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')", "set autocommit=0", "set @@dolt_multi_branch_commit=1",
			"insert into t values (1)", "insert into `mydb/b1`.t values (2)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{Query: "call dolt_commit('-am', 'multi')", Expected: []sql.Row{{doltCommit}, {doltCommit}}},
			{Query: "select * from t as of 'HEAD'", Expected: []sql.Row{{1}}},
			{Query: "select * from `mydb/b1`.t as of 'HEAD'", Expected: []sql.Row{{2}}},
			{Query: "set @@dolt_multi_branch_commit=0", SkipResultsCheck: true},
			{Query: "insert into t values (3)", SkipResultsCheck: true},
			{Query: "insert into `mydb/b1`.t values (4)", SkipResultsCheck: true},
			{Query: "call dolt_commit('-am', 'rejected')", ExpectedErrStr: "Cannot commit changes on more than one branch / database"},
			{Query: "call dolt_commit_all('-am', 'explicit')", Expected: []sql.Row{{"b1", doltCommit}, {"main", doltCommit}}},
		},
	},
	{
		Name: "automatic multi branch commit stages new tables with a clean selected branch",
		SetUpScript: []string{
			"call dolt_branch('b1')", "call dolt_branch('b2')",
			"create table `mydb/b1`.created (pk int primary key)",
			"create table `mydb/b2`.created (pk int primary key)",
			"set autocommit=0", "set @@dolt_multi_branch_commit=1", "set @@dolt_transaction_commit=1",
			"insert into `mydb/b1`.created values (1)",
			"insert into `mydb/b2`.created values (2)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{Query: "commit", Expected: []sql.Row{}},
			{Query: "select * from `mydb/b1`.created as of 'HEAD'", Expected: []sql.Row{{1}}},
			{Query: "select * from `mydb/b2`.created as of 'HEAD'", Expected: []sql.Row{{2}}},
			{Query: "select message from `mydb/b1`.dolt_log limit 1", Expected: []sql.Row{{"Transaction commit"}}},
			{Query: "select database(), active_branch()", Expected: []sql.Row{{"mydb", "main"}}},
		},
	},
	{
		Name: "enabled multi branch commits still reject multiple physical databases",
		SetUpScript: []string{
			"create table t (pk int primary key)", "call dolt_commit('-Am','setup')",
			"create database other", "create table other.t (pk int primary key)",
			"set autocommit=0", "set @@dolt_multi_branch_commit=1",
			"insert into t values (1)", "insert into other.t values (2)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{Query: "call dolt_commit('-am','rejected')", ExpectedErrStr: "Cannot atomically commit changes to more than one database"},
			{Query: "set @@dolt_transaction_commit=1", SkipResultsCheck: true},
			{Query: "commit", ExpectedErrStr: "Cannot atomically commit changes to more than one database"},
			{Query: "rollback", Expected: []sql.Row{}},
			{Query: "select * from t", Expected: []sql.Row{}},
			{Query: "select * from other.t", Expected: []sql.Row{}},
		},
	},
}

// Exercise the full overlap/conflict/savepoint matrix through both entry points
// enabled by dolt_multi_branch_commit.
func multiBranchCommitVariableTransactions(automatic bool) []queries.TransactionTest {
	tests := multiBranchTransactionTests(true)
	for i := range tests {
		var settings []queries.ScriptTestAssertion
		for _, client := range []string{"a", "b", "c"} {
			settings = append(settings, queries.ScriptTestAssertion{Query: "/* client " + client + " */ set @@dolt_multi_branch_commit=1", SkipResultsCheck: true})
			if automatic {
				settings = append(settings, queries.ScriptTestAssertion{Query: "/* client " + client + " */ set @@dolt_transaction_commit=1", SkipResultsCheck: true})
			}
		}
		for j := range tests[i].Assertions {
			a := &tests[i].Assertions[j]
			if !strings.Contains(a.Query, "call dolt_commit_all") {
				continue
			}
			if automatic {
				a.Query = strings.Replace(a.Query, "call dolt_commit_all('-am', 'batch')", "commit", 1)
				if a.ExpectedErr == nil {
					a.Expected = []sql.Row{}
				}
			} else {
				a.Query = strings.Replace(a.Query, "dolt_commit_all", "dolt_commit", 1)
				for k := range a.Expected {
					a.Expected[k] = sql.Row{doltCommit}
				}
			}
		}
		tests[i].Assertions = append(settings, tests[i].Assertions...)
	}
	commitQuery := "call dolt_commit('-am', 'session setting')"
	if automatic {
		commitQuery = "commit"
	}
	tests = append(tests, queries.TransactionTest{
		Name: "multi branch opt in is session local",
		SetUpScript: []string{
			"create table t (pk int primary key)", "call dolt_commit('-Am','setup')", "call dolt_branch('b1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{Query: "/* client a */ set @@dolt_multi_branch_commit=1", SkipResultsCheck: true},
			{Query: "/* client b */ select @@dolt_multi_branch_commit", Expected: []sql.Row{{0}}},
			{Query: "/* client b */ set @@dolt_transaction_commit=1", SkipResultsCheck: true},
			{Query: "/* client b */ start transaction", SkipResultsCheck: true},
			{Query: "/* client b */ insert into t values (1)", SkipResultsCheck: true},
			{Query: "/* client b */ insert into `mydb/b1`.t values (2)", SkipResultsCheck: true},
			{Query: "/* client b */ " + commitQuery, ExpectedErrStr: "Cannot commit changes on more than one branch / database"},
			{Query: "/* client b */ rollback", Expected: []sql.Row{}},
			{Query: "/* client a */ select * from t", Expected: []sql.Row{}},
			{Query: "/* client a */ select * from `mydb/b1`.t", Expected: []sql.Row{}},
		},
	})
	return tests
}
