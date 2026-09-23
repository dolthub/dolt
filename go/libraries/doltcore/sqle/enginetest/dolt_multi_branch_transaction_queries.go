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
	"fmt"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
	"strings"
)

// multiBranchTransactionTests exercises the same interleavings with working-set
// transactions and transactions that also publish branch commits.
func multiBranchTransactionTests(commitHeads bool) []queries.TransactionTest {
	setup := []string{
		"create table t (pk int primary key, x int, y int)",
		"insert into t values (1,10,100),(2,20,200),(3,30,300)",
		"call dolt_commit('-Am', 'setup')",
		"call dolt_branch('b1')",
		"call dolt_branch('b2')",
		"call dolt_branch('b3')",
		"set autocommit = 0",
	}
	run := func(client, query string) queries.ScriptTestAssertion {
		return queries.ScriptTestAssertion{Query: "/* client " + client + " */ " + query, SkipResultsCheck: true}
	}
	commit := func(client string, branches ...string) queries.ScriptTestAssertion {
		if !commitHeads {
			return queries.ScriptTestAssertion{Query: "/* client " + client + " */ commit", Expected: []sql.Row{}}
		}
		rows := make([]sql.Row, len(branches))
		for i, branch := range branches {
			rows[i] = sql.Row{branch, doltCommit}
		}
		return queries.ScriptTestAssertion{Query: "/* client " + client + " */ call dolt_commit_all('-am', 'batch')", Expected: rows}
	}
	rows := func(client, branch string, expected ...sql.Row) queries.ScriptTestAssertion {
		return queries.ScriptTestAssertion{Query: fmt.Sprintf("/* client %s */ select * from `mydb/%s`.t order by pk", client, branch), Expected: expected}
	}
	conflict := func(client string) queries.ScriptTestAssertion {
		assertion := commit(client)
		assertion.Expected = nil
		assertion.ExpectedErr = sql.ErrLockDeadlock
		return assertion
	}
	tests := []queries.TransactionTest{
		{
			Name:        "three clients merge partially overlapping branch sets",
			SetUpScript: setup,
			Assertions: []queries.ScriptTestAssertion{
				run("a", "start transaction"), run("b", "start transaction"), run("c", "start transaction"),
				run("a", "insert into `mydb/main`.t values (4,40,400)"),
				run("a", "insert into `mydb/b1`.t values (4,40,400)"),
				run("b", "insert into `mydb/b1`.t values (5,50,500)"),
				run("b", "insert into `mydb/b2`.t values (5,50,500)"),
				run("c", "insert into `mydb/b2`.t values (6,60,600)"),
				run("c", "insert into `mydb/b3`.t values (6,60,600)"),
				commit("b", "b1", "b2"),
				rows("a", "b1", sql.Row{1, 10, 100}, sql.Row{2, 20, 200}, sql.Row{3, 30, 300}, sql.Row{4, 40, 400}),
				rows("c", "b2", sql.Row{1, 10, 100}, sql.Row{2, 20, 200}, sql.Row{3, 30, 300}, sql.Row{6, 60, 600}),
				commit("a", "b1", "main"), commit("c", "b2", "b3"), run("a", "start transaction"),
				rows("a", "main", sql.Row{1, 10, 100}, sql.Row{2, 20, 200}, sql.Row{3, 30, 300}, sql.Row{4, 40, 400}),
				rows("a", "b1", sql.Row{1, 10, 100}, sql.Row{2, 20, 200}, sql.Row{3, 30, 300}, sql.Row{4, 40, 400}, sql.Row{5, 50, 500}),
				rows("a", "b2", sql.Row{1, 10, 100}, sql.Row{2, 20, 200}, sql.Row{3, 30, 300}, sql.Row{5, 50, 500}, sql.Row{6, 60, 600}),
				rows("a", "b3", sql.Row{1, 10, 100}, sql.Row{2, 20, 200}, sql.Row{3, 30, 300}, sql.Row{6, 60, 600}),
			},
		},
		{
			Name:        "disjoint updates and deletes merge on overlapping branches",
			SetUpScript: setup,
			Assertions: []queries.ScriptTestAssertion{
				run("a", "start transaction"), run("b", "start transaction"),
				run("a", "update `mydb/b1`.t set x=11 where pk=1"),
				run("a", "delete from `mydb/main`.t where pk=3"),
				run("b", "delete from `mydb/b1`.t where pk=3"),
				run("b", "update `mydb/b2`.t set y=222 where pk=2"),
				commit("b", "b1", "b2"), commit("a", "b1", "main"),
				rows("a", "b1", sql.Row{1, 11, 100}, sql.Row{2, 20, 200}),
				rows("a", "main", sql.Row{1, 10, 100}, sql.Row{2, 20, 200}),
				rows("a", "b2", sql.Row{1, 10, 100}, sql.Row{2, 20, 222}, sql.Row{3, 30, 300}),
			},
		},
		{
			Name:        "different columns of the same row merge on two branches",
			SetUpScript: setup,
			Assertions: []queries.ScriptTestAssertion{
				run("a", "start transaction"), run("b", "start transaction"),
				run("a", "update `mydb/b1`.t set x=11 where pk=1"),
				run("a", "update `mydb/main`.t set x=11 where pk=1"),
				run("b", "update `mydb/b1`.t set y=111 where pk=1"),
				run("b", "update `mydb/main`.t set y=111 where pk=1"),
				commit("a", "b1", "main"), commit("b", "b1", "main"),
				rows("b", "b1", sql.Row{1, 11, 111}, sql.Row{2, 20, 200}, sql.Row{3, 30, 300}),
				rows("b", "main", sql.Row{1, 11, 111}, sql.Row{2, 20, 200}, sql.Row{3, 30, 300}),
			},
		},
		{
			Name:        "late branch conflict rolls back the complete batch and permits retry",
			SetUpScript: setup,
			Assertions: []queries.ScriptTestAssertion{
				run("a", "start transaction"), run("b", "start transaction"),
				run("a", "insert into `mydb/b1`.t values (4,40,400)"),
				run("a", "insert into `mydb/b2`.t values (4,40,400)"),
				run("a", "update `mydb/main`.t set x=11 where pk=1"),
				run("b", "update `mydb/main`.t set x=12 where pk=1"),
				run("b", "insert into `mydb/b3`.t values (5,50,500)"),
				commit("b", "b3", "main"), conflict("a"), run("b", "start transaction"),
				rows("b", "b1", sql.Row{1, 10, 100}, sql.Row{2, 20, 200}, sql.Row{3, 30, 300}),
				rows("b", "b2", sql.Row{1, 10, 100}, sql.Row{2, 20, 200}, sql.Row{3, 30, 300}),
				rows("a", "main", sql.Row{1, 12, 100}, sql.Row{2, 20, 200}, sql.Row{3, 30, 300}),
				run("a", "start transaction"),
				run("a", "insert into `mydb/b1`.t values (4,40,400)"),
				run("a", "update `mydb/main`.t set x=13 where pk=1"),
				commit("a", "b1", "main"),
				rows("a", "b1", sql.Row{1, 10, 100}, sql.Row{2, 20, 200}, sql.Row{3, 30, 300}, sql.Row{4, 40, 400}),
				rows("a", "main", sql.Row{1, 13, 100}, sql.Row{2, 20, 200}, sql.Row{3, 30, 300}),
			},
		},
		{
			Name:        "delete update conflict does not publish nonconflicting branches",
			SetUpScript: setup,
			Assertions: []queries.ScriptTestAssertion{
				run("a", "start transaction"), run("b", "start transaction"),
				run("a", "delete from `mydb/b1`.t where pk=1"),
				run("a", "insert into `mydb/b2`.t values (4,40,400)"),
				run("b", "update `mydb/b1`.t set x=11 where pk=1"),
				run("b", "insert into `mydb/b3`.t values (5,50,500)"),
				commit("b", "b1", "b3"), conflict("a"), run("b", "start transaction"),
				rows("b", "b2", sql.Row{1, 10, 100}, sql.Row{2, 20, 200}, sql.Row{3, 30, 300}),
				rows("a", "b1", sql.Row{1, 11, 100}, sql.Row{2, 20, 200}, sql.Row{3, 30, 300}),
			},
		},
		{
			Name:        "identical edits merge while preserving independent branch writes",
			SetUpScript: setup,
			Assertions: []queries.ScriptTestAssertion{
				run("a", "start transaction"), run("b", "start transaction"),
				run("a", "update `mydb/b1`.t set x=11 where pk=1"),
				run("b", "update `mydb/b1`.t set x=11 where pk=1"),
				run("a", "insert into `mydb/main`.t values (4,40,400)"),
				run("b", "insert into `mydb/b2`.t values (5,50,500)"),
				commit("a", "b1", "main"), commit("b", "b1", "b2"),
				rows("b", "b1", sql.Row{1, 11, 100}, sql.Row{2, 20, 200}, sql.Row{3, 30, 300}),
				rows("b", "main", sql.Row{1, 10, 100}, sql.Row{2, 20, 200}, sql.Row{3, 30, 300}, sql.Row{4, 40, 400}),
				rows("b", "b2", sql.Row{1, 10, 100}, sql.Row{2, 20, 200}, sql.Row{3, 30, 300}, sql.Row{5, 50, 500}),
			},
		},
		{
			Name:        "savepoint restores edits across branches before merging another client",
			SetUpScript: setup,
			Assertions: []queries.ScriptTestAssertion{
				run("a", "start transaction"), run("b", "start transaction"),
				run("a", "insert into `mydb/b1`.t values (4,40,400)"),
				run("a", "insert into `mydb/main`.t values (4,40,400)"),
				run("a", "savepoint keep"),
				run("a", "insert into `mydb/b3`.t values (6,60,600)"),
				run("a", "delete from `mydb/b1`.t where pk=1"),
				run("a", "update `mydb/main`.t set x=99 where pk=2"),
				run("b", "insert into `mydb/b1`.t values (5,50,500)"),
				run("b", "insert into `mydb/b2`.t values (5,50,500)"),
				commit("b", "b1", "b2"),
				run("a", "rollback to savepoint keep"),
				commit("a", "b1", "main"),
				rows("a", "b1", sql.Row{1, 10, 100}, sql.Row{2, 20, 200}, sql.Row{3, 30, 300}, sql.Row{4, 40, 400}, sql.Row{5, 50, 500}),
				rows("a", "main", sql.Row{1, 10, 100}, sql.Row{2, 20, 200}, sql.Row{3, 30, 300}, sql.Row{4, 40, 400}),
				rows("a", "b3", sql.Row{1, 10, 100}, sql.Row{2, 20, 200}, sql.Row{3, 30, 300}),
			},
		},
		{
			Name:        "rollback and no-op commit leave other clients branch writes intact",
			SetUpScript: setup,
			Assertions: []queries.ScriptTestAssertion{
				run("a", "start transaction"), run("b", "start transaction"),
				run("a", "insert into `mydb/b1`.t values (4,40,400)"),
				run("a", "insert into `mydb/main`.t values (4,40,400)"),
				run("b", "insert into `mydb/b1`.t values (5,50,500)"),
				run("b", "insert into `mydb/b2`.t values (5,50,500)"),
				commit("b", "b1", "b2"), run("a", "rollback"), run("a", "commit"),
				rows("a", "b1", sql.Row{1, 10, 100}, sql.Row{2, 20, 200}, sql.Row{3, 30, 300}, sql.Row{5, 50, 500}),
				rows("a", "main", sql.Row{1, 10, 100}, sql.Row{2, 20, 200}, sql.Row{3, 30, 300}),
				rows("a", "b2", sql.Row{1, 10, 100}, sql.Row{2, 20, 200}, sql.Row{3, 30, 300}, sql.Row{5, 50, 500}),
			},
		},
	}
	// Verify committed HEADs independently of the working-set reads above. Failed
	// and rolled-back batches must leave every nonparticipating head unchanged.
	for i := range tests {
		assertions := tests[i].Assertions
		lastWrite := 0
		for j, a := range assertions {
			if a.SkipResultsCheck || a.ExpectedErr != nil || (len(a.Query) > 0 && !strings.Contains(a.Query, "select *")) {
				lastWrite = j
			}
		}
		for _, a := range assertions[lastWrite+1:] {
			head := a
			head.Query = strings.Replace(a.Query, ".t order by pk", ".t as of 'HEAD' order by pk", 1)
			if !commitHeads {
				head.Expected = []sql.Row{{1, 10, 100}, {2, 20, 200}, {3, 30, 300}}
			}
			tests[i].Assertions = append(tests[i].Assertions, head)
		}
	}
	return tests
}
