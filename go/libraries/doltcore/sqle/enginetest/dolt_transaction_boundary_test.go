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
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
)

// These tests check what happens to a transaction's writes where the transaction could end partway through a
// statement. A CALL inside a transaction keeps the transaction's writes.

type autocommitMode int

const (
	autocommitOn autocommitMode = iota
	autocommitOff
)

var autocommitModes = []autocommitMode{autocommitOn, autocommitOff}

func (m autocommitMode) String() string {
	if m == autocommitOn {
		return "autocommit on"
	}
	return "autocommit off"
}

// boundaryCheck is a query whose result must hold after a boundary case's statements, both for the client that ran
// them and for another client once they are committed.
type boundaryCheck struct {
	query    string
	expected []sql.Row
}

// boundaryCase runs |statements| as client a, then |checks| as client a and as client b. With autocommit off, client a
// sets autocommit = 0 first and commits before client b checks. With |explicitTx|, client a runs |statements| inside
// START TRANSACTION ... COMMIT when autocommit is on.
type boundaryCase struct {
	name       string
	setup      []string
	explicitTx bool
	statements []string
	checks     []boundaryCheck
	// modes are the autocommit modes to run the case in. Nil means both.
	modes []autocommitMode
}

func (c boundaryCase) script(mode autocommitMode) queries.TransactionTest {
	var assertions []queries.ScriptTestAssertion
	stmt := func(q string) {
		assertions = append(assertions, queries.ScriptTestAssertion{Query: "/* client a */ " + q, SkipResultsCheck: true})
	}
	inTx := mode == autocommitOff || c.explicitTx
	if mode == autocommitOff {
		stmt("set autocommit = 0")
	} else if c.explicitTx {
		stmt("start transaction")
	}
	for _, s := range c.statements {
		stmt(s)
	}
	addChecks := func(client string) {
		for _, chk := range c.checks {
			assertions = append(assertions, queries.ScriptTestAssertion{
				Query:    "/* client " + client + " */ " + chk.query,
				Expected: chk.expected,
			})
		}
	}
	addChecks("a")
	if inTx {
		stmt("commit")
	}
	addChecks("b")
	return queries.TransactionTest{
		Name:        c.name + ", " + mode.String(),
		SetUpScript: c.setup,
		Assertions:  assertions,
	}
}

// withBoundarySetup returns the setup shared by the trigger cases, with |extra| run before the initial commit.
func withBoundarySetup(extra ...string) []string {
	setup := []string{
		"create table t (a int primary key, b int)",
		"create table t2 (a int primary key, b int)",
		"create table t3 (a int primary key)",
		"create procedure p() select 1",
	}
	setup = append(setup, extra...)
	return append(setup, "call dolt_commit('-Am', 'init')")
}

var threeRows = []sql.Row{{1, 1}, {2, 2}, {3, 3}}

var boundaryCases = []boundaryCase{
	{
		// #12356 as reported.
		name: "delete, then CALL of a procedure that only selects",
		setup: []string{
			"create table t (id int primary key)",
			"insert into t values (1), (2)",
			"create procedure p() select 1",
			"call dolt_commit('-Am', 'init')",
		},
		explicitTx: true,
		statements: []string{
			"delete from t where id = 1",
			"call p()",
		},
		checks: []boundaryCheck{
			// #12356: the deleted row came back after the CALL.
			{query: "select * from t order by id", expected: []sql.Row{{2}}},
		},
	},
	{
		// Control: the issue reports that the delete survives with autocommit on and no explicit transaction.
		name: "delete, then CALL, each statement autocommitted",
		setup: []string{
			"create table t (id int primary key)",
			"insert into t values (1), (2)",
			"create procedure p() select 1",
			"call dolt_commit('-Am', 'init')",
		},
		statements: []string{
			"delete from t where id = 1",
			"call p()",
		},
		checks: []boundaryCheck{
			{query: "select * from t order by id", expected: []sql.Row{{2}}},
		},
		modes: []autocommitMode{autocommitOn},
	},
	{
		name: "trigger inserts then calls a procedure that only selects, over several rows",
		setup: withBoundarySetup(`create trigger trg after insert on t for each row
begin
	insert into t2 values (new.a, new.b);
	call p();
end`),
		statements: []string{"insert into t values (1, 1), (2, 2), (3, 3)"},
		checks: []boundaryCheck{
			{query: "select * from t order by a", expected: threeRows},
			// #12356 and #7737: a CALL in a trigger started a new transaction, and t2 had only the first row.
			{query: "select * from t2 order by a", expected: threeRows},
		},
	},
	{
		name: "writes before and after a CALL of a procedure that writes another table",
		setup: []string{
			"create table t (id int primary key)",
			"create table u (id int primary key)",
			"insert into t values (1), (2)",
			"create procedure p2() insert into u values (10)",
			"call dolt_commit('-Am', 'init')",
		},
		explicitTx: true,
		statements: []string{
			"delete from t where id = 1",
			"insert into t values (3)",
			"call p2()",
			"insert into t values (4)",
		},
		checks: []boundaryCheck{
			// #12356: the delete and the insert of 3 were undone by the CALL.
			{query: "select * from t order by id", expected: []sql.Row{{2}, {3}, {4}}},
			{query: "select * from u order by id", expected: []sql.Row{{10}}},
		},
	},
}

// TestTransactionBoundary runs each case above with autocommit on and off.
func TestTransactionBoundary(t *testing.T) {
	for _, c := range boundaryCases {
		modes := c.modes
		if modes == nil {
			modes = autocommitModes
		}
		for _, mode := range modes {
			script := c.script(mode)
			func() {
				h := newDoltHarness(t)
				defer h.Close()
				enginetest.TestTransactionScript(t, h, script)
			}()
		}
	}
}
