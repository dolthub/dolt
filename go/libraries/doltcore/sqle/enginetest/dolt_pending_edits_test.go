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
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

// pendingEditsDerivingStatements read the whole working set or derive a new one from it. Each must include edits
// that are pending when it runs.
var pendingEditsDerivingStatements = []string{
	"create table n (id int primary key)",
	"alter table t add column z int",
	"alter table t modify column y bigint",
	"alter table t drop column y",
	"create index yi on t (y)",
	"alter table t rename to t2",
	"rename table t to t2",
	"create view v as select * from t",
	"create trigger trg before insert on o for each row set new.id = new.id",
	"create procedure p() select 1",
	"truncate table o",
	"drop table o",
	"alter table t add constraint chk check (x > 0)",
	"alter table o add foreign key (id) references t (x)",
	"call dolt_add('.')",
	"call dolt_commit('-Am', 'c')",
	"call dolt_branch('b2')",
	"select count(*) from dolt_status",
	"select count(*) from dolt_diff_t",
	"select dolt_hashof_db()",
	"savepoint s",
}

func TestPendingEditsSurviveDerivedRoots(t *testing.T) {
	for _, q := range pendingEditsDerivingStatements {
		t.Run(q, func(t *testing.T) {
			tblName := "t"
			if q == "alter table t rename to t2" || q == "rename table t to t2" {
				tblName = "t2"
			}
			script := queries.TransactionTest{
				Name:        q,
				SetUpScript: []string{"create table t (x int primary key, y int)", "create table o (id int primary key)", "insert into o values (1)", "call dolt_commit('-Am', 'init')"},
				Assertions: []queries.ScriptTestAssertion{
					{Query: "/* client a */ set autocommit = 0", SkipResultsCheck: true},
					{Query: "/* client a */ insert into t (x) values (1), (2)", SkipResultsCheck: true},
					{Query: "/* client a */ " + q, SkipResultsCheck: true},
					{Query: "/* client a */ select count(*) from " + tblName, Expected: []sql.Row{{2}}},
					{Query: "/* client a */ commit", Expected: []sql.Row{}},
					{Query: "/* client b */ select count(*) from " + tblName, Expected: []sql.Row{{2}}},
				},
			}
			h := newDoltHarness(t)
			defer h.Close()
			enginetest.TestTransactionScript(t, h, script)
		})
	}
}

// TestPendingEditsMaterializeOnCommit asserts that the edits of completed statements stay pending for the rest of
// their transaction, whether it's explicit or @@autocommit is off, and are materialized when it commits.
func TestPendingEditsMaterializeOnCommit(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	e := mustNewEngine(t, h)
	defer e.Close()
	ctx := enginetest.NewContext(h)

	hasPendingEdits := func() bool {
		state, ok, err := dsess.DSessFromSess(ctx.Session).LookupDbState(ctx, "mydb")
		require.NoError(t, err)
		require.True(t, ok)
		return state.WriteSession().HasPendingEdits()
	}

	enginetest.RunQueryWithContext(t, e, h, ctx, "create table t (x int primary key)")

	enginetest.RunQueryWithContext(t, e, h, ctx, "start transaction")
	enginetest.RunQueryWithContext(t, e, h, ctx, "insert into t values (1)")
	require.True(t, hasPendingEdits())
	enginetest.RunQueryWithContext(t, e, h, ctx, "commit")
	require.False(t, hasPendingEdits())

	enginetest.RunQueryWithContext(t, e, h, ctx, "set autocommit = 0")
	enginetest.RunQueryWithContext(t, e, h, ctx, "insert into t values (2)")
	require.True(t, hasPendingEdits())
	enginetest.RunQueryWithContext(t, e, h, ctx, "commit")
	require.False(t, hasPendingEdits())

	// With autocommit on, each statement's transaction commits when it completes
	enginetest.RunQueryWithContext(t, e, h, ctx, "set autocommit = 1")
	enginetest.RunQueryWithContext(t, e, h, ctx, "insert into t values (3)")
	require.False(t, hasPendingEdits())
}

// pendingEditsReadingStatements read or write rows without deriving a new working set. None should materialize the
// edits pending before it.
var pendingEditsReadingStatements = []string{
	"select * from t",
	"select * from t where x = 1",
	"select * from t where y = 1",
	"select count(*) from t",
	"select * from t join o on t.x = o.id",
	"select * from t where x in (select id from o)",
	"insert into t values (3, 3)",
	"update t set y = 2 where x = 1",
	"delete from t where x = 2",
	"insert into o values (5) on duplicate key update id = 5",
	"show tables",
	"describe t",
	"show indexes from t",
}

// TestReadsLeaveEditsPending asserts that statements that only read or write rows leave the edits of earlier
// statements pending.
func TestReadsLeaveEditsPending(t *testing.T) {
	for _, q := range pendingEditsReadingStatements {
		t.Run(q, func(t *testing.T) {
			h := newDoltHarness(t)
			defer h.Close()
			e := mustNewEngine(t, h)
			defer e.Close()
			ctx := enginetest.NewContext(h)

			hasPendingEdits := func() bool {
				state, ok, err := dsess.DSessFromSess(ctx.Session).LookupDbState(ctx, "mydb")
				require.NoError(t, err)
				require.True(t, ok)
				return state.WriteSession().HasPendingEdits()
			}

			for _, setup := range []string{
				"create table t (x int primary key, y int, index (y))",
				"create table o (id int primary key)",
				"insert into o values (1)",
				"call dolt_commit('-Am', 'init')",
				"set autocommit = 0",
				"insert into t values (1, 1), (2, 2)",
			} {
				enginetest.RunQueryWithContext(t, e, h, ctx, setup)
			}
			require.True(t, hasPendingEdits())
			enginetest.RunQueryWithContext(t, e, h, ctx, q)
			require.True(t, hasPendingEdits())
		})
	}
}
