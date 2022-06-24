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
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

// TODO: we need tests for manual DOLT_COMMIT as well, but that's difficult with the way that functions are resolved
//  in the engine.
func TestDoltTransactionCommitOneClient(t *testing.T) {
	// In this test, we're setting only one client to match transaction commits to dolt commits.
	// Autocommit is disabled for the enabled client, as it's the recommended way to use this feature.
	harness := newDoltHarness(t)
	harness.Setup(setup.MydbData)
	enginetest.TestTransactionScript(t, harness, queries.TransactionTest{
		Name: "dolt commit on transaction commit one client",
		SetUpScript: []string{
			"CREATE TABLE x (y BIGINT PRIMARY KEY, z BIGINT);",
			"INSERT INTO x VALUES (1,1);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ SET @@autocommit=0;",
				Expected: []sql.Row{{}},
			},
			// start transaction implicitly commits the current transaction, so we have to do so before we turn on dolt commits
			{
				Query:    "/* client a */ START TRANSACTION;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ START TRANSACTION;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ SET @@dolt_transaction_commit=1;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client a */ SET @initial_head=@@mydb_head;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client b */ SET @initial_head=@@mydb_head;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client a */ SELECT @@mydb_head like @initial_head;",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "/* client b */ SELECT @@mydb_head like @initial_head;",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "/* client a */ INSERT INTO x VALUES (2,2);",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ INSERT INTO x VALUES (3,3);",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ SELECT * FROM x ORDER BY y;",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:    "/* client b */ SELECT * FROM x ORDER BY y;",
				Expected: []sql.Row{{1, 1}, {3, 3}},
			},
			{
				Query:    "/* client a */ SELECT @@mydb_head like @initial_head;",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "/* client b */ SELECT @@mydb_head like @initial_head;",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "/* client b */ COMMIT;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ SELECT @@mydb_head like @initial_head;",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "/* client b */ SELECT @@mydb_head like @initial_head;",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "/* client a */ COMMIT;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ SELECT @@mydb_head like @initial_head;",
				Expected: []sql.Row{{false}},
			},
			{
				Query:    "/* client b */ SELECT @@mydb_head like @initial_head;",
				Expected: []sql.Row{{false}},
			},
			{
				Query:    "/* client a */ SELECT * FROM x ORDER BY y;",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
			{
				Query:    "/* client b */ SELECT * FROM x ORDER BY y;",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
			{
				Query:    "/* client a */ SELECT @@mydb_head like @initial_head;",
				Expected: []sql.Row{{false}},
			},
			{
				Query:    "/* client b */ SELECT @@mydb_head like @initial_head;",
				Expected: []sql.Row{{false}},
			},
			{
				Query:    "/* client c */ SELECT * FROM x ORDER BY y;",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
		},
	})
	_, err := harness.NewEngine(t)

	ctx := enginetest.NewContext(harness)
	db, ok := ctx.Session.(*dsess.DoltSession).GetDoltDB(ctx, "mydb")
	if !ok {
		t.Fatal("'mydb' database not found")
	}
	cs, err := doltdb.NewCommitSpec("HEAD")
	require.NoError(t, err)
	headRefs, err := db.GetHeadRefs(context.Background())
	require.NoError(t, err)
	commit, err := db.Resolve(context.Background(), cs, headRefs[0])
	require.NoError(t, err)
	cm, err := commit.GetCommitMeta(context.Background())
	require.NoError(t, err)
	require.Contains(t, cm.Description, "Transaction commit")

	as, err := doltdb.NewAncestorSpec("~1")
	require.NoError(t, err)
	initialCommit, err := commit.GetAncestor(context.Background(), as)
	require.NoError(t, err)
	icm, err := initialCommit.GetCommitMeta(context.Background())
	require.NoError(t, err)
	require.Equal(t, "checkpoint enginetest database mydb", icm.Description)
}

func TestDoltTransactionCommitTwoClients(t *testing.T) {
	// In this test, we're setting both clients to match transaction commits to dolt commits.
	// Autocommit is disabled, as it's the recommended way to use this feature.
	harness := newDoltHarness(t)
	enginetest.TestTransactionScript(t, harness, queries.TransactionTest{
		Name: "dolt commit on transaction commit two clients",
		SetUpScript: []string{
			"CREATE TABLE x (y BIGINT PRIMARY KEY, z BIGINT);",
			"INSERT INTO x VALUES (1,1);",
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
			// start transaction implicitly commits the current transaction, so we have to do so before we turn on dolt commits
			{
				Query:    "/* client a */ START TRANSACTION;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ START TRANSACTION;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ SET @@dolt_transaction_commit=1;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client b */ SET @@dolt_transaction_commit=1;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client a */ SET @initial_head=@@mydb_head;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client b */ SET @initial_head=@@mydb_head;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client a */ INSERT INTO x VALUES (2,2);",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ INSERT INTO x VALUES (3,3);",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ SELECT * FROM x ORDER BY y;",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:    "/* client b */ SELECT * FROM x ORDER BY y;",
				Expected: []sql.Row{{1, 1}, {3, 3}},
			},
			{
				Query:    "/* client a */ SELECT @@mydb_head like @initial_head;",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "/* client b */ SELECT @@mydb_head like @initial_head;",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "/* client b */ COMMIT;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ SELECT @@mydb_head like @initial_head;",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "/* client a */ COMMIT;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ SELECT @@mydb_head like @initial_head;",
				Expected: []sql.Row{{false}},
			},
			{
				Query:    "/* client b */ SELECT @@mydb_head like @initial_head;",
				Expected: []sql.Row{{false}},
			},
			{
				Query:    "/* client a */ SELECT * FROM x ORDER BY y;",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
			{
				Query:    "/* client b */ SELECT * FROM x ORDER BY y;",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
			{
				Query:    "/* client c */ SELECT * FROM x ORDER BY y;",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
		},
	})
	_, err := harness.NewEngine(t)

	ctx := enginetest.NewContext(harness)
	db, ok := ctx.Session.(*dsess.DoltSession).GetDoltDB(ctx, "mydb")
	if !ok {
		t.Fatal("'mydb' database not found")
	}
	cs, err := doltdb.NewCommitSpec("HEAD")
	require.NoError(t, err)
	headRefs, err := db.GetHeadRefs(context.Background())
	require.NoError(t, err)
	commit2, err := db.Resolve(context.Background(), cs, headRefs[0])
	require.NoError(t, err)
	cm2, err := commit2.GetCommitMeta(context.Background())
	require.NoError(t, err)
	require.Contains(t, cm2.Description, "Transaction commit")

	as, err := doltdb.NewAncestorSpec("~1")
	require.NoError(t, err)
	commit1, err := commit2.GetAncestor(context.Background(), as)
	require.NoError(t, err)
	cm1, err := commit1.GetCommitMeta(context.Background())
	require.NoError(t, err)
	require.Contains(t, cm1.Description, "Transaction commit")

	commit0, err := commit1.GetAncestor(context.Background(), as)
	require.NoError(t, err)
	cm0, err := commit0.GetCommitMeta(context.Background())
	require.NoError(t, err)
	require.Equal(t, "checkpoint enginetest database mydb", cm0.Description)
}

func TestDoltTransactionCommitAutocommit(t *testing.T) {
	// In this test, each insertion from both clients cause a commit as autocommit is enabled.
	// Not the recommended way to use the feature, but it's permitted.
	harness := newDoltHarness(t)
	enginetest.TestTransactionScript(t, harness, queries.TransactionTest{
		Name: "dolt commit with autocommit",
		SetUpScript: []string{
			"CREATE TABLE x (y BIGINT PRIMARY KEY, z BIGINT);",
			"INSERT INTO x VALUES (1,1);",
		},
		Assertions: []queries.ScriptTestAssertion{
			// these SET statements currently commit a transaction (since autocommit is on)
			{
				Query:    "/* client a */ SET @@dolt_transaction_commit=1;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client b */ SET @@dolt_transaction_commit=1;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client a */ INSERT INTO x VALUES (2,2);",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ INSERT INTO x VALUES (3,3);",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ SELECT * FROM x ORDER BY y;",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
			{
				Query:    "/* client b */ SELECT * FROM x ORDER BY y;",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
			{
				Query:    "/* client c */ SELECT * FROM x ORDER BY y;",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
		},
	})
	_, err := harness.NewEngine(t)

	ctx := enginetest.NewContext(harness)
	db, ok := ctx.Session.(*dsess.DoltSession).GetDoltDB(ctx, "mydb")
	if !ok {
		t.Fatal("'mydb' database not found")
	}
	cs, err := doltdb.NewCommitSpec("HEAD")
	require.NoError(t, err)
	headRefs, err := db.GetHeadRefs(context.Background())
	require.NoError(t, err)
	commit3, err := db.Resolve(context.Background(), cs, headRefs[0])
	require.NoError(t, err)
	cm3, err := commit3.GetCommitMeta(context.Background())
	require.NoError(t, err)
	require.Contains(t, cm3.Description, "Transaction commit")

	as, err := doltdb.NewAncestorSpec("~1")
	require.NoError(t, err)
	commit2, err := commit3.GetAncestor(context.Background(), as)
	require.NoError(t, err)
	cm2, err := commit2.GetCommitMeta(context.Background())
	require.NoError(t, err)
	require.Contains(t, cm2.Description, "Transaction commit")

	commit1, err := commit2.GetAncestor(context.Background(), as)
	require.NoError(t, err)
	cm1, err := commit1.GetCommitMeta(context.Background())
	require.NoError(t, err)
	require.Equal(t, "Transaction commit", cm1.Description)

	commit0, err := commit1.GetAncestor(context.Background(), as)
	require.NoError(t, err)
	cm0, err := commit0.GetCommitMeta(context.Background())
	require.NoError(t, err)
	require.Equal(t, "checkpoint enginetest database mydb", cm0.Description)
}

func TestDoltTransactionCommitLateFkResolution(t *testing.T) {
	harness := newDoltHarness(t)
	enginetest.TestTransactionScript(t, harness, queries.TransactionTest{
		Name: "delayed foreign key resolution with transaction commits",
		SetUpScript: []string{
			"SET foreign_key_checks=0;",
			"CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_late FOREIGN KEY (v1) REFERENCES parent (pk));",
			"SET foreign_key_checks=1;",
			"CREATE TABLE parent (pk BIGINT PRIMARY KEY);",
			"INSERT INTO parent VALUES (1), (2);",
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
				Query:    "/* client a */ INSERT INTO child VALUES (1, 1);",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ INSERT INTO child VALUES (2, 2);",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ COMMIT;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ COMMIT;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ SELECT * FROM child ORDER BY pk;",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:    "/* client b */ SELECT * FROM child ORDER BY pk;",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{ // This uses the index, which is automatically created by the late fk resolution, so it's also tested here
				Query:    "/* client a */ SELECT * FROM child WHERE v1 > 0 ORDER BY pk;",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{ // This uses the index, which is automatically created by the late fk resolution, so it's also tested here
				Query:    "/* client b */ SELECT * FROM child WHERE v1 > 0 ORDER BY pk;",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:       "/* client a */ INSERT INTO child VALUES (3, 3);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query:       "/* client b */ INSERT INTO child VALUES (3, 3);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
		},
	})
}

var internalMergeScripts = []queries.TransactionTest{
	{
		Name: "Internal merges that produce a constraint violation with dolt_force_transaction_commit = off should be rolled back.",
		SetUpScript: []string{
			"CREATE TABLE t (pk int PRIMARY KEY, col1 int UNIQUE);",
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
		Name: "Internal merges that produce a conflict with dolt_allow_commit_conflicts = off should be rolled back.",
		SetUpScript: []string{
			"CREATE TABLE t (pk int PRIMARY KEY, col1 int);",
			"CALL DOLT_COMMIT('-am', 'create table');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"INSERT INTO t values (1, 100);",
			"CALL DOLT_COMMIT('-am', 'right edit');",

			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO t VALUES (1, 200);",
			"CALL DOLT_COMMIT('-am', 'left edit');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "/* client a */ CALL DOLT_MERGE('right');",
				ExpectedErrStr: "Merge conflict detected, transaction rolled back. Merge conflicts must be resolved using the dolt_conflicts tables before committing a transaction. To commit transactions with merge conflicts, set @@dolt_allow_commit_conflicts = 1",
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
}

// Tests behavior of internal merges ( merges that occur inside a transaction ).
func TestInternalMerges(t *testing.T) {
	harness := newDoltHarness(t)
	for _, script := range internalMergeScripts {
		enginetest.TestTransactionScript(t, harness, script)
	}
}

var transactionMergeScripts = []queries.TransactionTest{
	{
		Name: "conflicts in a transaction merge should cause a rollback",
		SetUpScript: []string{
			"CREATE TABLE t (pk int PRIMARY KEY, col1 int);",
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
				Query:    "/* client a */ INSERT INTO t VALUES (1, 100);",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ INSERT into t VALUES (1, 200);",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ COMMIT;",
				Expected: []sql.Row{},
			},
			{
				Query:          "/* client b */ COMMIT;",
				ExpectedErrStr: "this transaction conflicts with a committed transaction from another client, please retry",
			},
			// TODO: uncommenting this query causes a weird reset script error @max??
			// failed query 'call dolt_commit('--allow-empty', '-am', 'checkpoint enginetest database mydb')': dataset head is not ancestor of commit
			//{
			//	Query:          "/* client b */ INSERT into t VALUES (1, 200);",
			//	ExpectedErrStr: "duplicate primary key given: [1]",
			//},
		},
	},
	{
		// TODO: make the error helpful! See comment in dsess/transactions.go
		Name: "conflicts from an internal merge cause a rollback in a three-way transaction merge with an initially unhelpful error.",
		SetUpScript: []string{
			"CREATE TABLE t (pk int PRIMARY KEY, col1 int);",
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
				ExpectedErrStr: "this transaction conflicts with a committed transaction from another client, please retry",
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
		Name: "constraint violations in a transaction merge should cause a rollback",
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
				ExpectedErrStr: "Committing this transaction resulted in a working set with constraint violations, transaction rolled back." +
					"This constraint violation may be the result of a previous merge or the result of transaction sequencing." +
					"Constraint violations from a merge can be resolved using the dolt_constraint_violations table before committing the transaction." +
					"To allow transactions to be committed with constraint violations from a merge or transaction sequencing set @@dolt_force_transaction_commit=1.",
			},
			{
				Query:          "/* client b */ INSERT INTO child VALUES (1, 1);",
				ExpectedErrStr: "cannot add or update a child row - Foreign key violation on fk: `nk01br56`, table: `child`, referenced table: `parent`, key: `[1]`",
			},
		},
	},
}

// Tests the behavior of transactions merges ( merges that resolve changes
// between transactions ) and their rollback behavior.
func TestTransactionMerges(t *testing.T) {
	harness := newDoltHarness(t)
	for _, script := range transactionMergeScripts {
		enginetest.TestTransactionScript(t, harness, script)
	}
}
