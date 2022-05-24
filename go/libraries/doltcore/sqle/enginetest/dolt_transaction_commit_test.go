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
	"github.com/dolthub/dolt/go/store/types"
)

// TODO: we need tests for manual DOLT_COMMIT as well, but that's difficult with the way that functions are resolved
//  in the engine.
func TestDoltTransactionCommitOneClient(t *testing.T) {
	if types.IsFormat_DOLT_1(types.Format_Default) {
		t.Skip()
	}

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
	if types.IsFormat_DOLT_1(types.Format_Default) {
		t.Skip()
	}

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
	if types.IsFormat_DOLT_1(types.Format_Default) {
		t.Skip()
	}

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
	if types.IsFormat_DOLT_1(types.Format_Default) {
		t.Skip()
	}

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
