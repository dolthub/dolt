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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

func TestDoltTransactionCommitOneClient(t *testing.T) {
	// In this test, we're setting only one client to match transaction commits to dolt commits.
	// Autocommit is disabled for the enabled client, as it's the recommended way to use this feature.
	harness := newDoltHarness(t)
	enginetest.TestTransactionScript(t, harness, enginetest.TransactionTest{
		Name: "dolt commit after transaction commit one client",
		SetUpScript: []string{
			"CREATE TABLE x (y BIGINT PRIMARY KEY, z BIGINT);",
			"INSERT INTO x VALUES (1,1);",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "/* client a */ SET @@autocommit=0;",
				Expected: []sql.Row{{}},
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
				Query:    "/* client a */ START TRANSACTION;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ START TRANSACTION;",
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
		},
	})

	db := harness.databases[0].GetDoltDB()
	cs, err := doltdb.NewCommitSpec("HEAD")
	require.NoError(t, err)
	headRefs, err := db.GetHeadRefs(context.Background())
	require.NoError(t, err)
	commit, err := db.Resolve(context.Background(), cs, headRefs[0])
	require.NoError(t, err)
	cm, err := commit.GetCommitMeta()
	require.NoError(t, err)
	require.Contains(t, cm.Description, "Transaction commit at")

	as, err := doltdb.NewAncestorSpec("~1")
	require.NoError(t, err)
	initialCommit, err := commit.GetAncestor(context.Background(), as)
	require.NoError(t, err)
	icm, err := initialCommit.GetCommitMeta()
	require.NoError(t, err)
	require.Equal(t, "Initialize data repository", icm.Description)
}

func TestDoltTransactionCommitTwoClients(t *testing.T) {
	// In this test, we're setting both clients to match transaction commits to dolt commits.
	// Autocommit is disabled, as it's the recommended way to use this feature.
	harness := newDoltHarness(t)
	enginetest.TestTransactionScript(t, harness, enginetest.TransactionTest{
		Name: "dolt commit after transaction commit two clients",
		SetUpScript: []string{
			"CREATE TABLE x (y BIGINT PRIMARY KEY, z BIGINT);",
			"INSERT INTO x VALUES (1,1);",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "/* client a */ SET @@autocommit=0;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client b */ SET @@autocommit=0;",
				Expected: []sql.Row{{}},
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
				Query:    "/* client a */ START TRANSACTION;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ START TRANSACTION;",
				Expected: []sql.Row{},
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
		},
	})
	db := harness.databases[0].GetDoltDB()
	cs, err := doltdb.NewCommitSpec("HEAD")
	require.NoError(t, err)
	headRefs, err := db.GetHeadRefs(context.Background())
	require.NoError(t, err)
	commit2, err := db.Resolve(context.Background(), cs, headRefs[0])
	require.NoError(t, err)
	cm2, err := commit2.GetCommitMeta()
	require.NoError(t, err)
	require.Contains(t, cm2.Description, "Transaction commit at")

	as, err := doltdb.NewAncestorSpec("~1")
	require.NoError(t, err)
	commit1, err := commit2.GetAncestor(context.Background(), as)
	require.NoError(t, err)
	cm1, err := commit1.GetCommitMeta()
	require.NoError(t, err)
	require.Contains(t, cm1.Description, "Transaction commit at")

	commit0, err := commit1.GetAncestor(context.Background(), as)
	require.NoError(t, err)
	cm0, err := commit0.GetCommitMeta()
	require.NoError(t, err)
	require.Equal(t, "Initialize data repository", cm0.Description)
}

func TestDoltTransactionCommitAutocommit(t *testing.T) {
	// In this test, each insertion from both clients cause a commit as autocommit is enabled.
	// Not the recommended way to use the feature, but it's permitted.
	harness := newDoltHarness(t)
	enginetest.TestTransactionScript(t, harness, enginetest.TransactionTest{
		Name: "dolt commit after transaction commit autocommit",
		SetUpScript: []string{
			"CREATE TABLE x (y BIGINT PRIMARY KEY, z BIGINT);",
			"INSERT INTO x VALUES (1,1);",
		},
		Assertions: []enginetest.ScriptTestAssertion{
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
		},
	})
	db := harness.databases[0].GetDoltDB()
	cs, err := doltdb.NewCommitSpec("HEAD")
	require.NoError(t, err)
	headRefs, err := db.GetHeadRefs(context.Background())
	require.NoError(t, err)
	commit2, err := db.Resolve(context.Background(), cs, headRefs[0])
	require.NoError(t, err)
	cm2, err := commit2.GetCommitMeta()
	require.NoError(t, err)
	require.Contains(t, cm2.Description, "Transaction commit at")

	as, err := doltdb.NewAncestorSpec("~1")
	require.NoError(t, err)
	commit1, err := commit2.GetAncestor(context.Background(), as)
	require.NoError(t, err)
	cm1, err := commit1.GetCommitMeta()
	require.NoError(t, err)
	require.Contains(t, cm1.Description, "Transaction commit at")

	commit0, err := commit1.GetAncestor(context.Background(), as)
	require.NoError(t, err)
	cm0, err := commit0.GetCommitMeta()
	require.NoError(t, err)
	require.Equal(t, "Initialize data repository", cm0.Description)
}
