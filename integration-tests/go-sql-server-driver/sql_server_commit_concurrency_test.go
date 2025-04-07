// Copyright 2024 Dolthub, Inc.
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

package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

func TestCommitConcurrency(t *testing.T) {
	t.Parallel()
	t.Run("SQL transaction with amend commit", testSQLTransactionWithAmendCommit)
	t.Run("SQL racing amend", testSQLRacingAmend)
}

// testSQLTransactionWithAmendCommit verifies that two transactions started at the same state will not both be able
// to commit using --amend. The first transaction will be able to commit, but the second should get an error.
func testSQLTransactionWithAmendCommit(t *testing.T) {
	ctx := context.Background()
	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() {
		u.Cleanup()
	})

	rs, err := u.MakeRepoStore()
	require.NoError(t, err)
	repo, err := rs.MakeRepo("commit_concurrency_test")
	require.NoError(t, err)

	srvSettings := &driver.Server{
		Args:        []string{"--port", `{{get_port "server"}}`},
		DynamicPort: "server",
	}
	var ports DynamicPorts
	ports.global = &GlobalPorts
	ports.t = t
	server := MakeServer(t, repo, srvSettings, &ports)
	server.DBName = "commit_concurrency_test"

	// Connect to the database
	db, err := server.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
	})

	_, err = db.ExecContext(ctx, `
CREATE TABLE test_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  value VARCHAR(20)
);`)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, "INSERT INTO test_table (value) VALUES ('initial')")
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, "CALL DOLT_COMMIT('-A','-m', 'initial commit')")
	require.NoError(t, err)

	// Create a new context for the first (failing) transaction
	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	tx1, err := db.BeginTx(ctx1, nil)
	require.NoError(t, err)
	_, err = tx1.ExecContext(ctx1, "UPDATE test_table SET value = 'amended by tx1' WHERE id = 1")
	require.NoError(t, err)

	// Create a new context for the second (succeeding) transaction
	ctx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()
	tx2, err := db.BeginTx(ctx2, nil)
	require.NoError(t, err)

	// Update data within the second transaction
	_, err = tx2.ExecContext(ctx2, "UPDATE test_table SET value = 'amended by tx2' WHERE id = 1")
	require.NoError(t, err)

	_, err = tx2.ExecContext(ctx2, "CALL DOLT_COMMIT('--amend', '-m', 'tx2 amended commit')")
	require.NoError(t, err)

	// Commit --amend will result in tx2 being committed. You can still make updates on tx1, but any commit should fail
	_, err = tx1.ExecContext(ctx1, "INSERT INTO test_table (value) VALUES ('new row by tx1')")
	require.NoError(t, err)

	_, err = tx1.ExecContext(ctx1, "CALL DOLT_COMMIT('--amend', '-m', 'should fail')")
	require.Error(t, err)
	require.Contains(t, err.Error(), "this transaction conflicts with a committed transaction from another client, try restarting transaction")

	// Verify that the data in the head is what we would expect
	row := db.QueryRowContext(ctx, "SELECT value FROM test_table WHERE id = 1")
	var value string
	err = row.Scan(&value)
	require.NoError(t, err)
	require.Equal(t, "amended by tx2", value)

	// Verify the commit message
	row = db.QueryRowContext(ctx, "SELECT message FROM dolt_log ORDER BY date DESC LIMIT 1")
	var commitMessage string
	err = row.Scan(&commitMessage)
	require.NoError(t, err)
	require.Equal(t, "tx2 amended commit", commitMessage)

}

func testSQLRacingAmend(t *testing.T) {
	ctx := context.Background()
	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() {
		u.Cleanup()
	})

	rs, err := u.MakeRepoStore()
	require.NoError(t, err)
	repo, err := rs.MakeRepo("racing_amend_test")
	require.NoError(t, err)

	srvSettings := &driver.Server{
		Args:        []string{"--port", `{{get_port "server"}}`},
		DynamicPort: "server",
	}
	var ports DynamicPorts
	ports.global = &GlobalPorts
	ports.t = t
	server := MakeServer(t, repo, srvSettings, &ports)
	server.DBName = "racing_amend_test"

	db, err := server.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
	})

	_, err = db.ExecContext(ctx, `
	CREATE TABLE test_table (
	  id INT AUTO_INCREMENT PRIMARY KEY,
	  value VARCHAR(20)
	);`)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, "INSERT INTO test_table VALUES (1, 'initial')")
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, "CALL DOLT_COMMIT('-A','-m', 'initial commit')")
	require.NoError(t, err)

	type txIdFunc struct {
		txNum  int
		txFunc func() error
	}

	var transactions []txIdFunc
	for txNum := 1; txNum <= 200; txNum++ {
		txCtx, cancel := context.WithTimeout(ctx, 15*time.Second) // Should never hit, but just in case
		tx, err := db.BeginTx(txCtx, nil)
		require.NoError(t, err)
		f := func() error {
			defer cancel()
			// update required to get a transaction conflict.
			_, e2 := tx.ExecContext(txCtx, "UPDATE test_table SET value = ? WHERE id = 1", fmt.Sprintf("tx%d value", txNum))
			require.NoError(t, e2)
			_, e2 = tx.ExecContext(txCtx, "INSERT INTO test_table (value) VALUES (?)", fmt.Sprintf("tx%d new row", txNum))
			require.NoError(t, e2)

			// We want other transactions to have the chance to mess with the db. We'll verify that what's committed is what we expect.
			time.Sleep(time.Duration(rand.Intn(1000)+500) * time.Millisecond)

			// This will commit the transaction, or error.
			_, e2 = tx.ExecContext(txCtx, "CALL DOLT_COMMIT('--amend','-a', '-m', ?)", fmt.Sprintf("tx%d amend", txNum))
			if e2 != nil {
				tx.Rollback()
			}
			return e2
		}
		transactions = append(transactions, txIdFunc{txNum: txNum, txFunc: f})
	}

	rand.Shuffle(len(transactions), func(i, j int) {
		transactions[i], transactions[j] = transactions[j], transactions[i]
	})

	var atomicInt atomic.Int32
	atomicInt.Store(-1)

	var wg sync.WaitGroup
	for _, txn := range transactions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// radomly sleep .5 - 1.5 seconds
			time.Sleep(time.Duration(rand.Intn(1000)+500) * time.Millisecond)
			err := txn.txFunc()

			if err == nil {
				// If there are multiple updates, something went wrong.
				require.True(t, atomicInt.CompareAndSwap(-1, int32(txn.txNum)))
			}
			// Errors are expected.
		}()
	}
	wg.Wait()

	winner := atomicInt.Load()
	require.NotEqual(t, -1, winner)

	// Verify there are only 2 rows in the table
	rows, err := db.QueryContext(ctx, "SELECT COUNT(*) FROM test_table")
	require.NoError(t, err)
	defer rows.Close()
	rows.Next()
	var count int
	err = rows.Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 2, count)

	// Verify the final state
	row := db.QueryRowContext(ctx, "SELECT value FROM test_table WHERE id = 1")
	var value string
	err = row.Scan(&value)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("tx%d value", winner), value)

	row = db.QueryRowContext(ctx, "SELECT value FROM test_table WHERE id != 1")
	err = row.Scan(&value)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("tx%d new row", winner), value)

	// Verify the commit message
	row = db.QueryRowContext(ctx, "SELECT message FROM dolt_log ORDER BY date DESC LIMIT 1")
	var commitMessage string
	err = row.Scan(&commitMessage)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("tx%d amend", winner), commitMessage)
}
