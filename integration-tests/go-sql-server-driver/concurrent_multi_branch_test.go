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

package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestConcurrentMultiBranchTransactions(t *testing.T) {
	runConcurrentMultiBranch(t, "working_sets")
}

func TestConcurrentMultiBranchTransactionCommits(t *testing.T) {
	runConcurrentMultiBranch(t, "transaction_commit")
}

func TestConcurrentMultiBranchCommitAll(t *testing.T) {
	runConcurrentMultiBranch(t, "commit_all")
}

type concurrentBranchTransaction struct {
	message    string
	statements []string
}

type concurrentBranchStats struct {
	committed  int
	restarted  int
	retries    int
	maxRetries int
}

// Every statement owns a distinct key, including across transactions. Updates and
// deletes start from committed fixture rows; inserts start from absent rows. Thus
// no later statement can overwrite or hide a lost statement's effect.
func runConcurrentMultiBranch(t *testing.T, mode string) {
	const writers = 20
	const batches = 50
	const branches = 3
	seed := int64(11928)
	if value := os.Getenv("DOLT_CONCURRENCY_SEED"); value != "" {
		var err error
		seed, err = strconv.ParseInt(value, 10, 64)
		require.NoError(t, err)
	}
	t.Logf("mode=%s seed=%d writers=%d transactions_per_writer=%d", mode, seed, writers, batches)
	rng := rand.New(rand.NewSource(seed))
	plans := make([][]concurrentBranchTransaction, writers)
	expected := make([]map[int]int, branches)
	fixtures := make([][]string, branches)
	messages := make([]map[string]bool, branches)
	for b := range expected {
		expected[b] = make(map[int]int)
		messages[b] = make(map[string]bool)
	}

	for w := range plans {
		for batch := 0; batch < batches; batch++ {
			tx := concurrentBranchTransaction{message: fmt.Sprintf("writer-%d-batch-%d", w, batch)}
			size := 1 + rng.Intn(10)
			touched := rng.Perm(branches)[:1+rng.Intn(min(branches, size))]
			for i := 0; i < size; i++ {
				// Visit every selected branch at least once, then mix their order.
				b := touched[i%len(touched)]
				key := w*batches*10 + batch*10 + i
				value := key + 1
				table := fmt.Sprintf("`concurrency/b%d`.items", b)
				var query string
				switch rng.Intn(3) {
				case 0:
					query = fmt.Sprintf("INSERT INTO %s VALUES (%d, %d)", table, key, value)
					expected[b][key] = value
				case 1:
					fixtures[b] = append(fixtures[b], fmt.Sprintf("(%d, 0)", key))
					query = fmt.Sprintf("UPDATE %s SET v = %d WHERE id = %d", table, value, key)
					expected[b][key] = value
				case 2:
					fixtures[b] = append(fixtures[b], fmt.Sprintf("(%d, 0)", key))
					query = fmt.Sprintf("DELETE FROM %s WHERE id = %d", table, key)
				}
				tx.statements = append(tx.statements, query)
				messages[b][tx.message] = true
			}

			rng.Shuffle(len(tx.statements), func(i, j int) {
				tx.statements[i], tx.statements[j] = tx.statements[j], tx.statements[i]
			})
			plans[w] = append(plans[w], tx)
		}
	}

	var ports DynamicResources
	ports.global = &GlobalPorts
	ports.t = t
	user, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() { user.Cleanup() })
	store, err := user.MakeRepoStore()
	require.NoError(t, err)
	_, err = store.MakeRepo("concurrency")
	require.NoError(t, err)
	server := MakeServer(t, store, &driver.Server{
		Args:          []string{"-P", `{{get_port "server_port"}}`},
		DynamicPort:   "server_port",
		LogNotMatches: []string{"caught panic"},
	}, &ports)
	pass, err := (driver.Connection{User: "root"}).Password()
	require.NoError(t, err)
	db, err := driver.ConnectDB("root", pass, "concurrency", "127.0.0.1", server.Port, nil)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(writers + 1)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	setup, err := db.Conn(ctx)
	require.NoError(t, err)
	exec := func(query string) {
		_, err := setup.ExecContext(ctx, query)
		require.NoError(t, err, query)
	}
	exec("CREATE TABLE items (id INT PRIMARY KEY, v INT NOT NULL)")
	exec("CALL dolt_commit('-Am', 'schema')")
	baseline := make([]map[string]bool, branches)
	for b := 0; b < branches; b++ {
		exec(fmt.Sprintf("CALL dolt_checkout('-b', 'b%d')", b))
		// Branch from the common schema, never another branch's fixture.
		if len(fixtures[b]) != 0 {
			exec("INSERT INTO items VALUES " + strings.Join(fixtures[b], ","))
		}
		exec("CALL dolt_commit('-am', 'fixture')")
		baseline[b] = readConcurrentBranchLog(t, ctx, setup, b)
		exec("CALL dolt_checkout('main')")
	}

	require.NoError(t, setup.Close())

	// Reserve and configure all sessions before releasing any writer.
	connections := make([]*sql.Conn, writers)
	for w := range connections {
		connections[w], err = db.Conn(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { connections[w].Close() })
		if mode == "transaction_commit" {
			_, err = connections[w].ExecContext(ctx, "SET dolt_transaction_commit = 1, dolt_multi_branch_commit = 1")
			require.NoError(t, err)
		}
	}
	stats := make([]concurrentBranchStats, writers)
	failures := make([]error, writers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for w := range connections {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			failures[w] = executeConcurrentBranchPlan(ctx, connections[w], mode, plans[w], &stats[w])
			if failures[w] != nil {
				cancel()
			}
		}()
	}

	close(start)
	wg.Wait()
	total := concurrentBranchStats{}
	for w, s := range stats {
		t.Logf("writer=%d committed=%d restarted=%d retries=%d max_retries=%d error=%v", w, s.committed, s.restarted, s.retries, s.maxRetries, failures[w])
		total.committed += s.committed
		total.restarted += s.restarted
		total.retries += s.retries
		total.maxRetries = max(total.maxRetries, s.maxRetries)
	}

	t.Logf("total: committed=%d restarted=%d retries=%d max_retries=%d", total.committed, total.restarted, total.retries, total.maxRetries)
	for _, err := range failures {
		require.NoError(t, err)
	}

	require.Equal(t, writers*batches, total.committed)
	for _, conn := range connections {
		require.NoError(t, conn.Close())
	}

	// A fresh single reader checks complete row sets, including absence of deleted
	// rows, and checks committed HEADs independently of working-set visibility.
	reader, err := db.Conn(ctx)
	require.NoError(t, err)
	defer reader.Close()
	for b := 0; b < branches; b++ {
		query := fmt.Sprintf("SELECT id, v FROM `concurrency/b%d`.items", b)
		require.Equal(t, expected[b], readConcurrentBranchRows(t, ctx, reader, query), "branch b%d working set", b)
		log := readConcurrentBranchLog(t, ctx, reader, b)
		if mode == "working_sets" {
			require.Equal(t, baseline[b], log, "plain transactions must not create commits")
		} else {
			require.Len(t, log, len(baseline[b])+len(messages[b]), "branch b%d commit count", b)
			for message := range baseline[b] {
				messages[b][message] = true
			}

			require.Equal(t, messages[b], log, "branch b%d reachable commit history", b)
			require.Equal(t, expected[b], readConcurrentBranchRows(t, ctx, reader, query+" AS OF 'HEAD'"), "branch b%d HEAD", b)
		}
	}
}

func executeConcurrentBranchPlan(ctx context.Context, conn *sql.Conn, mode string, plan []concurrentBranchTransaction, stats *concurrentBranchStats) error {
	for _, tx := range plan {
		for attempt := 0; ; attempt++ {
			err := executeConcurrentBranchTransaction(ctx, conn, mode, tx)
			if err == nil {
				stats.committed++
				stats.maxRetries = max(stats.maxRetries, attempt)
				break
			}

			// Only retry errors that unambiguously failed the transaction. Connection
			// failures have an unknown commit outcome and must fail the test.
			var mysqlErr *mysql.MySQLError
			retry := errors.As(err, &mysqlErr) && (mysqlErr.Number == 1213 || mysqlErr.Number == 1205)
			if _, rollbackErr := conn.ExecContext(ctx, "ROLLBACK"); rollbackErr != nil {
				return fmt.Errorf("%s: %w (rollback: %v)", tx.message, err, rollbackErr)
			}

			if !retry || attempt >= 99 {
				return fmt.Errorf("%s attempt %d: %w", tx.message, attempt+1, err)
			}

			if attempt == 0 {
				stats.restarted++
			}

			stats.retries++
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(1+attempt%10) * time.Millisecond):
			}
		}
	}

	return nil
}

func executeConcurrentBranchTransaction(ctx context.Context, conn *sql.Conn, mode string, tx concurrentBranchTransaction) error {
	if mode == "transaction_commit" {
		if _, err := conn.ExecContext(ctx, "SET dolt_transaction_commit_message = ?", tx.message); err != nil {
			return err
		}
	}

	if _, err := conn.ExecContext(ctx, "START TRANSACTION"); err != nil {
		return err
	}

	for _, stmt := range tx.statements {
		result, err := conn.ExecContext(ctx, stmt)
		if err != nil {
			return err
		}

		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}

		if affected != 1 {
			return fmt.Errorf("%s affected %d rows, expected 1", stmt, affected)
		}
	}

	if mode == "commit_all" {
		_, err := conn.ExecContext(ctx, "CALL dolt_commit_all('-am', ?)", tx.message)
		return err
	}

	_, err := conn.ExecContext(ctx, "COMMIT")
	return err
}

func readConcurrentBranchRows(t *testing.T, ctx context.Context, conn *sql.Conn, query string) map[int]int {
	t.Helper()
	rows, err := conn.QueryContext(ctx, query)
	require.NoError(t, err)
	defer rows.Close()
	result := make(map[int]int)
	for rows.Next() {
		var key, value int
		require.NoError(t, rows.Scan(&key, &value))
		result[key] = value
	}

	require.NoError(t, rows.Err())
	return result
}

func readConcurrentBranchLog(t *testing.T, ctx context.Context, conn *sql.Conn, branch int) map[string]bool {
	t.Helper()
	rows, err := conn.QueryContext(ctx, fmt.Sprintf("SELECT message FROM `concurrency/b%d`.dolt_log", branch))
	require.NoError(t, err)
	defer rows.Close()
	result := make(map[string]bool)
	for rows.Next() {
		var message string
		require.NoError(t, rows.Scan(&message))
		require.False(t, result[message], "duplicate commit message on branch b%d: %s", branch, message)
		result[message] = true
	}

	require.NoError(t, rows.Err())
	return result
}
