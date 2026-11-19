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
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/rowjournal"
)

// rowLockStep is one step of a rowLockScript: a statement run by one of the script's clients.
type rowLockStep struct {
	// client names the session that runs the statement. Sessions are created the first time they are named.
	client string
	// query is the statement to run. It is empty for a step that only awaits a statement that blocked earlier.
	query string
	// blocks says that the statement must block on a row lock. The script goes on to the next step while the
	// statement waits, and a later step for the same client with |unblocks| set checks how it turns out.
	blocks bool
	// unblocks says that the statement the client is blocked on must now finish. The step's |rows| and |err| are
	// checked against that statement's outcome.
	unblocks bool
	// rows is the expected result, with every value formatted as by fmt.Sprint. Nil means the result isn't checked.
	rows [][]any
	// err is a substring of the expected error. Empty means the statement must succeed.
	err string
	// locks, if not nil, is the row locks expected to be held once the step is done, by all transactions together.
	// Each is written as the table's name, a slash and the lock's mode, and they are compared in sorted order.
	locks []string
}

// rowLockScript is a test of the interaction of several sessions, in which statements can block on one another.
type rowLockScript struct {
	name  string
	setup []string
	steps []rowLockStep
}

// queryResult is how a statement turned out.
type queryResult struct {
	rows []sql.Row
	err  error
}

// rowLockRunner runs rowLockScripts against one engine.
type rowLockRunner struct {
	t       *testing.T
	harness *DoltHarness
	engine  enginetest.QueryEngine
	journal *rowjournal.Journal
	clients map[string]*rowJournalClient
	// pending holds, for each client blocked on a statement, the channel that statement's result arrives on.
	pending map[string]chan queryResult
}

func newRowLockRunner(t *testing.T) *rowLockRunner {
	harness := newDoltHarness(t)
	t.Cleanup(harness.Close)
	engine, err := harness.NewEngine(t)
	require.NoError(t, err)
	t.Cleanup(func() { engine.Close() })

	journal := harness.provider.RowJournal()
	require.NotNil(t, journal)
	journal.SetDeadlockTimeout(200 * time.Millisecond)

	return &rowLockRunner{
		t:       t,
		harness: harness,
		engine:  engine,
		journal: journal,
		clients: make(map[string]*rowJournalClient),
		pending: make(map[string]chan queryResult),
	}
}

func (r *rowLockRunner) client(name string) *rowJournalClient {
	c, ok := r.clients[name]
	if !ok {
		c = newRowJournalClient(r.t, r.harness, r.engine)
		r.clients[name] = c
	}

	return c
}

// query runs a statement for a client and returns how it turned out.
func (c *rowJournalClient) query(query string) queryResult {
	ctx := c.ctx.WithQuery(query)
	_, iter, _, err := c.engine.Query(ctx, query)
	if err != nil {
		return queryResult{err: err}
	}

	rows, err := sql.RowIterToRows(ctx, iter)
	return queryResult{rows: rows, err: err}
}

// waiting returns the number of lock requests waiting in the journal.
func (r *rowLockRunner) waiting() int {
	n := 0
	for _, lock := range r.journal.Locks() {
		if !lock.Granted {
			n++
		}
	}

	return n
}

// heldLocks returns the row locks held in the journal, each as its table's name, a slash and its mode, sorted.
func (r *rowLockRunner) heldLocks() []string {
	held := []string{}
	for _, lock := range r.journal.Locks() {
		if lock.Granted && !lock.Target.WholeTable {
			held = append(held, lock.Relation.Table.Name+"/"+lock.Mode.String())
		}
	}

	sort.Strings(held)
	return held
}

func (r *rowLockRunner) run(script rowLockScript) {
	r.t.Run(script.name, func(t *testing.T) {
		// Leave nothing behind for the next script, however this one ends.
		defer func() {
			for client, done := range r.pending {
				delete(r.pending, client)
				go func() { <-done }()
			}

			for _, client := range r.clients {
				client.query("rollback")
			}

			assert.Empty(t, r.journal.Locks(), "locks left behind")
			assert.Empty(t, r.journal.ActiveTxns(), "transactions left behind")
		}()

		setup := r.client("setup")
		for _, query := range script.setup {
			res := setup.query(query)
			require.NoError(t, res.err, "setup: %s: %v", query, res.err)
		}

		for i, step := range script.steps {
			label := fmt.Sprintf("step %d: /* client %s */ %s", i, step.client, step.query)
			switch {
			case step.unblocks:
				done, ok := r.pending[step.client]
				require.True(t, ok, "%s: client has no blocked statement", label)
				delete(r.pending, step.client)

				select {
				case res := <-done:
					checkStepResult(t, label, step, res)
				case <-time.After(10 * time.Second):
					t.Fatalf("%s: blocked statement did not finish", label)
				}
			case step.blocks:
				_, ok := r.pending[step.client]
				require.False(t, ok, "%s: client is already blocked", label)

				before := r.waiting()
				done := make(chan queryResult, 1)
				client := r.client(step.client)
				go func() {
					done <- client.query(step.query)
				}()

				require.Eventually(t, func() bool {
					return r.waiting() > before || len(done) > 0
				}, 10*time.Second, time.Millisecond, "%s: statement neither blocked nor finished", label)

				select {
				case res := <-done:
					t.Fatalf("%s: statement finished instead of blocking: rows %v, error %v", label, res.rows, res.err)
				default:
				}

				r.pending[step.client] = done
			default:
				_, ok := r.pending[step.client]
				require.False(t, ok, "%s: client is blocked on an earlier statement", label)
				checkStepResult(t, label, step, r.client(step.client).query(step.query))
			}

			if step.locks != nil {
				require.Equal(t, step.locks, r.heldLocks(), "%s: row locks held", label)
			}
		}

		require.Empty(t, r.pending, "script ended with blocked statements")
	})
}

func checkStepResult(t *testing.T, label string, step rowLockStep, res queryResult) {
	t.Helper()
	if step.err != "" {
		require.Error(t, res.err, label)
		require.Contains(t, res.err.Error(), step.err, label)
		return
	}

	require.NoError(t, res.err, label)
	if step.rows == nil {
		return
	}

	require.Equal(t, formatRows(step.rows), formatSqlRows(res.rows), label)
}

func formatRows(rows [][]any) []string {
	formatted := make([]string, len(rows))
	for i, row := range rows {
		cells := make([]string, len(row))
		for j, cell := range row {
			cells[j] = fmt.Sprint(cell)
		}

		formatted[i] = strings.Join(cells, ",")
	}

	return formatted
}

func formatSqlRows(rows []sql.Row) []string {
	generic := make([][]any, len(rows))
	for i, row := range rows {
		generic[i] = row
	}

	return formatRows(generic)
}

const (
	concurrentUpdateErr = "could not serialize access due to concurrent update"
	concurrentDeleteErr = "could not serialize access due to concurrent delete"
	concurrentInsertErr = "could not serialize access due to concurrent insert of the same key"
)

var rowLockWriteSetup = []string{
	"drop table if exists t",
	"create table t (pk int primary key, c1 int, c2 int)",
	"insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3)",
}

var rowLockWriteScripts = []rowLockScript{
	{
		name:  "update waits for an update of the same row, and fails when it commits",
		setup: rowLockWriteSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "update t set c1 = 10 where pk = 1"},
			{client: "b", query: "update t set c1 = 20 where pk = 1", blocks: true},
			{client: "a", query: "commit"},
			{client: "b", unblocks: true, err: concurrentUpdateErr},
			// The failure cost b its transaction, so it now sees what a committed and can try again.
			{client: "b", query: "select * from t where pk = 1", rows: [][]any{{1, 10, 1}}},
			{client: "b", query: "update t set c1 = c1 + 20 where pk = 1"},
			{client: "b", query: "select * from t where pk = 1", rows: [][]any{{1, 30, 1}}},
		},
	},
	{
		name:  "update waits for an update of the same row, and proceeds when it rolls back",
		setup: rowLockWriteSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "update t set c1 = 10 where pk = 1"},
			{client: "b", query: "update t set c1 = 20 where pk = 1", blocks: true},
			{client: "a", query: "rollback"},
			{client: "b", unblocks: true},
			{client: "b", query: "commit"},
			{client: "a", query: "select * from t where pk = 1", rows: [][]any{{1, 20, 1}}},
		},
	},
	{
		name:  "updates of different rows don't wait for each other",
		setup: rowLockWriteSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "update t set c1 = 10 where pk = 1"},
			{client: "b", query: "update t set c1 = 20 where pk = 2"},
			{client: "b", query: "delete from t where pk = 3"},
			{client: "b", query: "insert into t values (4, 4, 4)"},
			{client: "a", query: "insert into t values (5, 5, 5)"},
			{client: "a", query: "commit"},
			{client: "b", query: "commit"},
			{client: "a", query: "select * from t order by pk", rows: [][]any{{1, 10, 1}, {2, 20, 2}, {4, 4, 4}, {5, 5, 5}}},
		},
	},
	{
		name:  "updates of different columns of the same row conflict",
		setup: rowLockWriteSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "update t set c1 = 10 where pk = 1"},
			{client: "b", query: "update t set c2 = 20 where pk = 1", blocks: true},
			{client: "a", query: "commit"},
			{client: "b", unblocks: true, err: concurrentUpdateErr},
			{client: "b", query: "select * from t where pk = 1", rows: [][]any{{1, 10, 1}}},
		},
	},
	{
		name:  "update fails without waiting when the row changed since the transaction began",
		setup: rowLockWriteSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "a", query: "select * from t where pk = 1", rows: [][]any{{1, 1, 1}}},
			{client: "b", query: "update t set c1 = 20 where pk = 1"},
			{client: "b", query: "delete from t where pk = 2"},
			{client: "a", query: "select * from t where pk = 1", rows: [][]any{{1, 1, 1}}},
			{client: "a", query: "update t set c1 = 10 where pk = 1", err: concurrentUpdateErr},
			{client: "a", query: "begin"},
			{client: "b", query: "update t set c1 = 30 where pk = 3"},
			{client: "a", query: "delete from t where pk = 3", err: concurrentUpdateErr},
			{client: "a", query: "select * from t order by pk", rows: [][]any{{1, 20, 1}, {3, 30, 3}}},
		},
	},
	{
		name:  "update and delete wait for a delete, and fail when it commits",
		setup: rowLockWriteSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "c", query: "begin"},
			{client: "a", query: "delete from t where pk = 1"},
			{client: "b", query: "update t set c1 = 20 where pk = 1", blocks: true},
			{client: "c", query: "delete from t where pk = 1", blocks: true},
			{client: "a", query: "commit"},
			{client: "b", unblocks: true, err: concurrentDeleteErr},
			{client: "c", unblocks: true, err: concurrentDeleteErr},
			{client: "b", query: "select * from t order by pk", rows: [][]any{{2, 2, 2}, {3, 3, 3}}},
		},
	},
	{
		name:  "insert waits for an insert of the same key, and fails when it commits",
		setup: rowLockWriteSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "insert into t values (10, 1, 1)"},
			{client: "b", query: "insert into t values (11, 2, 2)"},
			{client: "b", query: "insert into t values (10, 2, 2)", blocks: true},
			{client: "a", query: "commit"},
			{client: "b", unblocks: true, err: concurrentInsertErr},
			{client: "b", query: "insert into t values (10, 2, 2)", err: "duplicate primary key"},
			{client: "b", query: "select * from t where pk >= 10", rows: [][]any{{10, 1, 1}}},
		},
	},
	{
		name:  "insert waits for an insert of the same key, and proceeds when it rolls back",
		setup: rowLockWriteSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "insert into t values (10, 1, 1)"},
			{client: "b", query: "insert into t values (10, 2, 2)", blocks: true},
			{client: "a", query: "rollback"},
			{client: "b", unblocks: true},
			{client: "b", query: "commit"},
			{client: "a", query: "select * from t where pk >= 10", rows: [][]any{{10, 2, 2}}},
		},
	},
	{
		name:  "changing a primary key locks the old key and the new one",
		setup: rowLockWriteSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "c", query: "begin"},
			{client: "a", query: "update t set pk = 10 where pk = 1"},
			{client: "b", query: "update t set c1 = 20 where pk = 1", blocks: true},
			{client: "c", query: "insert into t values (10, 3, 3)", blocks: true},
			{client: "a", query: "commit"},
			{client: "b", unblocks: true, err: concurrentUpdateErr},
			{client: "c", unblocks: true, err: concurrentInsertErr},
			{client: "a", query: "select * from t order by pk", rows: [][]any{{2, 2, 2}, {3, 3, 3}, {10, 1, 1}}},
		},
	},
	{
		name:  "rolling back to a savepoint releases the locks taken since",
		setup: rowLockWriteSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "update t set c1 = 10 where pk = 1"},
			{client: "a", query: "savepoint s"},
			{client: "a", query: "update t set c1 = 10 where pk = 2"},
			{client: "b", query: "update t set c1 = 20 where pk = 2", blocks: true},
			{client: "a", query: "rollback to savepoint s"},
			{client: "b", unblocks: true},
			{client: "b", query: "update t set c1 = 20 where pk = 1", blocks: true},
			{client: "a", query: "commit"},
			{client: "b", unblocks: true, err: concurrentUpdateErr},
			{client: "a", query: "select * from t order by pk", rows: [][]any{{1, 10, 1}, {2, 2, 2}, {3, 3, 3}}},
		},
	},
	{
		name:  "the transaction that closes a cycle of waits is refused",
		setup: rowLockWriteSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "update t set c1 = 10 where pk = 1"},
			{client: "b", query: "update t set c1 = 20 where pk = 2"},
			{client: "a", query: "update t set c1 = 10 where pk = 2", blocks: true},
			{client: "b", query: "update t set c1 = 20 where pk = 1", err: "deadlock detected"},
			// The deadlock cost b its transaction and its locks, which lets a go on.
			{client: "a", unblocks: true},
			{client: "b", query: "select * from t order by pk", rows: [][]any{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}}},
			{client: "a", query: "commit"},
			{client: "b", query: "select * from t order by pk", rows: [][]any{{1, 10, 1}, {2, 10, 2}, {3, 3, 3}}},
		},
	},
	{
		name:  "a statement waits for a row lock no longer than the lock wait timeout",
		setup: rowLockWriteSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "set @@innodb_lock_wait_timeout = 1"},
			{client: "b", query: "begin"},
			{client: "b", query: "update t set c1 = 20 where pk = 2"},
			{client: "a", query: "update t set c1 = 10 where pk = 1"},
			{client: "b", query: "update t set c1 = 20 where pk = 1", err: "lock wait timeout exceeded"},
			// Only the statement failed. The transaction goes on, with the changes and the locks it already had.
			{client: "b", query: "select * from t where pk = 2", rows: [][]any{{2, 20, 2}}},
			{client: "a", query: "commit"},
			{client: "b", query: "commit"},
			{client: "a", query: "select * from t order by pk", rows: [][]any{{1, 10, 1}, {2, 20, 2}, {3, 3, 3}}},
		},
	},
	{
		name:  "rows of the same table on another branch are locked separately",
		setup: append(append([]string{}, rowLockWriteSetup...), "call dolt_commit('--allow-empty', '-Am', 'setup')", "call dolt_branch('-f', 'other')"),
		steps: []rowLockStep{
			{client: "b", query: "call dolt_checkout('other')"},
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "update t set c1 = 10 where pk = 1"},
			{client: "b", query: "update t set c1 = 20 where pk = 1"},
			{client: "a", query: "commit"},
			{client: "b", query: "commit"},
			{client: "a", query: "select * from t where pk = 1", rows: [][]any{{1, 10, 1}}},
			{client: "b", query: "select * from t where pk = 1", rows: [][]any{{1, 20, 1}}},
			{client: "a", query: "select * from `mydb/other`.t where pk = 1", rows: [][]any{{1, 20, 1}}},
			// A statement that names the other branch's table locks that branch's row.
			{client: "b", query: "begin"},
			{client: "b", query: "update t set c1 = 21 where pk = 1"},
			{client: "a", query: "update `mydb/other`.t set c1 = 11 where pk = 1", blocks: true},
			{client: "b", query: "rollback"},
			{client: "a", unblocks: true},
			{client: "b", query: "select * from t where pk = 1", rows: [][]any{{1, 11, 1}}},
			{client: "b", query: "call dolt_checkout('main')"},
		},
	},
}

var rowLockKeylessSetup = []string{
	"drop table if exists k",
	"create table k (c1 int, c2 int)",
	"insert into k values (1, 1), (1, 1), (2, 2), (3, 3)",
}

// rowLockKeylessWriteScripts cover tables without a primary key, whose rows are identified by their contents. A lock
// on such a row covers every row with the same contents.
var rowLockKeylessWriteScripts = []rowLockScript{
	{
		name:  "keyless: update waits for an update of the same row, and fails when it commits",
		setup: rowLockKeylessSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "update k set c2 = 10 where c1 = 2"},
			{client: "b", query: "update k set c2 = 20 where c1 = 2", blocks: true},
			{client: "a", query: "commit"},
			{client: "b", unblocks: true, err: concurrentUpdateErr},
			{client: "b", query: "update k set c2 = c2 + 20 where c1 = 2"},
			{client: "a", query: "select * from k order by c1, c2", rows: [][]any{{1, 1}, {1, 1}, {2, 30}, {3, 3}}},
		},
	},
	{
		name:  "keyless: update waits for an update of the same row, and proceeds when it rolls back",
		setup: rowLockKeylessSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "update k set c2 = 10 where c1 = 2"},
			{client: "b", query: "update k set c2 = 20 where c1 = 2", blocks: true},
			{client: "a", query: "rollback"},
			{client: "b", unblocks: true},
			{client: "b", query: "commit"},
			{client: "a", query: "select * from k order by c1, c2", rows: [][]any{{1, 1}, {1, 1}, {2, 20}, {3, 3}}},
		},
	},
	{
		name:  "keyless: changes to rows with different contents don't wait for each other",
		setup: rowLockKeylessSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "update k set c2 = 10 where c1 = 2"},
			{client: "b", query: "update k set c2 = 20 where c1 = 3"},
			{client: "a", query: "insert into k values (4, 4)"},
			{client: "b", query: "insert into k values (5, 5)"},
			{client: "b", query: "delete from k where c1 = 1"},
			{client: "a", query: "commit"},
			{client: "b", query: "commit"},
			{client: "a", query: "select * from k order by c1, c2", rows: [][]any{{2, 10}, {3, 20}, {4, 4}, {5, 5}}},
		},
	},
	{
		name:  "keyless: identical rows are locked together",
		setup: rowLockKeylessSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			// Both rows (1, 1) are one entry in storage, so a change to one locks the other.
			{client: "a", query: "update k set c2 = 10 where c1 = 1 limit 1"},
			{client: "b", query: "delete from k where c1 = 1 limit 1", blocks: true},
			{client: "a", query: "commit"},
			{client: "b", unblocks: true, err: concurrentUpdateErr},
			{client: "b", query: "delete from k where c1 = 1 and c2 = 1"},
			{client: "a", query: "select * from k order by c1, c2", rows: [][]any{{1, 10}, {2, 2}, {3, 3}}},
		},
	},
	{
		name:  "keyless: delete waits for a delete, and fails when it commits",
		setup: rowLockKeylessSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "delete from k where c1 = 2"},
			{client: "b", query: "delete from k where c1 = 2", blocks: true},
			{client: "a", query: "commit"},
			{client: "b", unblocks: true, err: concurrentDeleteErr},
			{client: "b", query: "select * from k order by c1, c2", rows: [][]any{{1, 1}, {1, 1}, {3, 3}}},
		},
	},
	{
		name:  "keyless: inserts of identical rows are serialized, and none is lost",
		setup: rowLockKeylessSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "insert into k values (5, 5)"},
			{client: "b", query: "insert into k values (5, 5)", blocks: true},
			{client: "a", query: "commit"},
			{client: "b", unblocks: true, err: concurrentUpdateErr},
			{client: "b", query: "insert into k values (5, 5)"},
			{client: "a", query: "select * from k where c1 = 5", rows: [][]any{{5, 5}, {5, 5}}},
		},
	},
	{
		name:  "keyless: an update that makes a row identical to one being changed waits for it",
		setup: rowLockKeylessSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "delete from k where c1 = 3"},
			{client: "b", query: "update k set c1 = 3, c2 = 3 where c1 = 2", blocks: true},
			{client: "a", query: "rollback"},
			{client: "b", unblocks: true},
			{client: "b", query: "commit"},
			{client: "a", query: "select * from k order by c1, c2", rows: [][]any{{1, 1}, {1, 1}, {3, 3}, {3, 3}}},
		},
	},
}

// rowLockDisabledScripts cover the behavior with dolt_row_level_locking turned off, which is what Dolt did before it
// had row locks: nothing waits, and changes are reconciled when a transaction commits.
var rowLockDisabledScripts = []rowLockScript{
	{
		name:  "with row locking off, statements don't wait and changes are merged at commit",
		setup: append(append([]string{}, rowLockWriteSetup...), "set @@global.dolt_row_level_locking = 0"),
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "update t set c1 = 10 where pk = 1"},
			{client: "b", query: "update t set c2 = 20 where pk = 1"},
			{client: "a", query: "update t set c1 = 10 where pk = 2"},
			{client: "b", query: "update t set c1 = 20 where pk = 2"},
			{client: "a", query: "commit"},
			{client: "b", query: "commit", err: "this transaction conflicts with a committed transaction from another client"},
			{client: "b", query: "update t set c2 = 20 where pk = 1"},
			{client: "a", query: "select * from t order by pk", rows: [][]any{{1, 10, 20}, {2, 10, 2}, {3, 3, 3}}},
			// The setting takes effect for transactions that start after it changes.
			{client: "a", query: "begin"},
			{client: "setup", query: "set @@global.dolt_row_level_locking = 1"},
			{client: "b", query: "begin"},
			{client: "c", query: "begin"},
			{client: "b", query: "update t set c1 = 21 where pk = 1"},
			{client: "a", query: "update t set c1 = 11 where pk = 1"},
			{client: "c", query: "update t set c1 = 31 where pk = 1", blocks: true},
			{client: "b", query: "rollback"},
			{client: "c", unblocks: true},
		},
	},
}

const lockNotAvailableErr = "could not obtain lock on row in relation"

var rowLockReadSetup = []string{
	"drop table if exists t",
	"drop table if exists u",
	"create table t (pk int primary key, c1 int, c2 int, key (c1))",
	"insert into t values (1, 10, 100), (2, 20, 200), (3, 30, 300)",
	"create table u (pk int primary key, t_pk int, key (t_pk))",
	"insert into u values (11, 1), (12, 2), (13, 3)",
}

// rowLockReadScripts cover SELECT statements with a locking clause.
var rowLockReadScripts = []rowLockScript{
	{
		name:  "for update makes a write of the row wait, and lets it go on when the reader commits",
		setup: rowLockReadSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "select * from t where pk = 1 for update", rows: [][]any{{1, 10, 100}}, locks: []string{"t/Update"}},
			{client: "b", query: "update t set c2 = 0 where pk = 2"},
			{client: "b", query: "update t set c2 = 0 where pk = 1", blocks: true},
			{client: "a", query: "commit"},
			// The reader only locked the row, so nothing b saw of it is out of date.
			{client: "b", unblocks: true},
			{client: "b", query: "commit"},
			{client: "a", query: "select pk, c2 from t order by pk", rows: [][]any{{1, 0}, {2, 0}, {3, 300}}, locks: []string{}},
		},
	},
	{
		name:  "for update waits for a write of the row, and fails when it commits",
		setup: rowLockReadSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "update t set c2 = 0 where pk = 1"},
			{client: "b", query: "select * from t where pk = 1 for update", blocks: true},
			{client: "a", query: "commit"},
			{client: "b", unblocks: true, err: concurrentUpdateErr},
			{client: "b", query: "select * from t where pk = 1 for update", rows: [][]any{{1, 10, 0}}},
		},
	},
	{
		name:  "for update waits for a write of the row, and proceeds when it rolls back",
		setup: rowLockReadSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "delete from t where pk = 1"},
			{client: "b", query: "select * from t where pk = 1 for update", blocks: true},
			{client: "a", query: "rollback"},
			{client: "b", unblocks: true, rows: [][]any{{1, 10, 100}}},
			{client: "a", query: "delete from t where pk = 1", blocks: true},
			{client: "b", query: "commit"},
			{client: "a", unblocks: true},
		},
	},
	{
		name:  "for update fails without waiting when the row changed since the transaction began",
		setup: rowLockReadSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "a", query: "select * from t where pk = 1", rows: [][]any{{1, 10, 100}}},
			{client: "b", query: "update t set c2 = 0 where pk = 1"},
			{client: "b", query: "delete from t where pk = 2"},
			{client: "a", query: "select * from t where pk = 3 for update", rows: [][]any{{3, 30, 300}}},
			{client: "a", query: "select * from t where pk = 1 for update", err: concurrentUpdateErr},
			{client: "a", query: "begin"},
			{client: "b", query: "delete from t where pk = 3"},
			{client: "a", query: "select * from t where pk = 3 for update", err: concurrentDeleteErr},
		},
	},
	{
		name:  "share locks are compatible with each other and conflict with writes and for update",
		setup: rowLockReadSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "c", query: "begin"},
			{client: "a", query: "select * from t where pk = 1 lock in share mode", rows: [][]any{{1, 10, 100}}},
			{client: "b", query: "select * from t where pk = 1 lock in share mode", rows: [][]any{{1, 10, 100}}, locks: []string{"t/Share", "t/Share"}},
			{client: "c", query: "select * from t where pk = 1 for update", blocks: true},
			{client: "a", query: "commit"},
			{client: "b", query: "commit"},
			{client: "c", unblocks: true, rows: [][]any{{1, 10, 100}}, locks: []string{"t/Update"}},
			{client: "a", query: "select * from t where pk = 1 lock in share mode", blocks: true},
			{client: "c", query: "commit"},
			{client: "a", unblocks: true, rows: [][]any{{1, 10, 100}}},
		},
	},
	{
		name:  "nowait fails and skip locked leaves out a row that is locked",
		setup: rowLockReadSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "select * from t where pk = 1 for update", rows: [][]any{{1, 10, 100}}},
			{client: "b", query: "select * from t where pk = 1 for update nowait", err: lockNotAvailableErr},
			// Only the statement failed.
			{client: "b", query: "select * from t where pk = 1 for update skip locked", rows: [][]any{}},
			{client: "b", query: "select pk from t order by pk limit 1 for update skip locked", rows: [][]any{{2}}},
			{client: "b", query: "select pk from t order by pk for update skip locked", rows: [][]any{{2}, {3}}},
			{client: "a", query: "select pk from t order by pk for update skip locked", rows: [][]any{{1}}},
			{client: "a", query: "select pk from t where pk = 3 for update nowait", err: lockNotAvailableErr},
			{client: "b", query: "commit"},
			{client: "a", query: "select pk from t order by pk for update nowait", rows: [][]any{{1}, {2}, {3}}},
		},
	},
	{
		name:  "nowait and skip locked don't wait for a writer either",
		setup: rowLockReadSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "update t set c2 = 0 where pk = 2"},
			{client: "b", query: "select pk from t where pk = 2 for update nowait", err: lockNotAvailableErr},
			{client: "b", query: "select pk from t order by pk for update skip locked", rows: [][]any{{1}, {3}}},
		},
	},
	{
		name:  "an indexed lookup locks only the rows it returns",
		setup: rowLockReadSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "a", query: "select * from t where pk = 1 for update", rows: [][]any{{1, 10, 100}}, locks: []string{"t/Update"}},
			{client: "a", query: "rollback", locks: []string{}},
			// A lookup on a secondary index.
			{client: "a", query: "begin"},
			{client: "a", query: "select * from t where c1 = 20 for update", rows: [][]any{{2, 20, 200}}, locks: []string{"t/Update"}},
			{client: "b", query: "update t set c2 = 0 where pk in (1, 3)"},
			{client: "b", query: "select pk from t where pk = 2 for update nowait", err: lockNotAvailableErr},
			{client: "a", query: "rollback", locks: []string{}},
			// A lookup that the secondary index covers.
			{client: "a", query: "begin"},
			{client: "a", query: "select c1 from t where c1 = 20 for update", rows: [][]any{{20}}, locks: []string{"t/Update"}},
			{client: "b", query: "select pk from t where pk = 2 for update nowait", err: lockNotAvailableErr},
			{client: "a", query: "rollback", locks: []string{}},
			// A range of a secondary index.
			{client: "a", query: "begin"},
			{client: "a", query: "select pk from t where c1 >= 20 order by pk for update", rows: [][]any{{2}, {3}}, locks: []string{"t/Update", "t/Update"}},
			{client: "a", query: "rollback", locks: []string{}},
			// A lookup that finds nothing locks nothing.
			{client: "a", query: "begin"},
			{client: "a", query: "select pk from t where pk = 99 for update", rows: [][]any{}, locks: []string{}},
		},
	},
	{
		name:  "a scan locks every row it reads, including those a filter rejects",
		setup: rowLockReadSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "a", query: "select pk from t where c2 = 200 for update", rows: [][]any{{2}}, locks: []string{"t/Update", "t/Update", "t/Update"}},
		},
	},
	{
		name:  "only the projected columns are returned",
		setup: rowLockReadSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "a", query: "select c2 from t where pk = 2 for update", rows: [][]any{{200}}, locks: []string{"t/Update"}},
			{client: "a", query: "select c2, pk from t where c1 = 30 for update", rows: [][]any{{300, 3}}, locks: []string{"t/Update", "t/Update"}},
			{client: "a", query: "select c2 + pk from t where pk = 1 for update", rows: [][]any{{101}}, locks: []string{"t/Update", "t/Update", "t/Update"}},
		},
	},
	{
		name:  "a join locks the rows of every table, or of the tables the clause names",
		setup: rowLockReadSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{
				client: "a",
				query:  "select t.pk, u.pk from t join u on u.t_pk = t.pk where t.pk = 2 and u.pk = 12 for update",
				rows:   [][]any{{2, 12}},
				locks:  []string{"t/Update", "u/Update"},
			},
			{client: "b", query: "update u set t_pk = 3 where pk = 12", blocks: true},
			{client: "a", query: "rollback"},
			{client: "b", unblocks: true, locks: []string{}},
			{client: "a", query: "begin"},
			{
				client: "a",
				query:  "select t.pk, u.pk from t join u on u.t_pk = t.pk where t.pk = 1 and u.pk = 11 for update of u",
				rows:   [][]any{{1, 11}},
				locks:  []string{"u/Update"},
			},
			{client: "b", query: "update t set c2 = 0 where pk = 1"},
			{client: "a", query: "rollback"},
			{client: "a", query: "begin"},
			{
				client: "a",
				query:  "select x.pk, y.pk from t x join u y on y.t_pk = x.pk where x.pk = 3 and y.pk = 13 for update of x",
				rows:   [][]any{{3, 13}},
				locks:  []string{"t/Update"},
			},
			{client: "a", query: "select * from t for update of u", err: "table"},
		},
	},
	{
		name:  "a locking read outside a transaction holds its locks only while it runs",
		setup: rowLockReadSetup,
		steps: []rowLockStep{
			{client: "a", query: "select * from t where pk = 1 for update", rows: [][]any{{1, 10, 100}}, locks: []string{}},
			{client: "b", query: "begin"},
			{client: "b", query: "update t set c2 = 0 where pk = 1"},
			{client: "a", query: "select * from t where pk = 1 for update", blocks: true},
			{client: "b", query: "rollback"},
			{client: "a", unblocks: true, rows: [][]any{{1, 10, 100}}, locks: []string{}},
		},
	},
	{
		name:  "two locking reads that wait for each other are a deadlock",
		setup: rowLockReadSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "select * from t where pk = 1 for update"},
			{client: "b", query: "select * from t where pk = 2 for update"},
			{client: "a", query: "select * from t where pk = 2 for update", blocks: true},
			{client: "b", query: "select * from t where pk = 1 for update", err: "deadlock detected"},
			{client: "a", unblocks: true, rows: [][]any{{2, 20, 200}}},
		},
	},
	{
		name:  "upgrading a share lock while another holds one is a deadlock for the second to try",
		setup: rowLockReadSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "select * from t where pk = 1 lock in share mode"},
			{client: "b", query: "select * from t where pk = 1 lock in share mode"},
			{client: "a", query: "update t set c2 = 0 where pk = 1", blocks: true},
			{client: "b", query: "update t set c2 = 1 where pk = 1", err: "deadlock detected"},
			{client: "a", unblocks: true},
			{client: "a", query: "commit"},
			{client: "b", query: "select c2 from t where pk = 1", rows: [][]any{{0}}},
		},
	},
	{
		name:  "rows read as of a commit, or on a branch with no working set, are not locked",
		setup: append(append([]string{}, rowLockReadSetup...), "call dolt_commit('--allow-empty', '-Am', 'read setup')", "call dolt_tag('v1')"),
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "a", query: "select * from t as of 'HEAD' where pk = 1 for update", rows: [][]any{{1, 10, 100}}, locks: []string{}},
			{client: "a", query: "select * from `mydb/v1`.t where pk = 1 for update", rows: [][]any{{1, 10, 100}}, locks: []string{}},
		},
	},
	{
		name:  "rows of another branch's table are locked on that branch",
		setup: append(append([]string{}, rowLockReadSetup...), "call dolt_commit('--allow-empty', '-Am', 'read setup')", "call dolt_branch('-f', 'other')"),
		steps: []rowLockStep{
			{client: "b", query: "call dolt_checkout('other')"},
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "select * from `mydb/other`.t where pk = 1 for update", rows: [][]any{{1, 10, 100}}, locks: []string{"t/Update"}},
			{client: "b", query: "select * from `mydb/main`.t where pk = 1 for update", rows: [][]any{{1, 10, 100}}},
			{client: "b", query: "update t set c2 = 0 where pk = 1", blocks: true},
			{client: "a", query: "rollback"},
			{client: "b", unblocks: true},
			{client: "b", query: "rollback"},
			{client: "b", query: "call dolt_checkout('main')"},
		},
	},
	{
		name:  "with row locking off, a locking clause does nothing",
		setup: append(append([]string{}, rowLockReadSetup...), "set @@global.dolt_row_level_locking = 0"),
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "select * from t where pk = 1 for update", rows: [][]any{{1, 10, 100}}, locks: []string{}},
			{client: "b", query: "select c2 from t where pk = 1 for update nowait", rows: [][]any{{100}}},
			{client: "b", query: "update t set c2 = 0 where pk = 1"},
			{client: "setup", query: "set @@global.dolt_row_level_locking = 1"},
		},
	},
}

var rowLockKeylessReadSetup = []string{
	"drop table if exists k",
	"create table k (c1 int, c2 int, key (c1))",
	"insert into k values (1, 1), (1, 1), (2, 2), (3, 3)",
}

// rowLockKeylessReadScripts cover locking reads of tables without a primary key.
var rowLockKeylessReadScripts = []rowLockScript{
	{
		name:  "keyless: for update makes a write of the row wait",
		setup: rowLockKeylessReadSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "select * from k where c1 = 2 for update", rows: [][]any{{2, 2}}, locks: []string{"k/Update"}},
			{client: "b", query: "update k set c2 = 30 where c1 = 3"},
			{client: "b", query: "update k set c2 = 20 where c1 = 2", blocks: true},
			{client: "a", query: "update k set c2 = 10 where c1 = 2"},
			{client: "a", query: "commit"},
			{client: "b", unblocks: true, err: concurrentUpdateErr},
			{client: "a", query: "select * from k order by c1, c2", rows: [][]any{{1, 1}, {1, 1}, {2, 10}, {3, 3}}},
		},
	},
	{
		name:  "keyless: for update waits for a write of the row",
		setup: rowLockKeylessReadSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "delete from k where c1 = 2"},
			{client: "b", query: "select * from k where c1 = 2 for update", blocks: true},
			{client: "a", query: "rollback"},
			{client: "b", unblocks: true, rows: [][]any{{2, 2}}},
		},
	},
	{
		name:  "keyless: identical rows are returned individually and locked together",
		setup: rowLockKeylessReadSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			// One lock stands for both rows, taken once for each row returned.
			{client: "a", query: "select * from k where c1 = 1 for update", rows: [][]any{{1, 1}, {1, 1}}, locks: []string{"k/Update"}},
			{client: "b", query: "select * from k where c1 = 1 for update nowait", err: lockNotAvailableErr},
			{client: "b", query: "select * from k order by c1 for update skip locked", rows: [][]any{{2, 2}, {3, 3}}},
			// A row identical to the locked ones can't be added while they are locked.
			{client: "b", query: "insert into k values (1, 1)", blocks: true},
			{client: "a", query: "commit"},
			{client: "b", unblocks: true},
			{client: "b", query: "commit"},
			{client: "a", query: "select count(*) from k where c1 = 1", rows: [][]any{{3}}},
		},
	},
	{
		name:  "keyless: a scan locks every row, and share locks are compatible",
		setup: rowLockKeylessReadSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "select * from k where c2 = 3 lock in share mode", rows: [][]any{{3, 3}}, locks: []string{"k/Share", "k/Share", "k/Share"}},
			{client: "b", query: "select * from k where c2 = 2 lock in share mode", rows: [][]any{{2, 2}}},
			{client: "b", query: "delete from k where c1 = 3", blocks: true},
			{client: "a", query: "commit"},
			{client: "b", unblocks: true},
		},
	},
}

var rowLockTableSetup = []string{
	"drop table if exists t",
	"create table t (pk int primary key, c1 int, c2 int)",
	"insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3)",
	"call dolt_commit('--allow-empty', '-Am', 'table setup')",
	"call dolt_branch('-f', 'other')",
}

// rowLockTableScripts cover the statements that change a table without going through its rows one at a time: DDL,
// and version control operations that replace the working set. They lock the table as a whole.
var rowLockTableScripts = []rowLockScript{
	{
		name:  "altering a table waits for the transactions that hold locks on its rows",
		setup: rowLockTableSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "a", query: "update t set c1 = 10 where pk = 1"},
			{client: "b", query: "alter table t add column c3 int", blocks: true},
			{client: "a", query: "commit"},
			{client: "b", unblocks: true},
			{client: "a", query: "select * from t order by pk", rows: [][]any{{1, 10, 1, nil}, {2, 2, 2, nil}, {3, 3, 3, nil}}},
		},
	},
	{
		name:  "dropping a table waits for a locking read of its rows",
		setup: rowLockTableSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			// Even the weakest lock on a row keeps the table from being changed as a whole.
			{client: "a", query: "select pk from t where pk = 1 lock in share mode", rows: [][]any{{1}}},
			{client: "b", query: "drop table t", blocks: true},
			// A transaction that has locks in the table isn't made to wait behind the statement that waits for it.
			{client: "a", query: "select pk from t where pk = 2 for update", rows: [][]any{{2}}},
			// One that has none is, so that a stream of them can't hold the statement up forever.
			{client: "c", query: "select pk from t where pk = 3 for update", blocks: true},
			{client: "a", query: "rollback"},
			{client: "b", unblocks: true},
			{client: "c", unblocks: true, err: concurrentUpdateErr},
			{client: "a", query: "show tables like 't'", rows: [][]any{}},
		},
	},
	{
		name:  "a transaction that began before a table was altered can't lock or change its rows",
		setup: rowLockTableSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "a", query: "select count(*) from t", rows: [][]any{{3}}},
			{client: "b", query: "alter table t add column c3 int"},
			{client: "a", query: "update t set c1 = 10 where pk = 1", err: concurrentUpdateErr},
			{client: "a", query: "begin"},
			{client: "b", query: "alter table t drop column c3"},
			{client: "a", query: "select pk from t where pk = 1 for update", err: concurrentUpdateErr},
			{client: "a", query: "select * from t where pk = 1 for update", rows: [][]any{{1, 1, 1}}},
		},
	},
	{
		name:  "a table that is dropped and created again keeps no locks from before",
		setup: rowLockTableSetup,
		steps: []rowLockStep{
			{client: "a", query: "update t set c1 = 10 where pk = 1"},
			{client: "a", query: "drop table t"},
			{client: "a", query: "create table t (pk varchar(20) primary key, c1 int)"},
			{client: "a", query: "insert into t values ('one', 1), ('two', 2)"},
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "select * from t where pk = 'one' for update", rows: [][]any{{"one", 1}}},
			{client: "b", query: "update t set c1 = 20 where pk = 'two'"},
			{client: "b", query: "update t set c1 = 10 where pk = 'one'", blocks: true},
			{client: "a", query: "commit"},
			{client: "b", unblocks: true},
		},
	},
	{
		name:  "a merge waits for the transactions that hold locks on rows of the tables it changes",
		setup: rowLockTableSetup,
		steps: []rowLockStep{
			{client: "b", query: "call dolt_checkout('other')"},
			{client: "b", query: "update t set c1 = 33 where pk = 3"},
			{client: "b", query: "call dolt_commit('-Am', 'change on other')"},
			{client: "b", query: "call dolt_checkout('main')"},
			{client: "a", query: "begin"},
			{client: "a", query: "update t set c1 = 10 where pk = 1"},
			{client: "b", query: "call dolt_merge('other')", blocks: true},
			{client: "a", query: "commit"},
			{client: "b", unblocks: true},
			{client: "a", query: "select * from t order by pk", rows: [][]any{{1, 10, 1}, {2, 2, 2}, {3, 33, 3}}},
		},
	},
	{
		name:  "a reset waits for the transactions that hold locks on rows of the tables it changes",
		setup: rowLockTableSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "a", query: "update t set c1 = 10 where pk = 1"},
			{client: "b", query: "update t set c1 = 20 where pk = 2"},
			{client: "b", query: "call dolt_reset('--hard')", blocks: true},
			{client: "a", query: "rollback"},
			{client: "b", unblocks: true},
			{client: "a", query: "select * from t order by pk", rows: [][]any{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}}},
		},
	},
	{
		name:  "rolling back to a savepoint doesn't wait for other transactions' locks on the same table",
		setup: rowLockTableSetup,
		steps: []rowLockStep{
			{client: "a", query: "begin"},
			{client: "b", query: "begin"},
			{client: "b", query: "update t set c1 = 20 where pk = 2"},
			{client: "a", query: "savepoint s"},
			{client: "a", query: "update t set c1 = 10 where pk = 1"},
			{client: "a", query: "rollback to savepoint s"},
			{client: "a", query: "select * from t where pk = 1", rows: [][]any{{1, 1, 1}}},
		},
	},
}

func TestRowLockTables(t *testing.T) {
	runner := newRowLockRunner(t)
	for _, script := range rowLockTableScripts {
		runner.run(script)
	}
}

func TestRowLockReads(t *testing.T) {
	runner := newRowLockRunner(t)
	for _, script := range rowLockReadScripts {
		runner.run(script)
	}

	for _, script := range rowLockKeylessReadScripts {
		runner.run(script)
	}
}

func TestRowLockWrites(t *testing.T) {
	runner := newRowLockRunner(t)
	for _, script := range rowLockWriteScripts {
		runner.run(script)
	}

	for _, script := range rowLockKeylessWriteScripts {
		runner.run(script)
	}

	for _, script := range rowLockDisabledScripts {
		runner.run(script)
	}
}

// TestRowLockNoLostUpdates has several sessions increment the same counters at once. Every increment that reports
// success must be reflected in the final values.
func TestRowLockNoLostUpdates(t *testing.T) {
	runner := newRowLockRunner(t)
	runner.journal.SetDeadlockTimeout(5 * time.Millisecond)
	setup := runner.client("setup")
	for _, query := range rowLockWriteSetup {
		require.NoError(t, setup.query(query).err)
	}

	const workers = 8
	const increments = 25

	var wg sync.WaitGroup
	var retries [workers]int
	for worker := 0; worker < workers; worker++ {
		client := newRowJournalClient(t, runner.harness, runner.engine)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < increments; i++ {
				for {
					// Odd workers use explicit transactions that touch two rows, in opposite orders, to provoke
					// deadlocks as well as plain conflicts.
					var err error
					if worker%2 == 0 {
						err = client.query("update t set c1 = c1 + 1 where pk = 1").err
					} else {
						first, second := 1, 2
						if worker%4 == 3 {
							first, second = 2, 1
						}

						err = client.query("begin").err
						if err == nil {
							err = client.query(fmt.Sprintf("update t set c1 = c1 + 1 where pk = %d", first)).err
						}

						if err == nil {
							err = client.query(fmt.Sprintf("update t set c1 = c1 + 1 where pk = %d", second)).err
						}

						if err == nil {
							err = client.query("commit").err
						}
					}

					if err == nil {
						break
					}

					if !assert.True(t, sql.ErrLockDeadlock.Is(err) || sql.ErrRowLockDeadlock.Is(err), "unexpected error: %v", err) {
						return
					}

					retries[worker]++
				}
			}
		}()
	}

	wg.Wait()
	t.Logf("retries per worker: %v", retries)

	res := setup.query("select pk, c1 from t order by pk")
	require.NoError(t, res.err)
	// Row 1 starts at 1 and is incremented by every worker. Row 2 starts at 2 and is incremented by the odd workers.
	require.Equal(t, formatRows([][]any{{1, 1 + workers*increments}, {2, 2 + workers/2*increments}, {3, 3}}), formatSqlRows(res.rows))
	assert.Empty(t, runner.journal.Locks())
	assert.Empty(t, runner.journal.ActiveTxns())
}
