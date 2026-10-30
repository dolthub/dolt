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
	"github.com/dolthub/go-mysql-server/enginetest/setup"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rebasePlanHarness runs queries against an engine with a rebase of feat, which adds commits b and c, onto main.
type rebasePlanHarness struct {
	h        *DoltHarness
	e        enginetest.QueryEngine
	rebasing *sql.Context
}

func newRebasePlanHarness(t *testing.T) *rebasePlanHarness {
	h := newDoltHarness(t)
	t.Cleanup(h.Close)
	h.Setup(setup.MydbData)
	e := mustNewEngine(t, h)
	t.Cleanup(func() { e.Close() })

	r := &rebasePlanHarness{h: h, e: e, rebasing: enginetest.NewContext(h)}
	for _, q := range []string{
		"CREATE TABLE t (pk INT PRIMARY KEY);",
		"CALL DOLT_COMMIT('-Am', 'a');",
		"CALL DOLT_CHECKOUT('-b', 'feat');",
		"INSERT INTO t VALUES (1);",
		"CALL DOLT_COMMIT('-am', 'b');",
		"INSERT INTO t VALUES (2);",
		"CALL DOLT_COMMIT('-am', 'c');",
		"CALL DOLT_REBASE('-i', 'main');",
	} {
		r.run(t, r.rebasing, q)
	}
	return r
}

func (r *rebasePlanHarness) query(ctx *sql.Context, q string) ([]sql.Row, error) {
	_, iter, _, err := r.e.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return sql.RowIterToRows(ctx, iter)
}

func (r *rebasePlanHarness) run(t *testing.T, ctx *sql.Context, q string) []sql.Row {
	rows, err := r.query(ctx, q)
	require.NoError(t, err, q)
	return rows
}

// session returns a new session, with |branch| checked out when it is not empty.
func (r *rebasePlanHarness) session(t *testing.T, branch string) *sql.Context {
	ctx := enginetest.NewSession(r.h)
	if branch != "" {
		r.run(t, ctx, "CALL DOLT_CHECKOUT('"+branch+"');")
	}
	return ctx
}

// TestRebasePlanCommitsSurviveGC leaves the commits named by a rebase plan reachable only through the plan, and checks
// that garbage collection keeps them so that the rebase can finish.
func TestRebasePlanCommitsSurviveGC(t *testing.T) {
	amendTip := func(t *testing.T, r *rebasePlanHarness) {
		r.run(t, r.session(t, "feat"), "CALL DOLT_COMMIT('--amend', '-m', 'c2');")
	}
	for _, test := range []struct {
		name string
		// leave makes the plan's commits unreachable from any branch.
		leave func(t *testing.T, r *rebasePlanHarness)
		// expected is the number of rows in t on feat when the rebase is done.
		expected int64
	}{
		{
			name:     "branch rewritten",
			leave:    amendTip,
			expected: 2,
		},
		{
			name: "commit of a dropped step",
			leave: func(t *testing.T, r *rebasePlanHarness) {
				r.run(t, r.rebasing, "UPDATE dolt_rebase SET action = 'drop' WHERE commit_message = 'c';")
				amendTip(t, r)
			},
			expected: 1,
		},
		{
			name: "commit of a step that already ran",
			leave: func(t *testing.T, r *rebasePlanHarness) {
				r.run(t, r.rebasing, "UPDATE dolt_rebase SET action = 'edit' WHERE commit_message = 'c';")
				r.run(t, r.rebasing, "CALL DOLT_REBASE('--continue');")
				amendTip(t, r)
			},
			expected: 2,
		},
		{
			name: "commit added to the plan by hand",
			leave: func(t *testing.T, r *rebasePlanHarness) {
				other := r.session(t, "")
				r.run(t, other, "CALL DOLT_CHECKOUT('-b', 'other');")
				r.run(t, other, "INSERT INTO t VALUES (9);")
				added := r.run(t, other, "CALL DOLT_COMMIT('-am', 'added');")[0][0].(string)
				r.run(t, r.rebasing, "INSERT INTO dolt_rebase VALUES (3.00, 'pick', '"+added+"', 'added');")
				r.run(t, r.session(t, ""), "CALL DOLT_BRANCH('-D', 'other');")
			},
			expected: 3,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := newRebasePlanHarness(t)
			test.leave(t, r)
			r.run(t, r.session(t, ""), "CALL DOLT_GC();")

			after := r.session(t, "dolt_rebase_feat")
			r.run(t, after, "CALL DOLT_REBASE('--continue');")
			assert.Equal(t, []sql.Row{{"feat"}}, r.run(t, after, "SELECT active_branch();"))
			assert.Equal(t, []sql.Row{{test.expected}}, r.run(t, after, "SELECT count(*) FROM t;"))
		})
	}
}

// TestRebasePlanRejectsInvalidCommits checks that a plan cannot name anything but an existing commit, and that a
// rejected write doesn't reach the chunk store, where a dangling address fails every session sharing the memtable.
func TestRebasePlanRejectsInvalidCommits(t *testing.T) {
	r := newRebasePlanHarness(t)
	table := r.run(t, r.rebasing, "SELECT dolt_hashof_table('t');")[0][0].(string)
	for _, test := range []struct {
		name     string
		hash     string
		expected string
	}{
		{"malformed", "doesnotexist", "invalid commit hash: doesnotexist"},
		{
			"missing",
			"0123456789abcdef0123456789abcdef",
			"unable to resolve commit hash 0123456789abcdef0123456789abcdef: target commit not found",
		},
		{"not a commit", table, "unable to resolve commit hash " + table + ": not a commit"},
	} {
		t.Run(test.name, func(t *testing.T) {
			bystander := r.session(t, "main")
			r.run(t, bystander, "SET @@autocommit = 0;")
			r.run(t, bystander, "INSERT INTO t VALUES (100);")

			_, err := r.query(r.rebasing, "UPDATE dolt_rebase SET commit_hash = '"+test.hash+"' WHERE rebase_order = 1;")
			require.ErrorContains(t, err, test.expected)
			_, err = r.query(r.rebasing, "INSERT INTO dolt_rebase VALUES (5.00, 'pick', '"+test.hash+"', 'bad');")
			require.ErrorContains(t, err, test.expected)

			r.run(t, bystander, "COMMIT;")
			r.run(t, bystander, "DELETE FROM t;")
			r.run(t, bystander, "COMMIT;")
		})
	}

	assert.Equal(t, []sql.Row{{int64(2)}}, r.run(t, r.rebasing, "SELECT count(*) FROM dolt_rebase;"))
	r.run(t, r.rebasing, "CALL DOLT_REBASE('--continue');")
	assert.Equal(t, []sql.Row{{int64(2)}}, r.run(t, r.rebasing, "SELECT count(*) FROM t;"))
}

// TestRebasePlanHashesAreStrings checks that the commit addresses of a plan behave as the strings they replaced.
func TestRebasePlanHashesAreStrings(t *testing.T) {
	r := newRebasePlanHarness(t)
	b := r.run(t, r.session(t, "feat"), "SELECT commit_hash FROM dolt_log WHERE message = 'b';")[0][0].(string)
	assert.Equal(t, []sql.Row{{b, "b"}},
		r.run(t, r.rebasing, "SELECT commit_hash, commit_message FROM dolt_rebase WHERE commit_hash = '"+b+"';"))
	assert.Equal(t, []sql.Row{{int32(32)}}, r.run(t, r.rebasing, "SELECT length(commit_hash) FROM dolt_rebase LIMIT 1;"))
	r.run(t, r.rebasing, "UPDATE dolt_rebase SET commit_message = 'b2' WHERE commit_hash = '"+b+"';")
	assert.Equal(t, []sql.Row{{b}}, r.run(t, r.rebasing, "SELECT commit_hash FROM dolt_rebase WHERE commit_message = 'b2';"))
}

// TestRebasePlanSchemaCannotChange checks that the table can't be altered. Encodings aren't considered when deciding
// whether a change rewrites rows, so a change of the commit_hash column would read addresses as strings.
func TestRebasePlanSchemaCannotChange(t *testing.T) {
	r := newRebasePlanHarness(t)
	for _, q := range []string{
		"ALTER TABLE dolt_rebase MODIFY COLUMN commit_hash VARCHAR(64) NOT NULL;",
		"ALTER TABLE dolt_rebase MODIFY COLUMN commit_hash TEXT NOT NULL;",
		"ALTER TABLE dolt_rebase ADD COLUMN extra INT;",
	} {
		_, err := r.query(r.rebasing, q)
		assert.ErrorContains(t, err, "cannot be altered", q)
	}
	_, err := r.query(r.rebasing, "CREATE INDEX idx ON dolt_rebase (commit_hash);")
	assert.ErrorContains(t, err, "not indexable")
}

// TestRebasePlanConcurrentEdits checks that sessions editing different steps of a plan merge when they commit.
func TestRebasePlanConcurrentEdits(t *testing.T) {
	r := newRebasePlanHarness(t)
	first, second := r.session(t, "dolt_rebase_feat"), r.session(t, "dolt_rebase_feat")
	for _, ctx := range []*sql.Context{first, second} {
		r.run(t, ctx, "SET @@autocommit = 0;")
	}
	r.run(t, first, "UPDATE dolt_rebase SET action = 'drop' WHERE rebase_order = 1;")
	r.run(t, second, "UPDATE dolt_rebase SET action = 'reword', commit_message = 'c2' WHERE rebase_order = 2;")
	r.run(t, first, "COMMIT;")
	r.run(t, second, "COMMIT;")

	assert.Equal(t, []sql.Row{{"drop", "b"}, {"reword", "c2"}},
		r.run(t, r.session(t, "dolt_rebase_feat"), "SELECT CAST(action AS CHAR), commit_message FROM dolt_rebase ORDER BY rebase_order;"))
}
