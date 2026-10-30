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

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

// TestRebaseStateSurvivesGC starts an interactive rebase, leaves the pre-rebase root reachable only through the rebase
// state of the working set, and checks that garbage collection keeps it.
func TestRebaseStateSurvivesGC(t *testing.T) {
	for _, test := range []struct {
		name string
		// unreference makes the root of the branch being rebased unreachable from its branch.
		unreference string
		// abortedTo is the row count on feat after aborting the rebase, when feat still exists to abort to.
		abortedTo []sql.Row
	}{
		{
			name:        "branch deleted",
			unreference: "CALL DOLT_BRANCH('-D', 'feat');",
		},
		{
			name:        "branch moved",
			unreference: "CALL DOLT_BRANCH('-f', 'feat', 'main');",
			abortedTo:   []sql.Row{{int64(0)}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newDoltHarness(t)
			defer h.Close()
			h.Setup(setup.MydbData)
			e := mustNewEngine(t, h)
			defer e.Close()
			run := func(ctx *sql.Context, q string) []sql.Row {
				rows, err := queryRows(e, ctx, q)
				require.NoError(t, err, q)
				return rows
			}

			rebasing := enginetest.NewContext(h)
			for _, q := range []string{
				"CREATE TABLE t (pk INT PRIMARY KEY);",
				"CALL DOLT_COMMIT('-Am', 'a');",
				"CALL DOLT_CHECKOUT('-b', 'feat');",
				"INSERT INTO t VALUES (1);",
				"CALL DOLT_COMMIT('-am', 'b');",
				"CALL DOLT_REBASE('-i', 'main');",
			} {
				run(rebasing, q)
			}

			db, ok := h.provider.BaseDatabase(rebasing, "mydb")
			require.True(t, ok)
			ddb := db.(dsess.SqlDatabase).DbData().Ddb
			wsRef, err := ref.WorkingSetRefForHead(ref.NewBranchRef("dolt_rebase_feat"))
			require.NoError(t, err)
			ws, err := ddb.ResolveWorkingSet(rebasing, wsRef)
			require.NoError(t, err)
			preRebaseHash, err := ws.RebaseState().PreRebaseWorkingRoot().HashOf()
			require.NoError(t, err)

			run(enginetest.NewSession(h), test.unreference)
			run(enginetest.NewSession(h), "CALL DOLT_GC();")

			after := enginetest.NewSession(h)
			ws, err = ddb.ResolveWorkingSet(after, wsRef)
			require.NoError(t, err)
			kept, err := ws.RebaseState().PreRebaseWorkingRoot().HashOf()
			require.NoError(t, err)
			assert.Equal(t, preRebaseHash, kept)

			run(after, "CALL DOLT_CHECKOUT('dolt_rebase_feat');")
			assert.Equal(t, []sql.Row{{int64(0)}}, run(after, "SELECT count(*) FROM t;"))
			assert.Equal(t, []sql.Row{{int64(0)}}, run(after, "SELECT count(*) FROM dolt_status;"))
			assert.Equal(t, []sql.Row{{int64(1)}}, run(after, "SELECT count(*) FROM dolt_rebase;"))

			if test.abortedTo != nil {
				run(after, "CALL DOLT_REBASE('--abort');")
				assert.Equal(t, []sql.Row{{"feat"}}, run(after, "SELECT active_branch();"))
				assert.Equal(t, test.abortedTo, run(after, "SELECT count(*) FROM t;"))
			}
		})
	}
}

// TestRebaseConflictStateSurvivesGC stops a rebase on a conflict, moves the rebase branch off of the commit the
// cherry-pick recorded as its pre-merge HEAD, and checks that garbage collection keeps that commit.
func TestRebaseConflictStateSurvivesGC(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	h.Setup(setup.MydbData)
	e := mustNewEngine(t, h)
	defer e.Close()
	run := func(ctx *sql.Context, q string) []sql.Row {
		rows, err := queryRows(e, ctx, q)
		require.NoError(t, err, q)
		return rows
	}

	rebasing := enginetest.NewContext(h)
	for _, q := range []string{
		"CREATE TABLE t (pk INT PRIMARY KEY, v INT);",
		"INSERT INTO t VALUES (0, 0);",
		"CALL DOLT_COMMIT('-Am', 'a');",
		"CALL DOLT_CHECKOUT('-b', 'feat');",
		"INSERT INTO t VALUES (1, 1);",
		"CALL DOLT_COMMIT('-am', 'b');",
		"UPDATE t SET v = 10 WHERE pk = 0;",
		"CALL DOLT_COMMIT('-am', 'c');",
		"CALL DOLT_CHECKOUT('main');",
		"UPDATE t SET v = 20 WHERE pk = 0;",
		"CALL DOLT_COMMIT('-am', 'd');",
		"CALL DOLT_CHECKOUT('feat');",
		"SET @@dolt_allow_commit_conflicts = 1;",
		"CALL DOLT_REBASE('-i', 'main');",
	} {
		run(rebasing, q)
	}
	_, err := queryRows(e, rebasing, "CALL DOLT_REBASE('--continue');")
	require.ErrorContains(t, err, "conflict")
	require.Equal(t, []sql.Row{{"t", uint64(1)}}, run(rebasing, "SELECT `table`, num_conflicts FROM dolt_conflicts;"))

	db, ok := h.provider.BaseDatabase(rebasing, "mydb")
	require.True(t, ok)
	ddb := db.(dsess.SqlDatabase).DbData().Ddb
	wsRef, err := ref.WorkingSetRefForHead(ref.NewBranchRef("dolt_rebase_feat"))
	require.NoError(t, err)
	ws, err := ddb.ResolveWorkingSet(rebasing, wsRef)
	require.NoError(t, err)
	preMergeHead, err := ws.MergeState().PreMergeHeadCommit().HashOf()
	require.NoError(t, err)

	// This reset moves the branch, but fails to commit its working set because of the conflict, leaving the merge state.
	moving := enginetest.NewSession(h)
	run(moving, "CALL DOLT_CHECKOUT('dolt_rebase_feat');")
	_, err = queryRows(e, moving, "CALL DOLT_RESET('--soft', 'HEAD~1');")
	require.ErrorContains(t, err, "Merge conflict detected")
	head, err := ddb.ResolveCommitRef(rebasing, ref.NewBranchRef("dolt_rebase_feat"))
	require.NoError(t, err)
	moved, err := head.HashOf()
	require.NoError(t, err)
	require.NotEqual(t, preMergeHead, moved)

	run(enginetest.NewSession(h), "CALL DOLT_GC();")

	after := enginetest.NewSession(h)
	ws, err = ddb.ResolveWorkingSet(after, wsRef)
	require.NoError(t, err)
	kept, err := ws.MergeState().PreMergeHeadCommit().HashOf()
	require.NoError(t, err)
	assert.Equal(t, preMergeHead, kept)
	roots, err := ddb.WorkingSetHashes(after, ws)
	require.NoError(t, err)
	assert.Contains(t, roots, preMergeHead, "an in-memory working set must keep it too")

	run(after, "CALL DOLT_CHECKOUT('dolt_rebase_feat');")
	assert.Equal(t, []sql.Row{{"t", uint64(1)}}, run(after, "SELECT `table`, num_conflicts FROM dolt_conflicts;"))
}

func queryRows(e enginetest.QueryEngine, ctx *sql.Context, q string) ([]sql.Row, error) {
	_, iter, _, err := e.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return sql.RowIterToRows(ctx, iter)
}
