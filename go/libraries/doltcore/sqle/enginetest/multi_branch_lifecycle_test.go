// Copyright 2022 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

type batchCommitLifecycle struct{ commits int }

func (*batchCommitLifecycle) DoltgresTransactionStarted()        {}
func (l *batchCommitLifecycle) DoltgresTransactionCommitted()    { l.commits++ }
func (*batchCommitLifecycle) DoltgresTransactionRolledBack()     {}
func (*batchCommitLifecycle) DoltgresSavepointCreated(string)    {}
func (*batchCommitLifecycle) DoltgresSavepointRolledBack(string) {}
func (*batchCommitLifecycle) DoltgresSavepointReleased(string)   {}

func TestMultiBranchCommitLifecycle(t *testing.T) {
	for _, test := range []struct {
		name      string
		query     string
		automatic bool
		single    bool
		undo      bool
	}{
		{name: "working sets", query: "COMMIT"},
		{name: "automatic heads", query: "COMMIT", automatic: true},
		{name: "explicit heads", query: "CALL dolt_commit_all('-am', 'batch')"},
		{name: "implicit heads", query: "CALL dolt_commit('-am', 'batch')"},
		{name: "single working set helper"},
		{name: "single automatic head", query: "COMMIT", automatic: true, single: true},
		{name: "single explicit head", query: "CALL dolt_commit('-am', 'single')", single: true},
		{name: "single automatic empty commit", query: "COMMIT", automatic: true, single: true, undo: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newDoltHarness(t)
			defer h.Close()
			engine, err := h.NewEngine(t)
			require.NoError(t, err)
			defer engine.Close()
			ctx := enginetest.NewContext(h)
			for _, query := range []string{
				"CREATE TABLE t (pk INT PRIMARY KEY)", "CALL dolt_commit('-Am', 'setup')",
				"CALL dolt_branch('other')", "SET autocommit=0", "SET dolt_multi_branch_commit=1", "START TRANSACTION",
			} {
				enginetest.RunQueryWithContext(t, engine, h, ctx, query)
			}
			if test.automatic {
				enginetest.RunQueryWithContext(t, engine, h, ctx, "SET dolt_transaction_commit=1")
			}
			sess := dsess.DSessFromSess(ctx.Session)
			lifecycle := &batchCommitLifecycle{}
			sess.DoltgresSessObj = lifecycle
			enginetest.RunQueryWithContext(t, engine, h, ctx, "INSERT INTO t VALUES (1)")
			if test.undo {
				enginetest.RunQueryWithContext(t, engine, h, ctx, "DELETE FROM t")
			}
			if test.query == "" {
				require.NoError(t, sess.CommitWorkingSet(ctx, "mydb", ctx.GetTransaction()))
			} else {
				if !test.single {
					enginetest.RunQueryWithContext(t, engine, h, ctx, "INSERT INTO `mydb/other`.t VALUES (2)")
				}
				enginetest.RunQueryWithContext(t, engine, h, ctx, test.query)
			}
			require.Equal(t, 1, lifecycle.commits, "a successful batch must emit exactly one semantic commit event")
			require.Nil(t, ctx.GetTransaction())
		})
	}
}

// Inspect session state directly after committing, before a new SQL statement can start a transaction
// and reload it. Stored procedures such as revert rely on this state between internal commits.
func TestCommitBranchStatesSessionState(t *testing.T) {
	for _, mode := range []string{"single head", "mixed heads and working sets", "working set", "merged working set"} {
		t.Run(mode, func(t *testing.T) {
			h := newDoltHarness(t)
			defer h.Close()
			engine, err := h.NewEngine(t)
			require.NoError(t, err)
			defer engine.Close()
			ctx := enginetest.NewContext(h)
			for _, query := range []string{
				"CREATE TABLE t (pk INT PRIMARY KEY)", "CALL dolt_commit('-Am', 'setup')",
				"CALL dolt_branch('other')", "CALL dolt_branch('third')", "SET autocommit=0",
				"INSERT INTO t VALUES (1)",
			} {
				enginetest.RunQueryWithContext(t, engine, h, ctx, query)
			}

			names := []string{"mydb/main"}
			if mode == "mixed heads and working sets" {
				names = append(names, "mydb/other", "mydb/third")
				for _, name := range names[1:] {
					enginetest.RunQueryWithContext(t, engine, h, ctx, "INSERT INTO `"+name+"`.t VALUES (2)")
				}
			}

			sess := dsess.DSessFromSess(ctx.Session)
			pending := make([]*doltdb.PendingCommit, len(names))
			oldHeads := make([]*doltdb.Commit, len(names))
			for i, name := range names {
				oldHeads[i], err = sess.GetHeadCommit(ctx, name)
				require.NoError(t, err)
				if mode == "working set" || mode == "merged working set" || i == 1 {
					continue
				}

				roots, ok := sess.GetRoots(ctx, name)
				require.True(t, ok)
				roots.Staged = roots.Working
				props, _, err := dsess.NewCommitStagedProps(ctx, "state refresh")
				require.NoError(t, err)
				pending[i], err = sess.NewPendingCommit(ctx, name, roots, props)
				require.NoError(t, err)
				require.NotNil(t, pending[i])
			}

			before, err := sess.WorkingSet(ctx, names[0])
			require.NoError(t, err)
			if mode == "merged working set" {
				other := h.NewContextWithClient(sql.Client{Address: "localhost", User: "root"})
				enginetest.RunQueryWithContext(t, engine, h, other, "INSERT INTO t VALUES (9)")
			}

			lifecycle := &batchCommitLifecycle{}
			sess.DoltgresSessObj = lifecycle
			commits := make([]*doltdb.Commit, len(names))
			if mode == "mixed heads and working sets" {
				// A rejected batch must leave the transaction, cached HEADs, and dirty state intact.
				tx := ctx.GetTransaction()
				dirty := sess.DirtyBranches()
				_, err := sess.DoltCommitMulti(ctx, tx, []string{names[0], names[0]}, pending[:2])
				require.ErrorContains(t, err, "duplicate working set")
				require.Same(t, tx, ctx.GetTransaction())
				require.ElementsMatch(t, dirty, sess.DirtyBranches())
				require.Zero(t, lifecycle.commits)
				for i, name := range names {
					head, err := sess.GetHeadCommit(ctx, name)
					require.NoError(t, err)
					require.Same(t, oldHeads[i], head)
				}
			}

			switch mode {
			case "single head":
				commits[0], err = sess.DoltCommit(ctx, names[0], ctx.GetTransaction(), pending[0])
			case "working set", "merged working set":
				err = sess.CommitWorkingSet(ctx, names[0], ctx.GetTransaction())
			default:
				commits, err = sess.DoltCommitMulti(ctx, ctx.GetTransaction(), names, pending)
			}
			require.NoError(t, err)
			require.Nil(t, ctx.GetTransaction())
			require.Equal(t, 1, lifecycle.commits)

			for i, name := range names {
				head, err := sess.GetHeadCommit(ctx, name)
				require.NoError(t, err)
				if pending[i] == nil {
					require.Nil(t, commits[i])
					require.Same(t, oldHeads[i], head)
				} else {
					require.Same(t, commits[i], head)
					roots, ok := sess.GetRoots(ctx, name)
					require.True(t, ok)
					headHash, err := roots.Head.HashOf()
					require.NoError(t, err)
					stagedHash, err := roots.Staged.HashOf()
					require.NoError(t, err)
					require.Equal(t, headHash, stagedHash)
				}

				state, ok, err := sess.LookupDbState(ctx, name)
				require.NoError(t, err)
				require.True(t, ok)
				require.Same(t, state.WorkingSet(), state.WriteSession().GetWorkingSet())
				dbData, ok := sess.GetDbData(ctx, name)
				require.True(t, ok)
				stored, err := dbData.Ddb.ResolveWorkingSet(ctx, state.WorkingSet().Ref())
				require.NoError(t, err)
				storedHash, err := stored.WorkingRoot().HashOf()
				require.NoError(t, err)
				cachedHash, err := state.WorkingSet().WorkingRoot().HashOf()
				require.NoError(t, err)
				require.Equal(t, storedHash, cachedHash)
				if mode == "merged working set" {
					beforeHash, err := before.WorkingRoot().HashOf()
					require.NoError(t, err)
					require.NotEqual(t, beforeHash, cachedHash, "the session must retain the concurrent writer's changes")
				}
			}

			require.Empty(t, sess.DirtyBranches())
		})
	}
}
