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

package dsess

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// openWritersWriteSession reports open table writers as |open| says. Its other methods are unused.
type openWritersWriteSession struct {
	WriteSession
	open bool
}

func (s *openWritersWriteSession) HasOpenTableWriters() bool {
	return s.open
}

// TestDropBranchStateWithOpenWriters checks every path that drops a branch state while its write session has open table
// writers. Dynamic asserts are on under tests, so each one panics.
func TestDropBranchStateWithOpenWriters(t *testing.T) {
	newSession := func(open bool) (*DoltSession, *sql.Context, *branchState) {
		sess := DefaultSessionWithVariables(emptyDatabaseProvider(), nil, newTransactionTestVariables())
		dbState := newEmptyDatabaseSessionState()
		dbState.dbName = "db"
		dbState.checkedOutRevSpec = "main"
		sess.dbStates["db"] = dbState
		bs := sess.currentBranches().add(dbState, "main", RevisionTypeBranch)
		bs.writeSession = &openWritersWriteSession{open: open}
		return sess, sql.NewContext(context.Background(), sql.WithSession(sess)), bs
	}

	// Ending a transaction keeps the branch state.
	for _, d := range []struct {
		name string
		drop func(*DoltSession, *sql.Context) error
	}{
		{"StartTransaction", func(s *DoltSession, ctx *sql.Context) error {
			_, err := s.StartTransaction(ctx, sql.ReadWrite)
			return err
		}},
		{"Rollback", func(s *DoltSession, ctx *sql.Context) error {
			return s.Rollback(ctx, DisabledTransaction{})
		}},
	} {
		t.Run(d.name, func(t *testing.T) {
			sess, ctx, bs := newSession(true)
			require.PanicsWithValue(t, "cannot drop session state with open table writers: db/main", func() {
				_ = d.drop(sess, ctx)
			})
			assert.True(t, sess.isCurrentBranchState(bs), "branch state was dropped")
		})
	}

	// The database or branch is already gone, so its state is dropped anyway.
	t.Run("RemoveDbState", func(t *testing.T) {
		sess, ctx, _ := newSession(true)
		require.PanicsWithValue(t, "cannot drop session state with open table writers: db/main", func() {
			_ = sess.RemoveDbState(ctx, "db")
		})
		assert.NotContains(t, sess.dbStates, "db")
		assert.Empty(t, sess.currentBranches().all())
	})
	t.Run("RemoveBranchState", func(t *testing.T) {
		sess, ctx, bs := newSession(true)
		require.PanicsWithValue(t, "cannot drop session state with open table writers: db/main", func() {
			_ = sess.RemoveBranchState(ctx, "db", "main")
		})
		assert.False(t, sess.isCurrentBranchState(bs))
	})
	t.Run("RenameBranchState", func(t *testing.T) {
		sess, ctx, bs := newSession(true)
		require.PanicsWithValue(t, "cannot drop session state with open table writers: db/renamed", func() {
			_ = sess.RenameBranchState(ctx, "db", "main", "renamed")
		})
		renamed, ok := sess.currentBranches().get("db", "renamed")
		require.True(t, ok)
		assert.Same(t, bs, renamed)
		assert.True(t, sess.isCurrentBranchState(bs))
	})

	t.Run("no open writers", func(t *testing.T) {
		sess, ctx, bs := newSession(false)
		require.NoError(t, sess.Rollback(ctx, DisabledTransaction{}))
		assert.Empty(t, sess.currentBranches().all())
		assert.False(t, sess.isCurrentBranchState(bs))
	})

	// Changing the session's transaction drops the old branch states however the change happens.
	t.Run("SetTransaction ending a DoltTransaction", func(t *testing.T) {
		sess, ctx, _ := newSession(false)
		ctx.SetTransaction(&DoltTransaction{branches: newBranchStates()})
		bs := sess.currentBranches().add(sess.dbStates["db"], "main", RevisionTypeBranch)
		bs.writeSession = &openWritersWriteSession{open: true}
		require.PanicsWithValue(t, "cannot drop session state with open table writers: db/main", func() {
			ctx.SetTransaction(nil)
		})
	})
	t.Run("SetTransaction starting a DoltTransaction", func(t *testing.T) {
		_, ctx, _ := newSession(true)
		require.PanicsWithValue(t, "cannot drop session state with open table writers: db/main", func() {
			ctx.SetTransaction(&DoltTransaction{branches: newBranchStates()})
		})
	})

	// Another session's writers can't be inspected safely, so dropping a database's state from another session
	// doesn't check them.
	t.Run("RemoveDbState from another session", func(t *testing.T) {
		sess, _, _ := newSession(true)
		other := DefaultSessionWithVariables(emptyDatabaseProvider(), nil, newTransactionTestVariables())
		otherCtx := sql.NewContext(context.Background(), sql.WithSession(other))
		require.NoError(t, sess.RemoveDbState(otherCtx, "db"))
		assert.NotContains(t, sess.dbStates, "db")
		assert.Empty(t, sess.currentBranches().all())
	})
}

// TestBranchStatesBelongToTransaction checks that a DoltTransaction's branch states last only while it's the session's
// transaction, and that the ones loaded outside a DoltTransaction don't outlive the start of one.
func TestBranchStatesBelongToTransaction(t *testing.T) {
	sess := DefaultSessionWithVariables(emptyDatabaseProvider(), nil, newTransactionTestVariables())
	ctx := sql.NewContext(context.Background(), sql.WithSession(sess))
	dbState := newEmptyDatabaseSessionState()
	dbState.dbName = "db"
	sess.dbStates["db"] = dbState

	noTx := sess.currentBranches().add(dbState, "main", RevisionTypeBranch)
	tx := &DoltTransaction{branches: newBranchStates()}
	ctx.SetTransaction(tx)
	assert.False(t, sess.isCurrentBranchState(noTx), "branch state loaded outside the transaction is still current")

	inTx := sess.currentBranches().add(dbState, "main", RevisionTypeBranch)
	ctx.SetTransaction(tx)
	assert.True(t, sess.isCurrentBranchState(inTx), "setting the same transaction dropped its branch states")

	ctx.SetTransaction(nil)
	assert.False(t, sess.isCurrentBranchState(inTx), "ended transaction's branch state is still current")
	assert.Empty(t, sess.currentBranches().all())

	// Branch states loaded with transactions disabled last across statements.
	ctx.SetTransaction(DisabledTransaction{})
	disabled := sess.currentBranches().add(dbState, "main", RevisionTypeBranch)
	ctx.SetTransaction(DisabledTransaction{})
	assert.True(t, sess.isCurrentBranchState(disabled))
}
