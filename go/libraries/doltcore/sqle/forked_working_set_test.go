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

package sqle

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/types"
)

// TestForkedWorkingSet checks that a forked working set's flushes reach the branch state it was forked from, and that
// they're refused once something else changes that branch state's root or drops the branch state.
func TestForkedWorkingSet(t *testing.T) {
	ctx := context.Background()
	dEnv := CreateTestEnv()
	defer dEnv.DoltDB(ctx).Close()

	db, err := NewDatabase(ctx, "dolt", dEnv.DbData(ctx), editor.Options{})
	require.NoError(t, err)
	_, sqlCtx, err := NewTestEngine(dEnv, ctx, db)
	require.NoError(t, err)
	sess := dsess.DSessFromSess(sqlCtx.Session)

	state, ok, err := sess.LookupDbState(sqlCtx, "dolt")
	require.NoError(t, err)
	require.True(t, ok)
	ws, err := state.WorkingSet(sqlCtx)
	require.NoError(t, err)

	sch := schema.MustSchemaFromCols(schema.NewColCollection(schema.NewColumn("pk", 1, types.IntKind, true)))
	withTable := func(root doltdb.RootValue, name string) doltdb.RootValue {
		root, err := doltdb.CreateEmptyTable(sqlCtx, root, doltdb.TableName{Name: name}, sch)
		require.NoError(t, err)
		return root
	}
	hasTable := func(name string) bool {
		roots, ok := sess.GetRoots(sqlCtx, "dolt")
		require.True(t, ok)
		has, err := roots.Working.HasTable(sqlCtx, doltdb.TableName{Name: name})
		require.NoError(t, err)
		return has
	}

	// Each flush reaches the branch state, and a fork can flush more than once.
	owner, err := sess.ForkWorkingSet(sqlCtx, "dolt", ws, false)
	require.NoError(t, err)
	withT := withTable(ws.WorkingRoot(), "t")
	require.NoError(t, owner.SetFlushedWorkingSet(sqlCtx, ws.WithWorkingRoot(withT)))
	assert.True(t, hasTable("t"), "the flush didn't reach the branch state")
	require.NoError(t, owner.SetFlushedWorkingSet(sqlCtx, ws.WithWorkingRoot(withTable(withT, "t2"))))
	assert.True(t, hasTable("t2"), "the second flush didn't reach the branch state")

	// A flush would revert a change made to the branch state since, so it's refused.
	ws, err = state.WorkingSet(sqlCtx)
	require.NoError(t, err)
	owner, err = sess.ForkWorkingSet(sqlCtx, "dolt", ws, false)
	require.NoError(t, err)
	require.NoError(t, sess.SetWorkingRoot(sqlCtx, "dolt", withTable(ws.WorkingRoot(), "v")))
	require.PanicsWithValue(t, "cannot flush a forked working set into session state that changed after the fork: dolt/main", func() {
		_ = owner.SetFlushedWorkingSet(sqlCtx, ws.WithWorkingRoot(withTable(ws.WorkingRoot(), "u")))
	})
	assert.True(t, hasTable("v"), "the flush reverted the branch state's change")
	assert.False(t, hasTable("u"), "the refused flush reached the branch state")

	// Rolling back drops the branch state, so a flush through the fork would be lost.
	ws, err = state.WorkingSet(sqlCtx)
	require.NoError(t, err)
	owner, err = sess.ForkWorkingSet(sqlCtx, "dolt", ws, false)
	require.NoError(t, err)
	require.NoError(t, sess.Rollback(sqlCtx, sqlCtx.GetTransaction()))
	require.PanicsWithValue(t, "cannot flush into dropped session state: dolt/main", func() {
		_ = owner.SetFlushedWorkingSet(sqlCtx, ws.WithWorkingRoot(withTable(ws.WorkingRoot(), "u")))
	})
	assert.False(t, hasTable("u"), "the flush reached the new branch state")
}
