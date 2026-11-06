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
// they're refused once that branch state is dropped.
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
	ws := state.WorkingSet()

	sch := schema.MustSchemaFromCols(schema.NewColCollection(schema.NewColumn("pk", 1, types.IntKind, true)))
	withT, err := doltdb.CreateEmptyTable(sqlCtx, ws.WorkingRoot(), doltdb.TableName{Name: "t"}, sch)
	require.NoError(t, err)
	withU, err := doltdb.CreateEmptyTable(sqlCtx, ws.WorkingRoot(), doltdb.TableName{Name: "u"}, sch)
	require.NoError(t, err)

	owner, err := sess.ForkWorkingSet(sqlCtx, "dolt", ws, false)
	require.NoError(t, err)
	require.NoError(t, owner.SetFlushedWorkingSet(sqlCtx, ws.WithWorkingRoot(withT)))
	has, err := state.WorkingRoot().HasTable(sqlCtx, doltdb.TableName{Name: "t"})
	require.NoError(t, err)
	assert.True(t, has, "the flush didn't reach the branch state")

	// Rolling back drops the branch state, so a flush through the fork would be lost.
	require.NoError(t, sess.Rollback(sqlCtx, sqlCtx.GetTransaction()))
	require.PanicsWithValue(t, "cannot flush into dropped session state: dolt/main", func() {
		_ = owner.SetFlushedWorkingSet(sqlCtx, ws.WithWorkingRoot(withU))
	})
	roots, ok := sess.GetRoots(sqlCtx, "dolt")
	require.True(t, ok)
	has, err = roots.Working.HasTable(sqlCtx, doltdb.TableName{Name: "u"})
	require.NoError(t, err)
	assert.False(t, has, "the flush reached the new branch state")
}
