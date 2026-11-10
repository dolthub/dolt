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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
)

// TestRekeyedMergeBaseSurvivesGC checks that the base of the conflicts of a merge whose sides changed a primary key
// the same way, a dangling commit of the rekeyed ancestor, is kept by garbage collection because the conflicts
// reference it, and that it is rebuilt from the conflict metadata when a client that predates that reference has
// collected it.
func TestRekeyedMergeBaseSurvivesGC(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	h.Setup(setup.MydbData)
	e := mustNewEngine(t, h)
	defer e.Close()
	run := func(ctx *sql.Context, q string) []sql.Row {
		_, iter, _, err := e.Query(ctx, q)
		require.NoError(t, err, q)
		rows, err := sql.RowIterToRows(ctx, iter)
		require.NoError(t, err, q)
		return rows
	}

	ctx := enginetest.NewContext(h)
	for _, q := range samePkChangeSetup(pkChangeTable, pkChangeRows, pkChangeAlter,
		[]string{"UPDATE t SET v = 100 WHERE a = 1;"},
		[]string{"UPDATE t SET v = 11 WHERE a = 1;"}) {
		run(ctx, q)
	}
	merging := enginetest.NewSession(h)
	for _, q := range []string{"SET @@dolt_allow_commit_conflicts = 1;", "CALL DOLT_MERGE('feature');"} {
		run(merging, q)
	}
	query := "SELECT base_a, base_b, base_v, our_v, their_v FROM dolt_conflicts_t;"
	expected := []sql.Row{{int32(1), int32(1), int32(10), int32(100), int32(11)}}
	require.Equal(t, expected, run(merging, query))

	db, ok := h.provider.BaseDatabase(merging, "mydb")
	require.True(t, ok)
	ddb := db.(dsess.SqlDatabase).DbData().Ddb
	vrw, ns := ddb.ValueReadWriter(), ddb.NodeStore()
	meta := conflictMetadata(t, merging, ddb, "main")
	assert.Empty(t, meta.MergeBases)
	mergeBase := hash.Parse(run(merging, "SELECT DOLT_MERGE_BASE('main', 'feature');")[0][0].(string))
	assert.NotEqual(t, mergeBase, meta.BaseRootIsh, "the base of the conflicts is the rekeyed merge base")
	assert.Equal(t, mergeBase, meta.RekeyedFrom)
	theirs := hash.Parse(run(merging, "SELECT DOLT_HASHOF('feature');")[0][0].(string))

	run(merging, "CALL DOLT_GC();")
	stored, err := vrw.ReadValue(merging, meta.BaseRootIsh)
	require.NoError(t, err)
	require.NotNil(t, stored, "garbage collection keeps the rekeyed merge base")
	base, err := doltdb.LoadConflictBaseRoot(merging, vrw, ns, meta, theirs)
	require.NoError(t, err)
	tbl, ok, err := base.GetTable(merging, doltdb.TableName{Name: "t"})
	require.NoError(t, err)
	require.True(t, ok)
	sch, err := tbl.GetSchema(merging)
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, sch.GetPKCols().GetColumnNames())

	// A client that predates the address of the base in the conflict's value collects the base. The base is then
	// rebuilt from the merge base the conflict metadata records.
	collected := meta
	collected.BaseRootIsh = hash.Of([]byte("a collected rekeyed merge base"))
	rebuilt, err := doltdb.LoadConflictBaseRoot(merging, vrw, ns, collected, theirs)
	require.NoError(t, err)
	rebuiltTbl, ok, err := rebuilt.GetTable(merging, doltdb.TableName{Name: "t"})
	require.NoError(t, err)
	require.True(t, ok)
	tblHash, err := tbl.HashOf()
	require.NoError(t, err)
	rebuiltHash, err := rebuiltTbl.HashOf()
	require.NoError(t, err)
	assert.Equal(t, tblHash, rebuiltHash)

	after := enginetest.NewSession(h)
	assert.Equal(t, expected, run(after, query))
	run(after, "SET @@dolt_allow_commit_conflicts = 1;")
	run(after, "CALL DOLT_CONFLICTS_RESOLVE('--theirs', 't');")
	run(after, "CALL DOLT_COMMIT('-am', 'resolve merge of feature');")
	assert.Equal(t, []sql.Row{{int32(1), int32(1), int32(11)}, {int32(2), int32(2), int32(20)}, {int32(3), int32(3), int32(30)}},
		run(after, "SELECT * FROM t ORDER BY a;"))
}
