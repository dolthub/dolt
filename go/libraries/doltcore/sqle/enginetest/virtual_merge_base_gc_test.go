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
	"errors"
	"io"
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor/creation"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
)

// TestVirtualMergeBaseRebuiltAfterGC leaves a merge with conflicts against a virtual merge base in progress, garbage
// collects the virtual merge base, and checks that the conflicts read the same from a rebuilt one:
//
//	c1 -- x1 -- x2 -- x3    x
//	  \-- y1 -- y2 -- y3    y
//	x2 = merge(x1, y1), y2 = merge(y1, x1)
//	x3 = merge(x2, y2), y3 = merge(y2, x2)
//
// c1 seeds rows 1 and 2 with a = 'seed' and j = {}. x1 adds column c, and x1 and y1 conflict on row 1's a. x2
// resolves it to 'seed' and y2 to 'y', so x2 and y2 conflict again, and x2 and y2 add keys x and y to row 2's j, which
// merge cleanly only with JSON merging. x3 and y3 resolve row 1's a to 'y' and 'q', and set row 2's a to 'x3' and
// 'y3'. The virtual merge base of x3 and y3 merges x2 and y2 against the virtual merge base of x1 and y1.
func TestVirtualMergeBaseRebuiltAfterGC(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	h.Setup(setup.MydbData)
	e := mustNewEngine(t, h)
	defer e.Close()

	query := func(ctx *sql.Context, q string) []sql.Row {
		_, iter, _, err := e.Query(ctx, q)
		require.NoError(t, err, q)
		rows, err := sql.RowIterToRows(ctx, iter)
		require.NoError(t, err, q)
		return rows
	}
	ctx := enginetest.NewContext(h)
	for _, q := range []string{
		"SET @@dolt_allow_commit_conflicts = 1;",
		"CREATE TABLE t (id INT PRIMARY KEY, a VARCHAR(100), j JSON, INDEX ia (a));",
		"INSERT INTO t VALUES (1, 'seed', '{}'), (2, 'seed', '{}');",
		"CALL DOLT_COMMIT('-Am', 'c1');",
		"CALL DOLT_BRANCH('x');",
		"CALL DOLT_BRANCH('y');",
		"CALL DOLT_CHECKOUT('x');",
		"ALTER TABLE t ADD COLUMN c VARCHAR(100);",
		"UPDATE t SET a = 'x' WHERE id = 1;",
		"CALL DOLT_COMMIT('-am', 'x1');",
		"CALL DOLT_BRANCH('x1');",
		"CALL DOLT_CHECKOUT('y');",
		"UPDATE t SET a = 'y' WHERE id = 1;",
		"CALL DOLT_COMMIT('-am', 'y1');",
		"CALL DOLT_BRANCH('y1');",
		"CALL DOLT_MERGE('x1', '-m', 'y2');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"UPDATE t SET a = 'y' WHERE id = 1;",
		`UPDATE t SET j = '{"y": 1}' WHERE id = 2;`,
		"CALL DOLT_COMMIT('-am', 'y2');",
		"CALL DOLT_BRANCH('y2');",
		"CALL DOLT_CHECKOUT('x');",
		"CALL DOLT_MERGE('y1', '-m', 'x2');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"UPDATE t SET a = 'seed' WHERE id = 1;",
		`UPDATE t SET j = '{"x": 1}' WHERE id = 2;`,
		"CALL DOLT_COMMIT('-am', 'x2');",
		"CALL DOLT_BRANCH('x2');",
		"CALL DOLT_MERGE('y2', '-m', 'x3');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"UPDATE t SET a = 'y' WHERE id = 1;",
		"UPDATE t SET a = 'x3' WHERE id = 2;",
		"CALL DOLT_COMMIT('-am', 'x3');",
		"CALL DOLT_CHECKOUT('y');",
		"CALL DOLT_MERGE('x2', '-m', 'y3');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"UPDATE t SET a = 'q' WHERE id = 1;",
		"UPDATE t SET a = 'y3' WHERE id = 2;",
		"CALL DOLT_COMMIT('-am', 'y3');",
		"CALL DOLT_CHECKOUT('x');",
		"CALL DOLT_MERGE('y', '-m', 'merge y into x');",
	} {
		query(ctx, q)
	}

	conflictsQuery := "SELECT base_id, base_a, base_c, CAST(base_j AS CHAR), our_a, their_a FROM dolt_conflicts_t ORDER BY base_id;"
	expected := []sql.Row{
		{int32(1), "seed", nil, `{}`, "y", "q"},
		{int32(2), "seed", nil, `{"x": 1, "y": 1}`, "x3", "y3"},
	}
	require.Equal(t, expected, query(ctx, conflictsQuery))

	db, ok := h.provider.BaseDatabase(ctx, "mydb")
	require.True(t, ok)
	ddb := db.(dsess.SqlDatabase).DbData().Ddb
	vrw, ns := ddb.ValueReadWriter(), ddb.NodeStore()
	meta := conflictMetadata(t, ctx, ddb, "x")
	var mergeBases []hash.Hash
	for _, branch := range []string{"x2", "y2"} {
		mergeBases = append(mergeBases, hash.Parse(query(ctx, "SELECT hashof('"+branch+"');")[0][0].(string)))
	}
	assert.ElementsMatch(t, mergeBases, meta.MergeBases)

	stored, err := vrw.ReadValue(ctx, meta.BaseRootIsh)
	require.NoError(t, err)
	require.NotNil(t, stored, "the virtual merge base is stored until garbage collection")
	cached, err := doltdb.LoadConflictBaseRoot(ctx, vrw, ns, meta)
	require.NoError(t, err)
	cachedHash, err := cached.HashOf()
	require.NoError(t, err)

	query(ctx, "CALL DOLT_GC();")

	stored, err = vrw.ReadValue(ctx, meta.BaseRootIsh)
	require.NoError(t, err)
	require.Nil(t, stored, "garbage collection removes the virtual merge base")
	for _, mergeBase := range meta.MergeBases {
		source, err := vrw.ReadValue(ctx, mergeBase)
		require.NoError(t, err)
		require.NotNil(t, source, "garbage collection keeps the merge bases")
	}

	_, err = doltdb.LoadConflictBaseRoot(ctx, vrw, ns, prolly.ConflictMetadata{BaseRootIsh: meta.BaseRootIsh})
	assert.Error(t, err, "conflict metadata without merge bases cannot rebuild a collected virtual merge base")

	// Reading the conflicts rebuilds the virtual merge base, merging JSON documents whatever the session's setting,
	// and writes it again.
	after := enginetest.NewSession(h)
	query(after, "CALL DOLT_CHECKOUT('x');")
	query(after, "SET @@dolt_dont_merge_json = 1;")
	assert.Equal(t, expected, query(after, conflictsQuery))
	stored, err = vrw.ReadValue(ctx, meta.BaseRootIsh)
	require.NoError(t, err)
	assert.NotNil(t, stored, "rebuilding writes the same virtual merge base again")

	rebuilt, err := doltdb.LoadConflictBaseRoot(ctx, vrw, ns, meta)
	require.NoError(t, err)
	rebuiltHash, err := rebuilt.HashOf()
	require.NoError(t, err)
	assert.Equal(t, cachedHash, rebuiltHash)
	requireIndexesMatchRows(t, ctx, rebuilt, "t")

}

// conflictMetadata returns the metadata of the conflicts of table t in the working set of |branch|, which must all
// have the same metadata.
func conflictMetadata(t *testing.T, ctx *sql.Context, ddb *doltdb.DoltDB, branch string) prolly.ConflictMetadata {
	wsRef, err := ref.WorkingSetRefForHead(ref.NewBranchRef(branch))
	require.NoError(t, err)
	ws, err := ddb.ResolveWorkingSet(ctx, wsRef)
	require.NoError(t, err)
	tbl, ok, err := ws.WorkingRoot().GetTable(ctx, doltdb.TableName{Name: "t"})
	require.NoError(t, err)
	require.True(t, ok)
	artifacts, err := tbl.GetArtifacts(ctx)
	require.NoError(t, err)
	iter, err := durable.ProllyMapFromArtifactIndex(artifacts).IterAllConflicts(ctx)
	require.NoError(t, err)
	var metas []prolly.ConflictMetadata
	for {
		art, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		metas = append(metas, art.Metadata)
	}
	require.NotEmpty(t, metas)
	for _, m := range metas[1:] {
		require.Equal(t, metas[0], m)
	}
	return metas[0]
}

// requireIndexesMatchRows checks that each secondary index of table |name| in |root| is the index its rows build.
func requireIndexesMatchRows(t *testing.T, ctx *sql.Context, root doltdb.RootValue, name string) {
	tbl, ok, err := root.GetTable(ctx, doltdb.TableName{Name: name})
	require.NoError(t, err)
	require.True(t, ok)
	sch, err := tbl.GetSchema(ctx)
	require.NoError(t, err)
	rowData, err := tbl.GetRowData(ctx)
	require.NoError(t, err)
	rows, err := durable.ProllyMapFromIndex(rowData)
	require.NoError(t, err)
	for _, index := range sch.Indexes().AllIndexes() {
		stored, err := tbl.GetIndexRowData(ctx, index.Name())
		require.NoError(t, err)
		built, err := creation.BuildProllyIndexExternal(ctx, tbl.ValueReadWriter(), tbl.NodeStore(), sch, name, index, rows, nil, nil)
		require.NoError(t, err)
		storedHash, err := stored.HashOf()
		require.NoError(t, err)
		builtHash, err := built.HashOf()
		require.NoError(t, err)
		require.Equal(t, builtHash, storedHash, "index %s", index.Name())
	}
}
