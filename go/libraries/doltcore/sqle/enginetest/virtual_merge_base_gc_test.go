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
	dtu "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
)

// TestVirtualMergeBaseRebuiltAfterGC leaves a merge with conflicts against a virtual merge base in progress, garbage
// collects the virtual merge base, and checks that it is rebuilt the same way, for several kinds of virtual merge base.
func TestVirtualMergeBaseRebuiltAfterGC(t *testing.T) {
	ours := "CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');"
	for _, test := range []struct {
		name    string
		history dtu.CrissCross
		// query, when set, reads the conflicts, which must be |expected|.
		query    string
		expected []sql.Row
	}{
		{
			// Two criss-crosses, one on top of the other. x1 adds column c, and x1 and y1 conflict on row 1's a. x2
			// resolves it to 'seed' and y2 to 'y', so x2 and y2 conflict again, and x2 and y2 add keys x and y to row
			// 2's j, which merge cleanly only with JSON merging. x3 and y3 resolve row 1's a to 'y' and 'q', and set
			// row 2's a to 'x3' and 'y3'. The virtual merge base of x3 and y3 merges x2 and y2 against the virtual
			// merge base of x1 and y1.
			name: "nested, with a schema change and JSON documents",
			history: dtu.CrissCross{
				Seed: []string{
					"CREATE TABLE t (id INT PRIMARY KEY, a VARCHAR(100), j JSON, INDEX ia (a));",
					"INSERT INTO t VALUES (1, 'seed', '{}'), (2, 'seed', '{}');",
				},
				X1:     []string{"ALTER TABLE t ADD COLUMN c VARCHAR(100);", "UPDATE t SET a = 'x' WHERE id = 1;"},
				Y1:     []string{"UPDATE t SET a = 'y' WHERE id = 1;"},
				Y2:     []string{ours, "UPDATE t SET a = 'y' WHERE id = 1;", `UPDATE t SET j = '{"y": 1}' WHERE id = 2;`},
				X2:     []string{ours, "UPDATE t SET a = 'seed' WHERE id = 1;", `UPDATE t SET j = '{"x": 1}' WHERE id = 2;`},
				Nested: true,
				Y3:     []string{ours, "UPDATE t SET a = 'q' WHERE id = 1;", "UPDATE t SET a = 'y3' WHERE id = 2;"},
				X3:     []string{ours, "UPDATE t SET a = 'y' WHERE id = 1;", "UPDATE t SET a = 'x3' WHERE id = 2;"},
			},
			query: "SELECT base_id, base_a, base_c, CAST(base_j AS CHAR), our_a, their_a FROM dolt_conflicts_t ORDER BY base_id;",
			expected: []sql.Row{
				{int32(1), "seed", nil, `{}`, "y", "q"},
				{int32(2), "seed", nil, `{"x": 1, "y": 1}`, "x3", "y3"},
			},
		},
		{
			// The chained stored generated columns g and h, with an index on h.
			name: "chained stored generated columns",
			history: dtu.CrissCross{
				Seed: []string{
					"CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, g INT AS (a + b) STORED, h INT AS (g * 10) STORED, INDEX ih (h));",
					"INSERT INTO t (id, a, b) VALUES (1, 0, 0);",
				},
				X1: []string{"UPDATE t SET a = 1, b = 1 WHERE id = 1;"},
				Y1: []string{"UPDATE t SET a = 3 WHERE id = 1;"},
				Y2: []string{ours, "UPDATE t SET a = 20, b = 1 WHERE id = 1;"},
				X2: []string{ours, "UPDATE t SET a = 10, b = 1 WHERE id = 1;"},
			},
		},
		{
			// x1 and y1 conflict on row 1's indexed a, and x2 and y2 resolve it differently.
			name: "Full-Text index",
			history: dtu.CrissCross{
				Seed: []string{
					"CREATE TABLE t (id INT PRIMARY KEY, a VARCHAR(100), FULLTEXT INDEX fa (a));",
					"INSERT INTO t VALUES (1, 'seed'), (2, 'seed');",
				},
				X1: []string{"UPDATE t SET a = 'alpha';"},
				Y1: []string{"UPDATE t SET a = 'bravo' WHERE id = 1;"},
				Y2: []string{ours, "UPDATE t SET a = 'yresolved' WHERE id = 1;"},
				X2: []string{ours, "UPDATE t SET a = 'xresolved' WHERE id = 1;"},
			},
		},
		{
			name:    "keyless rows under their merge base's schema",
			history: keylessSchemaChangeMergeBases(),
		},
		{
			// x1 adds ts with a time default, and y1 adds row 2, which takes the default in the virtual merge base.
			name:    "time default",
			history: timeDefaultMergeBases(),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			checkRebuiltAfterGC(t, test.history, test.query, test.expected)
		})
	}
}

// checkRebuiltAfterGC builds |history|, merges branch y into x, which must conflict on table t against a virtual
// merge base, and garbage collects the virtual merge base. It checks that the conflicts record the merge bases, that
// the virtual merge base is gone while they remain, that metadata without merge bases cannot rebuild it, that a fresh
// session reads the same conflicts, |expected| from |query| when set, whatever its JSON merge setting and time zone,
// that rebuilding gives the same root with indexes that match its rows, and that resolving the conflicts commits.
func checkRebuiltAfterGC(t *testing.T, history dtu.CrissCross, query string, expected []sql.Row) {
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
	for _, q := range history.Script() {
		run(ctx, q)
	}
	merging := enginetest.NewSession(h)
	for _, q := range []string{"SET @@dolt_allow_commit_conflicts = 1;", "CALL DOLT_CHECKOUT('x');", "CALL DOLT_MERGE('y', '-m', 'merge y into x');"} {
		run(merging, q)
	}
	if query == "" {
		query = "SELECT * FROM dolt_conflicts_t ORDER BY dolt_conflict_id;"
	}
	before := run(merging, query)
	require.NotEmpty(t, before)
	if expected != nil {
		require.Equal(t, expected, before)
	}

	db, ok := h.provider.BaseDatabase(merging, "mydb")
	require.True(t, ok)
	ddb := db.(dsess.SqlDatabase).DbData().Ddb
	vrw, ns := ddb.ValueReadWriter(), ddb.NodeStore()
	meta := conflictMetadata(t, merging, ddb, "x")
	var mergeBases []hash.Hash
	for _, row := range run(merging, "SELECT * FROM dolt_merge_bases('x', 'y');") {
		mergeBases = append(mergeBases, hash.Parse(row[0].(string)))
	}
	assert.ElementsMatch(t, mergeBases, meta.MergeBases)
	cached, err := doltdb.LoadConflictBaseRoot(merging, vrw, ns, meta)
	require.NoError(t, err)
	cachedHash, err := cached.HashOf()
	require.NoError(t, err)

	run(merging, "CALL DOLT_GC();")
	stored, err := vrw.ReadValue(merging, meta.BaseRootIsh)
	require.NoError(t, err)
	require.Nil(t, stored, "garbage collection removes the virtual merge base")
	for _, mergeBase := range meta.MergeBases {
		source, err := vrw.ReadValue(merging, mergeBase)
		require.NoError(t, err)
		require.NotNil(t, source, "garbage collection keeps the merge bases")
	}
	_, err = doltdb.LoadConflictBaseRoot(merging, vrw, ns, prolly.ConflictMetadata{BaseRootIsh: meta.BaseRootIsh})
	assert.Error(t, err, "conflict metadata without merge bases cannot rebuild a collected virtual merge base")

	after := enginetest.NewSession(h)
	run(after, "CALL DOLT_CHECKOUT('x');")
	run(after, "SET @@dolt_dont_merge_json = 1;")
	run(after, "SET time_zone = '-07:00';")
	assert.Equal(t, before, run(after, query))
	rebuilt, err := doltdb.LoadConflictBaseRoot(after, vrw, ns, meta)
	require.NoError(t, err)
	rebuiltHash, err := rebuilt.HashOf()
	require.NoError(t, err)
	assert.Equal(t, cachedHash, rebuiltHash)
	stale, err := dtu.StaleIndexes(after, rebuilt)
	require.NoError(t, err)
	assert.Empty(t, stale)

	run(after, "SET @@dolt_allow_commit_conflicts = 1;")
	run(after, "CALL DOLT_CONFLICTS_RESOLVE('--theirs', 't');")
	run(after, "CALL DOLT_COMMIT('-am', 'resolve merge of y');")
	assert.Equal(t, []sql.Row{{int64(0)}}, run(after, "SELECT count(*) FROM dolt_status;"))
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
