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

package merge

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/variables"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/types"
)

// MergeBases lists merge bases by committer date, newest first, as git does. With git 2.39, swapping the committer
// dates of two merge bases swaps their order in `git merge-base --all`, whatever their heights. git leaves equal dates
// in no defined order; MergeBases falls back to hash order.
func TestMergeBasesNewestFirst(t *testing.T) {
	// root -- t1 -- tall -- x
	//    \-- short ------- y
	// x = merge(tall, short), y = merge(short, tall)
	//
	// tall is the taller merge base.
	tests := []struct {
		name                string
		tallDate, shortDate int64
		first               string
	}{
		{name: "the shorter merge base is newer", tallDate: 100, shortDate: 200, first: "short"},
		{name: "the taller merge base is newer", tallDate: 200, shortDate: 100, first: "tall"},
		{name: "equal dates fall back to hash order", tallDate: 100, shortDate: 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			_, root, commit := newTestHistory(t)
			t1 := commit(50, root)
			tall := commit(tt.tallDate, t1)
			short := commit(tt.shortDate, root)
			x := commit(300, tall, short)
			y := commit(301, short, tall)

			expected := []hash.Hash{mustHashOf(t, tall), mustHashOf(t, short)}
			switch {
			case tt.first == "short":
				expected[0], expected[1] = expected[1], expected[0]
			case tt.first == "" && expected[1].String() < expected[0].String():
				expected[0], expected[1] = expected[1], expected[0]
			}
			for _, sides := range [][2]*doltdb.Commit{{x, y}, {y, x}} {
				bases, err := MergeBases(ctx, sides[0], sides[1])
				require.NoError(t, err)
				assert.Equal(t, expected, bases)
			}
		})
	}
}

// The virtual merge base of a criss-cross is the same commit whichever way it is resolved, and it can be rebuilt from
// its merge bases after garbage collection removes it.
func TestResolveMergeBaseRebuild(t *testing.T) {
	// root -- x1 -- x2
	//    \-- y1 -- y2
	// x2 = merge(x1, y1), y2 = merge(y1, x1)
	ctx := sql.NewContextWithVariables(context.Background(), variables.NewEngineVariables())
	ddb, root, commit := newTestHistory(t)
	x1 := commit(100, root)
	y1 := commit(200, root)
	x2 := commit(300, x1, y1)
	y2 := commit(301, y1, x1)

	virtualBase, err := ResolveMergeBase(ctx, x2, y2)
	require.NoError(t, err)
	again, err := ResolveMergeBase(ctx, y2, x2)
	require.NoError(t, err)
	assert.Equal(t, mustHashOf(t, virtualBase), mustHashOf(t, again))
	parents, err := virtualBase.ParentHashes(ctx)
	require.NoError(t, err)
	assert.ElementsMatch(t, []hash.Hash{mustHashOf(t, x1), mustHashOf(t, y1)}, parents)

	virtualRoot, err := virtualBase.GetRootValue(ctx)
	require.NoError(t, err)
	virtualRootHash, err := virtualRoot.HashOf()
	require.NoError(t, err)

	collected := hash.Of([]byte("garbage collected virtual merge base"))
	rebuilt, err := doltdb.LoadConflictBaseRoot(ctx, ddb.ValueReadWriter(), ddb.NodeStore(), prolly.ConflictMetadata{
		BaseRootIsh: collected,
		MergeBases:  []hash.Hash{mustHashOf(t, y1), mustHashOf(t, x1)},
	})
	require.NoError(t, err)
	rebuiltHash, err := rebuilt.HashOf()
	require.NoError(t, err)
	assert.Equal(t, virtualRootHash, rebuiltHash)

	_, err = doltdb.LoadConflictBaseRoot(ctx, ddb.ValueReadWriter(), ddb.NodeStore(), prolly.ConflictMetadata{BaseRootIsh: collected})
	assert.Error(t, err)
}

func TestConflictedCellsAdd(t *testing.T) {
	table := doltdb.TableName{Name: "t"}
	var cells conflictedCells
	cells = cells.add(conflictedCells{table: {"row 1": {tags: []uint64{1}}, "row 2": {wholeRow: true}}})
	cells = cells.add(conflictedCells{table: {"row 1": {tags: []uint64{2}}, "row 2": {tags: []uint64{1}}, "row 3": {tags: []uint64{3}}}})
	assert.Equal(t, conflictedCells{table: {
		"row 1": {tags: []uint64{1, 2}},
		"row 2": {wholeRow: true, tags: []uint64{1}},
		"row 3": {tags: []uint64{3}},
	}}, cells)
}

// newTestHistory returns an in-memory database, its root commit, and a function that commits with |parents| and a
// committer date of |date| seconds. Each commit gets its own branch, so that any parent list is legal.
func newTestHistory(t *testing.T) (*doltdb.DoltDB, *doltdb.Commit, func(date int64, parents ...*doltdb.Commit) *doltdb.Commit) {
	ctx := context.Background()
	ddb, err := doltdb.LoadDoltDB(ctx, types.Format_DOLT, doltdb.InMemDoltDB, filesys.LocalFS)
	require.NoError(t, err)
	t.Cleanup(func() { ddb.Close() })
	require.NoError(t, ddb.WriteEmptyRepo(ctx, "main", name, email))
	cs, err := doltdb.NewCommitSpec("main")
	require.NoError(t, err)
	optCmt, err := ddb.Resolve(ctx, cs, nil)
	require.NoError(t, err)
	root, ok := optCmt.ToCommit()
	require.True(t, ok)
	rootValue, err := root.GetRootValue(ctx)
	require.NoError(t, err)
	_, rootHash, err := ddb.WriteRootValue(ctx, rootValue)
	require.NoError(t, err)

	branch := 0
	commit := func(date int64, parents ...*doltdb.Commit) *doltdb.Commit {
		ident := datas.CommitIdent{Name: name, Email: email, Date: datas.CommitDateAt(time.Unix(date, 0))}
		meta, err := datas.NewCommitMetaWithAuthorCommitter(ident, ident, "commit")
		require.NoError(t, err)
		specs := make([]*doltdb.CommitSpec, len(parents))
		for i, p := range parents {
			specs[i], err = doltdb.NewCommitSpec(mustHashOf(t, p).String())
			require.NoError(t, err)
		}
		branch++
		c, err := ddb.CommitWithParentSpecs(ctx, rootHash, ref.NewBranchRef(fmt.Sprintf("b%d", branch)), specs, meta)
		require.NoError(t, err)
		return c
	}
	return ddb, root, commit
}

func mustHashOf(t *testing.T, c *doltdb.Commit) hash.Hash {
	h, err := c.HashOf()
	require.NoError(t, err)
	return h
}
