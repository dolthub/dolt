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

package doltdb

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// testHistory builds commits with any parents in an in-memory database. Each commit gets its own branch, so that any
// parent list is legal, and a committer date one second after the previous commit.
type testHistory struct {
	t        *testing.T
	ddb      *DoltDB
	root     *Commit
	rootHash hash.Hash
	date     time.Time
	count    int
}

func newTestHistory(t *testing.T) *testHistory {
	ctx := context.Background()
	ddb, err := LoadDoltDB(ctx, types.Format_DOLT, InMemDoltDB, filesys.LocalFS)
	require.NoError(t, err)
	t.Cleanup(func() { ddb.Close() })
	require.NoError(t, ddb.WriteEmptyRepo(ctx, "main", "Bill Billerson", "bigbillieb@fake.horse"))
	cs, err := NewCommitSpec("main")
	require.NoError(t, err)
	optCmt, err := ddb.Resolve(ctx, cs, nil)
	require.NoError(t, err)
	root, ok := optCmt.ToCommit()
	require.True(t, ok)
	rootValue, err := root.GetRootValue(ctx)
	require.NoError(t, err)
	_, rootHash, err := ddb.WriteRootValue(ctx, rootValue)
	require.NoError(t, err)
	return &testHistory{t: t, ddb: ddb, root: root, rootHash: rootHash, date: time.UnixMilli(0)}
}

func (h *testHistory) commit(parents ...*Commit) *Commit {
	h.date = h.date.Add(time.Second)
	h.count++
	meta, err := datas.NewCommitMetaWithAuthor("Bill Billerson", "bigbillieb@fake.horse", "commit", h.date)
	require.NoError(h.t, err)
	specs := make([]*CommitSpec, len(parents))
	for i, p := range parents {
		specs[i], err = NewCommitSpec(h.hashOf(p).String())
		require.NoError(h.t, err)
	}
	c, err := h.ddb.CommitWithParentSpecs(context.Background(), h.rootHash, ref.NewBranchRef(fmt.Sprintf("c%d", h.count)), specs, meta)
	require.NoError(h.t, err)
	return c
}

func (h *testHistory) hashOf(c *Commit) hash.Hash {
	addr, err := c.HashOf()
	require.NoError(h.t, err)
	return addr
}

func (h *testHistory) height(c *Commit) uint64 {
	height, err := c.Height()
	require.NoError(h.t, err)
	return height
}

func TestCommitAncestry(t *testing.T) {
	// root -- a -- b
	//    \-- c
	ctx := context.Background()
	h := newTestHistory(t)
	a := h.commit(h.root)
	b := h.commit(a)
	c := h.commit(h.root)

	bAncestry, err := b.GetAncestry(ctx)
	require.NoError(t, err)
	for _, tt := range []struct {
		name     string
		commit   *Commit
		ancestor bool
	}{
		{"itself", b, true},
		{"its parent", a, true},
		{"the root", h.root, true},
		{"a commit at its parent's height on another branch", c, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			contains, err := bAncestry.Contains(ctx, h.hashOf(tt.commit), h.height(tt.commit))
			require.NoError(t, err)
			assert.Equal(t, tt.ancestor, contains)
		})
	}

	height, err := bAncestry.HeightOf(ctx, h.hashOf(h.root))
	require.NoError(t, err)
	assert.Equal(t, h.height(h.root), height)
	_, err = bAncestry.HeightOf(ctx, h.hashOf(c))
	assert.Error(t, err)

	// The root has no parents, so its closure is empty.
	rootAncestry, err := h.root.GetAncestry(ctx)
	require.NoError(t, err)
	contains, err := rootAncestry.Contains(ctx, h.hashOf(h.root), h.height(h.root))
	require.NoError(t, err)
	assert.True(t, contains)
	contains, err = rootAncestry.Contains(ctx, h.hashOf(a), h.height(a))
	require.NoError(t, err)
	assert.False(t, contains)
}

// GetCommitAncestor must keep returning what datas.FindCommonAncestor returned before it: the tallest common ancestor,
// and on a tie the one with the larger hash. This runs the real CommitResolver, with parent closures from storage, on
// random histories where branches merge each other.
func TestGetCommitAncestorMatchesFindCommonAncestor(t *testing.T) {
	ctx := context.Background()
	h := newTestHistory(t)
	findCommonAncestor := func(left, right *Commit) hash.Hash {
		addr, ok, err := datas.FindCommonAncestor(ctx, left.dCommit, right.dCommit, h.ddb.vrw, h.ddb.vrw, h.ddb.ns, h.ddb.ns)
		require.NoError(t, err)
		require.True(t, ok)
		return addr
	}

	rnd := rand.New(rand.NewSource(12050))
	ties := 0
	for graph := 0; graph < 30; graph++ {
		branches := 2 + rnd.Intn(4)
		tips := make([]*Commit, branches)
		for i := range tips {
			tips[i] = h.commit(h.root)
		}
		all := append([]*Commit{}, tips...)
		for i := 0; i < 30; i++ {
			b := rnd.Intn(branches)
			parents := []*Commit{tips[b]}
			if other := tips[rnd.Intn(branches)]; rnd.Float64() < 0.5 && h.hashOf(other) != h.hashOf(tips[b]) {
				parents = append(parents, other)
			}
			tips[b] = h.commit(parents...)
			all = append(all, tips[b])
		}
		for pair := 0; pair < 20; pair++ {
			left, right := all[rnd.Intn(len(all))], all[rnd.Intn(len(all))]
			optCmt, err := GetCommitAncestor(ctx, left, right)
			require.NoError(t, err)
			require.Equal(t, findCommonAncestor(left, right), optCmt.Addr, "graph %d, pair %d", graph, pair)

			bases, err := GetCommitAncestors(ctx, left, right)
			require.NoError(t, err)
			heights := map[uint64]int{}
			for _, base := range bases {
				c, ok := base.ToCommit()
				require.True(t, ok)
				heights[h.height(c)]++
			}
			for _, n := range heights {
				if n > 1 {
					ties++
				}
			}
		}
	}
	assert.Greater(t, ties, 0, "the random histories should include merge bases of equal height")
}
