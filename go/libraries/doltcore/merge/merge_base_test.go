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
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

func TestMergeBases(t *testing.T) {
	ctx := context.Background()
	fs := filesys.NewInMemFS([]string{"/home", "/work"}, nil, "/work")
	dEnv := env.LoadWithoutDB(ctx, func() (string, error) { return "/home", nil }, fs, doltdb.InMemDoltDB, "test")
	require.NoError(t, dEnv.InitRepo(ctx, types.Format_DOLT, name, email, env.DefaultInitBranch))
	ddb := dEnv.DoltDB(ctx)

	initial := resolveCommit(t, ddb, env.DefaultInitBranch)
	root, err := initial.GetRootValue(ctx)
	require.NoError(t, err)
	_, rootHash, err := ddb.WriteRootValue(ctx, root)
	require.NoError(t, err)

	commitTime := time.UnixMilli(0)
	commitOn := func(branch string, parents ...*doltdb.Commit) *doltdb.Commit {
		commitTime = commitTime.Add(time.Second)
		meta, err := datas.NewCommitMetaWithAuthor(name, email, "commit", commitTime)
		require.NoError(t, err)
		specs := make([]*doltdb.CommitSpec, len(parents))
		for i, p := range parents {
			specs[i], err = doltdb.NewCommitSpec(mustHashOf(t, p).String())
			require.NoError(t, err)
		}
		c, err := ddb.CommitWithParentSpecs(ctx, rootHash, ref.NewBranchRef(branch), specs, meta)
		require.NoError(t, err)
		return c
	}

	// Criss-cross: x1 and y1 are both merged into x2 and y2.
	x1 := commitOn("x", initial)
	y1 := commitOn("y", initial)
	x2 := commitOn("x", x1, y1)
	y2 := commitOn("y", y1, x1)

	bases, err := MergeBases(ctx, x2, y2)
	require.NoError(t, err)
	assert.ElementsMatch(t, []hash.Hash{mustHashOf(t, x1), mustHashOf(t, y1)}, bases)

	ancestors, err := doltdb.GetCommitAncestors(ctx, x2, y2)
	require.NoError(t, err)
	ancestorHashes := make([]hash.Hash, len(ancestors))
	for i, a := range ancestors {
		c, ok := a.ToCommit()
		require.True(t, ok)
		ancestorHashes[i] = mustHashOf(t, c)
	}
	assert.ElementsMatch(t, bases, ancestorHashes)

	bases, err = MergeBases(ctx, x1, y1)
	require.NoError(t, err)
	assert.Equal(t, []hash.Hash{mustHashOf(t, initial)}, bases)
}

func resolveCommit(t *testing.T, ddb *doltdb.DoltDB, branch string) *doltdb.Commit {
	cs, err := doltdb.NewCommitSpec(branch)
	require.NoError(t, err)
	opt, err := ddb.Resolve(context.Background(), cs, ref.NewBranchRef(branch))
	require.NoError(t, err)
	c, ok := opt.ToCommit()
	require.True(t, ok)
	return c
}

func mustHashOf(t *testing.T, c *doltdb.Commit) hash.Hash {
	h, err := c.HashOf()
	require.NoError(t, err)
	return h
}

// GetCommitAncestor must keep returning what datas.FindCommonAncestor returns: the tallest common ancestor, and on a
// tie the one with the larger hash.
func TestGetCommitAncestorMatchesFindCommonAncestor(t *testing.T) {
	ctx := context.Background()
	fs := filesys.NewInMemFS([]string{"/home", "/work"}, nil, "/work")
	dEnv := env.LoadWithoutDB(ctx, func() (string, error) { return "/home", nil }, fs, doltdb.InMemDoltDB, "test")
	require.NoError(t, dEnv.InitRepo(ctx, types.Format_DOLT, name, email, env.DefaultInitBranch))
	ddb := dEnv.DoltDB(ctx)

	initial := resolveCommit(t, ddb, env.DefaultInitBranch)
	root, err := initial.GetRootValue(ctx)
	require.NoError(t, err)
	_, rootHash, err := ddb.WriteRootValue(ctx, root)
	require.NoError(t, err)

	commitTime := time.UnixMilli(0)
	commitCount := 0
	commit := func(parents ...*doltdb.Commit) *doltdb.Commit {
		commitTime = commitTime.Add(time.Second)
		commitCount++
		meta, err := datas.NewCommitMetaWithAuthor(name, email, "commit", commitTime)
		require.NoError(t, err)
		specs := make([]*doltdb.CommitSpec, len(parents))
		for i, p := range parents {
			specs[i], err = doltdb.NewCommitSpec(mustHashOf(t, p).String())
			require.NoError(t, err)
		}
		// Every commit gets its own branch, so that any parent list is legal.
		c, err := ddb.CommitWithParentSpecs(ctx, rootHash, ref.NewBranchRef(fmt.Sprintf("c%d", commitCount)), specs, meta)
		require.NoError(t, err)
		return c
	}
	findCommonAncestor := func(left, right *doltdb.Commit) hash.Hash {
		l, err := datas.LoadCommitAddr(ctx, ddb.ValueReadWriter(), mustHashOf(t, left))
		require.NoError(t, err)
		r, err := datas.LoadCommitAddr(ctx, ddb.ValueReadWriter(), mustHashOf(t, right))
		require.NoError(t, err)
		h, ok, err := datas.FindCommonAncestor(ctx, l, r, ddb.ValueReadWriter(), ddb.ValueReadWriter(), ddb.NodeStore(), ddb.NodeStore())
		require.NoError(t, err)
		require.True(t, ok)
		return h
	}

	rnd := rand.New(rand.NewSource(12050))
	ties := 0
	for graph := 0; graph < 30; graph++ {
		branches := 2 + rnd.Intn(4)
		tips := make([]*doltdb.Commit, branches)
		for i := range tips {
			tips[i] = commit(initial)
		}
		all := append([]*doltdb.Commit{}, tips...)
		for i := 0; i < 30; i++ {
			b := rnd.Intn(branches)
			parents := []*doltdb.Commit{tips[b]}
			if other := tips[rnd.Intn(branches)]; rnd.Float64() < 0.5 && mustHashOf(t, other) != mustHashOf(t, tips[b]) {
				parents = append(parents, other)
			}
			tips[b] = commit(parents...)
			all = append(all, tips[b])
		}
		for pair := 0; pair < 20; pair++ {
			left, right := all[rnd.Intn(len(all))], all[rnd.Intn(len(all))]
			expected := findCommonAncestor(left, right)
			optCmt, err := doltdb.GetCommitAncestor(ctx, left, right)
			require.NoError(t, err)
			require.Equal(t, expected, optCmt.Addr, "graph %d, pair %d", graph, pair)

			bases, err := MergeBases(ctx, left, right)
			require.NoError(t, err)
			if len(bases) > 1 {
				heights := map[uint64]int{}
				for _, b := range bases {
					optCmt, err := ddb.ReadCommit(ctx, b)
					require.NoError(t, err)
					c, ok := optCmt.ToCommit()
					require.True(t, ok)
					h, err := c.Height()
					require.NoError(t, err)
					heights[h]++
				}
				for _, n := range heights {
					if n > 1 {
						ties++
					}
				}
			}
		}
	}
	assert.Greater(t, ties, 0, "the random histories should include merge bases of equal height")
}
