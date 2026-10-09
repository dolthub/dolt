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
