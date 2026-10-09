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

package datas

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// commitGraph builds commits in a test database. Every commit gets its own dataset so that any parent list is legal.
type commitGraph struct {
	t       *testing.T
	db      *database
	commits map[string]types.Value
}

func newCommitGraph(t *testing.T, db *database) *commitGraph {
	return &commitGraph{t: t, db: db, commits: map[string]types.Value{}}
}

func (g *commitGraph) add(name string, parents ...string) {
	parentValues := make([]types.Value, len(parents))
	for i, p := range parents {
		parentValues[i] = g.commits[p]
	}
	g.commits[name], _ = addCommit(g.t, g.db, "ds-"+name, name, parentValues...)
}

func (g *commitGraph) commit(name string) *Commit {
	c, err := LoadCommitRef(context.Background(), g.db, mustRef(types.NewRef(g.commits[name], g.db.Format())))
	require.NoError(g.t, err)
	return c
}

func (g *commitGraph) addr(name string) hash.Hash {
	return mustRef(types.NewRef(g.commits[name], g.db.Format())).TargetHash()
}

func (g *commitGraph) assertMergeBases(t *testing.T, left, right string, expected ...string) {
	t.Helper()
	expectedAddrs := make([]hash.Hash, len(expected))
	for i, e := range expected {
		expectedAddrs[i] = g.addr(e)
	}
	sort.Slice(expectedAddrs, func(i, j int) bool { return expectedAddrs[i].Less(expectedAddrs[j]) })

	for _, order := range [][2]string{{left, right}, {right, left}} {
		found, err := FindAllCommonAncestors(context.Background(), g.commit(order[0]), g.commit(order[1]), g.db, g.db)
		require.NoError(t, err)
		sorted := append([]hash.Hash{}, found...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i].Less(sorted[j]) })
		assert.Equal(t, expectedAddrs, sorted, "merge bases of %s and %s", order[0], order[1])
	}
}

func TestFindAllCommonAncestors(t *testing.T) {
	storage := &chunks.TestStorage{}
	db := NewDatabase(storage.NewViewWithDefaultFormat()).(*database)
	defer db.Close()
	g := newCommitGraph(t, db)

	// Linear history and a single fork.
	//
	//   root <- a1 <- a2 <- a3
	//                   \
	//                    b3
	g.add("root")
	g.add("a1", "root")
	g.add("a2", "a1")
	g.add("a3", "a2")
	g.add("b3", "a2")
	t.Run("self", func(t *testing.T) { g.assertMergeBases(t, "a2", "a2", "a2") })
	t.Run("ancestor", func(t *testing.T) { g.assertMergeBases(t, "a1", "a3", "a1") })
	t.Run("fork", func(t *testing.T) { g.assertMergeBases(t, "a3", "b3", "a2") })

	// Criss-cross: x1 and y1 are both merged into x2 and y2.
	//
	//   a2 <- x1 <- x2
	//     \      \/
	//      \     /\
	//       <- y1 <- y2
	g.add("x1", "a2")
	g.add("y1", "a2")
	g.add("x2", "x1", "y1")
	g.add("y2", "y1", "x1")
	t.Run("criss-cross", func(t *testing.T) { g.assertMergeBases(t, "x2", "y2", "x1", "y1") })

	// Three bases: p, q, r are all merged into both m1 and m2.
	g.add("p", "a2")
	g.add("q", "a2")
	g.add("r", "a2")
	g.add("m1", "p", "q", "r")
	g.add("m2", "r", "q", "p")
	t.Run("three bases", func(t *testing.T) { g.assertMergeBases(t, "m1", "m2", "p", "q", "r") })

	// The schema-branch pattern from https://github.com/dolthub/dolt/issues/12050: the schema tip s2 is taller than
	// the fork point f, and both are merged into main and feature.
	g.add("f", "root")
	g.add("main1", "f")
	g.add("feat1", "f")
	g.add("s1", "root")
	g.add("s2", "s1")
	g.add("s3", "s2")
	g.add("main2", "main1", "s3")
	g.add("feat2", "feat1", "s3")
	t.Run("uneven heights", func(t *testing.T) { g.assertMergeBases(t, "main2", "feat2", "f", "s3") })

	// A common ancestor reachable from a base through another path is not a base. k1 and k2 reach p1 without passing
	// through the base p3, but p1 is an ancestor of p3.
	//
	//   root <- p1 <- p2 <- p3 <- c1, c2
	//             \
	//              k1 <- c1
	//              k2 <- c2
	g.add("p1", "root")
	g.add("p2", "p1")
	g.add("p3", "p2")
	g.add("k1", "p1")
	g.add("k2", "p1")
	g.add("c1", "p3", "k1")
	g.add("c2", "p3", "k2")
	t.Run("ancestor of a base is not a base", func(t *testing.T) { g.assertMergeBases(t, "c1", "c2", "p3") })

	g.add("unrelated")
	t.Run("no common ancestor", func(t *testing.T) { g.assertMergeBases(t, "a3", "unrelated") })
}

func TestFindAllCommonAncestorsDifferentValueReaders(t *testing.T) {
	ldb := NewDatabase((&chunks.TestStorage{}).NewViewWithDefaultFormat()).(*database)
	defer ldb.Close()
	rdb := NewDatabase((&chunks.TestStorage{}).NewViewWithDefaultFormat()).(*database)
	defer rdb.Close()
	left, right := newCommitGraph(t, ldb), newCommitGraph(t, rdb)

	for _, g := range []*commitGraph{left, right} {
		g.add("root")
		g.add("x1", "root")
		g.add("y1", "root")
	}
	left.add("x2", "x1", "y1")
	right.add("y2", "y1", "x1")

	found, err := FindAllCommonAncestors(context.Background(), left.commit("x2"), right.commit("y2"), ldb, rdb)
	require.NoError(t, err)
	assert.ElementsMatch(t, []hash.Hash{left.addr("x1"), left.addr("y1")}, found)
}
