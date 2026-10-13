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

package commitgraph

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
)

// testGraph is an in-memory HashResolver. Commits are named, and their heights are computed from their parents. With
// |withIndex| set, it is also an AncestryIndex, as the stored parent closures of Dolt commits are.
type testGraph struct {
	infos   map[hash.Hash]*CommitInfo
	names   map[hash.Hash]string
	heights map[string]uint64
	// parents are the parents the walk can see. A ghost has none.
	parents map[string][]string
	// indexedParents are the true parents, which an ancestry index knows even for a ghost.
	indexedParents map[string][]string
	order          []string
	withIndex      bool
	resolved       int
	indexLookups   int
}

func newTestGraph() *testGraph {
	return &testGraph{
		infos:          map[hash.Hash]*CommitInfo{},
		names:          map[hash.Hash]string{},
		heights:        map[string]uint64{},
		parents:        map[string][]string{},
		indexedParents: map[string][]string{},
	}
}

// indexedTestGraph adds an AncestryIndex to a testGraph.
type indexedTestGraph struct {
	*testGraph
}

func (g indexedTestGraph) IsAncestor(_ context.Context, ancestor *CommitInfo, descendant hash.Hash) (bool, error) {
	g.indexLookups++
	return g.ancestorsIn(g.indexedParents, g.names[descendant])[g.names[ancestor.Hash]], nil
}

func (g indexedTestGraph) GhostHeight(_ context.Context, ghost hash.Hash, _ *CommitInfo) (uint64, error) {
	return g.heights[g.names[ghost]], nil
}

func (g *testGraph) resolver() HashResolver {
	if g.withIndex {
		return indexedTestGraph{g}
	}
	return g
}

// parseTestGraph builds a graph from lines of "commit: parent1 parent2", listing each commit after its parents. A
// commit prefixed with "~" is a ghost: its parents are listed only to compute heights, and the walk cannot see them.
func parseTestGraph(spec string) *testGraph {
	g := newTestGraph()
	for _, line := range strings.Split(spec, "\n") {
		commit, parents, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		name, ghost := strings.CutPrefix(strings.TrimSpace(commit), "~")
		g.add(name, ghost, strings.Fields(parents)...)
	}
	return g
}

func (g *testGraph) add(name string, ghost bool, parents ...string) {
	if _, ok := g.heights[name]; ok {
		panic("duplicate commit " + name)
	}
	height := uint64(1)
	parentHashes := make([]hash.Hash, len(parents))
	for i, p := range parents {
		ph, ok := g.heights[p]
		if !ok {
			panic(fmt.Sprintf("commit %s has unknown parent %s", name, p))
		}
		height = max(height, ph+1)
		parentHashes[i] = hashOfName(p)
	}
	h := hashOfName(name)
	g.heights[name] = height
	g.parents[name] = parents
	g.indexedParents[name] = parents
	g.names[h] = name
	g.order = append(g.order, name)
	if ghost {
		g.infos[h] = &CommitInfo{Hash: h, IsGhost: true}
		g.parents[name] = nil
	} else {
		g.infos[h] = &CommitInfo{Hash: h, Height: height, Parents: parentHashes}
	}
}

func hashOfName(name string) hash.Hash {
	return hash.Of([]byte(name))
}

func (g *testGraph) ResolveCommitHash(_ context.Context, h hash.Hash) (*CommitInfo, error) {
	info, ok := g.infos[h]
	if !ok {
		return nil, fmt.Errorf("unknown commit %s", h)
	}
	g.resolved++
	return info, nil
}

func (g *testGraph) mergeBases(t *testing.T, left, right string) []string {
	t.Helper()
	bases, err := GetMergeBases(context.Background(), g.resolver(), hashOfName(left), g.resolver(), hashOfName(right))
	require.NoError(t, err)
	names := make([]string, len(bases))
	for i, b := range bases {
		names[i] = g.names[b]
	}
	sort.Strings(names)
	return names
}

// referenceMergeBases computes the best common ancestors by definition, the way `git merge-base --all` defines them:
// collect every ancestor of |left| and of |right|, keep the commits in both, and drop each one that is an ancestor of
// another. It shares no code with GetMergeBases and is slow, but short enough to check by reading. Ghosts have no known
// parents.
func (g *testGraph) referenceMergeBases(left, right string) []string {
	leftAncestors, rightAncestors := g.ancestors(left), g.ancestors(right)
	var common []string
	for c := range leftAncestors {
		if rightAncestors[c] {
			common = append(common, c)
		}
	}
	var commonParents []string
	for _, c := range common {
		commonParents = append(commonParents, g.parents[c]...)
	}
	properAncestorsOfCommon := g.ancestors(commonParents...)
	bases := []string{}
	for _, c := range common {
		if !properAncestorsOfCommon[c] {
			bases = append(bases, c)
		}
	}
	sort.Strings(bases)
	return bases
}

// ancestors returns |names| and all of their ancestors that the walk can see.
func (g *testGraph) ancestors(names ...string) map[string]bool {
	return g.ancestorsIn(g.parents, names...)
}

func (g *testGraph) ancestorsIn(parents map[string][]string, names ...string) map[string]bool {
	seen := map[string]bool{}
	stack := append([]string{}, names...)
	for len(stack) > 0 {
		c := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[c] {
			continue
		}
		seen[c] = true
		stack = append(stack, parents[c]...)
	}
	return seen
}

// crissCrossRounds builds two branches from |fork| that merge each other every round:
//
//	fork -- x0 -- x1 -- x2 -- ... -- x<rounds>
//	fork -- y0 -- y1 -- y2 -- ... -- y<rounds>
//	x<i> = merge(x<i-1>, y<i-1>), y<i> = merge(y<i-1>, x<i-1>)
func crissCrossRounds(fork string, rounds int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "x0: %s\ny0: %s\n", fork, fork)
	for i := 1; i <= rounds; i++ {
		fmt.Fprintf(&b, "x%d: x%d y%d\ny%d: y%d x%d\n", i, i-1, i-1, i, i-1, i-1)
	}
	return b.String()
}

// chain builds |n| commits on top of |from| and returns them and the last one:
//
//	from -- <prefix>1 -- <prefix>2 -- ... -- <prefix><n>
func chain(prefix, from string, n int) (string, string) {
	var b strings.Builder
	prev := from
	for i := 1; i <= n; i++ {
		name := fmt.Sprintf("%s%d", prefix, i)
		fmt.Fprintf(&b, "%s: %s\n", name, prev)
		prev = name
	}
	return b.String(), prev
}

// fanIn builds |n| independent commits p1..p<n> on top of root. m1 merges them in order and m2 in reverse order, so m1
// and m2 have |n| merge bases. For n = 3:
//
//	root -- p1
//	root -- p2
//	root -- p3
//	m1_1 = merge(p1, p2), m1 = merge(m1_1, p3)
//	m2_1 = merge(p3, p2), m2 = merge(m2_1, p1)
func fanIn(n int) string {
	var b strings.Builder
	b.WriteString("root:\n")
	var ps []string
	for i := 1; i <= n; i++ {
		p := fmt.Sprintf("p%d", i)
		ps = append(ps, p)
		fmt.Fprintf(&b, "%s: root\n", p)
	}
	reversed := make([]string, n)
	for i, p := range ps {
		reversed[n-1-i] = p
	}
	for _, side := range []struct {
		name string
		ps   []string
	}{{"m1", ps}, {"m2", reversed}} {
		prev := side.ps[0]
		for i, p := range side.ps[1:] {
			name := fmt.Sprintf("%s_%d", side.name, i+1)
			if i == n-2 {
				name = side.name
			}
			fmt.Fprintf(&b, "%s: %s %s\n", name, prev, p)
			prev = name
		}
	}
	return b.String()
}

// linearHistory is a fork and an unrelated root:
//
//	root -- a1 -- a2 -- a3
//	              a2 -- b3
//	unrelated
const linearHistory = `
	root:
	a1: root
	a2: a1
	a3: a2
	b3: a2
	unrelated:
`

// crissCross is the smallest history with two merge bases:
//
//	root -- x0 -- x1
//	root -- y0 -- y1
//	x1 = merge(x0, y0), y1 = merge(y0, x0)
const crissCross = `
	root:
	x0: root
	y0: root
	x1: x0 y0
	y1: y0 x0
`

// fourBranches has branches a, b, c and d from root. c and d merge each other every round, and a and b merge from the
// others now and then:
//
//	root -- a0 -- a1 -- ... -- a10
//	root -- b0 -- b1 -- ... -- b10
//	root -- c0 -- c1 -- ... -- c10    c<i> = merge(c<i-1>, d<i-1>)
//	root -- d0 -- d1 -- ... -- d10    d<i> = merge(d<i-1>, c<i-1>)
//	a3 = merge(a2, c3), a6 = merge(a5, b5), a7 = merge(a6, d7)
//	b4 = merge(b3, d4), b8 = merge(b7, c8)
const fourBranches = `
	root:
	a0: root
	b0: root
	c0: root
	d0: root
	c1: c0 d0
	d1: d0 c0
	a1: a0
	b1: b0
	c2: c1 d1
	d2: d1 c1
	a2: a1
	b2: b1
	c3: c2 d2
	d3: d2 c2
	a3: a2 c3
	b3: b2
	c4: c3 d3
	d4: d3 c3
	a4: a3
	b4: b3 d4
	c5: c4 d4
	d5: d4 c4
	a5: a4
	b5: b4
	c6: c5 d5
	d6: d5 c5
	a6: a5 b5
	b6: b5
	c7: c6 d6
	d7: d6 c6
	a7: a6 d7
	b7: b6
	c8: c7 d7
	d8: d7 c7
	a8: a7
	b8: b7 c8
	c9: c8 d8
	d9: d8 c8
	a9: a8
	b9: b8
	c10: c9 d9
	d10: d9 c9
	a10: a9
	b10: b9
`

type mergeBasesScenario struct {
	name        string
	graph       string
	left, right string
	bases       []string
}

// mergeBasesScenarios lists named histories and their expected merge bases. TestGetMergeBases also checks each expected
// result against referenceMergeBases. Every scenario without a ghost was built in a git repository with the same
// commits, and `git merge-base --all` (git 2.39) returned the same merge bases.
func mergeBasesScenarios() []mergeBasesScenario {
	// root -- f -- l1 -- ... -- l500
	//      f -- r1
	lopsided, lopsidedTip := chain("l", "f", 500)
	// root -- t1 -- ... -- t3000 -- x0 -- x1
	//                      t3000 -- y0 -- y1
	//                      x1 = merge(x0, y0), y1 = merge(y0, x0)
	trunk, trunkTip := chain("t", "root", 3000)
	return []mergeBasesScenario{
		{name: "same commit", graph: linearHistory, left: "a2", right: "a2", bases: []string{"a2"}},
		{name: "ancestor", graph: linearHistory, left: "a1", right: "a3", bases: []string{"a1"}},
		{name: "fork", graph: linearHistory, left: "a3", right: "b3", bases: []string{"a2"}},
		{name: "unrelated histories", graph: linearHistory, left: "a3", right: "unrelated"},
		{name: "criss-cross", graph: crissCross, left: "x1", right: "y1", bases: []string{"x0", "y0"}},
		{name: "a criss-cross commit against one of its bases", graph: crissCross, left: "x0", right: "y1", bases: []string{"x0"}},
		{name: "3 bases", graph: fanIn(3), left: "m1", right: "m2", bases: []string{"p1", "p2", "p3"}},
		{name: "4 bases", graph: fanIn(4), left: "m1", right: "m2", bases: []string{"p1", "p2", "p3", "p4"}},
		{name: "5 bases", graph: fanIn(5), left: "m1", right: "m2", bases: []string{"p1", "p2", "p3", "p4", "p5"}},
		{name: "6 bases", graph: fanIn(6), left: "m1", right: "m2", bases: []string{"p1", "p2", "p3", "p4", "p5", "p6"}},
		{
			name:  "repeated criss-cross keeps only the last round",
			graph: "root:\n" + crissCrossRounds("root", 20),
			left:  "x20", right: "y20", bases: []string{"x19", "y19"},
		},
		{
			name:  "repeated criss-cross, tip against an older round",
			graph: "root:\n" + crissCrossRounds("root", 20),
			left:  "x20", right: "y5", bases: []string{"y5"},
		},
		{
			// The history from https://github.com/dolthub/dolt/issues/12050:
			//
			//	root -- f -- main1 -- main2
			//	        f -- feat1 -- feat2
			//	root -- s1 -- s2 -- s3
			//	main2 = merge(main1, s3), feat2 = merge(feat1, s3)
			name: "schema branch: the fork point and the schema tip",
			graph: `
				root:
				f: root
				main1: f
				feat1: f
				s1: root
				s2: s1
				s3: s2
				main2: main1 s3
				feat2: feat1 s3`,
			left: "main2", right: "feat2", bases: []string{"f", "s3"},
		},
		{
			//	root -- low
			//	root -- h1 -- h2 -- h3
			//	left = merge(low, h3), right = merge(h3, low)
			name: "bases at different heights",
			graph: `
				root:
				low: root
				h1: root
				h2: h1
				h3: h2
				left: low h3
				right: h3 low`,
			left: "left", right: "right", bases: []string{"h3", "low"},
		},
		{
			// p1 is a common ancestor through k3 and j1, but it is an ancestor of the merge base p3.
			//
			//	root -- p1 -- p2 -- p3
			//	        p1 -- k1 -- k2 -- k3
			//	        p1 -- j1
			//	c1 = merge(p3, k3), c2 = merge(p3, j1)
			name: "a common ancestor reached by another path is not a base",
			graph: `
				root:
				p1: root
				p2: p1
				p3: p2
				k1: p1
				k2: k1
				k3: k2
				j1: p1
				c1: p3 k3
				c2: p3 j1`,
			left: "c1", right: "c2", bases: []string{"p3"},
		},
		{name: "four branches, two of which cross-merge every round", graph: fourBranches, left: "a10", right: "b10", bases: []string{"b5", "d7"}},
		{name: "four branches, the cross-merging pair", graph: fourBranches, left: "c10", right: "d10", bases: []string{"c9", "d9"}},
		{name: "four branches, one side against a cross-merging branch", graph: fourBranches, left: "a10", right: "d10", bases: []string{"d7"}},
		{
			name:  "lopsided: 500 commits on one side, 1 on the other",
			graph: "root:\nf: root\n" + lopsided + "r1: f\n",
			left:  lopsidedTip, right: "r1", bases: []string{"f"},
		},
		{
			// root -- x0 -- x1 -- l1 -- ... -- l500
			// root -- y0 -- y1
			// x1 = merge(x0, y0), y1 = merge(y0, x0)
			name:  "lopsided criss-cross: 500 commits after the cross merge on one side",
			graph: crissCross + func() string { s, _ := chain("l", "x1", 500); return s }(),
			left:  "l500", right: "y1", bases: []string{"x0", "y0"},
		},
		{
			name:  "criss-cross on top of a long history",
			graph: "root:\n" + trunk + crissCrossRounds(trunkTip, 1),
			left:  "x1", right: "y1", bases: []string{"x0", "y0"},
		},
	}
}

// forEachWalk runs |test| walking both sides, and walking one side against an AncestryIndex of the other.
func forEachWalk(t *testing.T, test func(t *testing.T, withIndex bool)) {
	t.Run("both sides", func(t *testing.T) { test(t, false) })
	t.Run("one side with an ancestry index", func(t *testing.T) { test(t, true) })
}

func TestGetMergeBases(t *testing.T) {
	forEachWalk(t, func(t *testing.T, withIndex bool) {
		for _, s := range mergeBasesScenarios() {
			t.Run(s.name, func(t *testing.T) {
				g := parseTestGraph(s.graph)
				g.withIndex = withIndex
				expected := append([]string{}, s.bases...)
				sort.Strings(expected)
				require.Equal(t, expected, g.referenceMergeBases(s.left, s.right), "scenario disagrees with the reference")
				assert.Equal(t, expected, g.mergeBases(t, s.left, s.right))
				assert.Equal(t, expected, g.mergeBases(t, s.right, s.left))
			})
		}
	})
}

// A ghost's parents are unknown and could be any commit, so a walk of both sides cannot tell which common ancestors lie
// beyond it. In these histories, ~g is the ghost.
func TestGetMergeBasesErrorsOnGhosts(t *testing.T) {
	tests := []struct {
		name        string
		graph       string
		left, right string
	}{
		{
			//	root -- ~g -- x
			//	        ~g -- y
			name: "a ghost reachable from both sides",
			graph: `
				root:
				~g: root
				x: g
				y: g`,
			left: "x", right: "y",
		},
		{
			//	root -- ~g -- x0 -- x1
			//	root -- y0 -- y1
			//	x1 = merge(x0, y0), y1 = merge(y0, x0)
			name: "a ghost reachable from one side",
			graph: `
				root:
				~g: root
				x0: g
				y0: root
				x1: x0 y0
				y1: y0 x0`,
			left: "x1", right: "y1",
		},
		{
			// With full history the merge bases are a and b, but a is reachable from l only through ~g.
			//
			//	root -- a -- ~g
			//	root -- b
			//	l = merge(g, b), r = merge(a, b)
			name: "a ghost hiding a merge base",
			graph: `
				root:
				a: root
				b: root
				~g: a
				l: g b
				r: a b`,
			left: "l", right: "r",
		},
		{
			//	root -- ~g
			//	root -- x
			name: "a ghost as an argument",
			graph: `
				root:
				~g: root
				x: root`,
			left: "g", right: "x",
		},
		{
			// a is found first, but b is still undecided when the walk reads a's parent ~g.
			//
			//	root -- ~g -- a
			//	root -- b
			//	l = merge(a, b), r = merge(a, b)
			name: "a ghost below one merge base while another commit is undecided",
			graph: `
				root:
				~g: root
				a: g
				b: root
				l: a b
				r: a b`,
			left: "l", right: "r",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := parseTestGraph(tt.graph)
			for _, order := range [][2]string{{tt.left, tt.right}, {tt.right, tt.left}} {
				_, err := GetMergeBases(context.Background(), g, hashOfName(order[0]), g, hashOfName(order[1]))
				var ghostErr *GhostCommitError
				if assert.ErrorAs(t, err, &ghostErr, "merge bases of %s and %s", order[0], order[1]) {
					assert.Equal(t, hashOfName("g"), ghostErr.Hash)
				}
			}
		})
	}
}

// The walk stops once every commit left to visit is an ancestor of a merge base, before reading the parents of the
// last merge base, so a ghost below the merge bases is never reached. In these histories, ~g is a shallow clone's
// missing history.
func TestGetMergeBasesDoesNotReachGhostsBelowTheMergeBases(t *testing.T) {
	tests := []struct {
		name        string
		graph       string
		left, right string
		bases       []string
	}{
		{
			//	~g -- root -- x
			//	      root -- y
			name: "a fork whose base is a shallow clone's root",
			graph: `
				~g:
				root: g
				x: root
				y: root`,
			left: "x", right: "y", bases: []string{"root"},
		},
		{
			//	~g -- root -- x0 -- x1
			//	      root -- y0 -- y1
			//	x1 = merge(x0, y0), y1 = merge(y0, x0)
			name: "a criss-cross above a ghost",
			graph: `
				~g:
				root: g
				x0: root
				y0: root
				x1: x0 y0
				y1: y0 x0`,
			left: "x1", right: "y1", bases: []string{"x0", "y0"},
		},
		{
			//	~g -- root -- x
			name: "one side is the shallow clone's root",
			graph: `
				~g:
				root: g
				x: root`,
			left: "root", right: "x", bases: []string{"root"},
		},
	}
	forEachWalk(t, func(t *testing.T, withIndex bool) {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				g := parseTestGraph(tt.graph)
				g.withIndex = withIndex
				assert.Equal(t, tt.bases, g.mergeBases(t, tt.left, tt.right))
				assert.Equal(t, tt.bases, g.mergeBases(t, tt.right, tt.left))
			})
		}
	})
}

// With an ancestry index, only the side with the shorter tip is walked, and not below a common commit. A ghost is
// harmless when it is on the other side, or below a merge base; otherwise its unknown parents could hide a merge base.
// In these histories, names starting with ~ are ghosts.
func TestGetMergeBasesWithIndexGhosts(t *testing.T) {
	ok := []struct {
		name        string
		graph       string
		left, right string
		bases       []string
	}{
		{
			// r is walked; l3, behind ~g, is only looked up in the index.
			//
			//	root -- a -- ~g -- l1 -- l2 -- l3
			//	        a -- r
			name: "a ghost on the side that is not walked",
			graph: `
				root:
				a: root
				~g: a
				l1: g
				l2: l1
				l3: l2
				r: a`,
			left: "l3", right: "r", bases: []string{"a"},
		},
		{
			// shallow-clone.bats "clone depth 5 on complex main", whose clone lacks c and v1. v8 is walked: it reaches
			// v3, a merge base, and then ~c, which the index shows is below v3.
			//
			//	~c -- ~v1 -- v2 -- v3 -- v4 -- v5
			//	~c -- v6 -- v7 -- m1 -- v8
			//	m1 = merge(v7, v3)
			name: "a ghost below the merge base on the walked side",
			graph: `
				~c:
				~v1: c
				v2: v1
				v3: v2
				v4: v3
				v5: v4
				v6: c
				v7: v6
				m1: v7 v3
				v8: m1`,
			left: "v5", right: "v8", bases: []string{"v3"},
		},
		{
			// w is walked. root is reached through k, and the index shows it is below the merge base b.
			//
			//	root -- ~g -- b -- o
			//	root -- k
			//	w = merge(b, k)
			name: "a commit below a ghost below the merge base",
			graph: `
				root:
				~g: root
				b: g
				k: root
				w: b k
				o: b`,
			left: "o", right: "w", bases: []string{"b"},
		},
	}
	for _, tt := range ok {
		t.Run(tt.name, func(t *testing.T) {
			g := parseTestGraph(tt.graph)
			g.withIndex = true
			assert.Equal(t, tt.bases, g.mergeBases(t, tt.left, tt.right))
			assert.Equal(t, tt.bases, g.mergeBases(t, tt.right, tt.left))
		})
	}

	fails := []struct {
		name        string
		graph       string
		left, right string
	}{
		{
			// r is walked and reaches ~g, which is not below any merge base.
			//
			//	root -- a -- ~g -- r
			//	        a -- l1 -- l2 -- l3 -- l4
			name: "a ghost on the walked side above the merge base",
			graph: `
				root:
				a: root
				~g: a
				r: g
				l1: a
				l2: l1
				l3: l2
				l4: l3`,
			left: "r", right: "l4",
		},
		{
			//	root -- ~g -- x
			//	        ~g -- y
			name: "a ghost merge base",
			graph: `
				root:
				~g: root
				x: g
				y: g`,
			left: "x", right: "y",
		},
	}
	for _, tt := range fails {
		t.Run(tt.name, func(t *testing.T) {
			g := parseTestGraph(tt.graph)
			g.withIndex = true
			for _, order := range [][2]string{{tt.left, tt.right}, {tt.right, tt.left}} {
				_, err := GetMergeBases(context.Background(), g.resolver(), hashOfName(order[0]), g.resolver(), hashOfName(order[1]))
				var ghostErr *GhostCommitError
				if assert.ErrorAs(t, err, &ghostErr, "merge bases of %s and %s", order[0], order[1]) {
					assert.Equal(t, hashOfName("g"), ghostErr.Hash)
				}
			}
		})
	}
}

func TestGetMergeBasesWithIndexWalksTheShorterSide(t *testing.T) {
	// root -- f -- l1 -- ... -- l500
	//      f -- r1
	tall, tallTip := chain("l", "f", 500)
	g := parseTestGraph("root:\nf: root\n" + tall + "r1: f\n")
	g.withIndex = true
	assert.Equal(t, []string{"f"}, g.mergeBases(t, tallTip, "r1"))
	assert.Equal(t, []string{"f"}, g.mergeBases(t, "r1", tallTip))
	assert.Less(t, g.resolved, 10, "only the short side should be walked")
	assert.Less(t, g.indexLookups, 10)
}

func TestGetMergeBasesStopsAtTheBases(t *testing.T) {
	forEachWalk(t, func(t *testing.T, withIndex bool) {
		// root -- t1 -- ... -- t3000 -- x0 -- x1
		//                      t3000 -- y0 -- y1
		//                      x1 = merge(x0, y0), y1 = merge(y0, x0)
		trunk, trunkTip := chain("t", "root", 3000)
		g := parseTestGraph("root:\n" + trunk + crissCrossRounds(trunkTip, 1))
		g.withIndex = withIndex
		assert.Equal(t, []string{"x0", "y0"}, g.mergeBases(t, "x1", "y1"))
		assert.Less(t, g.resolved, 20, "the walk should not descend into the history below the merge bases")
	})
}

// Each side's commits are resolved by its own resolver, as when pushing compares a local and a remote commit. Both
// resolvers know the shared history:
//
//	root -- x0 -- x1    (x1 only in the left resolver)
//	root -- y0 -- y1    (y1 only in the right resolver)
//	x1 = merge(x0, y0), y1 = merge(y0, x0)
func TestGetMergeBasesPerSideResolvers(t *testing.T) {
	shared := "root:\nx0: root\ny0: root\n"
	left := parseTestGraph(shared + "x1: x0 y0\n")
	right := parseTestGraph(shared + "y1: y0 x0\n")
	bases, err := GetMergeBases(context.Background(), left, hashOfName("x1"), right, hashOfName("y1"))
	require.NoError(t, err)
	assert.ElementsMatch(t, []hash.Hash{hashOfName("x0"), hashOfName("y0")}, bases)

	_, err = GetMergeBases(context.Background(), left, hashOfName("x1"), left, hashOfName("y1"))
	assert.Error(t, err, "y1 is unknown to the left resolver")
}

// randomGraph builds |n| commits on |branches| branches. Each commit extends a random branch, and with probability
// |mergeProb| also merges the tip of another branch, so branches cross-merge repeatedly.
func randomGraph(rnd *rand.Rand, n, branches int, mergeProb float64) (*testGraph, []string) {
	g := newTestGraph()
	g.add("root", false)
	tips := make([]string, branches)
	for i := range tips {
		tips[i] = "root"
	}
	for i := 0; i < n; i++ {
		branch := rnd.Intn(branches)
		name := fmt.Sprintf("c%d", i)
		parents := []string{tips[branch]}
		if rnd.Float64() < mergeProb {
			other := tips[rnd.Intn(branches)]
			if other != tips[branch] {
				parents = append(parents, other)
			}
		}
		g.add(name, false, parents...)
		tips[branch] = name
	}
	return g, g.order
}

// TestGetMergeBasesMatchesReferenceOnRandomGraphs checks GetMergeBases against referenceMergeBases on 3000 pairs of
// commits from 300 random histories, where branches repeatedly merge each other and often have several merge bases.
// Nobody wrote down the expected results: they come from referenceMergeBases, which computes them by definition. The
// seed is fixed, so the histories are the same on every run. 200 pairs from histories generated the same way were also
// built in a git repository, and `git merge-base --all` (git 2.39) agreed with both.
func TestGetMergeBasesMatchesReferenceOnRandomGraphs(t *testing.T) {
	forEachWalk(t, func(t *testing.T, withIndex bool) {
		rnd := rand.New(rand.NewSource(12050))
		maxBases := 0
		for seed := 0; seed < 300; seed++ {
			g, commits := randomGraph(rnd, 30+rnd.Intn(50), 2+rnd.Intn(5), 0.2+rnd.Float64()*0.6)
			g.withIndex = withIndex
			for pair := 0; pair < 10; pair++ {
				left, right := commits[rnd.Intn(len(commits))], commits[rnd.Intn(len(commits))]
				expected := g.referenceMergeBases(left, right)
				maxBases = max(maxBases, len(expected))
				if !assert.Equal(t, expected, g.mergeBases(t, left, right), "graph %d: merge bases of %s and %s", seed, left, right) {
					return
				}
			}
		}
		assert.GreaterOrEqual(t, maxBases, 3, "random graphs should include pairs with several merge bases")
	})
}
