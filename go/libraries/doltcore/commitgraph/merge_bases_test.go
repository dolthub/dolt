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

// testGraph is an in-memory HashResolver. Commits are named, and their heights are computed from their parents.
type testGraph struct {
	infos    map[hash.Hash]*CommitInfo
	names    map[hash.Hash]string
	heights  map[string]uint64
	parents  map[string][]string
	order    []string
	resolved int
}

func newTestGraph() *testGraph {
	return &testGraph{
		infos:   map[hash.Hash]*CommitInfo{},
		names:   map[hash.Hash]string{},
		heights: map[string]uint64{},
		parents: map[string][]string{},
	}
}

// parseTestGraph builds a graph from lines of "name parent1 parent2 ...". A name prefixed with "~" is a ghost: a commit
// whose parents are listed for computing heights but cannot be resolved.
func parseTestGraph(spec string) *testGraph {
	g := newTestGraph()
	for _, line := range strings.Split(spec, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		name, ghost := strings.CutPrefix(fields[0], "~")
		g.add(name, ghost, fields[1:]...)
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
	bases, err := GetMergeBases(context.Background(), g, hashOfName(left), g, hashOfName(right))
	require.NoError(t, err)
	names := make([]string, len(bases))
	for i, b := range bases {
		names[i] = g.names[b]
	}
	sort.Strings(names)
	return names
}

// referenceMergeBases computes the best common ancestors by definition: the common ancestors of |left| and |right| that
// are not ancestors of another common ancestor. Ghosts have no known parents.
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

// ancestors returns |names| and all of their ancestors.
func (g *testGraph) ancestors(names ...string) map[string]bool {
	seen := map[string]bool{}
	stack := append([]string{}, names...)
	for len(stack) > 0 {
		c := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[c] {
			continue
		}
		seen[c] = true
		stack = append(stack, g.parents[c]...)
	}
	return seen
}

type mergeBasesScenario struct {
	name        string
	graph       string
	left, right string
	bases       []string
}

// crissCrossRounds builds two branches that merge each other every round, starting from |fork|. Round i adds x<i>
// and y<i>, each merging the other branch's previous commit.
func crissCrossRounds(fork string, rounds int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "x0 %s\ny0 %s\n", fork, fork)
	for i := 1; i <= rounds; i++ {
		fmt.Fprintf(&b, "x%d x%d y%d\ny%d y%d x%d\n", i, i-1, i-1, i, i-1, i-1)
	}
	return b.String()
}

// chain adds |n| commits named <prefix>1..<prefix>n on top of |from| and returns the graph lines and the last name.
func chain(prefix, from string, n int) (string, string) {
	var b strings.Builder
	prev := from
	for i := 1; i <= n; i++ {
		name := fmt.Sprintf("%s%d", prefix, i)
		fmt.Fprintf(&b, "%s %s\n", name, prev)
		prev = name
	}
	return b.String(), prev
}

// fanIn builds |n| independent commits p1..pn on top of root, merged into m1 in order and into m2 in reverse order,
// either as one octopus merge or as a sequence of two-parent merges.
func fanIn(n int, octopus bool) string {
	var b strings.Builder
	b.WriteString("root\n")
	var ps []string
	for i := 1; i <= n; i++ {
		p := fmt.Sprintf("p%d", i)
		ps = append(ps, p)
		fmt.Fprintf(&b, "%s root\n", p)
	}
	reversed := make([]string, n)
	for i, p := range ps {
		reversed[n-1-i] = p
	}
	if octopus {
		fmt.Fprintf(&b, "m1 %s\nm2 %s\n", strings.Join(ps, " "), strings.Join(reversed, " "))
		return b.String()
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
			fmt.Fprintf(&b, "%s %s %s\n", name, prev, p)
			prev = name
		}
	}
	return b.String()
}

// fourBranches builds branches a, b, c and d from root. c and d merge each other every round. a and b each pull in
// c or d occasionally, and a and b merge each other once.
func fourBranches() string {
	var b strings.Builder
	b.WriteString("root\na0 root\nb0 root\nc0 root\nd0 root\n")
	for i := 1; i <= 10; i++ {
		fmt.Fprintf(&b, "c%d c%d d%d\nd%d d%d c%d\n", i, i-1, i-1, i, i-1, i-1)
		switch i {
		case 3:
			fmt.Fprintf(&b, "a%d a%d c%d\nb%d b%d\n", i, i-1, i, i, i-1)
		case 4:
			fmt.Fprintf(&b, "a%d a%d\nb%d b%d d%d\n", i, i-1, i, i-1, i)
		case 6:
			fmt.Fprintf(&b, "a%d a%d b%d\nb%d b%d\n", i, i-1, i-1, i, i-1)
		case 7:
			fmt.Fprintf(&b, "a%d a%d d%d\nb%d b%d\n", i, i-1, i, i, i-1)
		case 8:
			fmt.Fprintf(&b, "a%d a%d\nb%d b%d c%d\n", i, i-1, i, i-1, i)
		default:
			fmt.Fprintf(&b, "a%d a%d\nb%d b%d\n", i, i-1, i, i-1)
		}
	}
	return b.String()
}

func mergeBasesScenarios() []mergeBasesScenario {
	linear := "root\na1 root\na2 a1\na3 a2\nb3 a2\nunrelated\n"
	tallLeft, tallLeftTip := chain("l", "f", 500)
	trunk, trunkTip := chain("t", "root", 3000)
	longSide, longSideTip := chain("k", "p1", 100)
	return []mergeBasesScenario{
		{name: "same commit", graph: linear, left: "a2", right: "a2", bases: []string{"a2"}},
		{name: "ancestor", graph: linear, left: "a1", right: "a3", bases: []string{"a1"}},
		{name: "fork", graph: linear, left: "a3", right: "b3", bases: []string{"a2"}},
		{name: "unrelated histories", graph: linear, left: "a3", right: "unrelated"},
		{
			name:  "criss-cross",
			graph: "root\n" + crissCrossRounds("root", 1),
			left:  "x1", right: "y1", bases: []string{"x0", "y0"},
		},
		{name: "3 bases, octopus merges", graph: fanIn(3, true), left: "m1", right: "m2", bases: []string{"p1", "p2", "p3"}},
		{name: "4 bases, octopus merges", graph: fanIn(4, true), left: "m1", right: "m2", bases: []string{"p1", "p2", "p3", "p4"}},
		{name: "5 bases, octopus merges", graph: fanIn(5, true), left: "m1", right: "m2", bases: []string{"p1", "p2", "p3", "p4", "p5"}},
		{name: "3 bases, two-parent merges", graph: fanIn(3, false), left: "m1", right: "m2", bases: []string{"p1", "p2", "p3"}},
		{name: "4 bases, two-parent merges", graph: fanIn(4, false), left: "m1", right: "m2", bases: []string{"p1", "p2", "p3", "p4"}},
		{name: "5 bases, two-parent merges", graph: fanIn(5, false), left: "m1", right: "m2", bases: []string{"p1", "p2", "p3", "p4", "p5"}},
		{name: "6 bases, two-parent merges", graph: fanIn(6, false), left: "m1", right: "m2", bases: []string{"p1", "p2", "p3", "p4", "p5", "p6"}},
		{
			name:  "repeated criss-cross keeps only the last round",
			graph: "root\n" + crissCrossRounds("root", 20),
			left:  "x20", right: "y20", bases: []string{"x19", "y19"},
		},
		{
			name:  "repeated criss-cross, tip against an older round",
			graph: "root\n" + crissCrossRounds("root", 20),
			left:  "x20", right: "y5", bases: []string{"y5"},
		},
		{
			name: "schema branch: the fork point and the schema tip",
			graph: "root\nf root\nmain1 f\nfeat1 f\ns1 root\ns2 s1\ns3 s2\n" +
				"main2 main1 s3\nfeat2 feat1 s3\n",
			left: "main2", right: "feat2", bases: []string{"f", "s3"},
		},
		{
			name: "bases far apart in height",
			graph: "root\nlow root\n" + func() string { s, _ := chain("h", "root", 60); return s }() +
				"left low h60\nright h60 low\n",
			left: "left", right: "right", bases: []string{"h60", "low"},
		},
		{
			name:  "a common ancestor reached by a long side path is not a base",
			graph: "root\np1 root\np2 p1\np3 p2\n" + longSide + "c1 p3 " + longSideTip + "\nj1 p1\nc2 p3 j1\n",
			left:  "c1", right: "c2", bases: []string{"p3"},
		},
		{
			name:  "four branches, two of which cross-merge every round",
			graph: fourBranches(),
			left:  "a10", right: "b10", bases: []string{"b5", "d7"},
		},
		{
			name:  "four branches, the cross-merging pair",
			graph: fourBranches(),
			left:  "c10", right: "d10", bases: []string{"c9", "d9"},
		},
		{
			name:  "four branches, one side against a cross-merging branch",
			graph: fourBranches(),
			left:  "a10", right: "d10", bases: []string{"d7"},
		},
		{
			name:  "lopsided: 500 commits on one side, 1 on the other",
			graph: "root\nf root\n" + tallLeft + "r1 f\n",
			left:  tallLeftTip, right: "r1", bases: []string{"f"},
		},
		{
			name:  "lopsided criss-cross: 500 commits after the cross merge on one side",
			graph: "root\n" + crissCrossRounds("root", 1) + func() string { s, _ := chain("l", "x1", 500); return s }(),
			left:  "l500", right: "y1", bases: []string{"x0", "y0"},
		},
		{
			name:  "criss-cross on top of a long history",
			graph: "root\n" + trunk + crissCrossRounds(trunkTip, 1),
			left:  "x1", right: "y1", bases: []string{"x0", "y0"},
		},
		{
			name:  "a criss-cross commit against one of its bases",
			graph: "root\n" + crissCrossRounds("root", 1),
			left:  "x0", right: "y1", bases: []string{"x0"},
		},
		{
			name: "octopus merge of related commits",
			graph: "root\na root\nb a\nc root\n" +
				"m1 a b c\nm2 c b\n",
			left: "m1", right: "m2", bases: []string{"b", "c"},
		},
	}
}

func TestGetMergeBases(t *testing.T) {
	for _, s := range mergeBasesScenarios() {
		t.Run(s.name, func(t *testing.T) {
			g := parseTestGraph(s.graph)
			expected := append([]string{}, s.bases...)
			sort.Strings(expected)
			require.Equal(t, expected, g.referenceMergeBases(s.left, s.right), "scenario disagrees with the reference")
			assert.Equal(t, expected, g.mergeBases(t, s.left, s.right))
			assert.Equal(t, expected, g.mergeBases(t, s.right, s.left))
		})
	}
}

// A ghost's parents are unknown and could be any commit, so the walk cannot tell which common ancestors lie beyond it.
func TestGetMergeBasesErrorsOnGhosts(t *testing.T) {
	tests := []struct {
		name        string
		graph       string
		left, right string
	}{
		{name: "a ghost reachable from both sides", graph: "root\n~g root\nx g\ny g\n", left: "x", right: "y"},
		{name: "a ghost reachable from one side", graph: "root\n~g root\nx0 g\ny0 root\nx1 x0 y0\ny1 y0 x0\n", left: "x1", right: "y1"},
		{name: "a ghost hiding a merge base", graph: "root\na root\nb root\n~g a\nl g b\nr a b\n", left: "l", right: "r"},
		{name: "a ghost as an argument", graph: "root\n~g root\nx root\n", left: "g", right: "x"},
		{
			name:  "a ghost below one merge base while another commit is undecided",
			graph: "root\n~g root\na g\nb root\nl a b\nr a b\n", left: "l", right: "r",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := parseTestGraph(tt.graph)
			for _, order := range [][2]string{{tt.left, tt.right}, {tt.right, tt.left}} {
				_, err := GetMergeBases(context.Background(), g, hashOfName(order[0]), g, hashOfName(order[1]))
				assert.ErrorIs(t, err, ErrGhostCommit, "merge bases of %s and %s", order[0], order[1])
			}
		})
	}
}

// The walk stops once every commit left to visit is an ancestor of a merge base, before reading the parents of the
// last merge base, so a ghost below the merge bases is never reached.
func TestGetMergeBasesDoesNotReachGhostsBelowTheMergeBases(t *testing.T) {
	tests := []struct {
		name        string
		graph       string
		left, right string
		bases       []string
	}{
		{name: "a fork whose base is a shallow clone's root", graph: "~g\nroot g\nx root\ny root\n", left: "x", right: "y", bases: []string{"root"}},
		{name: "a criss-cross above a ghost", graph: "~g\nroot g\nx0 root\ny0 root\nx1 x0 y0\ny1 y0 x0\n", left: "x1", right: "y1", bases: []string{"x0", "y0"}},
		{name: "one side is the shallow clone's root", graph: "~g\nroot g\nx root\n", left: "root", right: "x", bases: []string{"root"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := parseTestGraph(tt.graph)
			assert.Equal(t, tt.bases, g.mergeBases(t, tt.left, tt.right))
			assert.Equal(t, tt.bases, g.mergeBases(t, tt.right, tt.left))
		})
	}
}

func TestGetMergeBasesStopsAtTheBases(t *testing.T) {
	trunk, trunkTip := chain("t", "root", 3000)
	g := parseTestGraph("root\n" + trunk + crissCrossRounds(trunkTip, 1))
	assert.Equal(t, []string{"x0", "y0"}, g.mergeBases(t, "x1", "y1"))
	assert.Less(t, g.resolved, 20, "the walk should not descend into the history below the merge bases")
}

func TestGetMergeBasesPerSideResolvers(t *testing.T) {
	shared := "root\nx0 root\ny0 root\n"
	left := parseTestGraph(shared + "x1 x0 y0\n")
	right := parseTestGraph(shared + "y1 y0 x0\n")
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

func TestGetMergeBasesMatchesReferenceOnRandomGraphs(t *testing.T) {
	rnd := rand.New(rand.NewSource(12050))
	maxBases := 0
	for seed := 0; seed < 300; seed++ {
		g, commits := randomGraph(rnd, 30+rnd.Intn(50), 2+rnd.Intn(5), 0.2+rnd.Float64()*0.6)
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
}
