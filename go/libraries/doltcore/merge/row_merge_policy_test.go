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

package merge_test

import (
	"context"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// fastPathSchema has no secondary index and no non-null column, so it
// satisfies canFastMergeProllyTrees. differPathSchema adds an index, which
// forces needsSecondaryIndexMerge and routes the merge through ThreeWayDiffer.
var fastPathSchema = sch("CREATE TABLE t (pk int PRIMARY KEY, a int, b int)")
var differPathSchema = sch("CREATE TABLE t (pk int PRIMARY KEY, a int, b int, INDEX idx_a (a))")

var policyTestSchema = fastPathSchema

type policyInvocation struct {
	table             string
	left, right, base bool // whether each tuple was present
}

// recordingRowMergePolicy captures every invocation and returns a scripted answer.
type recordingRowMergePolicy struct {
	calls  []policyInvocation
	answer func() (val.Tuple, tree.RowMergeStatus)
}

func (p *recordingRowMergePolicy) opt() merge.RowMergePolicy {
	return func(_ *sql.Context, table doltdb.TableName, left, right, base val.Tuple) (val.Tuple, tree.RowMergeStatus, error) {
		p.calls = append(p.calls, policyInvocation{
			table: table.Name,
			left:  left != nil,
			right: right != nil,
			base:  base != nil,
		})
		if p.answer == nil {
			return nil, tree.RowMergeDefer, nil
		}
		merged, status := p.answer()
		return merged, status, nil
	}
}

func runPolicyMerge(t *testing.T, data dataTest, policy merge.RowMergePolicy) (*merge.Result, error) {
	return runPolicyMergeWithSchema(t, policyTestSchema, data, policy)
}

func runPolicyMergeWithSchema(t *testing.T, schema namedSchema, data dataTest, policy merge.RowMergePolicy) (*merge.Result, error) {
	t.Helper()
	ctx := context.Background()
	a, l, r, _ := setupDataMergeTest(ctx, t, schema, data)

	var eo editor.Options
	mo := merge.MergeOpts{RowMergePolicy: policy}
	return merge.MergeRoots(sql.NewContext(ctx), doltdb.SimpleTableResolver{}, l, r, a, rootish{r}, rootish{a}, eo, mo)
}

// A policy installed through MergeOpts must reach the merge, be told which
// table it is deciding, and be consulted for a convergent edit -- the case the
// fast path merges without inspection.
func TestRowMergePolicy_ReachesMergeAndSeesConvergentEdit(t *testing.T) {
	data := dataTest{
		name:     "both sides set a=1 on row 1",
		ancestor: []sql.Row{sql.NewRow(1, 0, 0), sql.NewRow(2, 0, 0)},
		left:     []sql.Row{sql.NewRow(1, 1, 0), sql.NewRow(2, 7, 0)},
		right:    []sql.Row{sql.NewRow(1, 1, 0), sql.NewRow(2, 0, 0)},
		merged:   []sql.Row{sql.NewRow(1, 1, 0), sql.NewRow(2, 7, 0)},
	}

	p := &recordingRowMergePolicy{}
	_, err := runPolicyMerge(t, data, p.opt())
	require.NoError(t, err)

	require.Len(t, p.calls, 1, "policy must be consulted for the convergent edit: %#v", p.calls)
	require.Equal(t, "t", p.calls[0].table, "policy must be told which table it is deciding")
	require.True(t, p.calls[0].left)
	require.True(t, p.calls[0].right)
	require.True(t, p.calls[0].base)
}

// Returning RowMergeConflict for a convergent edit must produce a data
// conflict. Without a policy the same merge is clean, which is what makes this
// the load-bearing case.
func TestRowMergePolicy_ConflictOnConvergentEdit(t *testing.T) {
	data := dataTest{
		name:     "both sides set a=1 on row 1",
		ancestor: []sql.Row{sql.NewRow(1, 0, 0), sql.NewRow(2, 0, 0)},
		left:     []sql.Row{sql.NewRow(1, 1, 0), sql.NewRow(2, 7, 0)},
		right:    []sql.Row{sql.NewRow(1, 1, 0), sql.NewRow(2, 0, 0)},
		merged:   []sql.Row{sql.NewRow(1, 1, 0), sql.NewRow(2, 7, 0)},
	}

	// Baseline: no policy, and the convergent edit merges without a conflict.
	baseline, err := runPolicyMerge(t, data, nil)
	require.NoError(t, err, "precondition: a convergent edit merges cleanly without a policy")
	require.Zero(t, dataConflictCount(baseline),
		"precondition: no conflict is recorded for a convergent edit without a policy")

	p := &recordingRowMergePolicy{answer: func() (val.Tuple, tree.RowMergeStatus) {
		return nil, tree.RowMergeConflict
	}}
	result, err := runPolicyMerge(t, data, p.opt())
	require.NoError(t, err)
	require.Len(t, p.calls, 1, "only the convergent row is a three-way decision")
	require.Equal(t, 1, dataConflictCount(result),
		"RowMergeConflict on a convergent edit must record a data conflict")
}

func dataConflictCount(r *merge.Result) int {
	total := 0
	for _, s := range r.Stats {
		total += s.DataConflicts
	}
	return total
}

// A policy that always defers must not change the merge result.
func TestRowMergePolicy_DeferMatchesNoPolicy(t *testing.T) {
	data := dataTest{
		name:     "disjoint column edits",
		ancestor: []sql.Row{sql.NewRow(1, 0, 0), sql.NewRow(2, 0, 0)},
		left:     []sql.Row{sql.NewRow(1, 1, 0), sql.NewRow(2, 0, 0)},
		right:    []sql.Row{sql.NewRow(1, 0, 2), sql.NewRow(2, 5, 0)},
		merged:   []sql.Row{sql.NewRow(1, 1, 2), sql.NewRow(2, 5, 0)},
	}

	withoutPolicy, err := runPolicyMerge(t, data, nil)
	require.NoError(t, err)

	p := &recordingRowMergePolicy{}
	withDefer, err := runPolicyMerge(t, data, p.opt())
	require.NoError(t, err)

	require.NotEmpty(t, p.calls, "the deferring policy must still have been consulted")
	requireSameMergedTable(t, withoutPolicy, withDefer)
}

func requireSameMergedTable(t *testing.T, a, b *merge.Result) {
	t.Helper()
	ctx := context.Background()
	ah, err := doltdb.MapTableHashes(ctx, a.Root)
	require.NoError(t, err)
	bh, err := doltdb.MapTableHashes(ctx, b.Root)
	require.NoError(t, err)
	require.Equal(t, ah, bh, "an always-defer policy must produce the same merged tables")
}

// Both merge paths must offer the policy the same decisions. If the fast path
// stops consulting it, the convergent edit disappears from its invocation list
// while the differ path still reports it.
func TestRowMergePolicy_BothPathsSeeTheSameDecisions(t *testing.T) {
	data := dataTest{
		name:     "convergent edit on row 1, left-only edit on row 2",
		ancestor: []sql.Row{sql.NewRow(1, 0, 0), sql.NewRow(2, 0, 0)},
		left:     []sql.Row{sql.NewRow(1, 1, 0), sql.NewRow(2, 7, 0)},
		right:    []sql.Row{sql.NewRow(1, 1, 0), sql.NewRow(2, 0, 0)},
		merged:   []sql.Row{sql.NewRow(1, 1, 0), sql.NewRow(2, 7, 0)},
	}

	for _, tc := range []struct {
		name   string
		schema namedSchema
	}{
		{"fast path", fastPathSchema},
		{"differ path", differPathSchema},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &recordingRowMergePolicy{answer: func() (val.Tuple, tree.RowMergeStatus) {
				return nil, tree.RowMergeConflict
			}}
			result, err := runPolicyMergeWithSchema(t, tc.schema, data, p.opt())
			require.NoError(t, err)
			require.Len(t, p.calls, 1, "the convergent edit is the only three-way decision")
			require.True(t, p.calls[0].left && p.calls[0].right && p.calls[0].base)
			require.Equal(t, 1, dataConflictCount(result))
		})
	}
}

// Both sides rewriting a large region identically produces equal chunk
// addresses above the leaf level. The fast path skips those subtrees without
// visiting rows, so a policy only sees them if the merge descends.
func TestRowMergePolicy_SeesConvergentEditsInsideIdenticalSubtrees(t *testing.T) {
	const rowCount = 64
	const converged = 40

	wide := strings.Repeat("1", 255)
	schema := sch("CREATE TABLE t (pk int PRIMARY KEY, t char(255), a int)")

	rows := func(mutateUpTo int, extra bool) []sql.Row {
		var out []sql.Row
		for i := 0; i < rowCount; i++ {
			a := 0
			if i < mutateUpTo {
				a = 1
			}
			if extra && i == rowCount-1 {
				a = 9
			}
			out = append(out, sql.NewRow(i, wide, a))
		}
		return out
	}

	data := dataTest{
		name:     "both sides make the same bulk edit",
		ancestor: rows(0, false),
		left:     rows(converged, true),
		right:    rows(converged, false),
		merged:   rows(converged, true),
	}

	p := &recordingRowMergePolicy{}
	_, err := runPolicyMergeWithSchema(t, schema, data, p.opt())
	require.NoError(t, err)

	require.Len(t, p.calls, converged,
		"every row both sides changed identically must reach the policy, including those inside subtrees the two sides rewrote to the same chunk")
}

// A policy returns a bare tuple and has no builder for the merged schema, so a
// merge that also changes schema must not consult it. The merge still succeeds
// with Dolt's own semantics even though the policy would have conflicted every
// row it was offered.
func TestRowMergePolicy_DefersWhenSchemasDiffer(t *testing.T) {
	test := schemaMergeTest{
		name:     "right side adds a nullable column",
		ancestor: *tbl(sch("CREATE TABLE t (pk int PRIMARY KEY, a int)"), sql.NewRow(1, 0), sql.NewRow(2, 0)),
		left:     tbl(sch("CREATE TABLE t (pk int PRIMARY KEY, a int)"), sql.NewRow(1, 1), sql.NewRow(2, 0)),
		right:    tbl(sch("CREATE TABLE t (pk int PRIMARY KEY, a int, c int)"), sql.NewRow(1, 1, nil), sql.NewRow(2, 0, nil)),
		merged:   *tbl(sch("CREATE TABLE t (pk int PRIMARY KEY, a int, c int)"), sql.NewRow(1, 1, nil), sql.NewRow(2, 0, nil)),
	}

	ctx := context.Background()
	a, l, r, _ := setupSchemaMergeTest(ctx, t, test)

	p := &recordingRowMergePolicy{answer: func() (val.Tuple, tree.RowMergeStatus) {
		return nil, tree.RowMergeConflict
	}}

	var eo editor.Options
	mo := merge.MergeOpts{RowMergePolicy: p.opt()}
	result, err := merge.MergeRoots(sql.NewContext(ctx), doltdb.SimpleTableResolver{}, l, r, a, rootish{r}, rootish{a}, eo, mo)
	require.NoError(t, err)
	require.Empty(t, p.calls, "a schema-changing merge must not consult the policy")
	require.Zero(t, dataConflictCount(result), "the merge must use Dolt's own semantics")
}
