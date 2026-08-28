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
	"bytes"
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

// ---------------------------------------------------------------------------
// The four merge strictness levels, as policies over SQL rows.
//
// A row is the document and a column is the field, so the levels can be
// written against val.Tuple without any document format. Each is a pure
// predicate: it answers RowMergeConflict or defers, because deciding whether
// two edits collide is the level's job and composing a clean merge is Dolt's.
// ---------------------------------------------------------------------------

// changedFields reports the column indexes where |row| differs from |base|.
// A nil base is an insert, so every column of |row| is new; a nil row is a
// delete, so every column that existed changed.
func changedFields(base, row val.Tuple) map[int]struct{} {
	changed := map[int]struct{}{}
	switch {
	case base == nil:
		if row != nil {
			for i := 0; i < row.Count(); i++ {
				changed[i] = struct{}{}
			}
		}
	case row == nil:
		for i := 0; i < base.Count(); i++ {
			changed[i] = struct{}{}
		}
	default:
		for i := 0; i < base.Count(); i++ {
			if !bytes.Equal(base.GetField(i), row.GetField(i)) {
				changed[i] = struct{}{}
			}
		}
	}
	return changed
}

func intersects(a, b map[int]struct{}) bool {
	for i := range a {
		if _, ok := b[i]; ok {
			return true
		}
	}
	return false
}

func rowsEqual(left, right val.Tuple) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return bytes.Equal(left, right)
}

type strictnessLevel struct {
	name     string
	conflict func(left, right, base val.Tuple) bool
}

var strictnessLevels = []strictnessLevel{
	{
		// Both sides wrote this document at all.
		name: "documentTouched",
		conflict: func(left, right, base val.Tuple) bool {
			return len(changedFields(base, left)) > 0 && len(changedFields(base, right)) > 0
		},
	},
	{
		// Both sides wrote the same field, even to the same value.
		name: "fieldTouched",
		conflict: func(left, right, base val.Tuple) bool {
			return intersects(changedFields(base, left), changedFields(base, right))
		},
	},
	{
		// Both sides wrote the same field, to different values. Dolt's own rule.
		name: "fieldDivergent",
		conflict: func(left, right, base val.Tuple) bool {
			fo, ft := changedFields(base, left), changedFields(base, right)
			for i := range fo {
				if _, ok := ft[i]; !ok {
					continue
				}
				var l, r []byte
				if left != nil {
					l = left.GetField(i)
				}
				if right != nil {
					r = right.GetField(i)
				}
				if !bytes.Equal(l, r) {
					return true
				}
			}
			return false
		},
	},
	{
		// Both sides wrote this document, to different results.
		name: "documentDivergent",
		conflict: func(left, right, base val.Tuple) bool {
			return len(changedFields(base, left)) > 0 &&
				len(changedFields(base, right)) > 0 &&
				!rowsEqual(left, right)
		},
	},
}

func (l strictnessLevel) policy(calls *int) merge.RowMergePolicy {
	return func(_ *sql.Context, _ doltdb.TableName, left, right, base val.Tuple) (val.Tuple, tree.RowMergeStatus, error) {
		*calls++
		if l.conflict(left, right, base) {
			return nil, tree.RowMergeConflict, nil
		}
		return nil, tree.RowMergeDefer, nil
	}
}

// ---------------------------------------------------------------------------
// The case matrix. pk 1 or 2 carries the case; pk 99 is ballast that only the
// left side edits, so the two roots always differ (an identical pair short
// circuits before any row is compared) and so every run also asserts that a
// one-sided edit is not offered to the policy.
// ---------------------------------------------------------------------------

type levelCase struct {
	name                  string
	ancestor, left, right []sql.Row
	// expected verdict per level, indexed as strictnessLevels
	conflicts [4]bool
	// calls is the number of three-way decisions in the case
	calls int
}

func ballast(rows ...sql.Row) []sql.Row { return rows }

var levelCases = []levelCase{
	{
		name:      "1 identical edit, same column",
		ancestor:  ballast(sql.NewRow(1, 0, 0), sql.NewRow(99, 0, 0)),
		left:      ballast(sql.NewRow(1, 1, 0), sql.NewRow(99, 7, 0)),
		right:     ballast(sql.NewRow(1, 1, 0), sql.NewRow(99, 0, 0)),
		conflicts: [4]bool{true, true, false, false},
		calls:     1,
	},
	{
		name:      "2 disjoint columns",
		ancestor:  ballast(sql.NewRow(1, 0, 0), sql.NewRow(99, 0, 0)),
		left:      ballast(sql.NewRow(1, 1, 0), sql.NewRow(99, 7, 0)),
		right:     ballast(sql.NewRow(1, 0, 2), sql.NewRow(99, 0, 0)),
		conflicts: [4]bool{true, false, false, true},
		calls:     1,
	},
	{
		name:      "3 same column, different values",
		ancestor:  ballast(sql.NewRow(1, 0, 0), sql.NewRow(99, 0, 0)),
		left:      ballast(sql.NewRow(1, 1, 0), sql.NewRow(99, 7, 0)),
		right:     ballast(sql.NewRow(1, 2, 0), sql.NewRow(99, 0, 0)),
		conflicts: [4]bool{true, true, true, true},
		calls:     1,
	},
	{
		name:      "4 identical edit plus a disjoint one",
		ancestor:  ballast(sql.NewRow(1, 0, 0), sql.NewRow(99, 0, 0)),
		left:      ballast(sql.NewRow(1, 1, 5), sql.NewRow(99, 7, 0)),
		right:     ballast(sql.NewRow(1, 1, 0), sql.NewRow(99, 0, 0)),
		conflicts: [4]bool{true, true, false, true},
		calls:     1,
	},
	{
		name:      "5 one side only",
		ancestor:  ballast(sql.NewRow(1, 0, 0), sql.NewRow(99, 0, 0)),
		left:      ballast(sql.NewRow(1, 1, 0), sql.NewRow(99, 7, 0)),
		right:     ballast(sql.NewRow(1, 0, 0), sql.NewRow(99, 0, 0)),
		conflicts: [4]bool{false, false, false, false},
		calls:     0,
	},
	{
		name:      "6 add/add identical",
		ancestor:  ballast(sql.NewRow(1, 0, 0), sql.NewRow(99, 0, 0)),
		left:      ballast(sql.NewRow(1, 0, 0), sql.NewRow(2, 1, 1), sql.NewRow(99, 7, 0)),
		right:     ballast(sql.NewRow(1, 0, 0), sql.NewRow(2, 1, 1), sql.NewRow(99, 0, 0)),
		conflicts: [4]bool{true, true, false, false},
		calls:     1,
	},
	{
		name:      "7 add/add different",
		ancestor:  ballast(sql.NewRow(1, 0, 0), sql.NewRow(99, 0, 0)),
		left:      ballast(sql.NewRow(1, 0, 0), sql.NewRow(2, 1, 1), sql.NewRow(99, 7, 0)),
		right:     ballast(sql.NewRow(1, 0, 0), sql.NewRow(2, 9, 9), sql.NewRow(99, 0, 0)),
		conflicts: [4]bool{true, true, true, true},
		calls:     1,
	},
	{
		name:      "8 modify vs delete",
		ancestor:  ballast(sql.NewRow(1, 0, 0), sql.NewRow(99, 0, 0)),
		left:      ballast(sql.NewRow(1, 1, 0), sql.NewRow(99, 7, 0)),
		right:     ballast(sql.NewRow(99, 0, 0)),
		conflicts: [4]bool{true, true, true, true},
		calls:     1,
	},
	{
		name:      "9 delete vs delete",
		ancestor:  ballast(sql.NewRow(1, 0, 0), sql.NewRow(99, 0, 0)),
		left:      ballast(sql.NewRow(99, 7, 0)),
		right:     ballast(sql.NewRow(99, 0, 0)),
		conflicts: [4]bool{true, true, false, false},
		calls:     1,
	},
}

// Each level must produce its whole column of the matrix, on both merge paths.
// Four different answer sets from one interface is the expressiveness proof:
// any single level could be satisfied by a hook with less information.
func TestRowMergePolicy_FourStrictnessLevels(t *testing.T) {
	for _, shape := range []struct {
		name   string
		schema namedSchema
	}{
		{"fast path", fastPathSchema},
		{"differ path", differPathSchema},
	} {
		t.Run(shape.name, func(t *testing.T) {
			for li, level := range strictnessLevels {
				t.Run(level.name, func(t *testing.T) {
					for _, tc := range levelCases {
						t.Run(tc.name, func(t *testing.T) {
							calls := 0
							data := dataTest{
								name:     tc.name,
								ancestor: tc.ancestor,
								left:     tc.left,
								right:    tc.right,
								merged:   tc.left,
							}
							result, err := runPolicyMergeWithSchema(t, shape.schema, data, level.policy(&calls))
							require.NoError(t, err)

							require.Equal(t, tc.calls, calls,
								"three-way decisions offered to the policy; a one-sided edit is not one")

							if tc.conflicts[li] {
								require.Equal(t, 1, dataConflictCount(result),
									"%s must conflict on %q", level.name, tc.name)
							} else {
								require.Zero(t, dataConflictCount(result),
									"%s must merge %q", level.name, tc.name)
							}
						})
					}
				})
			}
		})
	}
}

// Inertness, by hash: for every case, a nil policy and an always-defer policy
// must produce identical merged roots. Stronger than comparing outcomes case
// by case, because it cannot be fooled by a case nobody thought to check.
func TestRowMergePolicy_AlwaysDeferIsInertByHash(t *testing.T) {
	for _, shape := range []struct {
		name   string
		schema namedSchema
	}{
		{"fast path", fastPathSchema},
		{"differ path", differPathSchema},
	} {
		t.Run(shape.name, func(t *testing.T) {
			for _, tc := range levelCases {
				t.Run(tc.name, func(t *testing.T) {
					data := dataTest{
						name:     tc.name,
						ancestor: tc.ancestor,
						left:     tc.left,
						right:    tc.right,
						merged:   tc.left,
					}
					withoutPolicy, err := runPolicyMergeWithSchema(t, shape.schema, data, nil)
					require.NoError(t, err)

					p := &recordingRowMergePolicy{}
					withDefer, err := runPolicyMergeWithSchema(t, shape.schema, data, p.opt())
					require.NoError(t, err)

					require.Len(t, p.calls, tc.calls)
					requireSameMergedTable(t, withoutPolicy, withDefer)
				})
			}
		})
	}
}
