// Copyright 2023 Dolthub, Inc.
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

package tree

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/val"
)

type testDiff struct {
	l  []int
	r  []int
	m  []int
	k  int
	op DiffOp
}

func (d testDiff) String() string {
	return fmt.Sprintf("%s(key=%d)", d.op, d.k)
}

func TestThreeWayDiffer(t *testing.T) {
	tests := []struct {
		name  string
		base  [][]int
		left  [][]int
		right [][]int
		exp   []testDiff
	}{
		{
			name:  "left adds",
			base:  [][]int{{1, 1}, {2, 2}},
			left:  [][]int{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}},
			right: [][]int{{1, 1}, {2, 2}, {4, 4}},
			exp: []testDiff{
				{op: DiffOpLeftAdd, k: 3},
				{op: DiffOpConvergentAdd, k: 4},
				{op: DiffOpLeftAdd, k: 5},
				{op: DiffOpLeftAdd, k: 6},
			},
		},
		{
			name:  "right adds",
			base:  [][]int{{1, 1}, {2, 2}},
			left:  [][]int{{1, 1}, {2, 2}, {4, 4}},
			right: [][]int{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}},
			exp: []testDiff{
				{op: DiffOpRightAdd, k: 3},
				{op: DiffOpConvergentAdd, k: 4},
				{op: DiffOpRightAdd, k: 5},
				{op: DiffOpRightAdd, k: 6},
			},
		},
		{
			name:  "left deletes",
			base:  [][]int{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}},
			left:  [][]int{{1, 1}, {2, 2}},
			right: [][]int{{1, 1}, {2, 2}, {3, 3}, {5, 5}, {6, 6}},
			exp: []testDiff{
				{op: DiffOpLeftDelete, k: 3},
				{op: DiffOpConvergentDelete, k: 4},
				{op: DiffOpLeftDelete, k: 5},
				{op: DiffOpLeftDelete, k: 6},
			},
		},
		{
			name:  "right deletes",
			base:  [][]int{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}},
			left:  [][]int{{1, 1}, {2, 2}, {3, 3}, {5, 5}, {6, 6}},
			right: [][]int{{1, 1}, {2, 2}},
			exp: []testDiff{
				{op: DiffOpRightDelete, k: 3},
				{op: DiffOpConvergentDelete, k: 4},
				{op: DiffOpRightDelete, k: 5},
				{op: DiffOpRightDelete, k: 6},
			},
		},
		{
			name:  "left edits",
			base:  [][]int{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}},
			left:  [][]int{{1, 1}, {2, 3}, {3, 3}, {4, 5}, {5, 6}, {6, 7}},
			right: [][]int{{1, 1}, {2, 2}, {3, 3}, {4, 5}, {5, 5}, {6, 6}},
			exp: []testDiff{
				{op: DiffOpLeftModify, k: 2},
				{op: DiffOpConvergentModify, k: 4},
				{op: DiffOpLeftModify, k: 5},
				{op: DiffOpLeftModify, k: 6},
			},
		},
		{
			name:  "right edits",
			base:  [][]int{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}},
			left:  [][]int{{1, 1}, {2, 2}, {3, 3}, {4, 5}, {5, 5}, {6, 6}},
			right: [][]int{{1, 1}, {2, 3}, {3, 3}, {4, 5}, {5, 6}, {6, 7}},
			exp: []testDiff{
				{op: DiffOpRightModify, k: 2},
				{op: DiffOpConvergentModify, k: 4},
				{op: DiffOpRightModify, k: 5},
				{op: DiffOpRightModify, k: 6},
			},
		},
		{
			name:  "delete conflicts",
			base:  [][]int{{1, 1}, {2, 2}},
			left:  [][]int{{1, 1}},
			right: [][]int{{1, 1}, {2, 3}},
			exp: []testDiff{
				{op: DiffOpDivergentDeleteConflict, k: 2},
			},
		},
		{
			name:  "convergent edits",
			base:  [][]int{{1, 1}, {4, 4}},
			left:  [][]int{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}},
			right: [][]int{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}},
			exp: []testDiff{
				{op: DiffOpConvergentAdd, k: 2},
				{op: DiffOpConvergentAdd, k: 3},
				{op: DiffOpConvergentAdd, k: 5},
			},
		},
		{
			name:  "clash edits",
			base:  [][]int{{1, 1}, {4, 4}},
			left:  [][]int{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}},
			right: [][]int{{1, 1}, {2, 3}, {3, 4}, {4, 4}, {5, 6}},
			exp: []testDiff{
				{op: DiffOpDivergentModifyConflict, k: 2},
				{op: DiffOpDivergentModifyConflict, k: 3},
				{op: DiffOpDivergentModifyConflict, k: 5},
			},
		},
		{
			name:  "resolvable edits",
			base:  [][]int{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}, {4, 4, 4}, {5, 5, 5}},
			left:  [][]int{{1, 1, 1}, {2, 2, 3}, {3, 3, 4}, {4, 4, 4}, {5, 5, 6}},
			right: [][]int{{1, 1, 1}, {2, 3, 2}, {3, 4, 3}, {4, 4, 4}, {5, 6, 5}},
			exp: []testDiff{
				{op: DiffOpDivergentModifyResolved, k: 2, m: []int{3, 3}},
				{op: DiffOpDivergentModifyResolved, k: 3, m: []int{4, 4}},
				{op: DiffOpDivergentModifyResolved, k: 5, m: []int{6, 6}},
			},
		},
		{
			name:  "combine types",
			base:  [][]int{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}, {4, 4, 4}, {5, 5, 5}, {8, 8, 8}},
			left:  [][]int{{1, 1, 1}, {2, 2, 3}, {3, 3, 4}, {5, 5, 6}, {6, 6, 6}},
			right: [][]int{{1, 1, 1}, {2, 3, 4}, {3, 4, 3}, {4, 4, 4}, {5, 6, 5}, {7, 7, 7}},
			exp: []testDiff{
				{op: DiffOpDivergentModifyConflict, k: 2},
				{op: DiffOpDivergentModifyResolved, k: 3, m: []int{4, 4}},
				{op: DiffOpLeftDelete, k: 4},
				{op: DiffOpDivergentModifyResolved, k: 5, m: []int{6, 6}},
				{op: DiffOpLeftAdd, k: 6},
				{op: DiffOpRightAdd, k: 7},
				{op: DiffOpConvergentDelete, k: 8},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			ns := NewTestNodeStore()

			var valTypes []val.Type
			for i := 0; i < len(tt.base[0])-1; i++ {
				valTypes = append(valTypes, val.Type{Enc: val.Int64Enc, Nullable: true})
			}

			valDesc := &val.TupleDesc{Types: valTypes}

			base := newTestMap(t, ctx, tt.base, ns, valDesc)
			left := newTestMap(t, ctx, tt.left, ns, valDesc)
			right := newTestMap(t, ctx, tt.right, ns, valDesc)

			var diffInfo ThreeWayDiffInfo
			iter, err := NewThreeWayDiffer(ctx, ns, left, right, base, testResolver(t, ns, valDesc, val.NewTupleBuilder(valDesc, ns)), nil, false, diffInfo, keyDesc)
			require.NoError(t, err)

			var cmp []testDiff
			for {
				diff, err := iter.Next(ctx)
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err)
				cmp = append(cmp, formatTestDiff(t, diff, keyDesc, valDesc))
			}

			require.Equal(t, len(cmp), len(tt.exp), "number of diffs not equal")

			for i, exp := range tt.exp {
				cmp := cmp[i]
				compareDiffs(t, exp, cmp)
			}
		})
	}
}

// policyCall records one invocation of a RowMergePolicy under test.
type policyCall struct {
	key               int
	left, right, base []int
}

// rowMergePolicyFixture records every invocation and returns a scripted answer.
type rowMergePolicyFixture struct {
	calls  []policyCall
	answer func(left, right, base []int) (val.Tuple, RowMergeStatus)
}

func (f *rowMergePolicyFixture) policy(t *testing.T, valDesc *val.TupleDesc) RowMergePolicy {
	return func(_ *sql.Context, left, right, base val.Tuple) (val.Tuple, RowMergeStatus, error) {
		l, r, b := extractTestVal(t, valDesc, left), extractTestVal(t, valDesc, right), extractTestVal(t, valDesc, base)
		f.calls = append(f.calls, policyCall{left: l, right: r, base: b})
		if f.answer == nil {
			return nil, RowMergeDefer, nil
		}
		merged, status := f.answer(l, r, b)
		return merged, status, nil
	}
}

func runThreeWayDifferWithPolicy(
	t *testing.T,
	base, left, right [][]int,
	policy func(valDesc *val.TupleDesc) RowMergePolicy,
) []testDiff {
	return runThreeWayDifferWithPolicyKeyless(t, base, left, right, policy, false)
}

func runThreeWayDifferWithPolicyKeyless(
	t *testing.T,
	base, left, right [][]int,
	policy func(valDesc *val.TupleDesc) RowMergePolicy,
	keyless bool,
) []testDiff {
	t.Helper()
	ctx := sql.NewEmptyContext()
	ns := NewTestNodeStore()

	var valTypes []val.Type
	for i := 0; i < len(base[0])-1; i++ {
		valTypes = append(valTypes, val.Type{Enc: val.Int64Enc, Nullable: true})
	}
	valDesc := &val.TupleDesc{Types: valTypes}

	baseMap := newTestMap(t, ctx, base, ns, valDesc)
	leftMap := newTestMap(t, ctx, left, ns, valDesc)
	rightMap := newTestMap(t, ctx, right, ns, valDesc)

	var p RowMergePolicy
	if policy != nil {
		p = policy(valDesc)
	}

	var diffInfo ThreeWayDiffInfo
	iter, err := NewThreeWayDiffer(ctx, ns, leftMap, rightMap, baseMap,
		testResolver(t, ns, valDesc, val.NewTupleBuilder(valDesc, ns)), p, keyless, diffInfo, keyDesc)
	require.NoError(t, err)

	var out []testDiff
	for {
		diff, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		out = append(out, formatTestDiff(t, diff, keyDesc, valDesc))
	}
	return out
}

// A nil policy must leave the differ's classification untouched.
func TestRowMergePolicy_NilIsInert(t *testing.T) {
	base := [][]int{{1, 0, 0}, {2, 0, 0}, {3, 0, 0}}
	left := [][]int{{1, 1, 0}, {2, 1, 0}, {3, 1, 0}}
	right := [][]int{{1, 1, 0}, {2, 2, 0}, {3, 0, 2}}

	withoutPolicy := runThreeWayDifferWithPolicy(t, base, left, right, nil)
	deferring := runThreeWayDifferWithPolicy(t, base, left, right, func(vd *val.TupleDesc) RowMergePolicy {
		f := &rowMergePolicyFixture{}
		return f.policy(t, vd)
	})
	require.Equal(t, withoutPolicy, deferring, "an always-defer policy must not change classification")
}

// The policy must be consulted for every three-way decision, including the
// convergent branches that short-circuit today, and for nothing else.
func TestRowMergePolicy_SeesEveryThreeWayDecision(t *testing.T) {
	// key 1 convergent modify (identical bytes), key 2 divergent modify,
	// key 3 convergent delete, key 4 divergent delete, key 5 convergent add,
	// key 6 left-only edit (not a three-way decision), key 7 right-only edit.
	base := [][]int{{1, 0, 0}, {2, 0, 0}, {3, 0, 0}, {4, 0, 0}, {6, 0, 0}, {7, 0, 0}}
	left := [][]int{{1, 1, 0}, {2, 1, 0}, {4, 1, 0}, {5, 9, 9}, {6, 5, 0}, {7, 0, 0}}
	right := [][]int{{1, 1, 0}, {2, 2, 0}, {5, 9, 9}, {6, 0, 0}, {7, 5, 0}}

	f := &rowMergePolicyFixture{}
	runThreeWayDifferWithPolicy(t, base, left, right, func(vd *val.TupleDesc) RowMergePolicy {
		return f.policy(t, vd)
	})

	require.Len(t, f.calls, 5, "policy must see exactly the five three-way decisions: %#v", f.calls)

	// convergent modify: both sides moved the row to the same value
	require.Equal(t, []int{1, 0}, f.calls[0].left)
	require.Equal(t, []int{1, 0}, f.calls[0].right)
	require.Equal(t, []int{0, 0}, f.calls[0].base)
	// divergent modify
	require.Equal(t, []int{1, 0}, f.calls[1].left)
	require.Equal(t, []int{2, 0}, f.calls[1].right)
	// convergent delete: both sides removed the row
	require.Nil(t, f.calls[2].left)
	require.Nil(t, f.calls[2].right)
	require.Equal(t, []int{0, 0}, f.calls[2].base)
	// divergent delete: right removed, left modified
	require.Equal(t, []int{1, 0}, f.calls[3].left)
	require.Nil(t, f.calls[3].right)
	// convergent add: no base
	require.Nil(t, f.calls[4].base)
	require.Equal(t, []int{9, 9}, f.calls[4].left)
}

// Keyless tables always use the default reconciler; the policy is not consulted.
func TestRowMergePolicy_NotConsultedForKeylessTables(t *testing.T) {
	base := [][]int{{1, 0, 0}, {2, 0, 0}}
	left := [][]int{{1, 1, 0}, {2, 1, 0}}
	right := [][]int{{1, 1, 0}, {2, 2, 0}}

	f := &rowMergePolicyFixture{answer: func(_, _, _ []int) (val.Tuple, RowMergeStatus) {
		return nil, RowMergeConflict
	}}
	diffs := runThreeWayDifferWithPolicyKeyless(t, base, left, right, func(vd *val.TupleDesc) RowMergePolicy {
		return f.policy(t, vd)
	}, true)

	require.Empty(t, f.calls, "a keyless table must not consult the policy")
	require.Equal(t, DiffOpConvergentModify, diffs[0].op)
}

// Each status must map to the diff op the merge layer acts on.
func TestRowMergePolicy_StatusMapping(t *testing.T) {
	base := [][]int{{1, 0, 0}, {2, 0, 0}}
	left := [][]int{{1, 1, 0}, {2, 1, 0}}
	right := [][]int{{1, 1, 0}, {2, 2, 0}}

	t.Run("conflict on a convergent edit", func(t *testing.T) {
		// Baseline: without a policy, key 1 is byte-identical on both sides and
		// the differ merges it silently. That short-circuit is what a policy has
		// to be able to override.
		baseline := runThreeWayDifferWithPolicy(t, base, left, right, nil)
		require.Equal(t, DiffOpConvergentModify, baseline[0].op,
			"precondition: key 1 must be a convergent edit without a policy")

		diffs := runThreeWayDifferWithPolicy(t, base, left, right, func(vd *val.TupleDesc) RowMergePolicy {
			f := &rowMergePolicyFixture{answer: func(_, _, _ []int) (val.Tuple, RowMergeStatus) {
				return nil, RowMergeConflict
			}}
			return f.policy(t, vd)
		})
		require.Len(t, diffs, 2)
		// Key 1 is byte-identical on both sides; without a policy it merges silently.
		require.Equal(t, DiffOpDivergentModifyConflict, diffs[0].op)
		require.Equal(t, DiffOpDivergentModifyConflict, diffs[1].op)
	})

	t.Run("resolved supplies the row", func(t *testing.T) {
		diffs := runThreeWayDifferWithPolicy(t, base, left, right, func(vd *val.TupleDesc) RowMergePolicy {
			b := val.NewTupleBuilder(vd, NewTestNodeStore())
			f := &rowMergePolicyFixture{answer: func(_, _, _ []int) (val.Tuple, RowMergeStatus) {
				b.PutInt64(0, 42)
				b.PutInt64(1, 43)
				tup, err := b.Build(context.Background(), NewTestNodeStore().Pool())
				require.NoError(t, err)
				return tup, RowMergeResolved
			}}
			return f.policy(t, vd)
		})
		require.Len(t, diffs, 2)
		require.Equal(t, DiffOpDivergentModifyResolved, diffs[0].op)
		require.Equal(t, []int{42, 43}, diffs[0].m)
	})

	t.Run("resolved with a nil row deletes", func(t *testing.T) {
		diffs := runThreeWayDifferWithPolicy(t, base, left, right, func(vd *val.TupleDesc) RowMergePolicy {
			f := &rowMergePolicyFixture{answer: func(_, _, _ []int) (val.Tuple, RowMergeStatus) {
				return nil, RowMergeResolved
			}}
			return f.policy(t, vd)
		})
		require.Len(t, diffs, 2)
		require.Equal(t, DiffOpDivergentDeleteResolved, diffs[0].op)
	})
}

// A conflict where both sides deleted the row must be representable: this is
// the delete/delete case at the documentTouched level.
func TestRowMergePolicy_ConflictOnConvergentDelete(t *testing.T) {
	base := [][]int{{1, 0, 0}}
	left := [][]int{}
	right := [][]int{}

	diffs := runThreeWayDifferWithPolicy(t, base, left, right, func(vd *val.TupleDesc) RowMergePolicy {
		f := &rowMergePolicyFixture{answer: func(_, _, _ []int) (val.Tuple, RowMergeStatus) {
			return nil, RowMergeConflict
		}}
		return f.policy(t, vd)
	})
	require.Len(t, diffs, 1)
	require.Equal(t, DiffOpDivergentDeleteConflict, diffs[0].op)
}

func testResolver(t *testing.T, ns NodeStore, valDesc *val.TupleDesc, valBuilder *val.TupleBuilder) func(*sql.Context, val.Tuple, val.Tuple, val.Tuple) (val.Tuple, bool, error) {
	return func(_ *sql.Context, l, r, b val.Tuple) (val.Tuple, bool, error) {
		for i := range valDesc.Types {
			var base, left, right int64
			var ok bool
			if b != nil {
				base, ok = valDesc.GetInt64(i, b)
				require.True(t, ok)
			}

			if l != nil {
				left, ok = valDesc.GetInt64(i, l)
				require.True(t, ok)
			}

			if r != nil {
				right, ok = valDesc.GetInt64(i, r)
				require.True(t, ok)
			}

			if base != left && base != right && left != right {
				return nil, false, nil
			} else if base != left {
				valBuilder.PutInt64(i, left)
			} else if base != right {
				valBuilder.PutInt64(i, right)
			} else {
				valBuilder.PutInt64(i, base)
			}
		}
		tup, err := valBuilder.Build(context.Background(), ns.Pool())
		return tup, true, err
	}
}

func compareDiffs(t *testing.T, exp, cmp testDiff) {
	require.Equal(t, exp.op, cmp.op, fmt.Sprintf("unequal diffs:\nexp: %s\nfnd: %s", exp, cmp))
	require.Equal(t, exp.k, cmp.k, fmt.Sprintf("unequal diffs:\nexp: %s\nfnd: %s", exp, cmp))
	switch exp.op {
	case DiffOpDivergentModifyResolved:
		require.Equal(t, exp.m, cmp.m, fmt.Sprintf("unequal resolved:\nexp: %#v\nfnd: %#v", exp.m, cmp.m))
	}
}

func formatTestDiff(t *testing.T, d ThreeWayDiff, keyDesc, valDesc *val.TupleDesc) testDiff {
	key, ok := keyDesc.GetInt64(0, d.Key)
	require.True(t, ok)

	return testDiff{
		op: d.Op,
		k:  int(key),
		l:  extractTestVal(t, valDesc, d.Left),
		r:  extractTestVal(t, valDesc, d.Right),
		m:  extractTestVal(t, valDesc, d.Merged),
	}
}

func extractTestVal(t *testing.T, valDesc *val.TupleDesc, tuple val.Tuple) []int {
	if tuple == nil {
		return nil
	}
	ret := make([]int, len(valDesc.Types))
	for i, _ := range valDesc.Types {
		val, ok := valDesc.GetInt64(i, tuple)
		require.True(t, ok)
		ret[i] = int(val)
	}
	return ret
}

// newTestMap makes a prolly tree from a matrix of integers. Each row corresponds
// to a row in the prolly map. The first value in a row will be the primary key.
// The rest of the values will be the value fields.
func newTestMap(t *testing.T, ctx context.Context, rows [][]int, ns NodeStore, valDesc *val.TupleDesc) StaticMap[val.Tuple, val.Tuple, *val.TupleDesc] {
	serializer := message.NewProllyMapSerializer(keyDesc, valDesc, ns.Pool())
	chkr, err := newEmptyChunker(ctx, ns, serializer)
	require.NoError(t, err)

	keyBuilder := val.NewTupleBuilder(keyDesc, ns)
	valBuilder := val.NewTupleBuilder(valDesc, ns)

	for _, row := range rows {
		keyBuilder.PutInt64(0, int64(row[0]))
		key, err := keyBuilder.Build(context.Background(), ns.Pool())
		require.NoError(t, err)
		for j := 1; j < len(row); j++ {
			valBuilder.PutInt64(j-1, int64(row[j]))
			require.NoError(t, err)
		}
		val, err := valBuilder.Build(context.Background(), ns.Pool())
		require.NoError(t, err)
		err = chkr.AddPair(ctx, Item(key), Item(val))
		require.NoError(t, err)
	}

	root, err := chkr.Done(ctx)
	require.NoError(t, err)
	return StaticMap[val.Tuple, val.Tuple, *val.TupleDesc]{
		Root:      root,
		NodeStore: ns,
		Order:     keyDesc,
	}
}
