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

package tree

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/val"
)

// In every test, base, left and right are maps of key to one value. Key 2 is changed on the left only, key 4 on both
// sides alike, and the other keys are unchanged.
var (
	flaggedTestBase  = [][]int{{1, 10}, {2, 20}, {3, 30}, {4, 40}, {6, 60}}
	flaggedTestLeft  = [][]int{{1, 10}, {2, 21}, {3, 30}, {4, 41}, {6, 60}}
	flaggedTestRight = [][]int{{1, 10}, {2, 20}, {3, 30}, {4, 41}, {6, 60}}
)

func TestFlaggedKeyDiffer(t *testing.T) {
	passThrough := func(diff ThreeWayDiff) ThreeWayDiff { return diff }
	conflict := func(diff ThreeWayDiff) ThreeWayDiff {
		diff.Op = DiffOpDivergentModifyConflict
		return diff
	}
	tests := []struct {
		name    string
		flagged []int
		check   func(ThreeWayDiff) ThreeWayDiff
		// checked lists the diffs the check sees, and exp the diffs the differ returns.
		checked []testDiff
		exp     []testDiff
	}{
		{
			name:  "no flagged keys",
			check: conflict,
			exp: []testDiff{
				{op: DiffOpLeftModify, k: 2, l: []int{21}},
				{op: DiffOpConvergentModify, k: 4, l: []int{41}},
			},
		},
		{
			name:    "a flagged key that one side changed",
			flagged: []int{2},
			check:   conflict,
			checked: []testDiff{{op: DiffOpLeftModify, k: 2, l: []int{21}}},
			exp: []testDiff{
				{op: DiffOpDivergentModifyConflict, k: 2, l: []int{21}},
				{op: DiffOpConvergentModify, k: 4, l: []int{41}},
			},
		},
		{
			name:    "a flagged key that neither side changed is dropped unless the check replaces it",
			flagged: []int{3},
			check:   passThrough,
			checked: []testDiff{{op: DiffOpUnchanged, k: 3, l: []int{30}, r: []int{30}}},
			exp: []testDiff{
				{op: DiffOpLeftModify, k: 2, l: []int{21}},
				{op: DiffOpConvergentModify, k: 4, l: []int{41}},
			},
		},
		{
			name:    "flagged keys before, between and after the changes, and absent everywhere, in any order",
			flagged: []int{7, 3, 0, 6},
			check:   conflict,
			checked: []testDiff{
				{op: DiffOpUnchanged, k: 0},
				{op: DiffOpUnchanged, k: 3, l: []int{30}, r: []int{30}},
				{op: DiffOpUnchanged, k: 6, l: []int{60}, r: []int{60}},
				{op: DiffOpUnchanged, k: 7},
			},
			exp: []testDiff{
				{op: DiffOpDivergentModifyConflict, k: 0},
				{op: DiffOpLeftModify, k: 2, l: []int{21}},
				{op: DiffOpDivergentModifyConflict, k: 3, l: []int{30}, r: []int{30}},
				{op: DiffOpConvergentModify, k: 4, l: []int{41}},
				{op: DiffOpDivergentModifyConflict, k: 6, l: []int{60}, r: []int{60}},
				{op: DiffOpDivergentModifyConflict, k: 7},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			ns := NewTestNodeStore()
			valDesc := &val.TupleDesc{Types: []val.Type{{Enc: val.Int64Enc, Nullable: true}}}
			base := newTestMap(t, ctx, flaggedTestBase, ns, valDesc)
			left := newTestMap(t, ctx, flaggedTestLeft, ns, valDesc)
			right := newTestMap(t, ctx, flaggedTestRight, ns, valDesc)
			inner, err := NewThreeWayDiffer(ctx, ns, left, right, base, nil, nil, false, ThreeWayDiffInfo{}, keyDesc)
			require.NoError(t, err)

			var checked []testDiff
			check := func(ctx *sql.Context, diff ThreeWayDiff) (ThreeWayDiff, error) {
				checked = append(checked, formatTestDiff(t, diff, keyDesc, valDesc))
				if diff.Op == DiffOpUnchanged {
					require.Equal(t, diff.Base, diff.Left)
					require.Equal(t, diff.Base, diff.Right)
				}
				return tt.check(diff), nil
			}
			iter, err := NewFlaggedKeyDiffer(ctx, inner, base, testKeys(t, ctx, ns, tt.flagged), check)
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
			require.Equal(t, tt.checked, checked)
			require.Equal(t, tt.exp, out)
		})
	}
}

func TestFlaggedKeyDifferCheckError(t *testing.T) {
	ctx := sql.NewEmptyContext()
	ns := NewTestNodeStore()
	valDesc := &val.TupleDesc{Types: []val.Type{{Enc: val.Int64Enc, Nullable: true}}}
	base := newTestMap(t, ctx, flaggedTestBase, ns, valDesc)
	inner, err := NewThreeWayDiffer(ctx, ns, base, base, base, nil, nil, false, ThreeWayDiffInfo{}, keyDesc)
	require.NoError(t, err)
	checkErr := errors.New("check failed")
	iter, err := NewFlaggedKeyDiffer(ctx, inner, base, testKeys(t, ctx, ns, []int{3}),
		func(*sql.Context, ThreeWayDiff) (ThreeWayDiff, error) { return ThreeWayDiff{}, checkErr })
	require.NoError(t, err)
	_, err = iter.Next(ctx)
	require.ErrorIs(t, err, checkErr)
}

func testKeys(t *testing.T, ctx context.Context, ns NodeStore, keys []int) []val.Tuple {
	builder := val.NewTupleBuilder(keyDesc, ns)
	tuples := make([]val.Tuple, len(keys))
	for i, k := range keys {
		builder.PutInt64(0, int64(k))
		key, err := builder.Build(ctx, ns.Pool())
		require.NoError(t, err)
		tuples[i] = key
	}
	return tuples
}
