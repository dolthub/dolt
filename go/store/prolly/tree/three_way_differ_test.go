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
	op      DiffOp
	k       int
	l, r, m []int
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

			valDesc := val.TupleDesc{Types: valTypes}

			base := newTestMap(t, ctx, tt.base, ns, valDesc)
			left := newTestMap(t, ctx, tt.left, ns, valDesc)
			right := newTestMap(t, ctx, tt.right, ns, valDesc)

			var diffInfo ThreeWayDiffInfo
			iter, err := NewThreeWayDiffer(ctx, ns, left, right, base, testResolver(t, ns, valDesc, val.NewTupleBuilder(valDesc, ns)), false, diffInfo, keyDesc)
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

func testResolver(t *testing.T, ns NodeStore, valDesc val.TupleDesc, valBuilder *val.TupleBuilder) func(*sql.Context, val.Tuple, val.Tuple, val.Tuple) (val.Tuple, bool, error) {
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
		tup, err := valBuilder.Build(ns.Pool())
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

func formatTestDiff(t *testing.T, d ThreeWayDiff, keyDesc, valDesc val.TupleDesc) testDiff {
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

func extractTestVal(t *testing.T, valDesc val.TupleDesc, tuple val.Tuple) []int {
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
func newTestMap(t *testing.T, ctx context.Context, rows [][]int, ns NodeStore, valDesc val.TupleDesc) StaticMap[val.Tuple, val.Tuple, val.TupleDesc] {
	serializer := message.NewProllyMapSerializer(valDesc, ns.Pool())
	chkr, err := newEmptyChunker(ctx, ns, serializer)
	require.NoError(t, err)

	keyBuilder := val.NewTupleBuilder(keyDesc, ns)
	valBuilder := val.NewTupleBuilder(valDesc, ns)

	for _, row := range rows {
		keyBuilder.PutInt64(0, int64(row[0]))
		key, err := keyBuilder.Build(ns.Pool())
		require.NoError(t, err)
		for j := 1; j < len(row); j++ {
			valBuilder.PutInt64(j-1, int64(row[j]))
			require.NoError(t, err)
		}
		val, err := valBuilder.Build(ns.Pool())
		require.NoError(t, err)
		err = chkr.AddPair(ctx, Item(key), Item(val))
		require.NoError(t, err)
	}

	root, err := chkr.Done(ctx)
	require.NoError(t, err)
	return StaticMap[val.Tuple, val.Tuple, val.TupleDesc]{
		Root:      root,
		NodeStore: ns,
		Order:     keyDesc,
	}
}
