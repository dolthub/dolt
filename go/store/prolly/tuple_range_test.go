// Copyright 2022 Dolthub, Inc.
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

package prolly

import (
	"context"
	"encoding/binary"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

var rangeTuples = []val.Tuple{
	intNullTuple(nil, &nine), // 0
	intTuple(1, 1),           // 1
	intTuple(1, 2),           // 2
	intTuple(1, 3),           // 3
	intTuple(2, 1),           // 4
	intTuple(2, 2),           // 5
	intTuple(2, 3),           // 6
	intTuple(3, 1),           // 7
	intTuple(3, 2),           // 8
	intTuple(3, 3),           // 9
	intTuple(4, 1),           // 10
	intTuple(4, 2),           // 11
	intTuple(4, 3),           // 12
	intNullTuple(&nine, nil), // 13
}

var nine = int32(9)

func TestRangeSearch(t *testing.T) {
	intType := val.Type{
		Enc:      val.Int32Enc,
		Nullable: true,
	}
	twoCol := val.NewTupleDescriptor(
		intType, // c0
		intType, // c1
	)

	tests := []struct {
		name      string
		logical   []int
		testRange Range
		physical  [2]int
	}{
		{
			name: "unbound range",
			testRange: Range{
				Fields: []RangeField{
					{
						Lo: Bound{Binding: false},
						Hi: Bound{Binding: false},
					},
				},
				Desc: twoCol,
			},
			physical: [2]int{0, 14},
			logical:  []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14},
		},

		// first column ranges
		{
			name: "c0 > 1",
			testRange: Range{
				Fields: []RangeField{
					{
						Lo: Bound{Binding: true, Inclusive: false, Value: intVal(1)},
					},
				},
				Desc: twoCol,
			},
			physical: [2]int{4, 14},
			logical:  []int{4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14},
		},
		{
			name: "c0 < 1",
			testRange: Range{
				Fields: []RangeField{
					{
						Lo: Bound{Binding: true, Inclusive: false, Value: nil},
						Hi: Bound{Binding: true, Inclusive: false, Value: intVal(1)},
					},
				},
				Desc: twoCol,
			},
			physical: [2]int{1, 1},
			logical:  []int{},
		},
		{
			name: "2 <= c0 <= 3",
			testRange: Range{
				Fields: []RangeField{
					{
						Lo: Bound{Binding: true, Inclusive: true, Value: intVal(2)},
						Hi: Bound{Binding: true, Inclusive: true, Value: intVal(3)},
					},
				},
				Desc: twoCol,
			},
			physical: [2]int{4, 10},
			logical:  []int{4, 5, 6, 7, 8, 9, 10, 11},
		},
		{
			name: "c0 = NULL",
			testRange: Range{
				Fields: []RangeField{
					{
						Lo:             Bound{Binding: true, Inclusive: true, Value: nil},
						Hi:             Bound{Binding: true, Inclusive: true, Value: nil},
						BoundsAreEqual: true,
					},
				},
				Desc: twoCol,
			},
			physical: [2]int{0, 1},
			logical:  []int{0},
		},

		// second column ranges
		{
			name: "c1 == 2",
			testRange: Range{
				Fields: []RangeField{
					{
						Lo: Bound{Binding: true, Inclusive: false, Value: nil},
					},
					{
						Lo:             Bound{Binding: true, Inclusive: true, Value: intVal(2)},
						Hi:             Bound{Binding: true, Inclusive: true, Value: intVal(2)},
						BoundsAreEqual: true,
					},
				},
				Desc: twoCol,
			},
			physical: [2]int{1, 14},
			logical:  []int{2, 5, 8, 11},
		},
	}

	ns := tree.NewTestNodeStore()

	values := make([]val.Tuple, len(rangeTuples))
	for i := range values {
		values[i] = make(val.Tuple, 2)
	}
	testNode := tree.NewTupleLeafNode(rangeTuples, values)
	tm := NewMap(testNode, ns, twoCol, &val.TupleDesc{})

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			rng := test.testRange
			lo, hi := test.physical[0], test.physical[1]

			startSearch := rangeStartSearchFn(rng)
			idx, err := startSearch(ctx, testNode)
			require.NoError(t, err)
			assert.Equal(t, lo, idx, "range should start at index %d", lo)

			stopSearch := rangeStopSearchFn(rng)
			idx, err = stopSearch(ctx, testNode)
			require.NoError(t, err)
			assert.Equal(t, hi, idx, "range should stop before index %d", hi)

			// validate physical range (unfiltered iter)
			iter, err := treeIterFromRange(ctx, testNode, ns, test.testRange)
			require.NoError(t, err)
			expected := rangeTuples[lo:hi]

			i := 0
			for {
				act, _, err := iter.Next(ctx)
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				require.True(t, i < len(expected))
				assert.Equal(t, expected[i], act)
				i++
			}

			// validate logical range
			iter2, err := tm.IterRange(ctx, rng)
			require.NoError(t, err)
			expected2 := test.logical

			i = 0
			for {
				act, _, err := iter2.Next(ctx)
				if err == io.EOF {
					break
				}
				exp := rangeTuples[expected2[i]]
				require.NoError(t, err)
				assert.Equal(t, exp, act)
				i++
			}
		})
	}
}

func intVal(i int32) (buf []byte) {
	buf = make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(i))
	return
}

func intNullTuple(ints ...*int32) val.Tuple {
	types := make([]val.Type, len(ints))
	for i := range types {
		types[i] = val.Type{
			Enc:      val.Int32Enc,
			Nullable: true,
		}
	}

	desc := val.NewTupleDescriptor(types...)
	tb := val.NewTupleBuilder(desc, ns)
	for i, val := range ints {
		if val != nil {
			tb.PutInt32(i, *val)
		}
	}
	tup, err := tb.Build(context.Background(), sharedPool)
	if err != nil {
		panic(err)
	}
	return tup
}

func TestMergeIntersectingRanges(t *testing.T) {
	ctx := context.Background()
	intType := val.Type{Enc: val.Int32Enc, Nullable: true}
	twoCol := val.NewTupleDescriptor(intType, intType)
	closed := func(lo, hi int32) RangeField {
		return RangeField{
			Lo:             Bound{Binding: true, Inclusive: true, Value: intVal(lo)},
			Hi:             Bound{Binding: true, Inclusive: true, Value: intVal(hi)},
			BoundsAreEqual: lo == hi,
		}
	}
	open := func(lo, hi int32) RangeField {
		return RangeField{
			Lo: Bound{Binding: true, Value: intVal(lo)},
			Hi: Bound{Binding: true, Value: intVal(hi)},
		}
	}
	atLeast := func(lo int32) RangeField {
		return RangeField{Lo: Bound{Binding: true, Inclusive: true, Value: intVal(lo)}}
	}
	rng := func(fields ...RangeField) Range {
		return Range{Fields: fields, Desc: twoCol, IsContiguous: true, SkipRangeMatchCallback: true}
	}
	spanned := func(fields ...RangeField) Range {
		return Range{Fields: fields, Desc: twoCol}
	}

	tests := []struct {
		name   string
		ranges []Range
		merged []Range
	}{
		{
			name:   "disjoint ranges are kept",
			ranges: []Range{rng(closed(1, 2)), rng(closed(3, 4))},
			merged: []Range{rng(closed(1, 2)), rng(closed(3, 4))},
		},
		{
			name:   "ranges touching at an exclusive bound are kept",
			ranges: []Range{rng(closed(1, 2)), rng(open(2, 4))},
			merged: []Range{rng(closed(1, 2)), rng(open(2, 4))},
		},
		{
			name:   "equal points are merged",
			ranges: []Range{rng(closed(2, 2)), rng(closed(2, 2))},
			merged: []Range{spanned(closed(2, 2))},
		},
		{
			name:   "overlapping ranges are merged",
			ranges: []Range{rng(closed(1, 3)), rng(open(2, 5))},
			merged: []Range{spanned(RangeField{
				Lo: Bound{Binding: true, Inclusive: true, Value: intVal(1)},
				Hi: Bound{Binding: true, Value: intVal(5)},
			})},
		},
		{
			name:   "unbound range absorbs others",
			ranges: []Range{rng(closed(1, 3)), rng(atLeast(2))},
			merged: []Range{spanned(atLeast(1))},
		},
		{
			name:   "a merged range is merged again with ranges it grew into",
			ranges: []Range{rng(closed(1, 2)), rng(closed(4, 5)), rng(closed(2, 4)), rng(closed(7, 8))},
			merged: []Range{spanned(closed(1, 5)), rng(closed(7, 8))},
		},
		{
			name:   "unsorted ranges are merged",
			ranges: []Range{rng(closed(7, 8)), rng(closed(2, 4)), rng(closed(1, 2))},
			merged: []Range{spanned(closed(1, 4)), rng(closed(7, 8))},
		},
		{
			name:   "a wide range absorbs ranges it holds",
			ranges: []Range{rng(closed(1, 10)), rng(closed(2, 3)), rng(closed(5, 6)), rng(closed(11, 12))},
			merged: []Range{spanned(closed(1, 10)), rng(closed(11, 12))},
		},
		{
			name:   "ranges disjoint on a second field are kept",
			ranges: []Range{rng(closed(1, 1), closed(1, 2)), rng(closed(1, 1), closed(3, 4))},
			merged: []Range{rng(closed(1, 1), closed(1, 2)), rng(closed(1, 1), closed(3, 4))},
		},
		{
			name:   "ranges overlapping on every field are merged",
			ranges: []Range{rng(closed(1, 1), closed(1, 3)), rng(closed(1, 1), closed(3, 4))},
			merged: []Range{spanned(closed(1, 1), closed(1, 4))},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			merged, err := MergeIntersectingRanges(ctx, test.ranges)
			require.NoError(t, err)
			assert.Equal(t, test.merged, merged)
		})
	}
}

func TestMergedRangesVisitEachTupleOnce(t *testing.T) {
	ctx := context.Background()
	intType := val.Type{Enc: val.Int32Enc, Nullable: true}
	twoCol := val.NewTupleDescriptor(intType, intType)
	values := make([]val.Tuple, len(rangeTuples))
	for i := range values {
		values[i] = make(val.Tuple, 2)
	}
	m := NewMap(tree.NewTupleLeafNode(rangeTuples, values), tree.NewTestNodeStore(), twoCol, &val.TupleDesc{})

	c0Between := func(lo, hi int32) Range {
		return Range{
			Fields: []RangeField{{
				Lo: Bound{Binding: true, Inclusive: true, Value: intVal(lo)},
				Hi: Bound{Binding: true, Inclusive: true, Value: intVal(hi)},
			}},
			Desc: twoCol,
		}
	}
	merged, err := MergeIntersectingRanges(ctx, []Range{c0Between(1, 2), c0Between(2, 3), c0Between(2, 2)})
	require.NoError(t, err)
	require.Len(t, merged, 1)

	iter, err := m.IterRange(ctx, merged[0])
	require.NoError(t, err)
	var visited []val.Tuple
	for {
		k, _, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		visited = append(visited, k)
	}
	assert.Equal(t, rangeTuples[1:10], visited)
}
