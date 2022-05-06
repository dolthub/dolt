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
	intTuple(1, 1),           // 0
	intTuple(1, 2),           // 1
	intTuple(1, 3),           // 2
	intTuple(2, 1),           // 3
	intTuple(2, 2),           // 4
	intTuple(2, 3),           // 5
	intTuple(3, 1),           // 6
	intTuple(3, 2),           // 7
	intTuple(3, 3),           // 8
	intTuple(4, 1),           // 9
	intTuple(4, 2),           // 10
	intTuple(4, 3),           // 11
	intNullTuple(&nine, nil), // 12
	intNullTuple(nil, &nine), // 13
}

var nine = int32(9)

func TestRangeSearch(t *testing.T) {
	intType := val.Type{Enc: val.Int32Enc}
	twoCol := val.NewTupleDescriptor(
		intType, // c0
		intType, // c1
	)

	tests := []struct {
		name      string
		testRange Range
		hi, lo    int
	}{
		{
			name: "unbound range",
			testRange: Range{
				Start: nil,
				Stop:  nil,
				Desc:  twoCol,
			},
			lo: 0,
			hi: 14,
		},

		// first column ranges
		{
			name: "c0 > 1",
			testRange: Range{
				Start: []RangeCut{
					{Value: intVal(1), Inclusive: false},
				},
				Stop: nil,
				Desc: twoCol,
			},
			lo: 3,
			hi: 14,
		},
		{
			name: "c0 < 1",
			testRange: Range{
				Start: nil,
				Stop: []RangeCut{
					{Value: intVal(1), Inclusive: false},
				},
				Desc: twoCol,
			},
			lo: 0,
			hi: 0,
		},
		{
			name: "2 <= c0 <= 3",
			testRange: Range{
				Start: []RangeCut{
					{Value: intVal(2), Inclusive: true},
				},
				Stop: []RangeCut{
					{Value: intVal(3), Inclusive: true},
				},
				Desc: twoCol,
			},
			lo: 3,
			hi: 9,
		},
		{
			name: "c0 = NULL",
			testRange: Range{
				Start: []RangeCut{
					{Null: true},
				},
				Stop: []RangeCut{
					{Null: true},
				},
				Desc: twoCol,
			},
			lo: 13,
			hi: 14,
		},

		// second column ranges
		{
			name: "c1 == 2",
			testRange: Range{
				Start: []RangeCut{
					{Value: nil},
					{Value: intVal(2), Inclusive: true},
				},
				Stop: []RangeCut{
					{Value: nil},
					{Value: intVal(2), Inclusive: true},
				},
				Desc: twoCol,
			},
			lo: 0,
			hi: 14,
		},
	}

	values := make([]val.Tuple, len(rangeTuples))
	for i := range values {
		values[i] = make(val.Tuple, 0)
	}
	testNode := tree.NewTupleLeafNode(rangeTuples, values)
	tm := NewMap(testNode, nil, twoCol, val.TupleDesc{})

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			rng := test.testRange

			startSearch := rangeStartSearchFn(rng)
			idx := startSearch(testNode)
			assert.Equal(t, test.lo, idx, "range should start at index %d", test.lo)

			stopSearch := rangeStopSearchFn(rng)
			idx = stopSearch(testNode)
			assert.Equal(t, test.hi, idx, "range should stop before index %d", test.hi)

			iter, err := tm.IterRange(ctx, rng)
			require.NoError(t, err)
			expected := rangeTuples[test.lo:test.hi]

			i := 0
			for {
				tup, _, err := iter.Next(ctx)
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				require.True(t, i < len(expected))
				assert.Equal(t, expected[i], tup)
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
	tb := val.NewTupleBuilder(desc)
	for i, val := range ints {
		if val != nil {
			tb.PutInt32(i, *val)
		}
	}
	return tb.Build(sharedPool)
}
