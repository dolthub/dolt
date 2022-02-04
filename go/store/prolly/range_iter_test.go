// Copyright 2021 Dolthub, Inc.
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
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/val"
)

type rangeTest struct {
	name      string
	testRange Range
	expCount  int
}

func testIterRange(t *testing.T, om orderedMap, tuples [][2]val.Tuple) {
	ctx := context.Background()
	desc := keyDescFromMap(om)

	for i := 0; i < 100; i++ {

		cnt := len(tuples)
		a, z := testRand.Intn(cnt), testRand.Intn(cnt)
		if a > z {
			a, z = z, a
		}
		start, stop := tuples[a][0], tuples[z][0]

		tests := []rangeTest{
			// two-sided ranges
			{
				name:      "OpenRange",
				testRange: OpenRange(start, stop, desc),
				expCount:  nonNegative((z - a) - 1),
			},
			{
				name:      "OpenStartRange",
				testRange: OpenStartRange(start, stop, desc),
				expCount:  z - a,
			},
			{
				name:      "OpenStopRange",
				testRange: OpenStopRange(start, stop, desc),
				expCount:  z - a,
			},
			{
				name:      "ClosedRange",
				testRange: ClosedRange(start, stop, desc),
				expCount:  (z - a) + 1,
			},

			// one-sided ranges
			{
				name:      "GreaterRange",
				testRange: GreaterRange(start, desc),
				expCount:  nonNegative(cnt - a - 1),
			},
			{
				name:      "GreaterOrEqualRange",
				testRange: GreaterOrEqualRange(start, desc),
				expCount:  cnt - a,
			},
			{
				name:      "LesserRange",
				testRange: LesserRange(stop, desc),
				expCount:  z,
			},
			{
				name:      "LesserOrEqualRange",
				testRange: LesserOrEqualRange(stop, desc),
				expCount:  z + 1,
			},
		}

		for _, test := range tests {
			//s := fmt.Sprintf(test.testRange.format())
			//fmt.Println(s)

			iter, err := om.IterRange(ctx, test.testRange)
			require.NoError(t, err)

			key, _, err := iter.Next(ctx)
			actCount := 0
			for err != io.EOF {
				actCount++
				prev := key
				key, _, err = iter.Next(ctx)

				if key != nil {
					assert.True(t, desc.Compare(prev, key) < 0)
				}
			}
			assert.Equal(t, io.EOF, err)
			assert.Equal(t, test.expCount, actCount)
			//fmt.Printf("a: %d \t z: %d cnt: %d", a, z, cnt)
		}
	}
}

func nonNegative(x int) int {
	if x < 0 {
		x = 0
	}
	return x
}

type prefixRangeTest struct {
	name      string
	testRange Range
}

func testIterPrefixRange(t *testing.T, om orderedMap, tuples [][2]val.Tuple) {
	ctx := context.Background()
	prefixDesc := getDescPrefix(keyDescFromMap(om), 1)

	for i := 0; i < 100; i++ {

		cnt := len(tuples)
		a, z := testRand.Intn(cnt), testRand.Intn(cnt)
		if a > z {
			a, z = z, a
		}
		start := getKeyPrefix(tuples[a][0], prefixDesc)
		stop := getKeyPrefix(tuples[z][0], prefixDesc)

		tests := []prefixRangeTest{
			// two-sided ranges
			{
				name:      "OpenRange",
				testRange: OpenRange(start, stop, prefixDesc),
			},
			{
				name:      "OpenStartRange",
				testRange: OpenStartRange(start, stop, prefixDesc),
			},
			{
				name:      "OpenStopRange",
				testRange: OpenStopRange(start, stop, prefixDesc),
			},
			{
				name:      "ClosedRange",
				testRange: ClosedRange(start, stop, prefixDesc),
			},

			// one-sided ranges
			{
				name:      "GreaterRange",
				testRange: GreaterRange(start, prefixDesc),
			},
			{
				name:      "GreaterOrEqualRange",
				testRange: GreaterOrEqualRange(start, prefixDesc),
			},
			{
				name:      "LesserRange",
				testRange: LesserRange(stop, prefixDesc),
			},
			{
				name:      "LesserOrEqualRange",
				testRange: LesserOrEqualRange(stop, prefixDesc),
			},
		}

		for _, test := range tests {
			//s := fmt.Sprintf(test.testRange.format())
			//fmt.Println(s)

			iter, err := om.IterRange(ctx, test.testRange)
			require.NoError(t, err)

			key, _, err := iter.Next(ctx)
			actCount := 0
			for err != io.EOF {
				actCount++
				prev := key
				key, _, err = iter.Next(ctx)

				if key != nil {
					assert.True(t, prefixDesc.Compare(prev, key) < 0)
				}
			}
			assert.Equal(t, io.EOF, err)

			expCount := getExpectedRangeSize(test.testRange, tuples)
			assert.Equal(t, expCount, actCount)
		}
	}
}

func getDescPrefix(desc val.TupleDesc, sz int) val.TupleDesc {
	return val.NewTupleDescriptor(desc.Types[:sz]...)
}

func getKeyPrefix(key val.Tuple, desc val.TupleDesc) (partial val.Tuple) {
	tb := val.NewTupleBuilder(desc)
	for i := range desc.Types {
		tb.PutRaw(i, key.GetField(i))
	}
	return tb.Build(sharedPool)
}

// computes expected range on full tuples set
func getExpectedRangeSize(rng Range, tuples [][2]val.Tuple) (sz int) {
	for i := range tuples {
		k := tuples[i][0]
		if rng.insideStart(k) && rng.insideStop(k) {
			sz++
		}
	}
	return
}

func TestMapIterRange(t *testing.T) {
	ctx := context.Background()
	ns := newTestNodeStore()
	kd := val.NewTupleDescriptor(
		val.Type{Enc: val.Int32Enc},
		val.Type{Enc: val.Int32Enc},
	)
	vd := val.NewTupleDescriptor()

	tuples := []val.Tuple{
		intTuple(1, 1), intTuple(),
		intTuple(1, 2), intTuple(),
		intTuple(1, 3), intTuple(),
		intTuple(2, 1), intTuple(),
		intTuple(2, 2), intTuple(),
		intTuple(2, 3), intTuple(),
		intTuple(3, 1), intTuple(),
		intTuple(3, 2), intTuple(),
		intTuple(3, 3), intTuple(),
		intTuple(4, 1), intTuple(),
		intTuple(4, 2), intTuple(),
		intTuple(4, 3), intTuple(),
	}
	require.Equal(t, 24, len(tuples))

	index, err := NewMapFromTuples(ctx, ns, kd, vd, tuples...)
	require.NoError(t, err)
	require.Equal(t, int(12), countOrderedMap(t, index))

	partialDesc := val.NewTupleDescriptor(
		val.Type{Enc: val.Int32Enc},
	)
	fullDesc := val.NewTupleDescriptor(
		val.Type{Enc: val.Int32Enc},
		val.Type{Enc: val.Int32Enc},
	)

	tests := []mapRangeTest{
		// partial-key range scan
		{
			name: "range [1:4]",
			rng: Range{
				Start: RangeCut{
					Key:       intTuple(1),
					Inclusive: true,
				},
				Stop: RangeCut{
					Key:       intTuple(4),
					Inclusive: true,
				},
				KeyDesc: partialDesc,
			},
			exp: tuples[:],
		},
		{
			name: "range (1:4]",
			rng: Range{
				Start: RangeCut{
					Key:       intTuple(1),
					Inclusive: false,
				},
				Stop: RangeCut{
					Key:       intTuple(4),
					Inclusive: true,
				},
				KeyDesc: partialDesc,
			},
			exp: tuples[6:],
		},
		{
			name: "range [1:4)",
			rng: Range{
				Start: RangeCut{
					Key:       intTuple(1),
					Inclusive: true,
				},
				Stop: RangeCut{
					Key:       intTuple(4),
					Inclusive: false,
				},
				KeyDesc: partialDesc,
			},
			exp: tuples[:18],
		},
		{
			name: "range (1:4)",
			rng: Range{
				Start: RangeCut{
					Key:       intTuple(1),
					Inclusive: false,
				},
				Stop: RangeCut{
					Key:       intTuple(4),
					Inclusive: false,
				},
				KeyDesc: partialDesc,
			},
			exp: tuples[6:18],
		},

		// full-key range scan
		{
			name: "range [1,2:4,2]",
			rng: Range{
				Start: RangeCut{
					Key:       intTuple(1, 2),
					Inclusive: true,
				},
				Stop: RangeCut{
					Key:       intTuple(4, 2),
					Inclusive: true,
				},
				KeyDesc: fullDesc,
			},
			exp: tuples[2:22],
		},
		{
			name: "range (1,2:4,2]",
			rng: Range{
				Start: RangeCut{
					Key:       intTuple(1, 2),
					Inclusive: false,
				},
				Stop: RangeCut{
					Key:       intTuple(4, 2),
					Inclusive: true,
				},
				KeyDesc: fullDesc,
			},
			exp: tuples[4:22],
		},
		{
			name: "range [1,2:4,2)",
			rng: Range{
				Start: RangeCut{
					Key:       intTuple(1, 2),
					Inclusive: true,
				},
				Stop: RangeCut{
					Key:       intTuple(4, 2),
					Inclusive: false,
				},
				KeyDesc: fullDesc,
			},
			exp: tuples[2:20],
		},
		{
			name: "range (1,2:4,2)",
			rng: Range{
				Start: RangeCut{
					Key:       intTuple(1, 2),
					Inclusive: false,
				},
				Stop: RangeCut{
					Key:       intTuple(4, 2),
					Inclusive: false,
				},
				KeyDesc: fullDesc,
			},
			exp: tuples[4:20], // 🌲
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testMapRange(t, index, test)
		})
	}
}

type mapRangeTest struct {
	name string
	rng  Range
	exp  []val.Tuple
}

func testMapRange(t *testing.T, idx Map, test mapRangeTest) {
	ctx := context.Background()

	iter, err := idx.IterRange(ctx, test.rng)
	require.NoError(t, err)

	var k, v val.Tuple
	act := make([]val.Tuple, 0, len(test.exp))
	for {
		k, v, err = iter.Next(ctx)
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)
		act = append(act, k, v)
	}
	assert.Error(t, io.EOF, err)

	require.Equal(t, len(test.exp), len(act))
	for i := range test.exp {
		assert.Equal(t, test.exp[i], act[i])
	}
}

func intTuple(ints ...int32) val.Tuple {
	types := make([]val.Type, len(ints))
	for i := range types {
		types[i] = val.Type{Enc: val.Int32Enc}
	}

	desc := val.NewTupleDescriptor(types...)
	tb := val.NewTupleBuilder(desc)
	for i := range ints {
		tb.PutInt32(i, ints[i])
	}
	return tb.Build(sharedPool)
}
