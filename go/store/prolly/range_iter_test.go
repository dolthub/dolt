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

	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

type rangeIterTest struct {
	name      string
	testRange Range
	expCount  int
}

func testIterRange(t *testing.T, om testMap, tuples [][2]val.Tuple) {
	ctx := context.Background()
	desc := keyDescFromMap(om)

	for i := 0; i < 100; i++ {

		cnt := len(tuples)
		a, z := testRand.Intn(cnt), testRand.Intn(cnt)
		if a > z {
			a, z = z, a
		}
		start, stop := tuples[a][0], tuples[z][0]

		tests := []rangeIterTest{
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

func testIterPrefixRange(t *testing.T, om testMap, tuples [][2]val.Tuple) {
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
		if rng.AboveStart(k) && rng.BelowStop(k) {
			sz++
		}
	}
	return
}

func TestMapIterRange(t *testing.T) {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()
	kd := val.NewTupleDescriptor(
		val.Type{Enc: val.Int32Enc},
		val.Type{Enc: val.Int32Enc},
	)
	vd := val.NewTupleDescriptor()

	tuples := []val.Tuple{
		intTuple(1, 1), intTuple(), // 0
		intTuple(1, 2), intTuple(), // 2
		intTuple(1, 3), intTuple(), // 4
		intTuple(2, 1), intTuple(), // 6
		intTuple(2, 2), intTuple(), // 8
		intTuple(2, 3), intTuple(), // 10
		intTuple(3, 1), intTuple(), // 12
		intTuple(3, 2), intTuple(), // 14
		intTuple(3, 3), intTuple(), // 16
		intTuple(4, 1), intTuple(), // 18
		intTuple(4, 2), intTuple(), // 20
		intTuple(4, 3), intTuple(), // 22
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

	tests := []struct {
		name    string
		rng     Range
		inRange []val.Tuple
	}{
		// partial-key range scan
		{
			name:    "range [1:4]",
			rng:     ClosedRange(intTuple(1), intTuple(4), partialDesc),
			inRange: tuples[:],
		},
		{
			name:    "range (1:4]",
			rng:     OpenStartRange(intTuple(1), intTuple(4), partialDesc),
			inRange: tuples[6:],
		},
		{
			name:    "range [1:4)",
			rng:     OpenStopRange(intTuple(1), intTuple(4), partialDesc),
			inRange: tuples[:18],
		},
		{
			name:    "range (1:4)",
			rng:     OpenRange(intTuple(1), intTuple(4), partialDesc),
			inRange: tuples[6:18],
		},

		// full-key range scan
		{
			name:    "range [1,2:4,2]",
			rng:     ClosedRange(intTuple(1, 2), intTuple(4, 2), fullDesc),
			inRange: tuples[:],
		},
		{
			name:    "range (1,2:4,2]",
			rng:     OpenStartRange(intTuple(1, 2), intTuple(4, 2), fullDesc),
			inRange: tuples[:],
		},
		{
			name:    "range [1,2:4,2)",
			rng:     OpenStopRange(intTuple(1, 2), intTuple(4, 2), fullDesc),
			inRange: tuples[:],
		},
		{
			name:    "range (1,2:4,2)",
			rng:     OpenRange(intTuple(1, 2), intTuple(4, 2), fullDesc),
			inRange: tuples[:],
		},
		{
			name:    "range [2,2:3,2]",
			rng:     ClosedRange(intTuple(2, 2), intTuple(3, 2), fullDesc),
			inRange: tuples[6:18],
		},
		{
			name:    "range (2,2:3,2]",
			rng:     OpenStartRange(intTuple(2, 2), intTuple(3, 2), fullDesc),
			inRange: tuples[6:18],
		},
		{
			name:    "range [2,2:3,2)",
			rng:     OpenStopRange(intTuple(2, 2), intTuple(3, 2), fullDesc),
			inRange: tuples[6:18],
		},
		{
			name:    "range (2,2:3,2)",
			rng:     OpenRange(intTuple(2, 2), intTuple(3, 2), fullDesc),
			inRange: tuples[6:18],
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()

			iter, err := index.IterRange(ctx, test.rng)
			require.NoError(t, err)

			var k, v val.Tuple
			act := make([]val.Tuple, 0, len(test.inRange))
			for {
				k, v, err = iter.Next(ctx)
				if err == io.EOF {
					break
				}
				assert.NoError(t, err)
				act = append(act, k, v)
			}
			assert.Error(t, io.EOF, err)

			assert.Equal(t, len(test.inRange), len(act))
			if len(test.inRange) == len(act) {
				for i := range test.inRange {
					assert.Equal(t, test.inRange[i], act[i])
				}
			}
		})
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

func concat(slices ...[]val.Tuple) (c []val.Tuple) {
	var n int
	for _, sl := range slices {
		n += len(sl)
	}
	c = make([]val.Tuple, n)

	n = 0
	for _, sl := range slices {
		copy(c[n:], sl)
		n += len(sl)
	}
	return
}

func testIterOrdinalRange(t *testing.T, om ordinalMap, tuples [][2]val.Tuple) {
	cnt := len(tuples)
	t.Run("test two sided bounds", func(t *testing.T) {
		bounds := make([][2]int, 100)
		for i := range bounds {
			bounds[i] = [2]int{testRand.Intn(cnt)}
		}
		testIterOrdinalRangeWithBounds(t, om, tuples, bounds)
	})
	t.Run("test one sided bounds", func(t *testing.T) {
		bounds := make([][2]int, 100)
		for i := range bounds {
			if i%2 == 0 {
				bounds[i] = [2]int{0, testRand.Intn(cnt)}
			} else {
				bounds[i] = [2]int{testRand.Intn(cnt), cnt}
			}
		}
		testIterOrdinalRangeWithBounds(t, om, tuples, bounds)
	})
}

func testIterOrdinalRangeWithBounds(t *testing.T, om ordinalMap, tuples [][2]val.Tuple, bounds [][2]int) {
	ctx := context.Background()
	for _, bound := range bounds {
		start, stop := bound[0], bound[1]
		if start > stop {
			start, stop = stop, start
		}
		if start == stop {
			continue
		}

		expected := tuples[start:stop]

		iter, err := om.IterOrdinalRange(ctx, uint64(start), uint64(stop))
		require.NoError(t, err)

		var actual [][2]val.Tuple
		var k, v val.Tuple

		for {
			k, v, err = iter.Next(ctx)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			actual = append(actual, [2]val.Tuple{k, v})
		}
		assert.Equal(t, len(expected), len(actual),
			"expected equal tuple slices for bounds (%d, %d)", start, stop)
		assert.Equal(t, expected, actual)
		assert.Equal(t, io.EOF, err)
	}
}
