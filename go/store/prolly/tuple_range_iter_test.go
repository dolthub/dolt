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
	"fmt"
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
		if a == z {
			continue
		} else if a > z {
			a, z = z, a
		}
		start, stop := tuples[a][0], tuples[z][0]

		tests := []rangeIterTest{
			// two-sided ranges
			{
				name:      "OpenRange",
				testRange: openRange(ctx, start, stop, desc),
				expCount:  nonNegative((z - a) - 1),
			},
			{
				name:      "OpenStartRange",
				testRange: openStartRange(ctx, start, stop, desc),
				expCount:  z - a,
			},
			{
				name:      "OpenStopRange",
				testRange: openStopRange(ctx, start, stop, desc),
				expCount:  z - a,
			},
			{
				name:      "closedRange",
				testRange: closedRange(ctx, start, stop, desc),
				expCount:  (z - a) + 1,
			},

			// one-sided ranges
			{
				name:      "GreaterRange",
				testRange: greaterRange(start, desc),
				expCount:  nonNegative(cnt - a - 1),
			},
			{
				name:      "GreaterOrEqualRange",
				testRange: greaterOrEqualRange(start, desc),
				expCount:  cnt - a,
			},
			{
				name:      "LesserRange",
				testRange: lesserRange(stop, desc),
				expCount:  z,
			},
			{
				name:      "LesserOrEqualRange",
				testRange: lesserOrEqualRange(stop, desc),
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
					assert.True(t, desc.Compare(ctx, prev, key) < 0)
				}
			}
			assert.Equal(t, io.EOF, err)

			if !assert.Equal(t, test.expCount, actCount) {
				fmt.Println("here")
			}
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
		start, err := getKeyPrefix(tuples[a][0], prefixDesc)
		require.NoError(t, err)
		stop, err := getKeyPrefix(tuples[z][0], prefixDesc)
		require.NoError(t, err)

		tests := []prefixRangeTest{
			// two-sided ranges
			{
				name:      "OpenRange",
				testRange: openRange(ctx, start, stop, prefixDesc),
			},
			{
				name:      "OpenStartRange",
				testRange: openStartRange(ctx, start, stop, prefixDesc),
			},
			{
				name:      "OpenStopRange",
				testRange: openStopRange(ctx, start, stop, prefixDesc),
			},
			{
				name:      "closedRange",
				testRange: closedRange(ctx, start, stop, prefixDesc),
			},

			// one-sided ranges
			{
				name:      "GreaterRange",
				testRange: greaterRange(start, prefixDesc),
			},
			{
				name:      "GreaterOrEqualRange",
				testRange: greaterOrEqualRange(start, prefixDesc),
			},
			{
				name:      "LesserRange",
				testRange: lesserRange(stop, prefixDesc),
			},
			{
				name:      "LesserOrEqualRange",
				testRange: lesserOrEqualRange(stop, prefixDesc),
			},
		}

		for _, test := range tests {
			iter, err := om.IterRange(ctx, test.testRange)
			require.NoError(t, err)

			key, _, err := iter.Next(ctx)
			actCount := 0
			for err != io.EOF {
				actCount++
				prev := key
				key, _, err = iter.Next(ctx)

				if key != nil {
					assert.True(t, prefixDesc.Compare(ctx, prev, key) < 0)
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

func getKeyPrefix(key val.Tuple, desc val.TupleDesc) (partial val.Tuple, err error) {
	tb := val.NewTupleBuilder(desc, ns)
	for i := range desc.Types {
		tb.PutRaw(i, key.GetField(i))
	}
	return tb.Build(sharedPool)
}

// computes expected range on full tuples set
func getExpectedRangeSize(rng Range, tuples [][2]val.Tuple) (sz int) {
	ctx := context.Background()
	for i := range tuples {
		k := tuples[i][0]
		if rng.aboveStart(ctx, k) && rng.belowStop(ctx, k) {
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
		name     string
		rng      Range
		physical [2]int
		logical  []int
	}{
		// partial-key range scan
		{
			name:     "range [1:4]",
			rng:      closedRange(ctx, intTuple(1), intTuple(4), partialDesc),
			physical: [2]int{0, 24},
			logical:  []int{0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22},
		},
		{
			name:     "range (1:4]",
			rng:      openStartRange(ctx, intTuple(1), intTuple(4), partialDesc),
			physical: [2]int{6, 24},
			logical:  []int{6, 8, 10, 12, 14, 16, 18, 20, 22},
		},
		{
			name:     "range [1:4)",
			rng:      openStopRange(ctx, intTuple(1), intTuple(4), partialDesc),
			physical: [2]int{0, 18},
			logical:  []int{0, 2, 4, 6, 8, 10, 12, 14, 16},
		},
		{
			name:     "range (1:4)",
			rng:      openRange(ctx, intTuple(1), intTuple(4), partialDesc),
			physical: [2]int{6, 18},
			logical:  []int{6, 8, 10, 12, 14, 16},
		},

		// full-key range scan
		{
			name:     "range (1,1:4,3)",
			rng:      openRange(ctx, intTuple(1, 1), intTuple(4, 3), fullDesc),
			physical: [2]int{0, 24},
			logical:  []int{2, 8, 14, 20},
		},
		{
			name:     "range (1,1:4,3]",
			rng:      openStartRange(ctx, intTuple(1, 1), intTuple(4, 3), fullDesc),
			physical: [2]int{0, 24},
			logical:  []int{2, 4, 8, 10, 14, 16, 20, 22},
		},
		{
			name:     "range [1,1:4,3)",
			rng:      openStopRange(ctx, intTuple(1, 1), intTuple(4, 3), fullDesc),
			physical: [2]int{0, 24},
			logical:  []int{0, 2, 6, 8, 12, 14, 18, 20},
		},
		{
			name:     "range [1,1:4,3]",
			rng:      closedRange(ctx, intTuple(1, 1), intTuple(4, 3), fullDesc),
			physical: [2]int{0, 24},
			logical:  []int{0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22},
		},
		{
			name:     "range [1,2:4,2]",
			rng:      closedRange(ctx, intTuple(1, 2), intTuple(4, 2), fullDesc),
			physical: [2]int{0, 24},
			logical:  []int{2, 8, 14, 20},
		},
		{
			name:     "range (1,2:4,2]",
			rng:      openStartRange(ctx, intTuple(1, 2), intTuple(4, 2), fullDesc),
			physical: [2]int{0, 24},
			logical:  []int{},
		},
		{
			name:     "range [2,2:3,2]",
			rng:      closedRange(ctx, intTuple(2, 2), intTuple(3, 2), fullDesc),
			physical: [2]int{6, 18},
			logical:  []int{8, 14},
		},
		{
			name:     "range [2,2:2,3]",
			rng:      closedRange(ctx, intTuple(2, 2), intTuple(2, 3), fullDesc),
			physical: [2]int{8, 12},
			logical:  []int{8, 10},
		},
		{
			name:     "range [2,2:2,2]",
			rng:      closedRange(ctx, intTuple(2, 2), intTuple(2, 2), fullDesc),
			physical: [2]int{8, 10},
			logical:  []int{8},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()

			// validate physical range (unfiltered iter)
			iter, err := treeIterFromRange(ctx, index.Node(), ns, test.rng)
			require.NoError(t, err)

			var k, v val.Tuple
			act := make([]val.Tuple, 0, len(test.physical))
			for {
				k, v, err = iter.Next(ctx)
				if err == io.EOF {
					break
				}
				assert.NoError(t, err)
				act = append(act, k, v)
			}
			assert.Error(t, io.EOF, err)

			inRange := tuples[test.physical[0]:test.physical[1]]
			assert.Equal(t, len(inRange), len(act))
			if len(inRange) == len(act) {
				for i := range inRange {
					assert.Equal(t, inRange[i], act[i])
				}
			}

			// validate logical range
			iter2, err := index.IterRange(ctx, test.rng)
			require.NoError(t, err)

			act2 := make([]val.Tuple, 0, len(test.logical))
			for {
				k, _, err = iter2.Next(ctx)
				if err == io.EOF {
					break
				}
				assert.NoError(t, err)
				act2 = append(act2, k)
			}
			assert.Error(t, io.EOF, err)

			exp2 := make([]val.Tuple, len(test.logical))
			for i := range test.logical {
				exp2[i] = tuples[test.logical[i]]
			}

			if !assert.Equal(t, len(exp2), len(act2)) {
				t.Fail()
			}
			if len(exp2) == len(act2) {
				for i := range exp2 {
					assert.Equal(t, exp2[i], act2[i])
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
	tb := val.NewTupleBuilder(desc, ns)
	for i := range ints {
		tb.PutInt32(i, ints[i])
	}
	tup, err := tb.Build(sharedPool)
	if err != nil {
		panic(err)
	}
	return tup
}

func testIterOrdinalRange(t *testing.T, om Map, tuples [][2]val.Tuple) {
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

func testIterOrdinalRangeWithBounds(t *testing.T, om Map, tuples [][2]val.Tuple, bounds [][2]int) {
	ctx := context.Background()
	t.Run("IterOrdinalRange", func(t *testing.T) {
		for _, bound := range bounds {
			start, stop := bound[0], bound[1]
			if start > stop {
				start, stop = stop, start
			} else if start == stop {
				continue
			}
			expected := tuples[start:stop]

			iter, err := om.IterOrdinalRange(ctx, uint64(start), uint64(stop))
			require.NoError(t, err)
			actual := iterOrdinalRange(t, ctx, iter)
			assert.Equal(t, len(expected), len(actual),
				"expected equal tuple slices for bounds (%d, %d)", start, stop)
			assert.Equal(t, expected, actual)
		}
	})
	t.Run("FetchOrdinalRange", func(t *testing.T) {
		for _, bound := range bounds {
			start, stop := bound[0], bound[1]
			if start > stop {
				start, stop = stop, start
			} else if start == stop {
				continue
			}
			expected := tuples[start:stop]

			iter, err := om.FetchOrdinalRange(ctx, uint64(start), uint64(stop))
			require.NoError(t, err)
			actual := iterOrdinalRange(t, ctx, iter)
			assert.Equal(t, len(expected), len(actual),
				"expected equal tuple slices for bounds (%d, %d)", start, stop)
			assert.Equal(t, expected, actual)
		}
	})
}

func testIterKeyRange(t *testing.T, m Map, tuples [][2]val.Tuple) {
	ctx := context.Background()

	t.Run("RandomKeyRange", func(t *testing.T) {
		bounds := generateInserts(t, m, m.keyDesc, m.valDesc, 2)
		start, stop := bounds[0][0], bounds[1][0]
		if m.keyDesc.Compare(ctx, start, stop) > 0 {
			start, stop = stop, start
		}
		kR := keyRange{kd: m.keyDesc, start: start, stop: stop}

		var expectedKeys []val.Tuple
		for _, kv := range tuples {
			if kR.includes(kv[0]) {
				expectedKeys = append(expectedKeys, kv[0])
			}
		}

		itr, err := m.IterKeyRange(ctx, start, stop)
		require.NoError(t, err)

		for _, eK := range expectedKeys {
			k, _, err := itr.Next(ctx)
			require.NoError(t, err)
			assert.Equal(t, eK, k)
		}

		_, _, err = itr.Next(ctx)
		require.Equal(t, io.EOF, err)
	})
}

func iterOrdinalRange(t *testing.T, ctx context.Context, iter MapIter) (actual [][2]val.Tuple) {
	for {
		k, v, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		assert.NotNil(t, k)
		assert.NotNil(t, v)
		actual = append(actual, [2]val.Tuple{k, v})
	}
	return
}
