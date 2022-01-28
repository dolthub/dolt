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
	"sort"
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

var index = [][2]int{
	{1, 1},
	{1, 2},
	{1, 3},
	{2, 1},
	{2, 2},
	{2, 3},
	{3, 1},
	{3, 2},
	{3, 3},
	{4, 1},
	{4, 2},
	{4, 3},
}

type interval struct {
	lo, hi bound
}

type bound struct {
	cut       int
	inclusive bool
}

type rangeSearchTest struct {
	name   string
	i      interval
	lo, hi int
}

func TestRangeSearch(t *testing.T) {
	tests := []rangeSearchTest{
		{
			name: "range [1,4]",
			i: interval{
				lo: bound{cut: 1, inclusive: true},
				hi: bound{cut: 4, inclusive: true},
			},
			lo: 0,
			hi: 12,
		},
		{
			name: "range (1,4]",
			i: interval{
				lo: bound{cut: 1, inclusive: false},
				hi: bound{cut: 4, inclusive: true},
			},
			lo: 3,
			hi: 12,
		},
		{
			name: "range [1,4)",
			i: interval{
				lo: bound{cut: 1, inclusive: true},
				hi: bound{cut: 4, inclusive: false},
			},
			lo: 0,
			hi: 9,
		},
		{
			name: "range (1,4)",
			i: interval{
				lo: bound{cut: 1, inclusive: false},
				hi: bound{cut: 4, inclusive: false},
			},
			lo: 3,
			hi: 9,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testRangeSearch(t, test)
		})
	}
}

func testRangeSearch(t *testing.T, test rangeSearchTest) {
	lo, hi := rangeSearch(test.i, index)
	assert.Equal(t, test.lo, lo)
	assert.Equal(t, test.hi, hi)

	act := index[lo:hi]
	exp := index[test.lo:test.hi]
	require.Equal(t, len(exp), len(act))
	for i := range exp {
		assert.Equal(t, exp[i], act[i])
	}
}

// range search returns the subset of |data| specified by |i|.
func rangeSearch(i interval, data [][2]int) (lo, hi int) {
	lo = lowerBoundSearch(i.lo, data)
	hi = upperBoundSearch(i.hi, data)
	return
}

func lowerBoundSearch(b bound, data [][2]int) int {
	less := func(i int) bool {
		if b.inclusive {
			return b.cut <= data[i][0]
		} else {
			return b.cut < data[i][0]
		}
	}
	return sort.Search(len(data), less)
}

func upperBoundSearch(b bound, data [][2]int) int {
	less := func(i int) bool {
		if b.inclusive {
			return b.cut < data[i][0]
		} else {
			return b.cut <= data[i][0]
		}
	}
	return sort.Search(len(data), less)
}
