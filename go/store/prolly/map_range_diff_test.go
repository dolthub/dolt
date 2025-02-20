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
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

func TestMapRangeDiff(t *testing.T) {
	scales := []int{
		10,
		100,
		1000,
		10000,
	}

	for _, s := range scales {
		name := fmt.Sprintf("test map RangeDiff at scale %d", s)
		t.Run(name, func(t *testing.T) {
			prollyMap, tuples := makeProllyMap(t, s)
			require.Equal(t, s, len(tuples))

			kd := prollyMap.(Map).keyDesc

			t.Run("OpenStopRange", func(t *testing.T) {
				rngTest := makeRandomOpenStopRangeTest(kd, tuples)
				runDiffTestsWithRange(t, s, prollyMap, tuples, rngTest)
			})

			t.Run("GreaterOrEqualRange", func(t *testing.T) {
				rngTest := makeRandomGreaterOrEqualRangeTest(kd, tuples)
				runDiffTestsWithRange(t, s, prollyMap, tuples, rngTest)
			})

			t.Run("LesserRange", func(t *testing.T) {
				rngTest := makeRandomLesserRangeTest(kd, tuples)
				runDiffTestsWithRange(t, s, prollyMap, tuples, rngTest)
			})
		})
	}
}

func runDiffTestsWithRange(t *testing.T, s int, prollyMap testMap, tuples [][2]val.Tuple, rngTest rangeDiffTest) {
	t.Run("map diff error handling", func(t *testing.T) {
		testRngMapDiffErrorHandling(t, prollyMap.(Map), rngTest)
	})
	t.Run("equal map diff", func(t *testing.T) {
		testRngEqualMapDiff(t, prollyMap.(Map), rngTest)
	})
	t.Run("map diff against empty", func(t *testing.T) {
		testRngMapDiffAgainstEmpty(t, s, rngTest)
	})

	// deletes
	t.Run("single delete diff", func(t *testing.T) {
		for k := 0; k < 100; k++ {
			testRngDeleteDiffs(t, prollyMap.(Map), tuples, 1, rngTest)
		}
	})
	t.Run("many delete diffs", func(t *testing.T) {
		for k := 0; k < 10; k++ {
			testRngDeleteDiffs(t, prollyMap.(Map), tuples, s/10, rngTest)
			testRngDeleteDiffs(t, prollyMap.(Map), tuples, s/2, rngTest)
		}
	})
	t.Run("diff against empty map", func(t *testing.T) {
		testRngDeleteDiffs(t, prollyMap.(Map), tuples, s, rngTest)
	})

	// inserts
	t.Run("single insert diff", func(t *testing.T) {
		for k := 0; k < 100; k++ {
			testRngInsertDiffs(t, prollyMap.(Map), tuples, 1, rngTest)
		}
	})
	t.Run("many insert diffs", func(t *testing.T) {
		for k := 0; k < 10; k++ {
			testRngInsertDiffs(t, prollyMap.(Map), tuples, s/10, rngTest)
			testRngInsertDiffs(t, prollyMap.(Map), tuples, s/2, rngTest)
		}
	})

	// updates
	t.Run("single update diff", func(t *testing.T) {
		for k := 0; k < 100; k++ {
			testRngUpdateDiffs(t, prollyMap.(Map), tuples, 1, rngTest)
		}
	})
	t.Run("many update diffs", func(t *testing.T) {
		for k := 0; k < 10; k++ {
			testRngUpdateDiffs(t, prollyMap.(Map), tuples, s/10, rngTest)
			testRngUpdateDiffs(t, prollyMap.(Map), tuples, s/2, rngTest)
		}
	})
}

func testRngMapDiffErrorHandling(t *testing.T, m Map, rngTest rangeDiffTest) {
	ctx := context.Background()

	expErr := errors.New("error case")
	err := RangeDiffMaps(ctx, m, m, rngTest.rng, func(ctx context.Context, diff tree.Diff) error {
		return expErr
	})
	require.Error(t, expErr, err)
}

func testRngEqualMapDiff(t *testing.T, m Map, rngTest rangeDiffTest) {
	ctx := context.Background()
	var counter int
	err := RangeDiffMaps(ctx, m, m, rngTest.rng, func(ctx context.Context, diff tree.Diff) error {
		counter++
		return nil
	})
	require.Error(t, io.EOF, err)
	assert.Equal(t, 0, counter)
}

func testRngMapDiffAgainstEmpty(t *testing.T, scale int, rngTest rangeDiffTest) {
	ctx := context.Background()
	m, tuples := makeProllyMap(t, scale)
	empty, _ := makeProllyMap(t, 0)

	inRange := getPairsInRange(tuples, rngTest.rng)
	cnt := 0
	err := RangeDiffMaps(ctx, m.(Map), empty.(Map), rngTest.rng, func(ctx context.Context, diff tree.Diff) error {
		assert.Equal(t, inRange[cnt][0], val.Tuple(diff.Key))
		assert.Equal(t, inRange[cnt][1], val.Tuple(diff.From))
		assert.Nil(t, val.Tuple(diff.To))
		assert.True(t, rngTest.rng.Matches(ctx, val.Tuple(diff.Key)))
		cnt++
		return nil
	})
	require.Error(t, io.EOF, err)
	assert.Equal(t, len(inRange), cnt)

	cnt = 0
	err = RangeDiffMaps(ctx, empty.(Map), m.(Map), rngTest.rng, func(ctx context.Context, diff tree.Diff) error {
		assert.Equal(t, inRange[cnt][0], val.Tuple(diff.Key))
		assert.Equal(t, inRange[cnt][1], val.Tuple(diff.To))
		assert.Nil(t, val.Tuple(diff.From))
		assert.True(t, rngTest.rng.Matches(ctx, val.Tuple(diff.Key)))
		cnt++
		return nil
	})
	require.Error(t, io.EOF, err)
	assert.Equal(t, len(inRange), cnt)
}

func testRngDeleteDiffs(t *testing.T, from Map, tups [][2]val.Tuple, numDeletes int, rngTest rangeDiffTest) {
	ctx := context.Background()
	rand.Shuffle(len(tups), func(i, j int) {
		tups[i], tups[j] = tups[j], tups[i]
	})

	deletes := tups[:numDeletes]
	sort.Slice(deletes, func(i, j int) bool {
		return from.keyDesc.Compare(ctx, deletes[i][0], deletes[j][0]) < 0
	})
	inRange := getPairsInRange(deletes, rngTest.rng)
	to := makeMapWithDeletes(t, from, deletes...)

	cnt := 0
	err := RangeDiffMaps(ctx, from, to, rngTest.rng, func(ctx context.Context, diff tree.Diff) error {
		assert.Equal(t, tree.RemovedDiff, diff.Type)
		assert.Equal(t, inRange[cnt][0], val.Tuple(diff.Key))
		assert.True(t, rngTest.rng.Matches(ctx, val.Tuple(diff.Key)))
		cnt++
		return nil
	})
	require.Error(t, io.EOF, err)
	assert.Equal(t, len(inRange), cnt)
}

func testRngInsertDiffs(t *testing.T, from Map, tups [][2]val.Tuple, numInserts int, rngTest rangeDiffTest) {
	ctx := context.Background()
	to, inserts := makeMapWithInserts(t, from, numInserts)

	inRange := getPairsInRange(inserts, rngTest.rng)
	cnt := 0
	err := RangeDiffMaps(ctx, from, to, rngTest.rng, func(ctx context.Context, diff tree.Diff) error {
		if !assert.Equal(t, tree.AddedDiff, diff.Type) {
			fmt.Println("")
		}
		assert.Equal(t, inRange[cnt][0], val.Tuple(diff.Key))
		assert.Equal(t, inRange[cnt][1], val.Tuple(diff.To))
		assert.True(t, rngTest.rng.Matches(ctx, val.Tuple(diff.Key)))
		cnt++
		return nil
	})
	require.Error(t, io.EOF, err)
	assert.Equal(t, len(inRange), cnt)
}

func testRngUpdateDiffs(t *testing.T, from Map, tups [][2]val.Tuple, numUpdates int, rngTest rangeDiffTest) {
	ctx := context.Background()

	rand.Shuffle(len(tups), func(i, j int) {
		tups[i], tups[j] = tups[j], tups[i]
	})

	sub := tups[:numUpdates]
	sort.Slice(sub, func(i, j int) bool {
		return from.keyDesc.Compare(ctx, sub[i][0], sub[j][0]) < 0
	})

	kd, vd := from.Descriptors()
	updates := makeUpdatesToTuples(kd, vd, sub...)
	to := makeMapWithUpdates(t, from, updates...)
	var inRange [][3]val.Tuple
	for _, pair := range updates {
		if rngTest.rng.Matches(ctx, pair[0]) {
			inRange = append(inRange, pair)
		}
	}

	var cnt int
	err := RangeDiffMaps(ctx, from, to, rngTest.rng, func(ctx context.Context, diff tree.Diff) error {
		assert.Equal(t, tree.ModifiedDiff, diff.Type)
		assert.Equal(t, inRange[cnt][0], val.Tuple(diff.Key))
		assert.Equal(t, inRange[cnt][1], val.Tuple(diff.From))
		assert.Equal(t, inRange[cnt][2], val.Tuple(diff.To))
		assert.True(t, rngTest.rng.Matches(ctx, val.Tuple(diff.Key)))
		cnt++
		return nil
	})
	require.Error(t, io.EOF, err)
	assert.Equal(t, len(inRange), cnt)
}

type rangeDiffTest struct {
	tuples [][2]val.Tuple
	rng    Range
}

func makeRandomOpenStopRangeTest(kd val.TupleDesc, tuples [][2]val.Tuple) rangeDiffTest {
	ctx := context.Background()
	i := rand.Intn(len(tuples))
	j := rand.Intn(len(tuples))
	if j < i {
		i, j = j, i
	}
	start := tuples[i][0]
	stop := tuples[j][0]

	return rangeDiffTest{tuples: tuples, rng: OpenStopRange(ctx, start, stop, kd)}
}

func makeRandomGreaterOrEqualRangeTest(kd val.TupleDesc, tuples [][2]val.Tuple) rangeDiffTest {
	i := rand.Intn(len(tuples))
	start := tuples[i][0]
	return rangeDiffTest{tuples: tuples, rng: GreaterOrEqualRange(start, kd)}
}

func makeRandomLesserRangeTest(kd val.TupleDesc, tuples [][2]val.Tuple) rangeDiffTest {
	i := rand.Intn(len(tuples))
	end := tuples[i][0]
	return rangeDiffTest{tuples: tuples, rng: LesserRange(end, kd)}
}

func getPairsInRange(tuples [][2]val.Tuple, rng Range) (keys [][2]val.Tuple) {
	ctx := context.Background()
	for _, pair := range tuples {
		if rng.Matches(ctx, pair[0]) {
			keys = append(keys, pair)
		}
	}
	return
}
