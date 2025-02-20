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

func TestDiffKeyRangeMaps(t *testing.T) {
	scales := []int{
		10,
		100,
		1000,
		10000,
	}

	for _, s := range scales {
		name := fmt.Sprintf("test map RangeDiff at scale %d", s)
		t.Run(name, func(t *testing.T) {
			om, tuples := makeProllyMap(t, s)
			require.Equal(t, s, len(tuples))
			prollyMap := om.(Map)
			kd, vd := prollyMap.Descriptors()

			t.Run("BoundedKeyRange", func(t *testing.T) {
				rngTest := makeRandomBoundedKeyRange(kd, tuples)
				runDiffTestsWithKeyRange(t, s, prollyMap, tuples, rngTest)
			})

			t.Run("BoundedKeyRangeWithMissingKeys", func(t *testing.T) {
				rngTest := makeBoundedKeyRangeWithMissingKeys(t, prollyMap, kd, vd, tuples)
				runDiffTestsWithKeyRange(t, s, prollyMap, tuples, rngTest)
			})

			t.Run("UnboundedLowerKeyRange", func(t *testing.T) {
				rngTest := makeRandomUnboundedLowerKeyRange(kd, tuples)
				runDiffTestsWithKeyRange(t, s, prollyMap, tuples, rngTest)
			})

			t.Run("UnboundedUpperKeyRange", func(t *testing.T) {
				rngTest := makeRandomUnboundedUpperKeyRange(kd, tuples)
				runDiffTestsWithKeyRange(t, s, prollyMap, tuples, rngTest)
			})
		})
	}
}

func runDiffTestsWithKeyRange(t *testing.T, s int, prollyMap testMap, tuples [][2]val.Tuple, rngTest keyRangeDiffTest) {
	t.Run("map diff error handling", func(t *testing.T) {
		testKeyRngMapDiffErrorHandling(t, prollyMap.(Map), rngTest)
	})
	t.Run("equal map diff", func(t *testing.T) {
		testKeyRngEqualMapDiff(t, prollyMap.(Map), rngTest)
	})
	t.Run("map diff against empty", func(t *testing.T) {
		testKeyRngMapDiffAgainstEmpty(t, s, rngTest)
	})

	// deletes
	t.Run("single delete diff", func(t *testing.T) {
		for k := 0; k < 100; k++ {
			testKeyRngDeleteDiffs(t, prollyMap.(Map), tuples, 1, rngTest)
		}
	})
	t.Run("many delete diffs", func(t *testing.T) {
		for k := 0; k < 10; k++ {
			testKeyRngDeleteDiffs(t, prollyMap.(Map), tuples, s/10, rngTest)
			testKeyRngDeleteDiffs(t, prollyMap.(Map), tuples, s/2, rngTest)
		}
	})
	t.Run("diff against empty map", func(t *testing.T) {
		testKeyRngDeleteDiffs(t, prollyMap.(Map), tuples, s, rngTest)
	})

	// inserts
	t.Run("single insert diff", func(t *testing.T) {
		for k := 0; k < 100; k++ {
			testKeyRngInsertDiffs(t, prollyMap.(Map), tuples, 1, rngTest)
		}
	})
	t.Run("many insert diffs", func(t *testing.T) {
		for k := 0; k < 10; k++ {
			testKeyRngInsertDiffs(t, prollyMap.(Map), tuples, s/10, rngTest)
			testKeyRngInsertDiffs(t, prollyMap.(Map), tuples, s/2, rngTest)
		}
	})

	// updates
	t.Run("single update diff", func(t *testing.T) {
		for k := 0; k < 100; k++ {
			testKeyRngUpdateDiffs(t, prollyMap.(Map), tuples, 1, rngTest)
		}
	})
	t.Run("many update diffs", func(t *testing.T) {
		for k := 0; k < 10; k++ {
			testKeyRngUpdateDiffs(t, prollyMap.(Map), tuples, s/10, rngTest)
			testKeyRngUpdateDiffs(t, prollyMap.(Map), tuples, s/2, rngTest)
		}
	})
}

func testKeyRngMapDiffErrorHandling(t *testing.T, m Map, test keyRangeDiffTest) {
	ctx := context.Background()

	expErr := errors.New("error case")
	err := DiffMapsKeyRange(ctx, m, m, test.keyRange.start, test.keyRange.stop, func(ctx context.Context, diff tree.Diff) error {
		return expErr
	})
	require.Error(t, expErr, err)
}

func testKeyRngEqualMapDiff(t *testing.T, m Map, rngTest keyRangeDiffTest) {
	ctx := context.Background()
	var counter int
	err := DiffMapsKeyRange(ctx, m, m, rngTest.keyRange.start, rngTest.keyRange.stop, func(ctx context.Context, diff tree.Diff) error {
		counter++
		return nil
	})
	require.Error(t, io.EOF, err)
	assert.Equal(t, 0, counter)
}

func testKeyRngMapDiffAgainstEmpty(t *testing.T, scale int, rngTest keyRangeDiffTest) {
	ctx := context.Background()
	m, tuples := makeProllyMap(t, scale)
	empty, _ := makeProllyMap(t, 0)

	inRange := getPairsInKeyRange(tuples, rngTest.keyRange)
	cnt := 0
	err := DiffMapsKeyRange(ctx, m.(Map), empty.(Map), rngTest.keyRange.start, rngTest.keyRange.stop, func(ctx context.Context, diff tree.Diff) error {
		assert.Equal(t, inRange[cnt][0], val.Tuple(diff.Key))
		assert.Equal(t, inRange[cnt][1], val.Tuple(diff.From))
		assert.Nil(t, val.Tuple(diff.To))
		assert.True(t, rngTest.keyRange.includes(val.Tuple(diff.Key)))
		cnt++
		return nil
	})
	require.Error(t, io.EOF, err)
	assert.Equal(t, len(inRange), cnt)

	cnt = 0
	err = DiffMapsKeyRange(ctx, empty.(Map), m.(Map), rngTest.keyRange.start, rngTest.keyRange.stop, func(ctx context.Context, diff tree.Diff) error {
		assert.Equal(t, inRange[cnt][0], val.Tuple(diff.Key))
		assert.Equal(t, inRange[cnt][1], val.Tuple(diff.To))
		assert.Nil(t, val.Tuple(diff.From))
		assert.True(t, rngTest.keyRange.includes(val.Tuple(diff.Key)))
		cnt++
		return nil
	})
	require.Error(t, io.EOF, err)
	assert.Equal(t, len(inRange), cnt)
}

func testKeyRngDeleteDiffs(t *testing.T, from Map, tups [][2]val.Tuple, numDeletes int, rngTest keyRangeDiffTest) {
	ctx := context.Background()
	rand.Shuffle(len(tups), func(i, j int) {
		tups[i], tups[j] = tups[j], tups[i]
	})

	deletes := tups[:numDeletes]
	sort.Slice(deletes, func(i, j int) bool {
		return from.keyDesc.Compare(ctx, deletes[i][0], deletes[j][0]) < 0
	})
	inRange := getPairsInKeyRange(deletes, rngTest.keyRange)
	to := makeMapWithDeletes(t, from, deletes...)

	cnt := 0
	err := DiffMapsKeyRange(ctx, from, to, rngTest.keyRange.start, rngTest.keyRange.stop, func(ctx context.Context, diff tree.Diff) error {
		assert.Equal(t, tree.RemovedDiff, diff.Type)
		assert.Equal(t, inRange[cnt][0], val.Tuple(diff.Key))
		assert.True(t, rngTest.keyRange.includes(val.Tuple(diff.Key)))
		cnt++
		return nil
	})
	require.Error(t, io.EOF, err)
	assert.Equal(t, len(inRange), cnt)
}

func testKeyRngInsertDiffs(t *testing.T, from Map, tups [][2]val.Tuple, numInserts int, rngTest keyRangeDiffTest) {
	ctx := context.Background()
	to, inserts := makeMapWithInserts(t, from, numInserts)

	inRange := getPairsInKeyRange(inserts, rngTest.keyRange)
	cnt := 0
	err := DiffMapsKeyRange(ctx, from, to, rngTest.keyRange.start, rngTest.keyRange.stop, func(ctx context.Context, diff tree.Diff) error {
		if !assert.Equal(t, tree.AddedDiff, diff.Type) {
			fmt.Println("")
		}
		assert.Equal(t, inRange[cnt][0], val.Tuple(diff.Key))
		assert.Equal(t, inRange[cnt][1], val.Tuple(diff.To))
		assert.True(t, rngTest.keyRange.includes(val.Tuple(diff.Key)))
		cnt++
		return nil
	})
	require.Error(t, io.EOF, err)
	assert.Equal(t, len(inRange), cnt)
}

func testKeyRngUpdateDiffs(t *testing.T, from Map, tups [][2]val.Tuple, numUpdates int, rngTest keyRangeDiffTest) {
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
		if rngTest.keyRange.includes(pair[0]) {
			inRange = append(inRange, pair)
		}
	}

	var cnt int
	err := DiffMapsKeyRange(ctx, from, to, rngTest.keyRange.start, rngTest.keyRange.stop, func(ctx context.Context, diff tree.Diff) error {
		assert.Equal(t, tree.ModifiedDiff, diff.Type)
		assert.Equal(t, inRange[cnt][0], val.Tuple(diff.Key))
		assert.Equal(t, inRange[cnt][1], val.Tuple(diff.From))
		assert.Equal(t, inRange[cnt][2], val.Tuple(diff.To))
		assert.True(t, rngTest.keyRange.includes(val.Tuple(diff.Key)))
		cnt++
		return nil
	})
	require.Error(t, io.EOF, err)
	assert.Equal(t, len(inRange), cnt)
}

func makeRandomBoundedKeyRange(kd val.TupleDesc, tuples [][2]val.Tuple) keyRangeDiffTest {
	i := rand.Intn(len(tuples))
	j := rand.Intn(len(tuples))
	if j < i {
		i, j = j, i
	}
	start := tuples[i][0]
	stop := tuples[j][0]

	kR := keyRange{kd: kd, start: start, stop: stop}

	return keyRangeDiffTest{tuples: tuples, keyRange: kR}
}

func makeRandomUnboundedLowerKeyRange(kd val.TupleDesc, tuples [][2]val.Tuple) keyRangeDiffTest {
	i := rand.Intn(len(tuples))
	end := tuples[i][0]

	kR := keyRange{kd: kd, stop: end}

	return keyRangeDiffTest{tuples: tuples, keyRange: kR}
}

func makeRandomUnboundedUpperKeyRange(kd val.TupleDesc, tuples [][2]val.Tuple) keyRangeDiffTest {
	i := rand.Intn(len(tuples))
	start := tuples[i][0]

	kR := keyRange{kd: kd, start: start}

	return keyRangeDiffTest{tuples: tuples, keyRange: kR}
}

func makeBoundedKeyRangeWithMissingKeys(t *testing.T, m Map, kd val.TupleDesc, vd val.TupleDesc, tuples [][2]val.Tuple) keyRangeDiffTest {
	ctx := context.Background()
	inserts := generateInserts(t, m, kd, vd, 2)
	low, hi := inserts[0][0], inserts[1][0]
	if kd.Compare(ctx, low, hi) > 0 {
		hi, low = low, hi
	}

	kR := keyRange{kd: kd, start: low, stop: hi}

	return keyRangeDiffTest{tuples: tuples, keyRange: kR}
}

func getPairsInKeyRange(tuples [][2]val.Tuple, rng keyRange) (keys [][2]val.Tuple) {
	for _, pair := range tuples {
		if rng.includes(pair[0]) {
			keys = append(keys, pair)
		}
	}
	return
}

type keyRange struct {
	start val.Tuple
	stop  val.Tuple
	kd    val.TupleDesc
}

func (kR keyRange) includes(k val.Tuple) bool {
	ctx := context.Background()
	if len(kR.start) != 0 && kR.kd.Compare(ctx, k, kR.start) < 0 {
		return false
	}
	if len(kR.stop) != 0 && kR.kd.Compare(ctx, k, kR.stop) >= 0 {
		return false
	}
	return true
}

type keyRangeDiffTest struct {
	tuples   [][2]val.Tuple
	keyRange keyRange
}
