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

func TestMapDiff(t *testing.T) {
	scales := []int{
		10,
		100,
		1000,
		10000,
	}

	for _, s := range scales {
		name := fmt.Sprintf("test proCur map at scale %d", s)
		t.Run(name, func(t *testing.T) {
			prollyMap, tuples := makeProllyMap(t, s)
			require.Equal(t, s, len(tuples))

			t.Run("map diff error handling", func(t *testing.T) {
				testMapDiffErrorHandling(t, prollyMap.(Map))
			})
			t.Run("equal map diff", func(t *testing.T) {
				testEqualMapDiff(t, prollyMap.(Map))
			})
			t.Run("map diff against empty", func(t *testing.T) {
				testMapDiffAgainstEmpty(t, s)
			})

			// deletes
			t.Run("single delete diff", func(t *testing.T) {
				for k := 0; k < 100; k++ {
					testDeleteDiffs(t, prollyMap.(Map), tuples, 1)
				}
			})
			t.Run("many delete diffs", func(t *testing.T) {
				for k := 0; k < 10; k++ {
					testDeleteDiffs(t, prollyMap.(Map), tuples, s/10)
					testDeleteDiffs(t, prollyMap.(Map), tuples, s/2)
				}
			})
			t.Run("diff against empty map", func(t *testing.T) {
				testDeleteDiffs(t, prollyMap.(Map), tuples, s)
			})

			// inserts
			t.Run("single insert diff", func(t *testing.T) {
				for k := 0; k < 100; k++ {
					testInsertDiffs(t, prollyMap.(Map), tuples, 1)
				}
			})
			t.Run("many insert diffs", func(t *testing.T) {
				for k := 0; k < 10; k++ {
					testInsertDiffs(t, prollyMap.(Map), tuples, s/10)
					testInsertDiffs(t, prollyMap.(Map), tuples, s/2)
				}
			})

			// updates
			t.Run("single update diff", func(t *testing.T) {
				for k := 0; k < 100; k++ {
					testUpdateDiffs(t, prollyMap.(Map), tuples, 1)
				}
			})
			t.Run("many update diffs", func(t *testing.T) {
				for k := 0; k < 10; k++ {
					testUpdateDiffs(t, prollyMap.(Map), tuples, s/10)
					testUpdateDiffs(t, prollyMap.(Map), tuples, s/2)
				}
			})

			// one-sided diffs
			var empty Map
			t.Run("empty from map", func(t *testing.T) {
				testOneSidedDiff(t, s, empty, prollyMap.(Map), tree.AddedDiff)
			})
			t.Run("empty to map", func(t *testing.T) {
				testOneSidedDiff(t, s, prollyMap.(Map), empty, tree.RemovedDiff)
			})
		})
	}
}

func testMapDiffErrorHandling(t *testing.T, m Map) {
	ctx := context.Background()

	expErr := errors.New("error case")
	err := DiffMaps(ctx, m, m, false, func(ctx context.Context, diff tree.Diff) error {
		return expErr
	})
	require.Error(t, expErr, err)
}

func testEqualMapDiff(t *testing.T, m Map) {
	ctx := context.Background()
	var counter int
	err := DiffMaps(ctx, m, m, false, func(ctx context.Context, diff tree.Diff) error {
		counter++
		return nil
	})
	require.Error(t, io.EOF, err)
	assert.Equal(t, 0, counter)
}

func testMapDiffAgainstEmpty(t *testing.T, scale int) {
	ctx := context.Background()
	m, tuples := makeProllyMap(t, scale)
	empty, _ := makeProllyMap(t, 0)

	cnt := 0
	err := DiffMaps(ctx, m.(Map), empty.(Map), false, func(ctx context.Context, diff tree.Diff) error {
		assert.Equal(t, tuples[cnt][0], val.Tuple(diff.Key))
		assert.Equal(t, tuples[cnt][1], val.Tuple(diff.From))
		assert.Nil(t, val.Tuple(diff.To))
		cnt++
		return nil
	})
	require.Error(t, io.EOF, err)
	assert.Equal(t, scale, cnt)

	cnt = 0
	err = DiffMaps(ctx, empty.(Map), m.(Map), false, func(ctx context.Context, diff tree.Diff) error {
		assert.Equal(t, tuples[cnt][0], val.Tuple(diff.Key))
		assert.Equal(t, tuples[cnt][1], val.Tuple(diff.To))
		assert.Nil(t, val.Tuple(diff.From))
		cnt++
		return nil
	})
	require.Error(t, io.EOF, err)
	assert.Equal(t, scale, cnt)
}

func testDeleteDiffs(t *testing.T, from Map, tups [][2]val.Tuple, numDeletes int) {
	ctx := context.Background()
	rand.Shuffle(len(tups), func(i, j int) {
		tups[i], tups[j] = tups[j], tups[i]
	})

	deletes := tups[:numDeletes]
	sort.Slice(deletes, func(i, j int) bool {
		return from.keyDesc.Compare(ctx, deletes[i][0], deletes[j][0]) < 0
	})
	to := makeMapWithDeletes(t, from, deletes...)

	var cnt int
	err := DiffMaps(ctx, from, to, false, func(ctx context.Context, diff tree.Diff) error {
		assert.Equal(t, tree.RemovedDiff, diff.Type)
		assert.Equal(t, deletes[cnt][0], val.Tuple(diff.Key))
		cnt++
		return nil
	})
	require.Error(t, io.EOF, err)
	assert.Equal(t, numDeletes, cnt)
}

func testInsertDiffs(t *testing.T, from Map, tups [][2]val.Tuple, numInserts int) {
	ctx := context.Background()
	to, inserts := makeMapWithInserts(t, from, numInserts)

	var cnt int
	err := DiffMaps(ctx, from, to, false, func(ctx context.Context, diff tree.Diff) error {
		if !assert.Equal(t, tree.AddedDiff, diff.Type) {
			fmt.Println("")
		}
		assert.Equal(t, inserts[cnt][0], val.Tuple(diff.Key))
		assert.Equal(t, inserts[cnt][1], val.Tuple(diff.To))
		cnt++
		return nil
	})
	require.Error(t, io.EOF, err)
	assert.Equal(t, numInserts, cnt)
}

func testUpdateDiffs(t *testing.T, from Map, tups [][2]val.Tuple, numUpdates int) {
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

	var cnt int
	err := DiffMaps(ctx, from, to, false, func(ctx context.Context, diff tree.Diff) error {
		assert.Equal(t, tree.ModifiedDiff, diff.Type)
		assert.Equal(t, updates[cnt][0], val.Tuple(diff.Key))
		assert.Equal(t, updates[cnt][1], val.Tuple(diff.From))
		assert.Equal(t, updates[cnt][2], val.Tuple(diff.To))
		cnt++
		return nil
	})
	require.Error(t, io.EOF, err)
	assert.Equal(t, numUpdates, cnt)
}

func testOneSidedDiff(t *testing.T, sz int, from, to Map, typ tree.DiffType) {
	var seen int
	err := DiffMaps(context.Background(), from, to, false, func(ctx context.Context, diff tree.Diff) error {
		assert.Equal(t, diff.Type, typ)
		seen++
		return nil
	})
	assert.Error(t, err, io.EOF)
	assert.Equal(t, sz, seen)
}

func makeMapWithDeletes(t *testing.T, m Map, deletes ...[2]val.Tuple) Map {
	ctx := context.Background()
	mut := m.Mutate()
	for _, pair := range deletes {
		err := mut.Delete(ctx, pair[0])
		require.NoError(t, err)
	}
	mm, err := mut.Map(ctx)
	require.NoError(t, err)
	return mm
}

func makeMapWithInserts(t *testing.T, m Map, numInserts int) (Map, [][2]val.Tuple) {
	ctx := context.Background()
	kd, vd := m.Descriptors()
	inserts := generateInserts(t, m, kd, vd, numInserts)
	mut := m.Mutate()
	for _, pair := range inserts {
		err := mut.Put(ctx, pair[0], pair[1])
		require.NoError(t, err)
	}
	mm, err := mut.Map(ctx)
	require.NoError(t, err)
	return mm, inserts
}

// generates tuple pairs not currently in |m|
func generateInserts(t *testing.T, m testMap, kd, vd val.TupleDesc, numInserts int) [][2]val.Tuple {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()
	tups := tree.RandomTuplePairs(ctx, numInserts*2, kd, vd, ns)
	inserts, extra := tups[:numInserts], tups[numInserts:]

	j := 0
	for i, pair := range inserts {
		ok, err := m.Has(ctx, pair[0])
		require.NoError(t, err)
		if !ok {
			// |pair[0]| is not in |m|
			continue
		}

		// replace the key from |pair|
		for {
			ok, err = m.Has(ctx, extra[j][0])
			require.NoError(t, err)
			if !ok {
				// |extra[j][0]| is not in |m|
				inserts[i][0] = extra[j][0]
				break
			}
			j++
			require.True(t, j < len(extra))
		}
	}
	tree.SortTuplePairs(ctx, inserts, kd)

	return inserts
}

func makeMapWithUpdates(t *testing.T, m Map, updates ...[3]val.Tuple) Map {
	ctx := context.Background()
	mut := m.Mutate()
	for _, pair := range updates {
		err := mut.Put(ctx, pair[0], pair[2])
		require.NoError(t, err)
	}
	mm, err := mut.Map(ctx)
	require.NoError(t, err)
	return mm
}

func makeUpdatesToTuples(kd, vd val.TupleDesc, tuples ...[2]val.Tuple) (updates [][3]val.Tuple) {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()
	updates = make([][3]val.Tuple, len(tuples))

	valBuilder := val.NewTupleBuilder(vd, ns)

	for i := range updates {
		updates[i][0] = tuples[i][0]
		updates[i][1] = tuples[i][1]
		updates[i][2] = tree.RandomTuple(valBuilder, ns)
	}

	sort.Slice(updates, func(i, j int) bool {
		return kd.Compare(ctx, updates[i][0], updates[j][0]) < 0
	})

	return
}
