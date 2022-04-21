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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

func Test3WayMapMerge(t *testing.T) {
	scales := []int{
		10,
		100,
		1000,
		10000,
	}

	kd := val.NewTupleDescriptor(
		val.Type{Enc: val.Uint32Enc, Nullable: false},
	)
	vd := val.NewTupleDescriptor(
		val.Type{Enc: val.Uint32Enc, Nullable: true},
		val.Type{Enc: val.Uint32Enc, Nullable: true},
		val.Type{Enc: val.Uint32Enc, Nullable: true},
	)

	for _, s := range scales {
		name := fmt.Sprintf("test proCur map at scale %d", s)
		t.Run(name, func(t *testing.T) {
			t.Run("merge identical maps", func(t *testing.T) {
				testEqualMapMerge(t, s)
			})
			t.Run("3way merge inserts", func(t *testing.T) {
				for k := 0; k < 10; k++ {
					testThreeWayMapMerge(t, kd, vd, s)
				}
			})
			t.Run("tuple merge fn", func(t *testing.T) {
				for k := 0; k < 10; k++ {
					testTupleMergeFn(t, kd, vd, s)
				}
			})
		})
	}
}

func testEqualMapMerge(t *testing.T, sz int) {
	om, _ := makeProllyMap(t, sz)
	m := om.(Map)
	ctx := context.Background()
	mm, err := MergeMaps(ctx, m, m, m, panicOnConflict)
	require.NoError(t, err)
	assert.NotNil(t, mm)
	assert.Equal(t, m.HashOf(), mm.HashOf())
}

func testThreeWayMapMerge(t *testing.T, kd, vd val.TupleDesc, sz int) {
	baseTuples, leftEdits, rightEdits := makeTuplesAndMutations(kd, vd, sz)
	om := prollyMapFromTuples(t, kd, vd, baseTuples)

	base := om.(Map)
	left := applyMutationSet(t, base, leftEdits)
	right := applyMutationSet(t, base, rightEdits)

	ctx := context.Background()
	final, err := MergeMaps(ctx, left, right, base, panicOnConflict)
	assert.NoError(t, err)

	for _, add := range leftEdits.adds {
		ok, err := final.Has(ctx, add[0])
		assert.NoError(t, err)
		assert.True(t, ok)
		err = final.Get(ctx, add[0], func(key, value val.Tuple) error {
			assert.Equal(t, value, add[1])
			return nil
		})
		assert.NoError(t, err)
	}
	for _, add := range rightEdits.adds {
		ok, err := final.Has(ctx, add[0])
		assert.NoError(t, err)
		assert.True(t, ok)
		err = final.Get(ctx, add[0], func(key, value val.Tuple) error {
			assert.Equal(t, value, add[1])
			return nil
		})
		assert.NoError(t, err)
	}

	for _, del := range leftEdits.deletes {
		ok, err := final.Has(ctx, del)
		assert.NoError(t, err)
		assert.False(t, ok)
	}
	for _, del := range rightEdits.deletes {
		ok, err := final.Has(ctx, del)
		assert.NoError(t, err)
		assert.False(t, ok)
	}

	for _, up := range leftEdits.updates {
		ok, err := final.Has(ctx, up[0])
		assert.NoError(t, err)
		assert.True(t, ok)
		err = final.Get(ctx, up[0], func(key, value val.Tuple) error {
			assert.Equal(t, value, up[1])
			return nil
		})
		assert.NoError(t, err)
	}
	for _, up := range rightEdits.updates {
		ok, err := final.Has(ctx, up[0])
		assert.NoError(t, err)
		assert.True(t, ok)
		err = final.Get(ctx, up[0], func(key, value val.Tuple) error {
			assert.Equal(t, value, up[1])
			return nil
		})
		assert.NoError(t, err)
	}
}

func testTupleMergeFn(t *testing.T, kd, vd val.TupleDesc, sz int) {
	ctx := context.Background()
	tuples := tree.RandomTuplePairs(sz, kd, vd)
	om := prollyMapFromTuples(t, kd, vd, tuples)
	base := om.(Map)

	mutSz := sz / 10
	testRand.Shuffle(len(tuples), func(i, j int) {
		tuples[i], tuples[j] = tuples[j], tuples[i]
	})

	// make overlapping edits
	left := makeUpdatesToTuples(kd, vd, tuples[:mutSz]...)
	right := makeUpdatesToTuples(kd, vd, tuples[:mutSz]...)

	l := base.Mutate()
	for _, update := range left {
		err := l.Put(ctx, update[0], update[2])
		require.NoError(t, err)
	}
	leftMap, err := l.Map(ctx)
	require.NoError(t, err)

	r := base.Mutate()
	for _, update := range right {
		err := r.Put(ctx, update[0], update[2])
		require.NoError(t, err)
	}
	rightMap, err := r.Map(ctx)
	require.NoError(t, err)

	idx := 0
	final, err := MergeMaps(ctx, leftMap, rightMap, base, func(l, r tree.Diff) (merged tree.Diff, ok bool) {
		assert.Equal(t, l.Key, r.Key)
		assert.Equal(t, l.From, r.From)

		assert.Equal(t, val.Tuple(l.To), left[idx][2])
		assert.Equal(t, val.Tuple(r.To), right[idx][2])

		// right diff wins
		merged, ok = r, true
		idx++
		return
	})
	require.NoError(t, err)

	for _, update := range left {
		err = final.Get(ctx, update[0], func(key, value val.Tuple) error {
			assert.Equal(t, key, update[0])
			assert.NotEqual(t, value, update[2])
			return nil
		})
		require.NoError(t, err)
	}

	for _, update := range right {
		err = final.Get(ctx, update[0], func(key, value val.Tuple) error {
			assert.Equal(t, key, update[0])
			assert.Equal(t, value, update[2])
			return nil
		})
		require.NoError(t, err)
	}
}

type mutationSet struct {
	adds    [][2]val.Tuple
	deletes []val.Tuple
	updates [][3]val.Tuple
}

func makeTuplesAndMutations(kd, vd val.TupleDesc, sz int) (base [][2]val.Tuple, left, right mutationSet) {
	mutSz := sz / 10
	totalSz := sz + (mutSz * 2)
	tuples := tree.RandomTuplePairs(totalSz, kd, vd)

	base = tuples[:sz]

	left = mutationSet{
		adds:    tuples[sz : sz+mutSz],
		deletes: make([]val.Tuple, mutSz),
	}
	right = mutationSet{
		adds:    tuples[sz+mutSz:],
		deletes: make([]val.Tuple, mutSz),
	}

	edits := make([][2]val.Tuple, len(base))
	copy(edits, base)
	testRand.Shuffle(len(edits), func(i, j int) {
		edits[i], edits[j] = edits[j], edits[i]
	})

	for i, pair := range edits[:mutSz] {
		left.deletes[i] = pair[0]
	}
	for i, pair := range edits[mutSz : mutSz*2] {
		right.deletes[i] = pair[0]
	}

	left.updates = makeUpdatesToTuples(kd, vd, edits[mutSz*2:mutSz*3]...)
	right.updates = makeUpdatesToTuples(kd, vd, edits[mutSz*3:mutSz*4]...)

	return
}

func applyMutationSet(t *testing.T, base Map, edits mutationSet) (m Map) {
	ctx := context.Background()
	mut := base.Mutate()

	var err error
	for _, add := range edits.adds {
		err = mut.Put(ctx, add[0], add[1])
		require.NoError(t, err)
	}
	for _, del := range edits.deletes {
		err = mut.Delete(ctx, del)
		require.NoError(t, err)
	}
	for _, up := range edits.updates {
		err = mut.Put(ctx, up[0], up[1])
		require.NoError(t, err)
	}

	m, err = mut.Map(ctx)
	require.NoError(t, err)
	return
}

func panicOnConflict(left, right tree.Diff) (tree.Diff, bool) {
	panic("cannot merge cells")
}
