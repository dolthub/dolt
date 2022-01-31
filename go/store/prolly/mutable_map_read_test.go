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

	"github.com/dolthub/dolt/go/store/val"
)

func TestMutableMapReads(t *testing.T) {
	scales := []int{
		10,
		100,
		1000,
		10_000,
	}

	for _, s := range scales {
		name := fmt.Sprintf("test mutable map at scale %d", s)
		t.Run(name, func(t *testing.T) {

			mutableMap, tuples := makeMutableMap(t, s)
			t.Run("get item from map", func(t *testing.T) {
				testGet(t, mutableMap, tuples)
			})
			t.Run("iter all from map", func(t *testing.T) {
				testIterAll(t, mutableMap, tuples)
			})
			t.Run("iter range", func(t *testing.T) {
				testIterRange(t, mutableMap, tuples)
			})
			t.Run("iter prefix range", func(t *testing.T) {
				testIterPrefixRange(t, mutableMap, tuples)
			})

			mutableIndex, idxTuples := makeMutableSecondaryIndex(t, s)
			t.Run("iter prefix range", func(t *testing.T) {
				testIterPrefixRange(t, mutableIndex, idxTuples)
			})

			mutableMap2, tuples2, deletes := deleteFromMutableMap(mutableMap.(MutableMap), tuples)
			t.Run("get item from map with deletes", func(t *testing.T) {
				testMutableMapGetAndHas(t, mutableMap2, tuples2, deletes)
			})
			t.Run("iter all from map with deletes", func(t *testing.T) {
				testIterAll(t, mutableMap2, tuples2)
			})
			t.Run("iter range with pending deletes", func(t *testing.T) {
				testIterRange(t, mutableMap2, tuples2)
			})

			mutableIndex2, idxTuples2, _ := deleteFromMutableMap(mutableIndex.(MutableMap), idxTuples)
			t.Run("iter prefix range", func(t *testing.T) {
				testIterPrefixRange(t, mutableIndex, idxTuples2)
			})

			prollyMap, err := mutableMap2.Map(context.Background())
			require.NoError(t, err)
			t.Run("get item from map after deletes applied", func(t *testing.T) {
				testHas(t, prollyMap, tuples2)
			})
			t.Run("iter all from map after deletes applied", func(t *testing.T) {
				testIterAll(t, prollyMap, tuples2)
			})
			t.Run("iter range after deletes applied", func(t *testing.T) {
				testIterRange(t, prollyMap, tuples2)
			})

			prollyIndex, err := mutableIndex2.Map(context.Background())
			require.NoError(t, err)
			t.Run("iter prefix range", func(t *testing.T) {
				testIterPrefixRange(t, prollyIndex, idxTuples2)
			})
		})
	}
}

func makeMutableMap(t *testing.T, count int) (orderedMap, [][2]val.Tuple) {
	ctx := context.Background()
	ns := newTestNodeStore()

	kd := val.NewTupleDescriptor(
		val.Type{Enc: val.Uint32Enc, Nullable: false},
	)
	vd := val.NewTupleDescriptor(
		val.Type{Enc: val.Uint32Enc, Nullable: true},
		val.Type{Enc: val.Uint32Enc, Nullable: true},
		val.Type{Enc: val.Uint32Enc, Nullable: true},
	)

	tuples := randomTuplePairs(count, kd, vd)
	// 2/3 of tuples in Map
	// 1/3 of tuples in memoryMap
	clone := cloneRandomTuples(tuples)
	split := (count * 2) / 3
	shuffleTuplePairs(clone)

	mapTuples := clone[:split]
	memTuples := clone[split:]
	sortTuplePairs(mapTuples, kd)
	sortTuplePairs(memTuples, kd)

	chunker, err := newEmptyTreeChunker(ctx, ns, newDefaultNodeSplitter)
	require.NoError(t, err)
	for _, pair := range mapTuples {
		_, err := chunker.Append(ctx, nodeItem(pair[0]), nodeItem(pair[1]))
		require.NoError(t, err)
	}
	root, err := chunker.Done(ctx)
	require.NoError(t, err)

	mut := MutableMap{
		prolly: Map{
			root:    root,
			keyDesc: kd,
			valDesc: vd,
			ns:      ns,
		},
		overlay: newMemoryMap(kd),
	}

	for _, pair := range memTuples {
		err = mut.Put(ctx, pair[0], pair[1])
		require.NoError(t, err)
	}

	return mut, tuples
}

func makeMutableSecondaryIndex(t *testing.T, count int) (orderedMap, [][2]val.Tuple) {
	m, tuples := makeProllySecondaryIndex(t, count)
	return newMutableMap(m.(Map)), tuples
}

func deleteFromMutableMap(mut MutableMap, tt [][2]val.Tuple) (MutableMap, [][2]val.Tuple, [][2]val.Tuple) {
	count := len(tt)
	testRand.Shuffle(count, func(i, j int) {
		tt[i], tt[j] = tt[j], tt[i]
	})

	// delete 1/4 of tuples
	deletes := tt[:count/4]

	// re-sort the remaining tuples
	remaining := tt[count/4:]
	desc := keyDescFromMap(mut)
	sortTuplePairs(remaining, desc)

	ctx := context.Background()
	for _, kv := range deletes {
		if err := mut.Delete(ctx, kv[0]); err != nil {
			panic(err)
		}
	}

	return mut, remaining, deletes
}

func testMutableMapGetAndHas(t *testing.T, mut MutableMap, tuples, deletes [][2]val.Tuple) {
	ctx := context.Background()
	for _, kv := range tuples {
		ok, err := mut.Has(ctx, kv[0])
		assert.True(t, ok)
		require.NoError(t, err)

		err = mut.Get(ctx, kv[0], func(key, val val.Tuple) (err error) {
			assert.NotNil(t, kv[0])
			assert.Equal(t, kv[0], key)
			assert.Equal(t, kv[1], val)
			return
		})
		require.NoError(t, err)
	}

	for _, kv := range deletes {
		ok, err := mut.Has(ctx, kv[0])
		assert.False(t, ok)
		require.NoError(t, err)

		err = mut.Get(ctx, kv[0], func(key, value val.Tuple) (err error) {
			assert.NotNil(t, kv[0])
			assert.Equal(t, val.Tuple(nil), key)
			assert.Equal(t, val.Tuple(nil), value)
			return
		})
		require.NoError(t, err)
	}
}
