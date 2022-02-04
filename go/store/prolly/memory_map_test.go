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

func TestMemMap(t *testing.T) {
	scales := []int{
		10,
		100,
		1000,
		10_000,
	}

	for _, s := range scales {
		name := fmt.Sprintf("test memory map at scale %d", s)
		t.Run(name, func(t *testing.T) {

			memMap, tuples := makeMemMap(t, s)
			t.Run("get item from map", func(t *testing.T) {
				testGet(t, memMap, tuples)
			})
			t.Run("iter all from map", func(t *testing.T) {
				testIterAll(t, memMap, tuples)
			})
			t.Run("iter range", func(t *testing.T) {
				testIterRange(t, memMap, tuples)
			})

			memIndex, idxTuples := makeMemSecondaryIndex(t, s)
			t.Run("iter prefix range", func(t *testing.T) {
				testIterPrefixRange(t, memIndex, idxTuples)
			})

			memMap2, tuples2, deletes := deleteFromMemoryMap(memMap.(memoryMap), tuples)
			t.Run("get item from map with deletes", func(t *testing.T) {
				testMemoryMapGetAndHas(t, memMap2, tuples2, deletes)
			})
			t.Run("iter all from map with deletes", func(t *testing.T) {
				testIterAll(t, memMap2, tuples2)
			})
			t.Run("iter range", func(t *testing.T) {
				testIterRange(t, memMap2, tuples2)
			})

			memIndex, idxTuples2, _ := deleteFromMemoryMap(memIndex.(memoryMap), idxTuples)
			t.Run("iter prefix range", func(t *testing.T) {
				testIterPrefixRange(t, memIndex, idxTuples2)
			})
		})
	}
}

func makeMemMap(t *testing.T, count int) (orderedMap, [][2]val.Tuple) {
	memKeyDesc := val.NewTupleDescriptor(
		val.Type{Enc: val.Uint32Enc, Nullable: false},
	)
	memValueDesc := val.NewTupleDescriptor(
		val.Type{Enc: val.Uint32Enc, Nullable: true},
		val.Type{Enc: val.Uint32Enc, Nullable: true},
		val.Type{Enc: val.Uint32Enc, Nullable: true},
	)

	tuples := randomTuplePairs(count, memKeyDesc, memValueDesc)
	mm := newMemoryMap(memKeyDesc)
	for _, pair := range tuples {
		mm.Put(pair[0], pair[1])
	}

	return mm, tuples
}

func makeMemSecondaryIndex(t *testing.T, count int) (orderedMap, [][2]val.Tuple) {
	memKeyDesc := val.NewTupleDescriptor(
		val.Type{Enc: val.Uint32Enc, Nullable: false},
		val.Type{Enc: val.Uint32Enc, Nullable: true},
	)
	memValueDesc := val.NewTupleDescriptor()

	tuples := randomCompositeTuplePairs(count, memKeyDesc, memValueDesc)

	mm := newMemoryMap(memKeyDesc)
	for _, pair := range tuples {
		mm.Put(pair[0], pair[1])
	}

	return mm, tuples
}

func deleteFromMemoryMap(mm memoryMap, tt [][2]val.Tuple) (memoryMap, [][2]val.Tuple, [][2]val.Tuple) {
	count := len(tt)
	testRand.Shuffle(count, func(i, j int) {
		tt[i], tt[j] = tt[j], tt[i]
	})

	// delete 1/4 of tuples
	deletes := tt[:count/4]

	// re-sort the remaining tuples
	remaining := tt[count/4:]
	desc := keyDescFromMap(mm)
	sortTuplePairs(remaining, desc)

	for _, kv := range deletes {
		mm.Put(kv[0], nil)
	}

	return mm, remaining, deletes
}

func testMemoryMapGetAndHas(t *testing.T, mem memoryMap, tuples, deletes [][2]val.Tuple) {
	ctx := context.Background()
	for _, kv := range tuples {
		err := mem.Get(ctx, kv[0], func(key, val val.Tuple) (err error) {
			assert.NotNil(t, kv[0])
			assert.Equal(t, kv[0], key)
			assert.Equal(t, kv[1], val)
			return
		})
		require.NoError(t, err)
	}

	for _, kv := range deletes {
		err := mem.Get(ctx, kv[0], func(key, value val.Tuple) (err error) {
			assert.NotNil(t, kv[0])
			assert.Equal(t, val.Tuple(nil), key)
			assert.Equal(t, val.Tuple(nil), value)
			return
		})
		require.NoError(t, err)
	}
}
