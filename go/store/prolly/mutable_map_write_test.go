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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/val"
)

func TestMutableMapWrites(t *testing.T) {
	scales := []int{
		10,
		100,
		1000,
		10_000,
	}

	for _, s := range scales {
		name := fmt.Sprintf("test mutable map at scale %d", s)
		t.Run(name, func(t *testing.T) {
			t.Run("point updates", func(t *testing.T) {
				testPointUpdates(t, s)
			})
			t.Run("point inserts", func(t *testing.T) {
				testPointInserts(t, s)
			})
			t.Run("point deletes", func(t *testing.T) {
				testPointDeletes(t, s)
			})
			t.Run("multiple point updates", func(t *testing.T) {
				testMultiplePointUpdates(t, s/2, s)
			})
			t.Run("multiple point inserts", func(t *testing.T) {
				testMultiplePointInserts(t, s/2, s)
			})
			t.Run("multiple point deletes", func(t *testing.T) {
				testMultiplePointDeletes(t, s/2, s)
			})
			t.Run("mixed inserts, updates, and deletes", func(t *testing.T) {
				testMixedMutations(t, s/2, s)
			})
			t.Run("insert outside of existing range", func(t *testing.T) {
				testInsertsOutsideExistingRange(t, s)
			})
			t.Run("bulk insert", func(t *testing.T) {
				testBulkInserts(t, s)
			})
			t.Run("deleting all keys", func(t *testing.T) {
				testMultiplePointDeletes(t, s, s)
			})
		})
	}
}

func testPointUpdates(t *testing.T, mapCount int) {
	orig := ascendingIntMap(t, mapCount)

	updates := make([][2]val.Tuple, mapCount)
	for i := range updates {
		updates[i][0], updates[i][1] = makePut(int64(i), int64(-i))
	}
	rand.Shuffle(len(updates), func(i, j int) {
		updates[i], updates[j] = updates[j], updates[i]
	})

	ctx := context.Background()
	for _, up := range updates {
		mut := orig.Mutate()
		err := mut.Put(ctx, up[0], up[1])
		require.NoError(t, err)

		m := materializeMap(t, mut)
		//assert.Equal(t, mapCount, countOrderedMap(t, m))

		err = m.Get(ctx, up[0], func(k, v val.Tuple) error {
			assert.Equal(t, up[0], k)
			assert.Equal(t, up[1], v)
			return nil
		})
		require.NoError(t, err)
	}
}

func testMultiplePointUpdates(t *testing.T, batch int, mapCount int) {
	orig := ascendingIntMap(t, mapCount)

	updates := make([][2]val.Tuple, mapCount)
	for i := range updates {
		updates[i][0], updates[i][1] = makePut(int64(i), int64(-i))
	}
	rand.Shuffle(len(updates), func(i, j int) {
		updates[i], updates[j] = updates[j], updates[i]
	})

	ctx := context.Background()
	for x := 0; x < len(updates); x += batch {
		b := updates[x : x+batch]

		mut := orig.Mutate()
		for _, up := range b {
			err := mut.Put(ctx, up[0], up[1])
			require.NoError(t, err)
		}
		m := materializeMap(t, mut)
		//assert.Equal(t, mapCount, countOrderedMap(t, m))

		for _, up := range b {
			err := m.Get(ctx, up[0], func(k, v val.Tuple) error {
				assert.Equal(t, up[0], k)
				assert.Equal(t, up[1], v)
				return nil
			})
			require.NoError(t, err)
		}
	}
}

func testPointInserts(t *testing.T, mapCount int) {
	// create map of even numbers
	orig := ascendingIntMapWithStep(t, mapCount, 2)

	inserts := make([][2]val.Tuple, mapCount)
	for i := range inserts {
		// create odd-numbered inserts
		v := int64(i*2) + 1
		inserts[i][0], inserts[i][1] = makePut(v, v)
	}
	rand.Shuffle(len(inserts), func(i, j int) {
		inserts[i], inserts[j] = inserts[j], inserts[i]
	})

	ctx := context.Background()
	for _, in := range inserts {
		mut := orig.Mutate()
		err := mut.Put(ctx, in[0], in[1])
		require.NoError(t, err)

		m := materializeMap(t, mut)
		//assert.Equal(t, mapCount+1, countOrderedMap(t, m))

		ok, err := m.Has(ctx, in[0])
		assert.NoError(t, err)
		assert.True(t, ok)

		err = m.Get(ctx, in[0], func(k, v val.Tuple) error {
			assert.Equal(t, in[0], k)
			assert.Equal(t, in[1], v)
			return nil
		})
		require.NoError(t, err)
	}
}

func testMultiplePointInserts(t *testing.T, batch int, mapCount int) {
	// create map of even numbers
	orig := ascendingIntMapWithStep(t, mapCount, 2)

	inserts := make([][2]val.Tuple, mapCount)
	for i := range inserts {
		// create odd-numbered inserts
		v := int64(i*2) + 1
		inserts[i][0], inserts[i][1] = makePut(v, v)
	}
	rand.Shuffle(len(inserts), func(i, j int) {
		inserts[i], inserts[j] = inserts[j], inserts[i]
	})

	ctx := context.Background()
	for x := 0; x < len(inserts); x += batch {
		b := inserts[x : x+batch]

		mut := orig.Mutate()
		for _, in := range b {
			err := mut.Put(ctx, in[0], in[1])
			require.NoError(t, err)
		}
		m := materializeMap(t, mut)
		//assert.Equal(t, mapCount+batch, countOrderedMap(t, m))

		for _, up := range b {
			ok, err := m.Has(ctx, up[0])
			assert.NoError(t, err)
			assert.True(t, ok)

			err = m.Get(ctx, up[0], func(k, v val.Tuple) error {
				assert.Equal(t, up[0], k)
				assert.Equal(t, up[1], v)
				return nil
			})
			require.NoError(t, err)
		}
	}
}

func testPointDeletes(t *testing.T, mapCount int) {
	orig := ascendingIntMap(t, mapCount)

	deletes := make([]val.Tuple, mapCount)
	for i := range deletes {
		deletes[i] = makeDelete(int64(i))
	}
	rand.Shuffle(len(deletes), func(i, j int) {
		deletes[i], deletes[j] = deletes[j], deletes[i]
	})

	ctx := context.Background()
	for _, del := range deletes {
		mut := orig.Mutate()
		err := mut.Put(ctx, del, nil)
		assert.NoError(t, err)

		m := materializeMap(t, mut)
		//assert.Equal(t, mapCount-1, countOrderedMap(t, m))

		ok, err := m.Has(ctx, del)
		assert.NoError(t, err)
		assert.False(t, ok)

		err = m.Get(ctx, del, func(k, v val.Tuple) error {
			assert.Nil(t, k)
			assert.Nil(t, v)
			return nil
		})
		require.NoError(t, err)
	}
}

func testMultiplePointDeletes(t *testing.T, batch int, mapCount int) {
	orig := ascendingIntMap(t, mapCount)

	deletes := make([]val.Tuple, mapCount)
	for i := range deletes {
		deletes[i] = makeDelete(int64(i))
	}
	rand.Shuffle(len(deletes), func(i, j int) {
		deletes[i], deletes[j] = deletes[j], deletes[i]
	})

	ctx := context.Background()
	for x := 0; x < len(deletes); x += batch {
		b := deletes[x : x+batch]

		mut := orig.Mutate()
		for _, del := range b {
			err := mut.Put(ctx, del, nil)
			require.NoError(t, err)
		}
		m := materializeMap(t, mut)
		//assert.Equal(t, mapCount-batch, countOrderedMap(t, m))

		for _, del := range b {
			ok, err := m.Has(ctx, del)
			assert.NoError(t, err)
			assert.False(t, ok)

			err = m.Get(ctx, del, func(k, v val.Tuple) error {
				assert.Nil(t, k)
				assert.Nil(t, v)
				return nil
			})
			require.NoError(t, err)
		}
	}
}

func testMixedMutations(t *testing.T, batch int, mapCount int) {
	// create map of first |mapCount| *even* numbers
	orig := ascendingIntMapWithStep(t, mapCount, 2)

	mutations := make([][2]val.Tuple, mapCount*2)
	for i := 0; i < len(mutations); i += 2 {
		// |v| is an existing key.
		v := int64(i * 2)

		// insert new key-value pair.
		mutations[i][0], mutations[i][1] = makePut(v+1, v+1)

		// create a delete or an update for |v|, but not both.
		if i%4 == 0 {
			// update existing key-value pair.
			mutations[i+1][0], mutations[i+1][1] = makePut(v, -v)
		} else {
			// delete existing key-value pair.
			mutations[i+1][0], mutations[i+1][1] = makeDelete(v), nil
		}
	}
	rand.Shuffle(len(mutations), func(i, j int) {
		mutations[i], mutations[j] = mutations[j], mutations[i]
	})

	var err error
	ctx := context.Background()
	for x := 0; x < len(mutations); x += batch {
		b := mutations[x : x+batch]

		mut := orig.Mutate()
		for _, edit := range b {
			err = mut.Put(ctx, edit[0], edit[1])
			require.NoError(t, err)
		}
		m := materializeMap(t, mut)

		for _, edit := range b {
			key, ok := mutKeyDesc.GetInt64(0, edit[0])
			assert.True(t, ok)

			if key%2 == 1 {
				// insert
				ok, err = m.Has(ctx, edit[0])
				assert.NoError(t, err)
				assert.True(t, ok)
			} else if edit[1] == nil {
				// delete
				ok, err = m.Has(ctx, edit[0])
				assert.NoError(t, err)
				assert.False(t, ok)
			} else {
				// update
				ok, err = m.Has(ctx, edit[0])
				assert.NoError(t, err)
				assert.True(t, ok)
			}
		}
	}
}

func testInsertsOutsideExistingRange(t *testing.T, mapCount int) {
	orig := ascendingIntMapWithStep(t, mapCount, 1)

	inserts := make([][2]val.Tuple, 2)
	// insert before beginning
	v := int64(-13)
	inserts[0][0], inserts[0][1] = makePut(v, v)
	// insert after end
	v = int64(mapCount + 13)
	inserts[1][0], inserts[1][1] = makePut(v, v)

	ctx := context.Background()
	for _, in := range inserts {
		mut := orig.Mutate()
		err := mut.Put(ctx, in[0], in[1])
		require.NoError(t, err)

		m := materializeMap(t, mut)
		//assert.Equal(t, mapCount+1, countOrderedMap(t, m))

		ok, err := m.Has(ctx, in[0])
		assert.NoError(t, err)
		assert.True(t, ok)

		err = m.Get(ctx, in[0], func(k, v val.Tuple) error {
			assert.Equal(t, in[0], k)
			assert.Equal(t, in[1], v)
			return nil
		})
		require.NoError(t, err)
	}
}

func testBulkInserts(t *testing.T, size int) {
	// create sparse map
	orig := ascendingIntMapWithStep(t, size, size)

	// make 10x as many inserts as the size of the map
	inserts := make([][2]val.Tuple, size*10)
	for i := range inserts {
		v := rand.Int63()
		inserts[i][0], inserts[i][1] = makePut(v, v)
	}
	rand.Shuffle(len(inserts), func(i, j int) {
		inserts[i], inserts[j] = inserts[j], inserts[i]
	})

	ctx := context.Background()
	mut := orig.Mutate()
	for _, in := range inserts {
		err := mut.Put(ctx, in[0], in[1])
		require.NoError(t, err)
	}

	m := materializeMap(t, mut)
	//assert.Equal(t, size*11, countOrderedMap(t, m))

	for _, in := range inserts {
		ok, err := m.Has(ctx, in[0])
		assert.NoError(t, err)
		assert.True(t, ok)

		err = m.Get(ctx, in[0], func(k, v val.Tuple) error {
			assert.Equal(t, in[0], k)
			assert.Equal(t, in[1], v)
			return nil
		})
		require.NoError(t, err)
	}
}

func ascendingIntMap(t *testing.T, count int) Map {
	return ascendingIntMapWithStep(t, count, 1)
}

func ascendingIntMapWithStep(t *testing.T, count, step int) Map {
	ctx := context.Background()
	ns := newTestNodeStore()

	tuples := make([][2]val.Tuple, count)
	for i := range tuples {
		v := int64(i * step)
		tuples[i][0], tuples[i][1] = makePut(v, v)
	}

	chunker, err := newEmptyTreeChunker(ctx, ns, newDefaultNodeSplitter)
	require.NoError(t, err)

	for _, pair := range tuples {
		_, err := chunker.Append(ctx, nodeItem(pair[0]), nodeItem(pair[1]))
		require.NoError(t, err)
	}
	root, err := chunker.Done(ctx)
	require.NoError(t, err)

	return Map{
		root:    root,
		keyDesc: mutKeyDesc,
		valDesc: mutValDesc,
		ns:      ns,
	}
}

var mutKeyDesc = val.NewTupleDescriptor(
	val.Type{Enc: val.Int64Enc, Nullable: false},
)
var mutValDesc = val.NewTupleDescriptor(
	val.Type{Enc: val.Int64Enc, Nullable: true},
)

var mutKeyBuilder = val.NewTupleBuilder(mutKeyDesc)
var mutValBuilder = val.NewTupleBuilder(mutValDesc)

func makePut(k, v int64) (key, value val.Tuple) {
	mutKeyBuilder.PutInt64(0, k)
	mutValBuilder.PutInt64(0, v)
	key = mutKeyBuilder.Build(sharedPool)
	value = mutValBuilder.Build(sharedPool)
	return
}

func makeDelete(k int64) (key val.Tuple) {
	mutKeyBuilder.PutInt64(0, k)
	key = mutKeyBuilder.Build(sharedPool)
	return
}

// validates edit provider and materializes map
func materializeMap(t *testing.T, mut MutableMap) Map {
	ctx := context.Background()

	// ensure edits are provided in order
	iter := mut.overlay.mutations()
	prev, _ := iter.nextMutation(ctx)
	require.NotNil(t, prev)
	for {
		next, _ := iter.nextMutation(ctx)
		if next == nil {
			break
		}
		cmp := mut.prolly.compareKeys(prev, next)
		assert.True(t, cmp < 0)
		prev = next
	}

	m, err := mut.Map(ctx)
	assert.NoError(t, err)
	return m
}
