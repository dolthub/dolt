package prolly

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/val"
)

func TestMutableMapReads(t *testing.T) {
	t.Run("get item from map", func(t *testing.T) {
		testOrderedMapGetAndHas(t, makeMutableMap, 10)
		testOrderedMapGetAndHas(t, makeMutableMap, 100)
		testOrderedMapGetAndHas(t, makeMutableMap, 1000)
		testOrderedMapGetAndHas(t, makeMutableMap, 10_000)
	})
	//t.Run("get from map at index", func(t *testing.T) {
	//	testOrderedMapGetIndex(t, makeMutableMap, 10)
	//	testOrderedMapGetIndex(t, makeMutableMap, 100)
	//	testOrderedMapGetIndex(t, makeMutableMap, 1000)
	//	testOrderedMapGetIndex(t, makeMutableMap, 10_000)
	//})
	//t.Run("get value range from map", func(t *testing.T) {
	//	testMapIterValueRange(t, 10)
	//	testMapIterValueRange(t, 100)
	//	testMapIterValueRange(t, 1000)
	//	testMapIterValueRange(t, 10_000)
	//})
	//t.Run("get index range from map", func(t *testing.T) {
	//	testOrderedMapIterIndexRange(t, makeMutableMap, 10)
	//	testOrderedMapIterIndexRange(t, makeMutableMap, 100)
	//	testOrderedMapIterIndexRange(t, makeMutableMap, 1000)
	//	testOrderedMapIterIndexRange(t, makeMutableMap, 10_000)
	//})
}

func makeMutableMap(t *testing.T, kd, vd val.TupleDesc, items [][2]val.Tuple) orderedMap {
	m := makeProllyMap(t, kd, vd, items)
	return m.(Map).Mutate()
}

var _ cartographer = makeMutableMap

func TestMutableMapWrites(t *testing.T) {
	t.Run("point updates", func(t *testing.T) {
		testPointUpdates(t, 10)
		testPointUpdates(t, 100)
		testPointUpdates(t, 1000)
		testPointUpdates(t, 10_000)
	})
	t.Run("point inserts", func(t *testing.T) {
		testPointInserts(t, 10)
		testPointInserts(t, 100)
		testPointInserts(t, 1000)
		testPointInserts(t, 10_000)
	})
	t.Run("point deletes", func(t *testing.T) {
		testPointDeletes(t, 10)
		testPointDeletes(t, 100)
		testPointDeletes(t, 1000)
		testPointDeletes(t, 10_000)
	})
	t.Run("multiple point updates", func(t *testing.T) {
		testMultiplePointUpdates(t, 10)
		testMultiplePointUpdates(t, 100)
		testMultiplePointUpdates(t, 1000)
		testMultiplePointUpdates(t, 10_000)
	})
	t.Run("multiple point inserts", func(t *testing.T) {
		testMultiplePointInserts(t, 10)
		testMultiplePointInserts(t, 100)
		testMultiplePointInserts(t, 1000)
		testMultiplePointInserts(t, 10_000)
	})
	t.Run("multiple point deletes", func(t *testing.T) {
		testPointDeletes(t, 10)
		testMultiplePointDeletes(t, 100)
		testMultiplePointDeletes(t, 1000)
		testMultiplePointDeletes(t, 10_000)
	})
}

func testPointUpdates(t *testing.T, count int) {
	orig := ascendingMap(t, count)

	puts := make([]int64, count)
	for i := range puts {
		puts[i] = int64(i)
	}
	rand.Shuffle(len(puts), func(i, j int) {
		puts[i], puts[j] = puts[j], puts[i]
	})

	ctx := context.Background()
	for _, idx := range puts {
		mut := orig.Mutate()
		key, value := putInts(mut, idx, -idx)

		m := materializeMap(t, mut)
		assert.Equal(t, count, int(m.Count()))

		err := m.Get(ctx, key, func(k, v val.Tuple) error {
			assert.Equal(t, key, k)
			assert.Equal(t, value, v)
			return nil
		})
		require.NoError(t, err)
	}
}

func testMultiplePointUpdates(t *testing.T, count int) {
	orig := ascendingMap(t, count)

	puts := make([]int64, count)
	for i := range puts {
		puts[i] = int64(i)
	}
	rand.Shuffle(len(puts), func(i, j int) {
		puts[i], puts[j] = puts[j], puts[i]
	})

	// batches of 5 updates
	const k = 5
	edits := make([][2]val.Tuple, k)
	ctx := context.Background()

	for x := 0; x < len(puts); x += k {

		mut := orig.Mutate()
		stop := x + k
		if stop > len(puts) {
			stop = len(puts)
		}
		for i, idx := range puts[x : x+k] {
			edits[i][0], edits[i][1] = putInts(mut, idx, idx)
		}

		m := materializeMap(t, mut)
		assert.Equal(t, count, int(m.Count()))

		for _, pair := range edits {
			key, value := pair[0], pair[1]
			err := m.Get(ctx, key, func(k, v val.Tuple) error {
				assert.Equal(t, key, k)
				assert.Equal(t, value, v)
				return nil
			})
			require.NoError(t, err)
		}
	}
}

func testPointInserts(t *testing.T, count int) {
	// create map of even numbers
	orig := ascendingMapWithStep(t, count, 2)

	// todo(andy): inserting past the end
	puts := make([]int64, count)
	for i := range puts {
		// create odd-number edits
		puts[i] = int64(i*2) + 1
	}
	rand.Shuffle(len(puts), func(i, j int) {
		puts[i], puts[j] = puts[j], puts[i]
	})

	ctx := context.Background()
	for _, idx := range puts {
		mut := orig.Mutate()
		key, value := putInts(mut, idx, idx)

		m := materializeMap(t, mut)
		assert.Equal(t, count+1, int(m.Count()))

		ok, err := m.Has(ctx, key)
		assert.NoError(t, err)
		assert.True(t, ok)

		err = m.Get(ctx, key, func(k, v val.Tuple) error {
			assert.Equal(t, key, k)
			assert.Equal(t, value, v)
			return nil
		})
		require.NoError(t, err)
	}
}

func testMultiplePointInserts(t *testing.T, count int) {
	// create map of even numbers
	orig := ascendingMapWithStep(t, count, 2)

	// todo(andy): inserting past the end
	puts := make([]int64, count)
	for i := range puts {
		// create odd-number edits
		puts[i] = int64(i*2) + 1
	}
	rand.Shuffle(len(puts), func(i, j int) {
		puts[i], puts[j] = puts[j], puts[i]
	})

	// batches of 5 inserts
	const k = 5
	edits := make([][2]val.Tuple, k)

	for x := 0; x < len(puts); x += k {

		mut := orig.Mutate()
		stop := x + k
		if stop > len(puts) {
			stop = len(puts)
		}
		for i, idx := range puts[x : x+k] {
			edits[i][0], edits[i][1] = putInts(mut, idx, -idx)
		}

		ctx := context.Background()
		m := materializeMap(t, mut)
		assert.Equal(t, count+k, int(m.Count()))

		for _, pair := range edits {
			key, value := pair[0], pair[1]

			ok, err := m.Has(ctx, key)
			assert.NoError(t, err)
			assert.True(t, ok)

			err = m.Get(ctx, key, func(k, v val.Tuple) error {
				assert.Equal(t, key, k)
				assert.Equal(t, value, v)
				return nil
			})
			require.NoError(t, err)
		}
	}
}

func testPointDeletes(t *testing.T, count int) {
	orig := ascendingMap(t, count)

	deletes := make([]int64, count)
	for i := range deletes {
		deletes[i] = int64(i)
	}
	rand.Shuffle(len(deletes), func(i, j int) {
		deletes[i], deletes[j] = deletes[j], deletes[i]
	})

	ctx := context.Background()
	for _, idx := range deletes {
		mut := orig.Mutate()
		key := deleteInt(mut, idx)

		m := materializeMap(t, mut)
		assert.Equal(t, count-1, int(m.Count()))

		ok, err := m.Has(ctx, key)
		assert.NoError(t, err)
		assert.False(t, ok)

		err = m.Get(ctx, key, func(k, v val.Tuple) error {
			assert.Nil(t, k)
			assert.Nil(t, v)
			return nil
		})
		require.NoError(t, err)
	}
}

func testMultiplePointDeletes(t *testing.T, count int) {
	orig := ascendingMap(t, count)

	deletes := make([]int64, count)
	for i := range deletes {
		deletes[i] = int64(i)
	}
	rand.Shuffle(len(deletes), func(i, j int) {
		deletes[i], deletes[j] = deletes[j], deletes[i]
	})

	// batches of 5 deletes
	const k = 5
	edits := make([]val.Tuple, k)
	ctx := context.Background()

	for x := 0; x < len(deletes); x += k {

		mut := orig.Mutate()
		stop := x + k
		if stop > len(deletes) {
			stop = len(deletes)
		}
		for i, idx := range deletes[x : x+k] {
			edits[i] = deleteInt(mut, idx)
		}

		m := materializeMap(t, mut)
		assert.Equal(t, count-k, int(m.Count()))

		for _, key := range edits {
			ok, err := m.Has(ctx, key)
			assert.NoError(t, err)
			assert.False(t, ok)

			err = m.Get(ctx, key, func(k, v val.Tuple) error {
				assert.Nil(t, k)
				assert.Nil(t, v)
				return nil
			})
			require.NoError(t, err)
		}
	}
}

func putInts(mut MutableMap, k, v int64) (key, value val.Tuple) {
	kb := val.NewTupleBuilder(mut.m.keyDesc)
	vb := val.NewTupleBuilder(mut.m.valDesc)
	kb.PutInt64(0, k)
	vb.PutInt64(0, v)
	key = kb.Build(sharedPool)
	value = vb.Build(sharedPool)
	_ = mut.Put(context.Background(), key, value)
	return
}

func deleteInt(mut MutableMap, k int64) (key val.Tuple) {
	kb := val.NewTupleBuilder(mut.m.keyDesc)
	kb.PutInt64(0, k)
	key = kb.Build(sharedPool)
	_ = mut.Put(context.Background(), key, nil)
	return
}

// validates edit provider and materializes map
func materializeMap(t *testing.T, mut MutableMap) Map {
	ctx := context.Background()

	// ensure edits are provided in order
	iter := mut.overlay.mutations()
	prev, _ := iter.next()
	require.NotNil(t, prev)
	for {
		next, _ := iter.next()
		if next == nil {
			break
		}
		cmp := mut.m.compareKeys(prev, next)
		assert.True(t, cmp < 0)
		prev = next
	}

	m, err := mut.Map(ctx)
	assert.NoError(t, err)
	return m
}

func ascendingMap(t *testing.T, count int) Map {
	return ascendingMapWithStep(t, count, 1)
}

func ascendingMapWithStep(t *testing.T, count, step int) Map {
	kd := val.NewTupleDescriptor(
		val.Type{Enc: val.Int64Enc, Nullable: false},
	)
	vd := val.NewTupleDescriptor(
		val.Type{Enc: val.Int64Enc, Nullable: true},
	)

	keyBuilder := val.NewTupleBuilder(kd)
	valBuilder := val.NewTupleBuilder(vd)
	items := make([][2]val.Tuple, count)
	for i := range items {
		v := int64(i * step)
		keyBuilder.PutInt64(0, v)
		valBuilder.PutInt64(0, v)
		items[i][0] = keyBuilder.Build(sharedPool)
		items[i][1] = valBuilder.Build(sharedPool)
	}

	return makeProllyMap(t, kd, vd, items).(Map)
}
