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
		// todo(andy): small map case
		//testPointDeletes(t, 10)
		testPointDeletes(t, 100)
		testPointDeletes(t, 1000)
		testPointDeletes(t, 10_000)
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

		// materialize map and query result
		m, err := mut.Map(ctx)
		assert.NoError(t, err)
		assert.Equal(t, count, int(m.Count()))

		err = m.Get(ctx, key, func(k, v val.Tuple) error {
			assert.Equal(t, key, k)
			assert.Equal(t, value, v)
			return nil
		})
		require.NoError(t, err)
	}

	// batches of 5 updates
	const k = 5
	for x := 0; x < len(puts); x += k {
		mut := orig.Mutate()

		edits := make([][2]val.Tuple, k)
		for i, idx := range puts[x : x+k] {
			edits[i][0], edits[i][1] = putInts(mut, idx, idx)
		}
		m, err := mut.Map(ctx)
		assert.NoError(t, err)
		assert.Equal(t, count, int(m.Count()))

		for _, pair := range edits {
			key, value := pair[0], pair[1]
			err = m.Get(ctx, key, func(k, v val.Tuple) error {
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

		// materialize map and query result
		m, err := mut.Map(ctx)
		assert.NoError(t, err)
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

	// batches of 5 inserts
	const k = 5
	for x := 0; x < len(puts); x += k {
		mut := orig.Mutate()

		edits := make([][2]val.Tuple, k)
		for i, idx := range puts[x : x+k] {
			edits[i][0], edits[i][1] = putInts(mut, idx, -idx)
		}
		m, err := mut.Map(ctx)
		assert.NoError(t, err)
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

		// materialize map and query result
		m, err := mut.Map(ctx)
		assert.NoError(t, err)
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

	// batches of 5 deletes
	const k = 5
	for x := 0; x < len(deletes); x += k {
		mut := orig.Mutate()

		edits := make([]val.Tuple, k)
		for i, idx := range deletes[x : x+k] {
			edits[i] = deleteInt(mut, idx)
		}
		m, err := mut.Map(ctx)
		assert.NoError(t, err)
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

func deleteInt(mut MutableMap, k int64) (key val.Tuple) {
	kb := val.NewTupleBuilder(mut.m.keyDesc)
	kb.PutInt64(0, k)
	key = kb.Build(sharedPool)
	_ = mut.Put(context.Background(), key, nil)
	return
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
