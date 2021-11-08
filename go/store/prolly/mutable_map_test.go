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
	t.Run("single point edit", func(t *testing.T) {
		testPointUpdates(t, 10)
		testPointUpdates(t, 100)
		testPointUpdates(t, 1000)
		testPointUpdates(t, 10_000)
	})
	t.Run("multiple range edits", func(t *testing.T) {
		testMultiplePointUpdates(t, 10)
		testMultiplePointUpdates(t, 100)
		testMultiplePointUpdates(t, 1000)
		testMultiplePointUpdates(t, 10_000)
	})
}

func testPointUpdates(t *testing.T, count int) {
	ctx := context.Background()
	orig := ascendingMap(t, count)

	puts := make([]int64, count)
	for i := range puts {
		puts[i] = int64(i)
	}
	rand.Shuffle(count, func(i, j int) {
		puts[i], puts[j] = puts[j], puts[i]
	})

	for _, idx := range puts {
		mut := orig.Mutate()
		key, value := putInts(mut, idx, -idx)

		// materialize map and query result
		m, err := mut.Map(ctx)
		require.NoError(t, err)
		err = m.Get(ctx, key, func(k, v val.Tuple) error {
			assert.Equal(t, key, k)
			assert.Equal(t, value, v)
			return nil
		})
		require.NoError(t, err)
	}
}


func testMultiplePointUpdates(t *testing.T, count int) {
	ctx := context.Background()
	orig := ascendingMap(t, count)

	puts := make([]int64, count)
	for i := range puts {
		puts[i] = int64(i)
	}
	rand.Shuffle(count, func(i, j int) {
		puts[i], puts[j] = puts[j], puts[i]
	})

	const k = 5
	for x := 0; x < len(puts); x += k {
		mut := orig.Mutate()

		edits := make([][2]val.Tuple, k)
		for i, idx := range puts[x:x+k] {
			edits[i][0], edits[i][1] = putInts(mut, idx, -idx)
		}
		m, err := mut.Map(ctx)
		require.NoError(t, err)

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

func ascendingMap(t *testing.T, count int) Map {
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
		keyBuilder.PutInt64(0, int64(i))
		items[i][0] = keyBuilder.Build(sharedPool)
		valBuilder.PutInt64(0, int64(i))
		items[i][1] = valBuilder.Build(sharedPool)
	}

	return makeProllyMap(t, kd, vd, items).(Map)
}
