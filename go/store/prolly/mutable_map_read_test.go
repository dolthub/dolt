package prolly

import (
	"context"
	"fmt"
	"testing"

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
				testOrderedMapGetAndHas(t, mutableMap, tuples)
			})
			t.Run("iter all from map", func(t *testing.T) {
				testOrderedMapIterAll(t, mutableMap, tuples)
			})
			t.Run("iter all backwards from map", func(t *testing.T) {
				testOrderedMapIterAllBackward(t, mutableMap, tuples)
			})
			t.Run("iter value range", func(t *testing.T) {
				testOrderedMapIterValueRange(t, mutableMap, tuples)
			})
		})
	}
}

func makeMutableMap(t *testing.T, count int) (orderedMap, [][2]val.Tuple) {
	ctx := context.Background()
	ns := newTestNodeStore()

	kd := val.NewTupleDescriptor(
		val.Type{Enc: val.Int64Enc, Nullable: false},
	)
	vd := val.NewTupleDescriptor(
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
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
		m: Map{
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
