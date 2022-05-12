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
	"io"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// todo(andy): randomize test seed
var testRand = rand.New(rand.NewSource(1))
var sharedPool = pool.NewBuffPool()

func TestMap(t *testing.T) {
	scales := []int{
		10,
		100,
		1000,
		10_000,
	}

	for _, s := range scales {
		name := fmt.Sprintf("test prolly map at scale %d", s)
		t.Run(name, func(t *testing.T) {
			prollyMap, tuples := makeProllyMap(t, s)

			t.Run("get item from map", func(t *testing.T) {
				testGet(t, prollyMap, tuples)
			})
			t.Run("iter all from map", func(t *testing.T) {
				testIterAll(t, prollyMap, tuples)
			})
			t.Run("iter range", func(t *testing.T) {
				testIterRange(t, prollyMap, tuples)
			})
			t.Run("iter ordinal range", func(t *testing.T) {
				testIterOrdinalRange(t, prollyMap.(ordinalMap), tuples)
			})

			indexMap, tuples2 := makeProllySecondaryIndex(t, s)
			t.Run("iter prefix range", func(t *testing.T) {
				testIterPrefixRange(t, indexMap, tuples2)
			})

			pm := prollyMap.(Map)
			t.Run("tuple exists in map", func(t *testing.T) {
				testHas(t, pm, tuples)
			})

			ctx := context.Background()
			t.Run("walk addresses smoke test", func(t *testing.T) {
				err := pm.WalkAddresses(ctx, func(_ context.Context, addr hash.Hash) error {
					assert.True(t, addr != hash.Hash{})
					return nil
				})
				assert.NoError(t, err)
			})
			t.Run("walk nodes smoke test", func(t *testing.T) {
				err := pm.WalkNodes(ctx, func(_ context.Context, nd tree.Node) error {
					assert.True(t, nd.Count() > 1)
					return nil
				})
				assert.NoError(t, err)
			})
		})
	}
}

func TestNewEmptyNode(t *testing.T) {
	empty := newEmptyMapNode(sharedPool)
	assert.Equal(t, 0, empty.Level())
	assert.Equal(t, 0, empty.Count())
	assert.Equal(t, 0, empty.TreeCount())
	assert.Equal(t, 72, empty.Size())
	assert.True(t, empty.IsLeaf())
}

// credit: https://github.com/tailscale/tailscale/commit/88586ec4a43542b758d6f4e15990573970fb4e8a
func TestMapGetAllocs(t *testing.T) {
	ctx := context.Background()
	m, tuples := makeProllyMap(t, 100_000)

	// assert no allocs for Map.Get()
	avg := testing.AllocsPerRun(100, func() {
		k := tuples[testRand.Intn(len(tuples))][0]
		_ = m.Get(ctx, k, func(key, val val.Tuple) (err error) {
			return
		})
	})
	assert.Equal(t, 0.0, avg)
}

func makeProllyMap(t *testing.T, count int) (testMap, [][2]val.Tuple) {
	kd := val.NewTupleDescriptor(
		val.Type{Enc: val.Uint32Enc, Nullable: false},
	)
	vd := val.NewTupleDescriptor(
		val.Type{Enc: val.Uint32Enc, Nullable: true},
		val.Type{Enc: val.Uint32Enc, Nullable: true},
		val.Type{Enc: val.Uint32Enc, Nullable: true},
	)

	tuples := tree.RandomTuplePairs(count, kd, vd)
	om := prollyMapFromTuples(t, kd, vd, tuples)

	return om, tuples
}

func makeProllySecondaryIndex(t *testing.T, count int) (testMap, [][2]val.Tuple) {
	kd := val.NewTupleDescriptor(
		val.Type{Enc: val.Uint32Enc, Nullable: true},
		val.Type{Enc: val.Uint32Enc, Nullable: false},
	)
	vd := val.NewTupleDescriptor()

	tuples := tree.RandomCompositeTuplePairs(count, kd, vd)
	om := prollyMapFromTuples(t, kd, vd, tuples)

	return om, tuples
}

func prollyMapFromTuples(t *testing.T, kd, vd val.TupleDesc, tuples [][2]val.Tuple) testMap {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()

	serializer := message.ProllyMapSerializer{Pool: ns.Pool()}
	chunker, err := tree.NewEmptyChunker(ctx, ns, serializer)
	require.NoError(t, err)

	for _, pair := range tuples {
		err := chunker.AddPair(ctx, tree.Item(pair[0]), tree.Item(pair[1]))
		require.NoError(t, err)
	}
	root, err := chunker.Done(ctx)
	require.NoError(t, err)

	return NewMap(root, ns, kd, vd)
}

func testGet(t *testing.T, om testMap, tuples [][2]val.Tuple) {
	ctx := context.Background()

	// test get
	for _, kv := range tuples {
		err := om.Get(ctx, kv[0], func(key, val val.Tuple) (err error) {
			assert.NotNil(t, kv[0])
			expKey, expVal := kv[0], kv[1]
			assert.Equal(t, key, expKey)
			assert.Equal(t, val, expVal)
			return
		})
		require.NoError(t, err)
	}

	desc := keyDescFromMap(om)

	// test point lookup
	for _, kv := range tuples {
		rng := pointRangeFromTuple(kv[0], desc)
		require.True(t, rng.isPointLookup(desc))

		iter, err := om.IterRange(ctx, rng)
		require.NoError(t, err)

		k, v, err := iter.Next(ctx)
		require.NoError(t, err)
		assert.Equal(t, kv[0], k)
		assert.Equal(t, kv[1], v)

		k, v, err = iter.Next(ctx)
		assert.Error(t, err, io.EOF)
		assert.Nil(t, k)
		assert.Nil(t, v)
	}
}

func testHas(t *testing.T, om Map, tuples [][2]val.Tuple) {
	ctx := context.Background()
	for _, kv := range tuples {
		ok, err := om.Has(ctx, kv[0])
		assert.True(t, ok)
		require.NoError(t, err)
	}
}

func testIterAll(t *testing.T, om testMap, tuples [][2]val.Tuple) {
	ctx := context.Background()
	iter, err := om.IterAll(ctx)
	require.NoError(t, err)

	actual := make([][2]val.Tuple, len(tuples)*2)

	idx := 0
	for {
		key, value, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		actual[idx][0], actual[idx][1] = key, value
		idx++
	}
	actual = actual[:idx]

	assert.Equal(t, len(tuples), idx)
	for i, kv := range actual {
		require.True(t, i < len(tuples))
		assert.Equal(t, tuples[i][0], kv[0])
		assert.Equal(t, tuples[i][1], kv[1])
	}
}

func pointRangeFromTuple(tup val.Tuple, desc val.TupleDesc) Range {
	start := make([]RangeCut, len(desc.Types))
	stop := make([]RangeCut, len(desc.Types))
	for i := range start {
		start[i].Value = tup.GetField(i)
		start[i].Inclusive = true
	}
	copy(stop, start)

	return Range{
		Start: start,
		Stop:  stop,
		Desc:  desc,
	}
}
