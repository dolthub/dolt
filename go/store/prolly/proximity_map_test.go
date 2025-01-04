// Copyright 2024 Dolthub, Inc.
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
	"os"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function/vector"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

func newJsonValue(t *testing.T, v interface{}) sql.JSONWrapper {
	doc, _, err := types.JSON.Convert(v)
	require.NoError(t, err)
	return doc.(sql.JSONWrapper)
}

// newJsonDocument creates a JSON value from a provided value.
func newJsonDocument(t *testing.T, ctx context.Context, ns tree.NodeStore, v interface{}) hash.Hash {
	doc := newJsonValue(t, v)
	root, err := tree.SerializeJsonToAddr(ctx, ns, doc)
	require.NoError(t, err)
	return root.HashOf()
}

var testKeyDesc = val.NewTupleDescriptor(
	val.Type{Enc: val.JSONAddrEnc, Nullable: true},
)

var testValDesc = val.NewTupleDescriptor(
	val.Type{Enc: val.Int64Enc, Nullable: true},
)

func createProximityMap(t *testing.T, ctx context.Context, ns tree.NodeStore, keyDesc val.TupleDesc, keys [][]interface{}, valueDesc val.TupleDesc, values [][]interface{}, logChunkSize uint8) (ProximityMap, [][]byte, [][]byte) {
	bp := pool.NewBuffPool()

	count := len(keys)
	require.Equal(t, count, len(values))

	distanceType := vector.DistanceL2Squared{}

	builder, err := NewProximityMapBuilder(ctx, ns, distanceType, keyDesc, valueDesc, logChunkSize)
	require.NoError(t, err)

	keyBytes := make([][]byte, count)
	valueBytes := make([][]byte, count)
	keyBuilder := val.NewTupleBuilder(keyDesc)
	valueBuilder := val.NewTupleBuilder(valueDesc)
	for i, key := range keys {
		for j, keyColumn := range key {
			err = tree.PutField(ctx, ns, keyBuilder, j, keyColumn)
			require.NoError(t, err)
		}

		nextKey := keyBuilder.Build(bp)
		keyBytes[i] = nextKey

		for j, valueColumn := range values[i] {
			err = tree.PutField(ctx, ns, valueBuilder, j, valueColumn)
			require.NoError(t, err)
		}

		nextValue := valueBuilder.Build(bp)
		valueBytes[i] = nextValue

		err = builder.Insert(ctx, nextKey, nextValue)
		require.NoError(t, err)
	}

	m, err := builder.Flush(ctx)
	require.NoError(t, err)

	mapCount, err := m.Count()
	require.NoError(t, err)
	require.Equal(t, count, mapCount)

	return m, keyBytes, valueBytes
}

func TestEmptyProximityMap(t *testing.T) {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()
	createProximityMap(t, ctx, ns, testKeyDesc, nil, testValDesc, nil, 10)
}

func TestSingleEntryProximityMap(t *testing.T) {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()
	m, keys, values := createProximityMap(t, ctx, ns, testKeyDesc, [][]interface{}{{"[1.0]"}}, testValDesc, [][]interface{}{{int64(1)}}, 10)
	matches := 0
	err := m.Get(ctx, keys[0], func(foundKey val.Tuple, foundValue val.Tuple) error {
		require.Equal(t, val.Tuple(keys[0]), foundKey)
		require.Equal(t, val.Tuple(values[0]), foundValue)
		matches++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, matches, 1)
}

func TestDoubleEntryProximityMapGetExact(t *testing.T) {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()
	m, keys, values := createProximityMap(t, ctx, ns, testKeyDesc, [][]interface{}{{"[0.0, 6.0]"}, {"[3.0, 4.0]"}}, testValDesc, [][]interface{}{{int64(1)}, {int64(2)}}, 10)
	matches := 0
	for i, key := range keys {
		err := m.Get(ctx, key, func(foundKey val.Tuple, foundValue val.Tuple) error {
			require.Equal(t, val.Tuple(key), foundKey)
			require.Equal(t, val.Tuple(values[i]), foundValue)
			matches++
			return nil
		})
		require.NoError(t, err)
	}
	require.Equal(t, matches, len(keys))
}

func TestDoubleEntryProximityMapGetClosest(t *testing.T) {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()
	m, keys, values := createProximityMap(t, ctx, ns, testKeyDesc, [][]interface{}{{"[0.0, 6.0]"}, {"[3.0, 4.0]"}}, testValDesc, [][]interface{}{{int64(1)}, {int64(2)}}, 10)
	matches := 0

	mapIter, err := m.GetClosest(ctx, newJsonValue(t, "[0.0, 0.0]"), 1)
	require.NoError(t, err)
	for {
		k, v, err := mapIter.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.Equal(t, val.Tuple(keys[1]), k)
		require.Equal(t, val.Tuple(values[1]), v)
		// require.InDelta(t, distance, 25.0, 0.1)
		matches++
	}

	require.NoError(t, err)
	require.Equal(t, matches, 1)
}

func TestProximityMapGetManyClosest(t *testing.T) {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()
	vectors := [][]interface{}{
		{"[0.0, 0.0]"},
		{"[0.0, 10.0]"},
		{"[10.0, 10.0]"},
		{"[10.0, 0.0]"},
	}
	queryVector := "[3.0, 1.0]"
	sortOrder := []int{0, 3, 1, 2} // indexes in sorted order: [0.0, 0.0], [10.0, 0.0], [0.0, 10.0], [10.0, 10.0]
	// distances := []float64{10.0, 50.0, 90.0, 130.0}
	m, keys, values := createProximityMap(t, ctx, ns, testKeyDesc, vectors, testValDesc, [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}}, 2)

	for limit := 0; limit <= 4; limit++ {
		t.Run(fmt.Sprintf("limit %d", limit), func(t *testing.T) {
			matches := 0

			mapIter, err := m.GetClosest(ctx, newJsonValue(t, queryVector), limit)
			require.NoError(t, err)
			for {
				k, v, err := mapIter.Next(ctx)
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				require.Equal(t, val.Tuple(keys[sortOrder[matches]]), k)
				require.Equal(t, val.Tuple(values[sortOrder[matches]]), v)
				// require.InDelta(t, distance, distances[matches], 0.1)
				matches++
			}
			require.NoError(t, err)
			require.Equal(t, limit, matches)
		})
	}
}

func getInt64(t *testing.T, tuple val.Tuple, idx int) int64 {
	res, ok := testValDesc.GetInt64(0, tuple)
	require.True(t, ok)
	return res
}

func TestProximityMapWithOverflowNode(t *testing.T) {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()

	// Create an index with enough rows that it can't fit in a single physical chunk
	vectors := make([][]interface{}, 0, 4000)
	pks := make([][]interface{}, 0, 4000)

	for i := int64(0); i < 4000; i++ {
		vectors = append(vectors, []interface{}{fmt.Sprintf("[%d]", i)})
		pks = append(pks, []interface{}{i})
	}

	// Set logChunkSize to a high enough value that everything goes in a single chunk
	m, _, _ := createProximityMap(t, ctx, ns, testKeyDesc, vectors, testValDesc, pks, 16)

	count, err := m.Count()
	require.NoError(t, err)
	require.Equal(t, 4000, count)
}

func TestMultilevelProximityMap(t *testing.T) {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()
	keyStrings := [][]interface{}{
		{"[0.0, 1.0]"},
		{"[3.0, 4.0]"},
		{"[5.0, 6.0]"},
		{"[7.0, 8.0]"},
	}
	valueStrings := [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}}
	m, keys, values := createProximityMap(t, ctx, ns, testKeyDesc, keyStrings, testValDesc, valueStrings, 1)
	matches := 0
	for i, key := range keys {
		err := m.Get(ctx, key, func(foundKey val.Tuple, foundValue val.Tuple) error {
			require.Equal(t, val.Tuple(key), foundKey)
			require.Equal(t, val.Tuple(values[i]), foundValue)
			matches++
			return nil
		})
		require.NoError(t, err)
	}
	require.Equal(t, matches, len(keys))
}

func TestLargerMultilevelProximityMap(t *testing.T) {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()
	keyStrings := [][]interface{}{
		{"[0.0, 1.0]"},
		{"[3.0, 4.0]"},
		{"[5.0, 6.0]"},
		{"[7.0, 8.0]"},
		{"[9.0, 10.0]"},
		{"[11.0, 12.0]"},
		{"[13.0, 14.0]"},
		{"[15.0, 16.0]"},
	}
	valueStrings := [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}, {int64(5)}, {int64(6)}, {int64(7)}, {int64(8)}}
	m, keys, values := createProximityMap(t, ctx, ns, testKeyDesc, keyStrings, testValDesc, valueStrings, 1)
	matches := 0
	for i, key := range keys {
		err := m.Get(ctx, key, func(foundKey val.Tuple, foundValue val.Tuple) error {
			require.Equal(t, val.Tuple(key), foundKey)
			require.Equal(t, val.Tuple(values[i]), foundValue)
			matches++
			return nil
		})
		require.NoError(t, err)
	}
	require.Equal(t, matches, len(keys))
}

func TestInsertOrderIndependence(t *testing.T) {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()
	keyStrings1 := [][]interface{}{
		{"[0.0, 1.0]"},
		{"[3.0, 4.0]"},
		{"[5.0, 6.0]"},
		{"[7.0, 8.0]"},
	}
	valueStrings1 := [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}}
	keyStrings2 := [][]interface{}{
		{"[7.0, 8.0]"},
		{"[5.0, 6.0]"},
		{"[3.0, 4.0]"},
		{"[0.0, 1.0]"},
	}
	valueStrings2 := [][]interface{}{{int64(4)}, {int64(3)}, {int64(2)}, {int64(1)}}
	m1, _, _ := createProximityMap(t, ctx, ns, testKeyDesc, keyStrings1, testValDesc, valueStrings1, 1)
	m2, _, _ := createProximityMap(t, ctx, ns, testKeyDesc, keyStrings2, testValDesc, valueStrings2, 1)

	if !assert.Equal(t, m1.tuples.Root.HashOf(), m2.tuples.Root.HashOf(), "trees have different hashes") {
		require.NoError(t, tree.OutputProllyNodeBytes(os.Stdout, m1.tuples.Root))
		require.NoError(t, tree.OutputProllyNodeBytes(os.Stdout, m2.tuples.Root))
	}
}

func TestIncrementalInserts(t *testing.T) {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()
	vectors1 := [][]interface{}{
		{"[0.0, 1.0]"},
		{"[3.0, 4.0]"},
		{"[5.0, 6.0]"},
		{"[7.0, 8.0]"},
	}
	pks1 := [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}}

	m1, _, _ := createProximityMap(t, ctx, ns, testKeyDesc, vectors1, testValDesc, pks1, 1)

	mutableMap := newProximityMutableMap(m1)

	vectors2 := []interface{}{
		"[9.0, 10.0]",
		"[11.0, 12.0]",
		"[13.0, 14.0]",
		"[15.0, 16.0]",
	}
	pks2 := []int64{5, 6, 7, 8}

	bp := pool.NewBuffPool()

	keyBuilder := val.NewTupleBuilder(testKeyDesc)
	valueBuilder := val.NewTupleBuilder(testValDesc)
	for i, keyString := range vectors2 {
		keyBuilder.PutJSONAddr(0, newJsonDocument(t, ctx, ns, keyString))
		nextKey := keyBuilder.Build(bp)

		valueBuilder.PutInt64(0, pks2[i])
		nextValue := valueBuilder.Build(bp)

		err := mutableMap.Put(ctx, nextKey, nextValue)
		require.NoError(t, err)
	}

	// Check that map looks how we expect.
	newMap, err := ProximityFlusher{}.Map(ctx, mutableMap)
	require.NoError(t, err)

	newCount, err := newMap.Count()
	require.NoError(t, err)

	require.Equal(t, 8, newCount)
}

func TestIncrementalUpdates(t *testing.T) {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()
	vectors1 := [][]interface{}{
		{"[0.0, 1.0]"},
		{"[3.0, 4.0]"},
		{"[5.0, 6.0]"},
		{"[7.0, 8.0]"},
	}
	pks1 := [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}}

	m1, _, _ := createProximityMap(t, ctx, ns, testKeyDesc, vectors1, testValDesc, pks1, 1)

	mutableMap := newProximityMutableMap(m1)

	bp := pool.NewBuffPool()

	keyBuilder := val.NewTupleBuilder(testKeyDesc)
	valueBuilder := val.NewTupleBuilder(testValDesc)

	// update leaf node
	{
		keyBuilder.PutJSONAddr(0, newJsonDocument(t, ctx, ns, "[0.0, 1.0]"))
		nextKey := keyBuilder.Build(bp)

		valueBuilder.PutInt64(0, 5)
		nextValue := valueBuilder.Build(bp)

		err := mutableMap.Put(ctx, nextKey, nextValue)
		require.NoError(t, err)

		newMap, err := ProximityFlusher{}.Map(ctx, mutableMap)
		require.NoError(t, err)

		newCount, err := newMap.Count()
		require.NoError(t, err)

		require.Equal(t, 4, newCount)
	}

	// update root node
	{
		keyBuilder.PutJSONAddr(0, newJsonDocument(t, ctx, ns, "[5.0, 6.0]"))
		nextKey := keyBuilder.Build(bp)

		valueBuilder.PutInt64(0, 6)
		nextValue := valueBuilder.Build(bp)

		err := mutableMap.Put(ctx, nextKey, nextValue)
		require.NoError(t, err)

		newMap, err := ProximityFlusher{}.Map(ctx, mutableMap)
		require.NoError(t, err)

		newCount, err := newMap.Count()
		require.NoError(t, err)

		require.Equal(t, 4, newCount)
	}
}

func TestIncrementalDeletes(t *testing.T) {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()
	vectors1 := [][]interface{}{
		{"[0.0, 1.0]"},
		{"[3.0, 4.0]"},
		{"[5.0, 6.0]"},
		{"[7.0, 8.0]"},
	}
	pks1 := [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}}

	logChunkSize := uint8(1)
	m1, _, _ := createProximityMap(t, ctx, ns, testKeyDesc, vectors1, testValDesc, pks1, logChunkSize)

	mutableMap := newProximityMutableMap(m1)

	bp := pool.NewBuffPool()

	keyBuilder := val.NewTupleBuilder(testKeyDesc)

	// update leaf node
	{
		keyBuilder.PutJSONAddr(0, newJsonDocument(t, ctx, ns, "[0.0, 1.0]"))
		nextKey := keyBuilder.Build(bp)

		err := mutableMap.Put(ctx, nextKey, nil)
		require.NoError(t, err)

		newMap, err := ProximityFlusher{logChunkSize: logChunkSize}.Map(ctx, mutableMap)
		require.NoError(t, err)

		newCount, err := newMap.Count()
		require.NoError(t, err)

		require.Equal(t, 3, newCount)
	}

	// update root node
	{
		keyBuilder.PutJSONAddr(0, newJsonDocument(t, ctx, ns, "[5.0, 6.0]"))
		nextKey := keyBuilder.Build(bp)

		err := mutableMap.Put(ctx, nextKey, nil)
		require.NoError(t, err)

		newMap, err := ProximityFlusher{}.Map(ctx, mutableMap)
		require.NoError(t, err)

		newCount, err := newMap.Count()
		require.NoError(t, err)

		require.Equal(t, 2, newCount)
	}
}

// As part of the algorithm for building proximity maps, we store the map keys as bytestrings in a temporary table.
// The sorting order of a key is not always the same as the lexographic ordering of these bytestrings.
// This test makes sure that even when this is not the case we still generate correct output.
func TestNonlexographicKey(t *testing.T) {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()
	vectors1 := [][]interface{}{
		{"[0.0, 0.0]", int64(4 + 0*256)},
		{"[0.0, 0.0]", int64(3 + 1*256)},
		{"[0.0, 0.0]", int64(2 + 2*256)},
		{"[0.0, 0.0]", int64(1 + 3*256)},
		{"[0.0, 0.0]", int64(0 + 4*256)},
	}
	pks1 := [][]interface{}{{}, {}, {}, {}, {}}

	keyDesc := val.NewTupleDescriptor(
		val.Type{Enc: val.JSONAddrEnc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
	)

	valDesc := val.NewTupleDescriptor()

	_, _, _ = createProximityMap(t, ctx, ns, keyDesc, vectors1, valDesc, pks1, 1)
}
