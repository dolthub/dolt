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
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
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

func newJsonValue(t *testing.T, ctx context.Context, v interface{}) sql.JSONWrapper {
	doc, _, err := types.JSON.Convert(ctx, v)
	require.NoError(t, err)
	return doc.(sql.JSONWrapper)
}

// newJsonDocument creates a JSON value from a provided value.
func newJsonDocument(t *testing.T, ctx context.Context, ns tree.NodeStore, v interface{}) hash.Hash {
	doc := newJsonValue(t, ctx, v)
	root, err := tree.SerializeJsonToAddr(ctx, ns, doc)
	require.NoError(t, err)
	return root.HashOf()
}

var jsonTestKeyDesc = val.NewTupleDescriptor(
	val.Type{Enc: val.JSONAddrEnc, Nullable: true},
)

var vectorTestKeyDesc = val.NewTupleDescriptor(
	val.Type{Enc: val.BytesAdaptiveEnc, Nullable: true},
)

var testValDesc = val.NewTupleDescriptor(
	val.Type{Enc: val.Int64Enc, Nullable: true},
)

func buildTuple(t *testing.T, ctx context.Context, ns tree.NodeStore, pool pool.BuffPool, desc *val.TupleDesc, row []interface{}) val.Tuple {
	builder := val.NewTupleBuilder(desc, ns)
	for i, column := range row {
		err := tree.PutField(ctx, ns, builder, i, column)
		require.NoError(t, err)
	}
	tup, err := builder.Build(pool)
	require.NoError(t, err)
	return tup
}

func buildTuples(t *testing.T, ctx context.Context, ns tree.NodeStore, pool pool.BuffPool, desc *val.TupleDesc, rows [][]interface{}) [][]byte {
	result := make([][]byte, len(rows))
	for i, row := range rows {
		result[i] = buildTuple(t, ctx, ns, pool, desc, row)
	}
	return result
}

func createAndValidateProximityMap(t *testing.T, ctx context.Context, ns tree.NodeStore, keyDesc *val.TupleDesc, keyBytes [][]byte, valueDesc *val.TupleDesc, valueBytes [][]byte, logChunkSize uint8) ProximityMap {
	m := createProximityMap(t, ctx, ns, keyDesc, keyBytes, valueDesc, valueBytes, logChunkSize)
	validateProximityMapSkipHistoryIndependenceCheck(t, ctx, ns, &m, keyDesc, valueDesc, keyBytes, valueBytes)
	return m
}

func createProximityMap(t *testing.T, ctx context.Context, ns tree.NodeStore, keyDesc *val.TupleDesc, keyBytes [][]byte, valueDesc *val.TupleDesc, valueBytes [][]byte, logChunkSize uint8) ProximityMap {
	count := len(keyBytes)
	require.Equal(t, count, len(valueBytes))

	distanceType := vector.DistanceL2Squared{}

	builder, err := NewProximityMapBuilder(ctx, ns, distanceType, keyDesc, valueDesc, logChunkSize)
	require.NoError(t, err)

	for i, key := range keyBytes {
		value := valueBytes[i]
		err = builder.Insert(ctx, key, value)
		require.NoError(t, err)
	}

	m, err := builder.Flush(ctx)
	require.NoError(t, err)

	mapCount, err := m.Count()
	require.NoError(t, err)
	require.Equal(t, count, mapCount)

	return m
}

func validateProximityMap(t *testing.T, ctx context.Context, ns tree.NodeStore, m *ProximityMap, keyDesc, valDesc *val.TupleDesc, keys, values [][]byte, logChunkSize uint8) {
	validateProximityMapSkipHistoryIndependenceCheck(t, ctx, ns, m, keyDesc, valDesc, keys, values)
	validateHistoryIndependence(t, ctx, ns, m, keyDesc, keys, valDesc, values, logChunkSize)
}

func validateProximityMapSkipHistoryIndependenceCheck(t *testing.T, ctx context.Context, ns tree.NodeStore, m *ProximityMap, keyDesc, valDesc *val.TupleDesc, keys, values [][]byte) {
	expectedSize := len(keys)
	actualSize, err := m.Count()
	require.NoError(t, err)
	require.Equal(t, expectedSize, actualSize)
	// Check that every key and value appears in the map exactly once.
	matches := 0
	for i := 0; i < actualSize; i++ {
		err = m.Get(ctx, keys[i], func(foundKey val.Tuple, foundValue val.Tuple) error {
			require.Equal(t, val.Tuple(keys[i]), foundKey)
			require.Equal(t, val.Tuple(values[i]), foundValue)
			matches++
			return nil
		})
		require.NoError(t, err)
	}
	require.Equal(t, expectedSize, matches)

	// Check that the invariant holds: each vector is closer to its parent than any of its uncles.
	err = tree.WalkNodes(ctx, m.tuples.Root, ns, func(ctx context.Context, nd *tree.Node) error {
		validateProximityMapNode(t, ctx, ns, nd, vector.DistanceL2Squared{}, keyDesc, valDesc)
		return nil
	})
	require.NoError(t, err)

	// Finally, build a new map with the supplied keys and values and confirm that it has the same root hash.
}

func validateHistoryIndependence(t *testing.T, ctx context.Context, ns tree.NodeStore, m *ProximityMap, keyDesc *val.TupleDesc, keyBytes [][]byte, valueDesc *val.TupleDesc, valueBytes [][]byte, logChunkSize uint8) {
	// Build a new map with the supplied keys and values and confirm that it has the same root hash.
	other := createProximityMap(t, ctx, ns, keyDesc, keyBytes, valueDesc, valueBytes, logChunkSize)
	require.Equal(t, other.HashOf(), m.HashOf())
}

func vectorFromKey(t *testing.T, keyDesc *val.TupleDesc, key []byte) []float32 {
	encodedVector := keyDesc.GetField(0, key)
	return decodeVector(t, keyDesc, encodedVector)
}

func validateProximityMapNode(t *testing.T, ctx context.Context, ns tree.NodeStore, nd tree.Node, distanceType vector.DistanceType, keyDesc *val.TupleDesc, desc *val.TupleDesc) {
	// For each node, the node's grandchildren should be closer to their parent than the other children.
	if nd.Level() == 0 {
		// Leaf node
		return
	}
	if nd.Count() <= 1 {
		// A node with only one child is trivially valid.
		return
	}
	// Get the vector in each key
	vectors := make([][]float32, nd.Count())
	for vectorIdx := 0; vectorIdx < nd.Count(); vectorIdx++ {
		vectorKey := nd.GetKey(vectorIdx)
		vectors[vectorIdx] = vectorFromKey(t, keyDesc, vectorKey)
	}
	for childIdx := 0; childIdx < nd.Count(); childIdx++ {
		// Get the child node
		childHash := hash.New(nd.GetValue(childIdx))
		childNode, err := ns.Read(ctx, childHash)
		require.NoError(t, err)
		for childKeyIdx := 0; childKeyIdx < childNode.Count(); childKeyIdx++ {
			childVectorKey := childNode.GetKey(childKeyIdx)
			childVector := vectorFromKey(t, keyDesc, childVectorKey)
			minDistance := math.MaxFloat64
			closestKeyIdx := -1
			for otherChildIdx := 0; otherChildIdx < nd.Count(); otherChildIdx++ {
				distance, err := distanceType.Eval(childVector, vectors[otherChildIdx])
				require.NoError(t, err)
				if distance < minDistance {
					minDistance = distance
					closestKeyIdx = otherChildIdx
				}
			}
			require.Equal(t, closestKeyIdx, childIdx)
		}
	}
}

func encodeVector(t *testing.T, keyDesc *val.TupleDesc, vec ...float32) []byte {
	enc := keyDesc.Types[0].Enc
	switch enc {
	case val.JSONAddrEnc:
		res, err := json.Marshal(vec)
		require.NoError(t, err)
		return res
	case val.BytesAdaptiveEnc:
		return sql.EncodeVector(vec)
	default:
		panic("unexpected encoding")
	}
}

func decodeVector(t *testing.T, keyDesc *val.TupleDesc, valBytes []byte) []float32 {
	ctx := context.Background()
	enc := keyDesc.Types[0].Enc
	var vectorValue interface{}
	var err error
	switch enc {
	case val.JSONAddrEnc:
		vectorValue, err = tree.NewJSONDoc(hash.New(valBytes), ns).ToIndexedJSONDocument(ctx)
		require.NoError(t, err)
	case val.BytesAdaptiveEnc:
		var ok bool
		vectorValue, ok, err = val.GetBytesAdaptiveValue(ctx, ns, valBytes)
		require.NoError(t, err)
		require.True(t, ok)
	default:
		panic("unexpected encoding")
	}
	res, err := sql.ConvertToVector(ctx, vectorValue)
	require.NoError(t, err)
	return res
}

func putVector(t *testing.T, keyBuilder *val.TupleBuilder, v []byte) {
	ctx := context.Background()
	enc := keyBuilder.Desc.Types[0].Enc
	switch enc {
	case val.JSONAddrEnc:
		keyBuilder.PutJSONAddr(0, newJsonDocument(t, ctx, ns, v))
	case val.BytesAdaptiveEnc:
		err := keyBuilder.PutAdaptiveBytesFromInline(ctx, 0, v)
		require.NoError(t, err)
	default:
		panic("unexpected encoding")
	}
}

func TestProximityMap(t *testing.T) {
	t.Run("JSON vector encoding", func(t *testing.T) {
		testProximityMapWithEncoding(t, jsonTestKeyDesc)
	})
	t.Run("VECTOR vector encoding", func(t *testing.T) {
		testProximityMapWithEncoding(t, vectorTestKeyDesc)
	})
}

func testProximityMapWithEncoding(t *testing.T, keyDesc *val.TupleDesc) {
	testEmptyProximityMap(t, keyDesc)
	testSingleEntryProximityMap(t, keyDesc)
	testDoubleEntryProximityMapGetExact(t, keyDesc)
	testDoubleEntryProximityMapGetClosest(t, keyDesc)
	testProximityMapGetManyClosest(t, keyDesc)
	testProximityMapWithOverflowNode(t, keyDesc)
	testMultilevelProximityMap(t, keyDesc)
	testLargerMultilevelProximityMap(t, keyDesc)
	testInsertOrderIndependence(t, keyDesc)
	testIncrementalInserts(t, keyDesc)
	testIncrementalUpdates(t, keyDesc)
	testIncrementalDeletes(t, keyDesc)
	testNonlexographicKey(t, keyDesc)
	testManyDimensions(t, keyDesc)
}

func testEmptyProximityMap(t *testing.T, keyDesc *val.TupleDesc) {
	t.Run("empty map", func(t *testing.T) {
		ctx := context.Background()
		ns := tree.NewTestNodeStore()
		createAndValidateProximityMap(t, ctx, ns, keyDesc, nil, testValDesc, nil, 10)
	})
}

func testSingleEntryProximityMap(t *testing.T, keyDesc *val.TupleDesc) {
	t.Run("single entry map", func(t *testing.T) {
		ctx := context.Background()
		ns := tree.NewTestNodeStore()
		pb := pool.NewBuffPool()
		keys := buildTuples(t, ctx, ns, pb, keyDesc, [][]interface{}{{encodeVector(t, keyDesc, 1.0)}})
		values := buildTuples(t, ctx, ns, pb, testValDesc, [][]interface{}{{int64(1)}})
		createAndValidateProximityMap(t, ctx, ns, keyDesc, keys, testValDesc, values, 10)
	})
}

func testDoubleEntryProximityMapGetExact(t *testing.T, keyDesc *val.TupleDesc) {
	t.Run("double entry map get exact", func(t *testing.T) {
		ctx := context.Background()
		ns := tree.NewTestNodeStore()
		pb := pool.NewBuffPool()

		keyRows := [][]interface{}{{encodeVector(t, keyDesc, 0.0, 6.0)}, {encodeVector(t, keyDesc, 3.0, 4.0)}}
		keys := buildTuples(t, ctx, ns, pb, keyDesc, keyRows)

		valueRows := [][]interface{}{{int64(1)}, {int64(2)}}
		values := buildTuples(t, ctx, ns, pb, testValDesc, valueRows)

		m := createAndValidateProximityMap(t, ctx, ns, keyDesc, keys, testValDesc, values, 10)
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
	})
}

func testDoubleEntryProximityMapGetClosest(t *testing.T, keyDesc *val.TupleDesc) {
	t.Run("double entry map get closest", func(t *testing.T) {
		ctx := context.Background()
		ns := tree.NewTestNodeStore()
		pb := pool.NewBuffPool()

		keyRows := [][]interface{}{
			{encodeVector(t, keyDesc, 0.0, 6.0)},
			{encodeVector(t, keyDesc, 3.0, 4.0)},
		}
		keys := buildTuples(t, ctx, ns, pb, keyDesc, keyRows)

		valueRows := [][]interface{}{{int64(1)}, {int64(2)}}
		values := buildTuples(t, ctx, ns, pb, testValDesc, valueRows)

		m := createAndValidateProximityMap(t, ctx, ns, keyDesc, keys, testValDesc, values, 10)

		matches := 0

		mapIter, err := m.GetClosest(ctx, sql.EncodeVector([]float32{0.0, 0.0}), 1)
		require.NoError(t, err)
		for {
			k, v, err := mapIter.Next(ctx)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			require.Equal(t, val.Tuple(keys[1]), k)
			require.Equal(t, val.Tuple(values[1]), v)
			matches++
		}

		require.NoError(t, err)
		require.Equal(t, matches, 1)
	})
}

func testProximityMapGetManyClosest(t *testing.T, keyDesc *val.TupleDesc) {
	t.Run("get many closest", func(t *testing.T) {
		ctx := context.Background()
		ns := tree.NewTestNodeStore()
		pb := pool.NewBuffPool()

		keyRows := [][]interface{}{
			{encodeVector(t, keyDesc, 0.0, 0.0)},
			{encodeVector(t, keyDesc, 0.0, 10.0)},
			{encodeVector(t, keyDesc, 10.0, 10.0)},
			{encodeVector(t, keyDesc, 10.0, 0.0)},
		}
		keys := buildTuples(t, ctx, ns, pb, keyDesc, keyRows)

		valueRows := [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}}
		values := buildTuples(t, ctx, ns, pb, testValDesc, valueRows)

		m := createAndValidateProximityMap(t, ctx, ns, keyDesc, keys, testValDesc, values, 10)

		queryVector := sql.EncodeVector([]float32{3.0, 1.0})
		sortOrder := []int{0, 3, 1, 2} // indexes in sorted order: [0.0, 0.0], [10.0, 0.0], [0.0, 10.0], [10.0, 10.0]

		for limit := 0; limit <= 4; limit++ {
			t.Run(fmt.Sprintf("limit %d", limit), func(t *testing.T) {
				matches := 0

				mapIter, err := m.GetClosest(ctx, queryVector, limit)
				require.NoError(t, err)
				for {
					k, v, err := mapIter.Next(ctx)
					if err == io.EOF {
						break
					}
					require.NoError(t, err)
					require.Equal(t, val.Tuple(keys[sortOrder[matches]]), k)
					require.Equal(t, val.Tuple(values[sortOrder[matches]]), v)
					matches++
				}
				require.NoError(t, err)
				require.Equal(t, limit, matches)
			})
		}
	})
}

func testProximityMapWithOverflowNode(t *testing.T, keyDesc *val.TupleDesc) {
	t.Run("node too large to fit in a single physical chunk", func(t *testing.T) {
		ctx := context.Background()
		ns := tree.NewTestNodeStore()
		pb := pool.NewBuffPool()

		// Create an index with enough rows that it can't fit in a single physical chunk
		keyRows := make([][]interface{}, 0, 4000)
		valueRows := make([][]interface{}, 0, 4000)

		for i := int64(0); i < 4000; i++ {
			keyRows = append(keyRows, []interface{}{encodeVector(t, keyDesc, float32(i))})
			valueRows = append(valueRows, []interface{}{i})
		}

		keys := buildTuples(t, ctx, ns, pb, keyDesc, keyRows)
		values := buildTuples(t, ctx, ns, pb, testValDesc, valueRows)

		// Set logChunkSize to a high enough value that everything goes in a single chunk
		m := createAndValidateProximityMap(t, ctx, ns, keyDesc, keys, testValDesc, values, 16)

		count, err := m.Count()
		require.NoError(t, err)
		require.Equal(t, 4000, count)
	})
}

func testMultilevelProximityMap(t *testing.T, keyDesc *val.TupleDesc) {
	t.Run("map with multiple levels", func(t *testing.T) {
		ctx := context.Background()
		ns := tree.NewTestNodeStore()
		pb := pool.NewBuffPool()

		keyRows := [][]interface{}{
			{encodeVector(t, keyDesc, 0.0, 1.0)},
			{encodeVector(t, keyDesc, 3.0, 4.0)},
			{encodeVector(t, keyDesc, 5.0, 6.0)},
			{encodeVector(t, keyDesc, 7.0, 8.0)},
		}
		keys := buildTuples(t, ctx, ns, pb, keyDesc, keyRows)

		valueRows := [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}}
		values := buildTuples(t, ctx, ns, pb, testValDesc, valueRows)

		m := createAndValidateProximityMap(t, ctx, ns, keyDesc, keys, testValDesc, values, 1)
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
	})
}

func testLargerMultilevelProximityMap(t *testing.T, keyDesc *val.TupleDesc) {
	t.Run("larger map with multiple levels", func(t *testing.T) {
		ctx := context.Background()
		ns := tree.NewTestNodeStore()
		pb := pool.NewBuffPool()

		keyRows := [][]interface{}{
			{encodeVector(t, keyDesc, 0.0, 1.0)},
			{encodeVector(t, keyDesc, 3.0, 4.0)},
			{encodeVector(t, keyDesc, 5.0, 6.0)},
			{encodeVector(t, keyDesc, 7.0, 8.0)},
			{encodeVector(t, keyDesc, 9.0, 10.0)},
			{encodeVector(t, keyDesc, 11.0, 12.0)},
			{encodeVector(t, keyDesc, 13.0, 14.0)},
			{encodeVector(t, keyDesc, 15.0, 16.0)},
		}
		keys := buildTuples(t, ctx, ns, pb, keyDesc, keyRows)

		valueRows := [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}, {int64(5)}, {int64(6)}, {int64(7)}, {int64(8)}}
		values := buildTuples(t, ctx, ns, pb, testValDesc, valueRows)

		m := createAndValidateProximityMap(t, ctx, ns, keyDesc, keys, testValDesc, values, 1)
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
	})
}

func testInsertOrderIndependence(t *testing.T, keyDesc *val.TupleDesc) {
	t.Run("insert order independence", func(t *testing.T) {
		ctx := context.Background()
		ns := tree.NewTestNodeStore()
		pb := pool.NewBuffPool()

		keyRows1 := [][]interface{}{
			{encodeVector(t, keyDesc, 0.0, 1.0)},
			{encodeVector(t, keyDesc, 3.0, 4.0)},
			{encodeVector(t, keyDesc, 5.0, 6.0)},
			{encodeVector(t, keyDesc, 7.0, 8.0)},
		}
		keys1 := buildTuples(t, ctx, ns, pb, keyDesc, keyRows1)

		valueRows1 := [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}}
		values1 := buildTuples(t, ctx, ns, pb, testValDesc, valueRows1)

		keyRows2 := [][]interface{}{
			{encodeVector(t, keyDesc, 7.0, 8.0)},
			{encodeVector(t, keyDesc, 5.0, 6.0)},
			{encodeVector(t, keyDesc, 3.0, 4.0)},
			{encodeVector(t, keyDesc, 0.0, 1.0)},
		}
		keys2 := buildTuples(t, ctx, ns, pb, keyDesc, keyRows2)

		valueRows2 := [][]interface{}{{int64(4)}, {int64(3)}, {int64(2)}, {int64(1)}}
		values2 := buildTuples(t, ctx, ns, pb, testValDesc, valueRows2)

		m1 := createAndValidateProximityMap(t, ctx, ns, keyDesc, keys1, testValDesc, values1, 1)
		m2 := createAndValidateProximityMap(t, ctx, ns, keyDesc, keys2, testValDesc, values2, 1)

		if !assert.Equal(t, m1.tuples.Root.HashOf(), m2.tuples.Root.HashOf(), "trees have different hashes") {
			require.NoError(t, tree.OutputProllyNodeBytes(os.Stdout, m1.tuples.Root))
			require.NoError(t, tree.OutputProllyNodeBytes(os.Stdout, m2.tuples.Root))
		}
	})
}

func testIncrementalInserts(t *testing.T, keyDesc *val.TupleDesc) {
	t.Run("incremental inserts", func(t *testing.T) {
		ctx := context.Background()
		ns := tree.NewTestNodeStore()
		pb := pool.NewBuffPool()
		logChunkSize := uint8(1)
		distanceType := vector.DistanceL2Squared{}
		flusher := ProximityFlusher{logChunkSize: logChunkSize, distanceType: distanceType}
		keyRows1 := [][]interface{}{
			{encodeVector(t, keyDesc, 0.0, 1.0)},
			{encodeVector(t, keyDesc, 3.0, 4.0)},
			{encodeVector(t, keyDesc, 5.0, 6.0)},
			{encodeVector(t, keyDesc, 7.0, 8.0)},
		}
		keys1 := buildTuples(t, ctx, ns, pb, keyDesc, keyRows1)

		valueRows1 := [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}}
		values1 := buildTuples(t, ctx, ns, pb, testValDesc, valueRows1)

		m1 := createAndValidateProximityMap(t, ctx, ns, keyDesc, keys1, testValDesc, values1, logChunkSize)

		l1 := m1.tuples.Root.Level()
		_ = l1
		mutableMap := newProximityMutableMap(m1)

		keyRows2 := [][]interface{}{
			{encodeVector(t, keyDesc, 9.0, 10.0)},
			{encodeVector(t, keyDesc, 11.0, 12.0)},
			{encodeVector(t, keyDesc, 13.0, 14.0)},
			{encodeVector(t, keyDesc, 15.0, 16.0)},
		}
		keys2 := buildTuples(t, ctx, ns, pb, keyDesc, keyRows2)

		valueRows2 := [][]interface{}{{int64(5)}, {int64(6)}, {int64(7)}, {int64(8)}}
		values2 := buildTuples(t, ctx, ns, pb, testValDesc, valueRows2)

		for i, key := range keys2 {
			err := mutableMap.Put(ctx, key, values2[i])
			require.NoError(t, err)
		}

		// Check that map looks how we expect.
		newMap, err := flusher.Map(ctx, mutableMap)
		require.NoError(t, err)

		l2 := m1.tuples.Root.Level()
		_ = l2

		combinedKeyRows := [][]interface{}{
			{encodeVector(t, keyDesc, 0.0, 1.0)},
			{encodeVector(t, keyDesc, 3.0, 4.0)},
			{encodeVector(t, keyDesc, 5.0, 6.0)},
			{encodeVector(t, keyDesc, 7.0, 8.0)},
			{encodeVector(t, keyDesc, 9.0, 10.0)},
			{encodeVector(t, keyDesc, 11.0, 12.0)},
			{encodeVector(t, keyDesc, 13.0, 14.0)},
			{encodeVector(t, keyDesc, 15.0, 16.0)},
		}
		combinedKeys := buildTuples(t, ctx, ns, pb, keyDesc, combinedKeyRows)

		combinedValueRows := [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}, {int64(5)}, {int64(6)}, {int64(7)}, {int64(8)}}
		combinedValues := buildTuples(t, ctx, ns, pb, testValDesc, combinedValueRows)

		validateProximityMap(t, ctx, ns, &newMap, keyDesc, testValDesc, combinedKeys, combinedValues, logChunkSize)
	})
}

func testIncrementalUpdates(t *testing.T, keyDesc *val.TupleDesc) {
	t.Run("incremental updates", func(t *testing.T) {
		ctx := context.Background()
		ns := tree.NewTestNodeStore()
		pb := pool.NewBuffPool()
		logChunkSize := uint8(1)
		distanceType := vector.DistanceL2Squared{}
		flusher := ProximityFlusher{logChunkSize: logChunkSize, distanceType: distanceType}
		keyRows1 := [][]interface{}{
			{encodeVector(t, keyDesc, 0.0, 1.0)},
			{encodeVector(t, keyDesc, 3.0, 4.0)},
			{encodeVector(t, keyDesc, 5.0, 6.0)},
			{encodeVector(t, keyDesc, 7.0, 8.0)},
		}
		keys1 := buildTuples(t, ctx, ns, pb, keyDesc, keyRows1)

		valueRows1 := [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}}
		values1 := buildTuples(t, ctx, ns, pb, testValDesc, valueRows1)

		m1 := createAndValidateProximityMap(t, ctx, ns, keyDesc, keys1, testValDesc, values1, logChunkSize)

		mutableMap := newProximityMutableMap(m1)

		bp := pool.NewBuffPool()

		keyBuilder := val.NewTupleBuilder(keyDesc, ns)
		valueBuilder := val.NewTupleBuilder(testValDesc, ns)

		// update leaf node
		{
			putVector(t, keyBuilder, encodeVector(t, keyDesc, 0.0, 1.0))
			nextKey, err := keyBuilder.Build(bp)
			require.NoError(t, err)

			valueBuilder.PutInt64(0, 5)
			nextValue, err := valueBuilder.Build(bp)
			require.NoError(t, err)

			err = mutableMap.Put(ctx, nextKey, nextValue)
			require.NoError(t, err)

			newMap, err := flusher.Map(ctx, mutableMap)
			require.NoError(t, err)

			newCount, err := newMap.Count()
			require.NoError(t, err)

			require.Equal(t, 4, newCount)

			// validate

			combinedKeyRows := [][]interface{}{
				{encodeVector(t, keyDesc, 0.0, 1.0)},
				{encodeVector(t, keyDesc, 3.0, 4.0)},
				{encodeVector(t, keyDesc, 5.0, 6.0)},
				{encodeVector(t, keyDesc, 7.0, 8.0)},
			}
			combinedKeys := buildTuples(t, ctx, ns, pb, keyDesc, combinedKeyRows)
			combinedValueRows := [][]interface{}{{int64(5)}, {int64(2)}, {int64(3)}, {int64(4)}}
			combinedValues := buildTuples(t, ctx, ns, pb, testValDesc, combinedValueRows)

			validateProximityMap(t, ctx, ns, &newMap, keyDesc, testValDesc, combinedKeys, combinedValues, logChunkSize)
		}

		// update root node
		{
			putVector(t, keyBuilder, encodeVector(t, keyDesc, 5.0, 6.0))
			nextKey, err := keyBuilder.Build(bp)
			require.NoError(t, err)

			valueBuilder.PutInt64(0, 6)
			nextValue, err := valueBuilder.Build(bp)
			require.NoError(t, err)

			err = mutableMap.Put(ctx, nextKey, nextValue)
			require.NoError(t, err)

			newMap, err := flusher.Map(ctx, mutableMap)
			require.NoError(t, err)

			combinedKeyRows := [][]interface{}{
				{encodeVector(t, keyDesc, 0.0, 1.0)},
				{encodeVector(t, keyDesc, 3.0, 4.0)},
				{encodeVector(t, keyDesc, 5.0, 6.0)},
				{encodeVector(t, keyDesc, 7.0, 8.0)},
			}
			combinedKeys := buildTuples(t, ctx, ns, pb, keyDesc, combinedKeyRows)
			combinedValueRows := [][]interface{}{{int64(5)}, {int64(2)}, {int64(6)}, {int64(4)}}
			combinedValues := buildTuples(t, ctx, ns, pb, testValDesc, combinedValueRows)

			validateProximityMap(t, ctx, ns, &newMap, keyDesc, testValDesc, combinedKeys, combinedValues, logChunkSize)

		}
	})
}

func testIncrementalDeletes(t *testing.T, keyDesc *val.TupleDesc) {
	t.Run("incremental deletes", func(t *testing.T) {
		ctx := context.Background()
		ns := tree.NewTestNodeStore()
		pb := pool.NewBuffPool()
		logChunkSize := uint8(1)
		distanceType := vector.DistanceL2Squared{}
		flusher := ProximityFlusher{logChunkSize: logChunkSize, distanceType: distanceType}
		keyRows1 := [][]interface{}{
			{encodeVector(t, keyDesc, 0.0, 1.0)},
			{encodeVector(t, keyDesc, 3.0, 4.0)},
			{encodeVector(t, keyDesc, 5.0, 6.0)},
			{encodeVector(t, keyDesc, 7.0, 8.0)},
		}
		keys1 := buildTuples(t, ctx, ns, pb, keyDesc, keyRows1)

		valueRows1 := [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}}
		values1 := buildTuples(t, ctx, ns, pb, testValDesc, valueRows1)

		m1 := createAndValidateProximityMap(t, ctx, ns, keyDesc, keys1, testValDesc, values1, logChunkSize)

		mutableMap := newProximityMutableMap(m1)

		bp := pool.NewBuffPool()

		keyBuilder := val.NewTupleBuilder(keyDesc, ns)

		// delete leaf node
		{
			putVector(t, keyBuilder, encodeVector(t, keyDesc, 0.0, 1.0))
			nextKey, err := keyBuilder.Build(bp)
			require.NoError(t, err)

			err = mutableMap.Put(ctx, nextKey, nil)
			require.NoError(t, err)

			newMap, err := flusher.Map(ctx, mutableMap)
			require.NoError(t, err)

			combinedKeyRows := [][]interface{}{
				{encodeVector(t, keyDesc, 3.0, 4.0)},
				{encodeVector(t, keyDesc, 5.0, 6.0)},
				{encodeVector(t, keyDesc, 7.0, 8.0)},
			}
			combinedKeys := buildTuples(t, ctx, ns, pb, keyDesc, combinedKeyRows)
			combinedValueRows := [][]interface{}{{int64(2)}, {int64(3)}, {int64(4)}}
			combinedValues := buildTuples(t, ctx, ns, pb, testValDesc, combinedValueRows)

			validateProximityMap(t, ctx, ns, &newMap, keyDesc, testValDesc, combinedKeys, combinedValues, logChunkSize)

		}

		// delete root node
		{
			putVector(t, keyBuilder, encodeVector(t, keyDesc, 5.0, 6.0))
			nextKey, err := keyBuilder.Build(bp)
			require.NoError(t, err)

			err = mutableMap.Put(ctx, nextKey, nil)
			require.NoError(t, err)

			newMap, err := flusher.Map(ctx, mutableMap)
			require.NoError(t, err)

			combinedKeyRows := [][]interface{}{
				{encodeVector(t, keyDesc, 3.0, 4.0)},
				{encodeVector(t, keyDesc, 7.0, 8.0)},
			}
			combinedKeys := buildTuples(t, ctx, ns, pb, keyDesc, combinedKeyRows)
			combinedValueRows := [][]interface{}{{int64(2)}, {int64(4)}}
			combinedValues := buildTuples(t, ctx, ns, pb, testValDesc, combinedValueRows)

			validateProximityMap(t, ctx, ns, &newMap, keyDesc, testValDesc, combinedKeys, combinedValues, logChunkSize)

		}
	})
}

// As part of the algorithm for building proximity maps, we store the map keys as bytestrings in a temporary table.
// The sorting order of a key is not always the same as the lexographic ordering of these bytestrings.
// This test makes sure that even when this is not the case we still generate correct output.
func testNonlexographicKey(t *testing.T, keyDesc *val.TupleDesc) {
	t.Run("non-lexographic key", func(t *testing.T) {
		ctx := context.Background()
		ns := tree.NewTestNodeStore()
		pb := pool.NewBuffPool()

		testKeyDesc := val.NewTupleDescriptor(
			keyDesc.Types[0],
			val.Type{Enc: val.Int64Enc, Nullable: true},
		)

		valDesc := val.NewTupleDescriptor()

		keyRows := [][]interface{}{
			{encodeVector(t, keyDesc, 0.0, 0.0), int64(4 + 0*256)},
			{encodeVector(t, keyDesc, 0.0, 0.0), int64(3 + 1*256)},
			{encodeVector(t, keyDesc, 0.0, 0.0), int64(2 + 2*256)},
			{encodeVector(t, keyDesc, 0.0, 0.0), int64(1 + 3*256)},
			{encodeVector(t, keyDesc, 0.0, 0.0), int64(0 + 4*256)},
		}
		keys := buildTuples(t, ctx, ns, pb, testKeyDesc, keyRows)

		valueRows := [][]interface{}{{}, {}, {}, {}, {}}
		values := buildTuples(t, ctx, ns, pb, valDesc, valueRows)

		// The way the validation test is currently written it assumes that all vectors are unique, but this is not a
		// requirement. Skip validation for now.
		_ = createProximityMap(t, ctx, ns, testKeyDesc, keys, valDesc, values, 1)
	})
}

func testManyDimensions(t *testing.T, keyDesc *val.TupleDesc) {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()
	numRows := 50
	dimensions := 50
	testManyDimensionsHelper(ctx, t, keyDesc, ns, numRows, dimensions)
}

func testManyDimensionsHelper(ctx context.Context, t *testing.T, keyDesc *val.TupleDesc, ns tree.NodeStore, numRows int, dimensions int) {
	pb := pool.NewBuffPool()
	testKeyDesc := val.NewTupleDescriptor(
		keyDesc.Types[0],
		val.Type{Enc: val.Int64Enc, Nullable: true},
	)

	valDesc := val.NewTupleDescriptor()

	t.Run(fmt.Sprintf("numRows = %d, dimensions = %d", numRows, dimensions), func(t *testing.T) {
		keyRows := make([][]interface{}, numRows)
		valueRows := make([][]interface{}, numRows)
		for i := 0; i < numRows; i++ {
			keyRows[i] = []interface{}{makeManyDimensionalVector(keyDesc.Types[0].Enc, dimensions, int64(i)), i}
			valueRows[i] = []interface{}{}
		}
		keys := buildTuples(t, ctx, ns, pb, testKeyDesc, keyRows)
		values := buildTuples(t, ctx, ns, pb, testKeyDesc, valueRows)

		_ = createAndValidateProximityMap(t, ctx, ns, testKeyDesc, keys, valDesc, values, 3)
	})
}

func makeManyDimensionalVector(encoding val.Encoding, dimensions int, seed int64) interface{} {
	rng := rand.New(rand.NewSource(seed))
	switch encoding {
	case val.JSONAddrEnc:
		var builder strings.Builder

		builder.WriteRune('[')
		if dimensions > 0 {

			builder.WriteString(strconv.Itoa(rng.Int()))
			for d := 1; d < dimensions; d++ {
				builder.WriteRune(',')
				builder.WriteString(strconv.Itoa(rng.Int()))
			}
		}
		builder.WriteRune(']')
		return builder.String()
	case val.BytesAdaptiveEnc:
		result := make([]float32, dimensions)
		for i := 0; i < dimensions; i++ {
			result[i] = rng.Float32()
		}
		return sql.EncodeVector(result)
	default:
		panic("unexpected encoding")
	}
}
