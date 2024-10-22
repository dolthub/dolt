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
	"os"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
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

func createProximityMap(t *testing.T, ctx context.Context, ns tree.NodeStore, vectors []interface{}, pks []int64, logChunkSize uint8) (ProximityMap, [][]byte, [][]byte) {
	bp := pool.NewBuffPool()

	count := len(vectors)
	require.Equal(t, count, len(pks))

	kd := val.NewTupleDescriptor(
		val.Type{Enc: val.JSONAddrEnc, Nullable: true},
	)

	vd := val.NewTupleDescriptor(
		val.Type{Enc: val.Int64Enc, Nullable: true},
	)

	distanceType := expression.DistanceL2Squared{}

	builder, err := NewProximityMapFromTuples(ctx, ns, distanceType, kd, vd, logChunkSize)
	require.NoError(t, err)

	keys := make([][]byte, count)
	values := make([][]byte, count)
	keyBuilder := val.NewTupleBuilder(kd)
	valueBuilder := val.NewTupleBuilder(vd)
	for i, vector := range vectors {
		keyBuilder.PutJSONAddr(0, newJsonDocument(t, ctx, ns, vector))
		nextKey := keyBuilder.Build(bp)
		keys[i] = nextKey

		valueBuilder.PutInt64(0, pks[i])
		nextValue := valueBuilder.Build(bp)
		values[i] = nextValue

		err = builder.Insert(ctx, nextKey, nextValue)
		require.NoError(t, err)
	}

	m, err := builder.Flush(ctx)
	require.NoError(t, err)

	require.NoError(t, err)
	mapCount, err := m.Count()
	require.NoError(t, err)
	require.Equal(t, count, mapCount)

	return m, keys, values
}

func TestEmptyProximityMap(t *testing.T) {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()
	createProximityMap(t, ctx, ns, nil, nil, 10)
}

func TestSingleEntryProximityMap(t *testing.T) {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()
	m, keys, values := createProximityMap(t, ctx, ns, []interface{}{"[1.0]"}, []int64{1}, 10)
	matches := 0
	vectorHash, _ := m.keyDesc.GetJSONAddr(0, keys[0])
	vectorDoc, err := tree.NewJSONDoc(vectorHash, ns).ToIndexedJSONDocument(ctx)
	require.NoError(t, err)
	err = m.Get(ctx, vectorDoc, func(foundKey val.Tuple, foundValue val.Tuple) error {
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
	m, keys, values := createProximityMap(t, ctx, ns, []interface{}{"[0.0, 6.0]", "[3.0, 4.0]"}, []int64{1, 2}, 10)
	matches := 0
	for i, key := range keys {
		vectorHash, _ := m.keyDesc.GetJSONAddr(0, key)
		vectorDoc, err := tree.NewJSONDoc(vectorHash, ns).ToIndexedJSONDocument(ctx)
		err = m.Get(ctx, vectorDoc, func(foundKey val.Tuple, foundValue val.Tuple) error {
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
	m, keys, values := createProximityMap(t, ctx, ns, []interface{}{"[0.0, 6.0]", "[3.0, 4.0]"}, []int64{1, 2}, 10)
	matches := 0

	cb := func(foundKey val.Tuple, foundValue val.Tuple, distance float64) error {
		require.Equal(t, val.Tuple(keys[1]), foundKey)
		require.Equal(t, val.Tuple(values[1]), foundValue)
		require.InDelta(t, distance, 25.0, 0.1)
		matches++
		return nil
	}

	err := m.GetClosest(ctx, newJsonValue(t, "[0.0, 0.0]"), cb, 1)
	require.NoError(t, err)
	require.Equal(t, matches, 1)
}

func TestMultilevelProximityMap(t *testing.T) {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()
	keyStrings := []interface{}{
		"[0.0, 1.0]",
		"[3.0, 4.0]",
		"[5.0, 6.0]",
		"[7.0, 8.0]",
	}
	valueStrings := []int64{1, 2, 3, 4}
	m, keys, values := createProximityMap(t, ctx, ns, keyStrings, valueStrings, 1)
	matches := 0
	for i, key := range keys {
		vectorHash, _ := m.keyDesc.GetJSONAddr(0, key)
		vectorDoc, err := tree.NewJSONDoc(vectorHash, ns).ToIndexedJSONDocument(ctx)
		require.NoError(t, err)
		err = m.Get(ctx, vectorDoc, func(foundKey val.Tuple, foundValue val.Tuple) error {
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
	keyStrings1 := []interface{}{
		"[0.0, 1.0]",
		"[3.0, 4.0]",
		"[5.0, 6.0]",
		"[7.0, 8.0]",
	}
	valueStrings1 := []int64{1, 2, 3, 4}
	keyStrings2 := []interface{}{
		"[7.0, 8.0]",
		"[5.0, 6.0]",
		"[3.0, 4.0]",
		"[0.0, 1.0]",
	}
	valueStrings2 := []int64{4, 3, 2, 1}
	m1, _, _ := createProximityMap(t, ctx, ns, keyStrings1, valueStrings1, 1)
	m2, _, _ := createProximityMap(t, ctx, ns, keyStrings2, valueStrings2, 1)

	if !assert.Equal(t, m1.tuples.Root.HashOf(), m2.tuples.Root.HashOf(), "trees have different hashes") {
		require.NoError(t, tree.OutputProllyNodeBytes(os.Stdout, m1.tuples.Root))
		require.NoError(t, tree.OutputProllyNodeBytes(os.Stdout, m2.tuples.Root))
	}
}
