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

package valuefile

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/shim"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

func TestReadWriteValueFile(t *testing.T) {
	const numMaps = 1
	const numMapValues = 1

	ctx := context.Background()
	store, err := NewFileValueStore(types.Format_Default)
	require.NoError(t, err)

	var values []types.Value
	for i := 0; i < numMaps; i++ {
		var kvs []types.Value
		for j := 0; j < numMapValues; j++ {
			kvs = append(kvs, types.Int(j), types.String(uuid.New().String()))
		}
		m, err := types.NewMap(ctx, store, kvs...)
		require.NoError(t, err)

		values = append(values, m)
	}

	path := filepath.Join(os.TempDir(), "file.nvf")
	err = WriteValueFile(ctx, path, store, values...)
	require.NoError(t, err)

	vf, err := ReadValueFile(ctx, path)
	require.NoError(t, err)
	require.NotNil(t, vf.Ns)
	require.Equal(t, len(values), len(vf.Values))

	for i := 0; i < len(values); i++ {
		require.True(t, values[i].Equals(vf.Values[i]))
	}
}

func TestRoundtripProllyMapIntoValueFile(t *testing.T) {
	const numMaps = 5
	const numMapEntries = 1000

	ctx := context.Background()
	store, err := NewFileValueStore(types.Format_DOLT)
	require.NoError(t, err)
	oldNs := tree.NewNodeStore(store)
	vrw := types.NewValueStore(store)

	var values []types.Value
	var expectedMaps []prolly.Map

	for i := 0; i < numMaps; i++ {
		m, _ := makeProllyMap(t, oldNs, numMapEntries)
		expectedMaps = append(expectedMaps, m)
		v := shim.ValueFromMap(m)

		ref, err := vrw.WriteValue(ctx, v)
		require.NoError(t, err)

		values = append(values, ref)
	}

	path := filepath.Join(os.TempDir(), "file.nvf")
	err = WriteValueFile(ctx, path, store, values...)
	require.NoError(t, err)

	vf, err := ReadValueFile(ctx, path)
	require.NoError(t, err)
	require.NotNil(t, vf.Ns)
	require.Equal(t, len(values), len(vf.Values))

	for i := 0; i < len(vf.Values); i++ {
		ref := vf.Values[i].(types.Ref)
		v, err := vrw.ReadValue(ctx, ref.TargetHash())
		require.NoError(t, err)
		rootNode, fileId, err := shim.NodeFromValue(v)
		require.NoError(t, err)
		require.Equal(t, fileId, serial.ProllyTreeNodeFileID)
		m := prolly.NewMap(rootNode, vf.Ns, kd, vd)
		assertProllyMapsEqual(t, expectedMaps[i], m)
	}
}

func assertProllyMapsEqual(t *testing.T, expected, received prolly.Map) {
	assert.Equal(t, expected.HashOf(), received.HashOf())

	s, err := prolly.DebugFormat(context.Background(), expected)
	require.NoError(t, err)
	s2, err := prolly.DebugFormat(context.Background(), received)
	require.NoError(t, err)
	require.Equal(t, s, s2)
}

var kd = val.NewTupleDescriptor(
	val.Type{Enc: val.Uint32Enc, Nullable: false},
)
var vd = val.NewTupleDescriptor(
	val.Type{Enc: val.Uint32Enc, Nullable: true},
	val.Type{Enc: val.Uint32Enc, Nullable: true},
	val.Type{Enc: val.Uint32Enc, Nullable: true},
)

func makeProllyMap(t *testing.T, ns tree.NodeStore, count int) (prolly.Map, [][2]val.Tuple) {
	tuples := tree.RandomTuplePairs(count, kd, vd, ns)
	om := mustProllyMapFromTuples(t, kd, vd, ns, tuples)

	for i := 0; i < len(tuples); i++ {
		var found bool
		err := om.Get(context.Background(), tuples[i][0], func(k, v val.Tuple) error {
			assert.Equal(t, tuples[i][0], k)
			assert.Equal(t, tuples[i][1], v)
			found = true
			return nil
		})
		require.NoError(t, err)
		assert.True(t, found)
	}

	return om, tuples
}

func mustProllyMapFromTuples(t *testing.T, kd, vd val.TupleDesc, ns tree.NodeStore, tuples [][2]val.Tuple) prolly.Map {
	ctx := context.Background()

	serializer := message.NewProllyMapSerializer(vd, ns.Pool())
	chunker, err := tree.NewEmptyChunker(ctx, ns, serializer)
	require.NoError(t, err)

	for _, pair := range tuples {
		err := chunker.AddPair(ctx, tree.Item(pair[0]), tree.Item(pair[1]))
		require.NoError(t, err)
	}
	root, err := chunker.Done(ctx)
	require.NoError(t, err)

	return prolly.NewMap(root, ns, kd, vd)
}
