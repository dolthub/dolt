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
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// testMap is a utility type that allows us to create a common test
// harness for Map, memoryMap, and MutableMap.
type testMap interface {
	Has(ctx context.Context, key val.Tuple) (bool, error)
	Get(ctx context.Context, key val.Tuple, cb tree.KeyValueFn[val.Tuple, val.Tuple]) (err error)
	IterAll(ctx context.Context) (MapIter, error)
	IterRange(ctx context.Context, rng Range) (MapIter, error)
	Descriptors() (val.TupleDesc, val.TupleDesc)
}

var _ testMap = Map{}
var _ testMap = &MutableMap{}

func countOrderedMap(t *testing.T, om testMap) (cnt int) {
	iter, err := om.IterAll(context.Background())
	require.NoError(t, err)
	for {
		_, _, err = iter.Next(context.Background())
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		cnt++
	}
	return cnt
}

func keyDescFromMap(om testMap) val.TupleDesc {
	switch m := om.(type) {
	case Map:
		return m.keyDesc
	case *MutableMap:
		return m.keyDesc
	default:
		panic("unknown ordered map")
	}
}

func mustProllyMapFromTuples(t *testing.T, kd, vd val.TupleDesc, tuples [][2]val.Tuple) Map {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()

	serializer := message.NewProllyMapSerializer(vd, ns.Pool())
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
