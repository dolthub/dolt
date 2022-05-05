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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// mutation is a single point edit
type mutation struct {
	key, value val.Tuple
}

// mutationProvider creates a set of mutations from a given leaf node.
type mutationProvider interface {
	makeMutations(ctx context.Context, leaf tree.Node) ([]mutation, error)
}

type deleteLastKey struct{}

func (lk deleteLastKey) makeMutations(ctx context.Context, leaf tree.Node) ([]mutation, error) {
	c := int(leaf.Count())
	return []mutation{{
		key:   val.Tuple(leaf.GetKey(c - 1)),
		value: nil,
	}}, nil
}

type deleteSingleKey struct{}

func (rk deleteSingleKey) makeMutations(ctx context.Context, leaf tree.Node) ([]mutation, error) {
	idx := testRand.Int() % int(leaf.Count())
	return []mutation{{
		key:   val.Tuple(leaf.GetKey(idx)),
		value: nil,
	}}, nil
}

//
//func TestWriteAmplification(t *testing.T) {
//	t.Skip("unskip for metrics")
//
//	t.Run("Key Splitter", func(t *testing.T) {
//		testWriteAmpWithSplitter(t, tree.newKeySplitter)
//	})
//	t.Run("Smooth Rolling Hasher", func(t *testing.T) {
//		testWriteAmpWithSplitter(t, tree.newRollingHashSplitter)
//	})
//}
//
//func TestNodeSplitterMetrics(t *testing.T) {
//	t.Skip("unskip for metrics")
//
//	const scale = 100_000
//	t.Run("Key Splitter", func(t *testing.T) {
//		tree.defaultSplitterFactory = tree.newKeySplitter
//		t.Run("Random Uints", func(t *testing.T) {
//			pm, _ := makeProllyMap(t, scale)
//			before := pm.(Map)
//			printMapSummary(t, before)
//		})
//		t.Run("Ascending Uints", func(t *testing.T) {
//			keys, values, desc := tree.AscendingCompositeIntTuples(scale)
//			before := prollyMapFromKeysAndValues(t, desc, desc, keys, values)
//			printMapSummary(t, before)
//		})
//	})
//	t.Run("Smooth Rolling Hasher", func(t *testing.T) {
//		tree.defaultSplitterFactory = tree.newRollingHashSplitter
//		t.Run("Random Uints", func(t *testing.T) {
//			pm, _ := makeProllyMap(t, scale)
//			before := pm.(Map)
//			printMapSummary(t, before)
//		})
//		t.Run("Ascending Uints", func(t *testing.T) {
//			keys, values, desc := tree.AscendingCompositeIntTuples(scale)
//			before := prollyMapFromKeysAndValues(t, desc, desc, keys, values)
//			printMapSummary(t, before)
//		})
//	})
//}
//
//func testWriteAmpWithSplitter(t *testing.T, factory tree.splitterFactory) {
//	const scale = 100_000
//	tree.defaultSplitterFactory = factory
//
//	t.Run("Random Uint Map", func(t *testing.T) {
//		pm, _ := makeProllyMap(t, scale)
//		before := pm.(Map)
//		t.Run("delete random key", func(t *testing.T) {
//			testWriteAmplification(t, before, deleteSingleKey{})
//		})
//		t.Run("delete last key", func(t *testing.T) {
//			testWriteAmplification(t, before, deleteLastKey{})
//		})
//	})
//	t.Run("Ascending Uint Map", func(t *testing.T) {
//		keys, values, desc := tree.AscendingCompositeIntTuples(scale)
//		before := prollyMapFromKeysAndValues(t, desc, desc, keys, values)
//		t.Run("delete random key", func(t *testing.T) {
//			testWriteAmplification(t, before, deleteSingleKey{})
//		})
//		t.Run("delete last key", func(t *testing.T) {
//			testWriteAmplification(t, before, deleteLastKey{})
//		})
//	})
//}

func testWriteAmplification(t *testing.T, before Map, method mutationProvider) {
	ctx := context.Background()
	mutations := collectMutations(t, before, method)

	var counts, sizes tree.Samples
	for _, mut := range mutations {
		mm := before.Mutate()
		err := mm.Put(ctx, mut.key, mut.value)
		require.NoError(t, err)
		after, err := mm.Map(ctx)
		require.NoError(t, err)
		c, s := measureWriteAmplification(t, before, after)
		counts = append(counts, c)
		sizes = append(sizes, s)
	}
	fmt.Println("post-edit write amplification: ")
	fmt.Printf("\t node counts %s \n", counts.Summary())
	fmt.Printf("\t node sizes  %s \n\n", sizes.Summary())
}

func collectMutations(t *testing.T, before Map, method mutationProvider) (muts []mutation) {
	ctx := context.Background()
	err := before.WalkNodes(ctx, func(ctx context.Context, nd tree.Node) error {
		if nd.IsLeaf() {
			mm, err := method.makeMutations(ctx, nd)
			require.NoError(t, err)
			muts = append(muts, mm...)
		}
		return nil
	})
	require.NoError(t, err)
	return
}

func measureWriteAmplification(t *testing.T, before, after Map) (count, size int) {
	ctx := context.Background()
	exclude := hash.NewHashSet()
	err := before.WalkAddresses(ctx, func(_ context.Context, addr hash.Hash) error {
		exclude.Insert(addr)
		return nil
	})
	require.NoError(t, err)

	novel := hash.NewHashSet()
	err = after.WalkAddresses(ctx, func(_ context.Context, addr hash.Hash) error {
		if !exclude.Has(addr) {
			novel.Insert(addr)
		}
		return nil
	})
	require.NoError(t, err)

	for addr := range novel {
		n, err := after.tuples.ns.Read(ctx, addr)
		require.NoError(t, err)
		size += n.Size()
	}
	size += after.tuples.root.Size()
	count = novel.Size() + 1
	return
}

func printMapSummary(t *testing.T, m Map) {
	tree.PrintTreeSummaryByLevel(t, m.tuples.root, m.tuples.ns)
}

func prollyMapFromKeysAndValues(t *testing.T, kd, vd val.TupleDesc, keys, values []val.Tuple) Map {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()
	require.Equal(t, len(keys), len(values))

	serializer := message.ProllyMapSerializer{Pool: ns.Pool()}
	chunker, err := tree.NewEmptyChunker(ctx, ns, serializer)
	require.NoError(t, err)

	for i := range keys {
		err := chunker.AddPair(ctx, tree.Item(keys[i]), tree.Item(values[i]))
		require.NoError(t, err)
	}
	root, err := chunker.Done(ctx)
	require.NoError(t, err)

	return NewMap(root, ns, kd, vd)
}
