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
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

type ProximityMutableMap = GenericMutableMap[ProximityMap, tree.ProximityMap[val.Tuple, val.Tuple, val.TupleDesc]]

type ProximityFlusher struct{}

var _ MutableMapFlusher[ProximityMap, tree.ProximityMap[val.Tuple, val.Tuple, val.TupleDesc]] = ProximityFlusher{}

func (f ProximityFlusher) ApplyMutations(
	ctx context.Context,
	ns tree.NodeStore,
	root tree.Node,
	order val.TupleDesc,
	edits tree.MutationIter,
) (tree.ProximityMap[val.Tuple, val.Tuple, val.TupleDesc], error) {
	serializer := message.NewVectorIndexSerializer(ns.Pool())
	return f.ApplyMutationsWithSerializer(ctx, ns, root, order, serializer, edits)
}

func (f ProximityFlusher) ApplyMutationsWithSerializer(
	ctx context.Context,
	ns tree.NodeStore,
	root tree.Node,
	order val.TupleDesc,
	serializer message.Serializer,
	edits tree.MutationIter,
) (tree.ProximityMap[val.Tuple, val.Tuple, val.TupleDesc], error) {
	panic("not implemented")
}

// newMutableMap returns a new MutableMap.
func newProximityMutableMap(m ProximityMap) *ProximityMutableMap {
	return &ProximityMutableMap{
		tuples:     m.tuples.Mutate(),
		keyDesc:    m.keyDesc,
		valDesc:    m.valDesc,
		maxPending: defaultMaxPending,
		flusher:    ProximityFlusher{},
	}
}

// newMutableMapWithDescriptors returns a new MutableMap with the key and value TupleDescriptors overridden to the
// values specified in |kd| and |vd|. This is useful if you are rewriting the data in a map to change its schema.
func newProximityMutableMapWithDescriptors(m ProximityMap, kd, vd val.TupleDesc) *ProximityMutableMap {
	return &ProximityMutableMap{
		tuples:     m.tuples.Mutate(),
		keyDesc:    kd,
		valDesc:    vd,
		maxPending: defaultMaxPending,
		flusher:    ProximityFlusher{},
	}
}

func (f ProximityFlusher) MapInterface(ctx context.Context, mut *ProximityMutableMap) (MapInterface, error) {
	return f.Map(ctx, mut)
}

// TreeMap materializes all pending and applied mutations in the MutableMap.
func (f ProximityFlusher) TreeMap(ctx context.Context, mut *ProximityMutableMap) (tree.ProximityMap[val.Tuple, val.Tuple, val.TupleDesc], error) {
	s := message.NewVectorIndexSerializer(mut.NodeStore().Pool())
	return mut.flushWithSerializer(ctx, s)
}

// TreeMap materializes all pending and applied mutations in the MutableMap.
func (f ProximityFlusher) Map(ctx context.Context, mut *ProximityMutableMap) (ProximityMap, error) {
	treeMap, err := f.TreeMap(ctx, mut)
	if err != nil {
		return ProximityMap{}, err
	}
	return ProximityMap{
		tuples:  treeMap,
		keyDesc: mut.keyDesc,
		valDesc: mut.valDesc,
	}, nil
}
