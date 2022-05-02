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

	"github.com/dolthub/dolt/go/store/prolly/tree"

	"github.com/dolthub/dolt/go/store/val"
)

const (
	maxPending = 64 * 1024
)

type MutableMap struct {
	tuples  orderedMap[val.Tuple, val.Tuple, val.TupleDesc]
	keyDesc val.TupleDesc
	valDesc val.TupleDesc
}

func newMutableMap(m Map) MutableMap {
	return MutableMap{
		tuples:  m.tuples.mutate(),
		keyDesc: m.keyDesc,
		valDesc: m.valDesc,
	}
}

// Map materializes the pending mutations in the MutableMap.
func (mut MutableMap) Map(ctx context.Context) (Map, error) {
	factory := newMapBuilder
	tr := mut.tuples.tree

	root, err := tree.ApplyMutations(ctx, tr.ns, tr.root, factory, mut.tuples.mutations(), tr.compareItems)
	if err != nil {
		return Map{}, err
	}

	return Map{
		tuples: orderedTree[val.Tuple, val.Tuple, val.TupleDesc]{
			root:  root,
			ns:    tr.ns,
			order: tr.order,
		},
		keyDesc: mut.keyDesc,
		valDesc: mut.valDesc,
	}, nil
}

// Put adds the Tuple pair |key|, |value| to the MutableMap.
func (mut MutableMap) Put(ctx context.Context, key, value val.Tuple) error {
	return mut.tuples.put(ctx, key, value)
}

// Delete deletes the pair keyed by |key| from the MutableMap.
func (mut MutableMap) Delete(ctx context.Context, key val.Tuple) error {
	return mut.tuples.delete(ctx, key)
}

// Get fetches the Tuple pair keyed by |key|, if it exists, and passes it to |cb|.
// If the |key| is not present in the MutableMap, a nil Tuple pair is passed to |cb|.
func (mut MutableMap) Get(ctx context.Context, key val.Tuple, cb KeyValueFn[val.Tuple, val.Tuple]) (err error) {
	return mut.tuples.get(ctx, key, cb)
}

// Has returns true if |key| is present in the MutableMap.
func (mut MutableMap) Has(ctx context.Context, key val.Tuple) (ok bool, err error) {
	return mut.tuples.has(ctx, key)
}

// IterAll returns a mutableMapIter that iterates over the entire MutableMap.
func (mut MutableMap) IterAll(ctx context.Context) (MapIter, error) {
	rng := Range{Start: nil, Stop: nil, Desc: mut.keyDesc}
	return mut.IterRange(ctx, rng)
}

// IterRange returns a MapIter that iterates over a Range.
func (mut MutableMap) IterRange(ctx context.Context, rng Range) (MapIter, error) {
	treeIter, err := treeIterFromRange(ctx, mut.tuples.tree.root, mut.tuples.tree.ns, rng)
	if err != nil {
		return nil, err
	}
	memIter := memIterFromRange(mut.tuples.edits, rng)

	return &mutableMapIter[val.Tuple, val.Tuple, val.TupleDesc]{
		memory: memIter,
		prolly: treeIter,
		order:  rng.Desc,
	}, nil
}
