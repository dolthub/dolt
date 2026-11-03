// Copyright 2026 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

type MapView struct {
	tuples  tree.MutableMap[val.Tuple, val.Tuple, *val.TupleDesc, tree.StaticMap[val.Tuple, val.Tuple, *val.TupleDesc]]
	keyDesc *val.TupleDesc
	valDesc *val.TupleDesc
}

var _ MapReader = MapView{}

// NewMapView returns a read-only view of MutableMap |mut|.
func NewMapView(mut *MutableMap) MapView {
	return MapView{
		tuples:  mut.tuples,
		keyDesc: mut.keyDesc,
		valDesc: mut.valDesc,
	}
}

// IterAll returns a MapIter that iterates over the entire MapView.
func (v MapView) IterAll(ctx context.Context) (MapIter, error) {
	return v.IterRange(ctx, Range{Fields: nil, Desc: v.keyDesc})
}

// IterRange returns a MapIter that iterates over a Range.
func (v MapView) IterRange(ctx context.Context, rng Range) (MapIter, error) {
	treeIter, err := treeIterFromRange(ctx, v.tuples.Static.GetRoot(), v.tuples.Static.GetNodeStore(), rng)
	if err != nil {
		return nil, err
	}
	memIter, err := memIterFromRange(ctx, v.tuples.Edits, rng)
	if err != nil {
		return nil, err
	}
	var iter MapIter = viewIter{
		edits: memIter,
		tree:  treeIter,
		order: v.keyDesc,
	}
	if !rng.SkipRangeMatchCallback || !rng.IsContiguous {
		iter = filteredIter{
			iter: iter,
			rng:  rng,
		}
	}
	return iter, nil
}

// viewIter merges the edits of a MapView with the tuples of its static tree, in the direction given by |reverse|.
// Edits win ties, and edits with nil values are deletes.
type viewIter struct {
	edits   rangeIter[val.Tuple, val.Tuple]
	tree    rangeIter[val.Tuple, val.Tuple]
	order   *val.TupleDesc
	reverse bool
}

var _ MapIter = viewIter{}

// Next returns the next pair of Tuples, or io.EOF if the iter is done.
func (it viewIter) Next(ctx context.Context) (key, value val.Tuple, err error) {
	// TODO: this is a blatant copy of mutableMapIter in prolly/tuple_range_iter.go, so probably not necessary
	for {
		ek, ev := it.edits.Current()
		tk, tv := it.tree.Current()

		var cmp int
		switch {
		case ek == nil && tk == nil:
			return nil, nil, io.EOF
		case ek == nil:
			cmp = 1
		case tk == nil:
			cmp = -1
		default:
			if cmp, err = it.order.Compare(ctx, ek, tk); err != nil {
				return nil, nil, err
			}
			if it.reverse {
				cmp = -cmp
			}
		}

		if cmp > 0 {
			if err = it.tree.Iterate(ctx); err != nil {
				return nil, nil, err
			}
			return tk, tv, nil
		}

		if err = it.edits.Iterate(ctx); err != nil {
			return nil, nil, err
		}
		if cmp == 0 {
			if err = it.tree.Iterate(ctx); err != nil {
				return nil, nil, err
			}
		}
		if ev == nil {
			continue // pending delete
		}
		return ek, ev, nil
	}
}
