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

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// MapReader is the read interface shared by Map and MapView.
type MapReader interface {
	Get(ctx context.Context, key val.Tuple, cb tree.KeyValueFn[val.Tuple, val.Tuple]) error
	GetPrefix(ctx context.Context, key val.Tuple, prefDesc *val.TupleDesc, cb tree.KeyValueFn[val.Tuple, val.Tuple]) error
	Has(ctx context.Context, key val.Tuple) (bool, error)
	IterAll(ctx context.Context) (MapIter, error)
	IterRange(ctx context.Context, rng Range) (MapIter, error)
	IterRangeReverse(ctx context.Context, rng Range) (MapIter, error)
	IterKeyRange(ctx context.Context, start, stop val.Tuple) (MapIter, error)
	Descriptors() (*val.TupleDesc, *val.TupleDesc)
	KeyDesc() *val.TupleDesc
	ValDesc() *val.TupleDesc
	NodeStore() tree.NodeStore
	Pool() pool.BuffPool
}

var _ MapReader = Map{}
var _ MapReader = MapView{}

// MapView is a read-only view of a MutableMap as of its last checkpoint. Its reads merge the MutableMap's in-memory
// edits with its static tree, so it can be read without applying those edits to a new tree.
type MapView struct {
	mut *MutableMap
}

// CheckpointView returns a MapView of |mut| as of its last checkpoint. Writes made to |mut| after its last checkpoint,
// including those made after the view is created, are not visible through the view.
func CheckpointView(ctx context.Context, mut *MutableMap) (MapView, error) {
	var tuples tree.MutableMap[val.Tuple, val.Tuple, *val.TupleDesc, tree.StaticMap[val.Tuple, val.Tuple, *val.TupleDesc]]
	switch {
	case mut.stash != nil:
		// |mut| reuses the stash's edits if it reverts, so the view needs its own copy
		tuples = mut.stash.Copy()
	case mut.tuples.Edits.EditedSinceCheckpoint():
		tuples = mut.tuples.Copy()
		if err := tuples.Edits.Revert(ctx); err != nil {
			return MapView{}, err
		}
	default:
		tuples = mut.tuples.Copy()
	}
	return MapView{mut: &MutableMap{
		tuples:     tuples,
		keyDesc:    mut.keyDesc,
		valDesc:    mut.valDesc,
		maxPending: mut.maxPending,
		flusher:    mut.flusher,
	}}, nil
}

// Get implements MapReader
func (v MapView) Get(ctx context.Context, key val.Tuple, cb tree.KeyValueFn[val.Tuple, val.Tuple]) error {
	return v.mut.Get(ctx, key, cb)
}

// GetPrefix implements MapReader
func (v MapView) GetPrefix(ctx context.Context, key val.Tuple, prefDesc *val.TupleDesc, cb tree.KeyValueFn[val.Tuple, val.Tuple]) error {
	return v.mut.GetPrefix(ctx, key, prefDesc, cb)
}

// Has implements MapReader
func (v MapView) Has(ctx context.Context, key val.Tuple) (bool, error) {
	return v.mut.Has(ctx, key)
}

// IterAll implements MapReader
func (v MapView) IterAll(ctx context.Context) (MapIter, error) {
	return v.mut.IterAll(ctx)
}

// IterRange implements MapReader
func (v MapView) IterRange(ctx context.Context, rng Range) (MapIter, error) {
	return v.mut.IterRange(ctx, rng)
}

// IterRangeReverse implements MapReader
func (v MapView) IterRangeReverse(ctx context.Context, rng Range) (MapIter, error) {
	return v.mut.IterRangeReverse(ctx, rng)
}

// IterKeyRange implements MapReader
func (v MapView) IterKeyRange(ctx context.Context, start, stop val.Tuple) (MapIter, error) {
	return v.mut.IterKeyRange(ctx, start, stop)
}

// EstimateCount returns an estimate of the number of Tuple pairs in the MapView. Each edit is assumed to insert or
// delete a pair, so the estimate is high when edits update existing pairs.
func (v MapView) EstimateCount() (int, error) {
	cnt, err := v.mut.tuples.Static.Count()
	if err != nil {
		return 0, err
	}
	for it := v.mut.tuples.Edits.IterAtStart(); ; it.Advance() {
		k, value := it.Current()
		if k == nil {
			break
		}
		if value == nil {
			cnt--
		} else {
			cnt++
		}
	}
	return max(cnt, 0), nil
}

// VisitGCRoots passes every chunk address reachable from the MapView to |roots|.
func (v MapView) VisitGCRoots(roots func(hash.Hash)) {
	visitMutableTreeGCRoots(v.mut.tuples, v.mut.keyDesc, v.mut.valDesc, roots)
}

// Descriptors implements MapReader
func (v MapView) Descriptors() (*val.TupleDesc, *val.TupleDesc) {
	return v.mut.Descriptors()
}

// KeyDesc implements MapReader
func (v MapView) KeyDesc() *val.TupleDesc {
	return v.mut.KeyDesc()
}

// ValDesc implements MapReader
func (v MapView) ValDesc() *val.TupleDesc {
	return v.mut.ValDesc()
}

// NodeStore implements MapReader
func (v MapView) NodeStore() tree.NodeStore {
	return v.mut.NodeStore()
}

// Pool implements MapReader
func (v MapView) Pool() pool.BuffPool {
	return v.mut.Pool()
}
