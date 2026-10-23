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

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/skip"
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
	tuples  tree.MutableMap[val.Tuple, val.Tuple, *val.TupleDesc, tree.StaticMap[val.Tuple, val.Tuple, *val.TupleDesc]]
	keyDesc *val.TupleDesc
	valDesc *val.TupleDesc
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
	return MapView{
		tuples:  tuples,
		keyDesc: mut.keyDesc,
		valDesc: mut.valDesc,
	}, nil
}

// CopyTo replaces the contents of |mut| with those of the MapView, discarding the edits and checkpoint of |mut|.
func (v MapView) CopyTo(mut *MutableMap) {
	mut.tuples = v.tuples.Copy()
	mut.stash = nil
}

// Get fetches the Tuple pair keyed by |key|, if it exists, and passes it to |cb|.
// If the |key| is not present in the MapView, a nil Tuple pair is passed to |cb|.
func (v MapView) Get(ctx context.Context, key val.Tuple, cb tree.KeyValueFn[val.Tuple, val.Tuple]) error {
	return v.tuples.Get(ctx, key, cb)
}

// GetPrefix fetches the first Tuple pair with a key matching |key| in the fields of |prefDesc|, and passes it to
// |cb|. If no key matches, a nil Tuple pair is passed to |cb|.
func (v MapView) GetPrefix(ctx context.Context, key val.Tuple, prefDesc *val.TupleDesc, cb tree.KeyValueFn[val.Tuple, val.Tuple]) error {
	// The first matching edit may be a delete that shadows a different matching key in the static tree, so we
	// merge the two rather than checking each in turn as tree.MutableMap does.
	rng, err := PrefixRange(ctx, key, prefDesc)
	if err != nil {
		return err
	}
	iter, err := v.IterRange(ctx, rng)
	if err != nil {
		return err
	}
	k, value, err := iter.Next(ctx)
	if err == io.EOF {
		return cb(nil, nil)
	} else if err != nil {
		return err
	}
	return cb(k, value)
}

// Has returns true if |key| is present in the MapView.
func (v MapView) Has(ctx context.Context, key val.Tuple) (bool, error) {
	return v.tuples.Has(ctx, key)
}

// HasPrefix returns true if the MapView contains any key matching |preKey| in the fields of |preDesc|.
func (v MapView) HasPrefix(ctx context.Context, preKey val.Tuple, preDesc *val.TupleDesc) (ok bool, err error) {
	err = v.GetPrefix(ctx, preKey, preDesc, func(k, _ val.Tuple) error {
		ok = k != nil
		return nil
	})
	return
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
	var iter MapIter = viewIter{edits: memIter, tree: treeIter, order: v.keyDesc}
	if !rng.SkipRangeMatchCallback || !rng.IsContiguous {
		iter = filteredIter{iter: iter, rng: rng}
	}
	return iter, nil
}

// IterRangeReverse returns a MapIter that iterates over a Range backwards.
func (v MapView) IterRangeReverse(ctx context.Context, rng Range) (MapIter, error) {
	treeIter, err := treeIterFromRangeReverse(ctx, v.tuples.Static.GetRoot(), v.tuples.Static.GetNodeStore(), rng)
	if err != nil {
		return nil, err
	}
	memIter, err := memIterFromRangeReverse(ctx, v.tuples.Edits, rng)
	if err != nil {
		return nil, err
	}
	iter := viewIter{edits: memIter, tree: treeIter, order: v.keyDesc, reverse: true}
	return filteredIter{iter: iter, rng: rng}, nil
}

// IterKeyRange iterates over a physical key range defined by |start| and |stop|. If |start| and/or |stop| is nil,
// the range will be open towards that end.
func (v MapView) IterKeyRange(ctx context.Context, start, stop val.Tuple) (MapIter, error) {
	treeIter, err := v.tuples.Static.IterKeyRange(ctx, start, stop)
	if err != nil {
		return nil, err
	}
	memIter, err := memIterFromKeyRange(ctx, v.tuples.Edits, v.keyDesc, start, stop)
	if err != nil {
		return nil, err
	}
	return viewIter{edits: memIter, tree: treeIter, order: v.keyDesc}, nil
}

// EstimateCount returns an estimate of the number of Tuple pairs in the MapView. Each edit is assumed to insert or
// delete a pair, so the estimate is high when edits update existing pairs.
func (v MapView) EstimateCount() (int, error) {
	cnt, err := v.tuples.Static.Count()
	if err != nil {
		return 0, err
	}
	for it := v.tuples.Edits.IterAtStart(); ; it.Advance() {
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
func (v MapView) VisitGCRoots(roots func(hash.Hash) bool) {
	visitMutableTreeGCRoots(v.tuples, v.keyDesc, v.valDesc, roots)
}

// Descriptors returns the key and value val.TupleDesc.
func (v MapView) Descriptors() (*val.TupleDesc, *val.TupleDesc) {
	return v.keyDesc, v.valDesc
}

// KeyDesc returns the key val.TupleDesc.
func (v MapView) KeyDesc() *val.TupleDesc {
	return v.keyDesc
}

// ValDesc returns the value val.TupleDesc.
func (v MapView) ValDesc() *val.TupleDesc {
	return v.valDesc
}

// NodeStore returns the view's NodeStore.
func (v MapView) NodeStore() tree.NodeStore {
	return v.tuples.Static.GetNodeStore()
}

// Pool returns the pool.BuffPool of the view's NodeStore.
func (v MapView) Pool() pool.BuffPool {
	return v.tuples.Static.GetNodeStore().Pool()
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

func memIterFromRangeReverse(ctx context.Context, list *skip.List, rng Range) (*memReverseRangeIter, error) {
	// seek to the first key at or beyond the end of |rng|
	iter, err := list.GetIterFromSeekFn(func(nodeKey []byte) (bool, error) {
		if nodeKey == nil {
			return false, nil
		}
		return rng.belowStop(ctx, nodeKey)
	})
	if err != nil {
		return nil, err
	}

	// enforce range end
	var key val.Tuple
	for {
		key, _ = iter.Current()
		if key == nil {
			break
		}
		below, err := rng.belowStop(ctx, key)
		if err != nil {
			return nil, err
		}
		if below {
			break // |iter| inside |rng|
		}
		iter.Retreat()
	}

	// enforce range start
	if key != nil {
		above, err := rng.aboveStart(ctx, key)
		if err != nil {
			return nil, err
		}
		if !above {
			iter = nil
		}
	} else {
		iter = nil
	}

	return &memReverseRangeIter{iter: iter, rng: rng}, nil
}

// memReverseRangeIter iterates backwards over the edits of a skip.List within a Range.
type memReverseRangeIter struct {
	iter *skip.ListIter
	rng  Range
}

// Current returns the iter's current Tuple pair, or nil Tuples if the iter has exhausted its range.
func (it *memReverseRangeIter) Current() (key, value val.Tuple) {
	if it.iter != nil {
		key, value = it.iter.Current()
	}
	return
}

// Iterate moves the iter backwards inside its range.
func (it *memReverseRangeIter) Iterate(ctx context.Context) error {
	it.iter.Retreat()
	k, _ := it.Current()
	if k == nil {
		it.iter = nil // range exhausted
		return nil
	}
	above, err := it.rng.aboveStart(ctx, k)
	if err != nil {
		return err
	}
	if !above {
		it.iter = nil // range exhausted
	}
	return nil
}

func memIterFromKeyRange(ctx context.Context, list *skip.List, order *val.TupleDesc, start, stop val.Tuple) (*memKeyRangeIter, error) {
	var iter *skip.ListIter
	var err error
	if start == nil {
		iter = list.IterAtStart()
	} else {
		if iter, err = list.GetIterAt(ctx, start); err != nil {
			return nil, err
		}
		// GetIterAt retreats to the last key when |start| is greater than every key
		if k, _ := iter.Current(); k != nil {
			cmp, err := order.Compare(ctx, k, start)
			if err != nil {
				return nil, err
			}
			if cmp < 0 {
				iter.Advance()
			}
		}
	}
	it := &memKeyRangeIter{iter: iter, order: order, stop: stop}
	if err = it.checkStop(ctx); err != nil {
		return nil, err
	}
	return it, nil
}

// memKeyRangeIter iterates over the edits of a skip.List in the physical key range from its start, inclusive, to
// |stop|, exclusive.
type memKeyRangeIter struct {
	iter  *skip.ListIter
	order *val.TupleDesc
	stop  val.Tuple
}

// Current returns the iter's current Tuple pair, or nil Tuples if the iter has exhausted its range.
func (it *memKeyRangeIter) Current() (key, value val.Tuple) {
	if it.iter != nil {
		key, value = it.iter.Current()
	}
	return
}

// Iterate progresses the iter inside its range.
func (it *memKeyRangeIter) Iterate(ctx context.Context) error {
	it.iter.Advance()
	return it.checkStop(ctx)
}

func (it *memKeyRangeIter) checkStop(ctx context.Context) error {
	k, _ := it.Current()
	if k == nil {
		it.iter = nil
		return nil
	}
	if it.stop == nil {
		return nil
	}
	cmp, err := it.order.Compare(ctx, k, it.stop)
	if err != nil {
		return err
	}
	if cmp >= 0 {
		it.iter = nil
	}
	return nil
}
