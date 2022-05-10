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

	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/skip"
	"github.com/dolthub/dolt/go/store/val"
)

const (
	maxPending = 64 * 1024
)

// MutableMap represents a Map that is able to store mutations in-memory. A MutableMap has two tiers of in-memory storage:
// pending and applied. All mutations are first written to the pending tier, which may be discarded at any time.
// However, once ApplyPending() is called, those mutations are moved to the applied tier, and the pending tier is
// cleared.
type MutableMap struct {
	uncommitted *skip.List
	tuples      orderedMap[val.Tuple, val.Tuple, val.TupleDesc]
	keyDesc     val.TupleDesc
	valDesc     val.TupleDesc
}

// newMutableMap returns a new MutableMap.
func newMutableMap(m Map) MutableMap {
	return MutableMap{
		uncommitted: skip.NewSkipList(func(left, right []byte) int {
			return m.tuples.order.Compare(left, right)
		}),
		tuples:  m.tuples.mutate(),
		keyDesc: m.keyDesc,
		valDesc: m.valDesc,
	}
}

// Map materializes all pending and applied mutations in the MutableMap.
func (mut MutableMap) Map(ctx context.Context) (Map, error) {
	if err := mut.ApplyPending(ctx); err != nil {
		return Map{}, err
	}
	tr := mut.tuples.tree
	serializer := message.ProllyMapSerializer{Pool: tr.ns.Pool()}

	root, err := tree.ApplyMutations(ctx, tr.ns, tr.root, serializer, mut.tuples.mutations(), tr.compareItems)
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
	mut.uncommitted.Put(key, value)
	return nil
}

// Delete deletes the pair keyed by |key| from the MutableMap.
func (mut MutableMap) Delete(ctx context.Context, key val.Tuple) error {
	mut.uncommitted.Put(key, nil)
	return nil
}

// Get fetches the Tuple pair keyed by |key|, if it exists, and passes it to |cb|.
// If the |key| is not present in the MutableMap, a nil Tuple pair is passed to |cb|.
func (mut MutableMap) Get(ctx context.Context, key val.Tuple, cb KeyValueFn[val.Tuple, val.Tuple]) (err error) {
	value, ok := mut.uncommitted.Get(key)
	if ok {
		if value == nil {
			// there is a pending delete of |key| in |mut.uncommitted|.
			key = nil
		}
		return cb(key, value)
	}
	return mut.tuples.get(ctx, key, cb)
}

// Has returns true if |key| is present in the MutableMap.
func (mut MutableMap) Has(ctx context.Context, key val.Tuple) (ok bool, err error) {
	value, inUncommitted := mut.uncommitted.Get(key)
	if inUncommitted {
		ok = value != nil
		return
	}
	return mut.tuples.has(ctx, key)
}

// ApplyPending moves all pending mutations to the underlying map.
func (mut *MutableMap) ApplyPending(ctx context.Context) error {
	if mut.uncommitted.Count() == 0 {
		return nil
	}
	uncommittedIter := memIterFromRange(mut.uncommitted, Range{Start: nil, Stop: nil, Desc: mut.keyDesc})
	for true {
		k, v := uncommittedIter.current()
		if k == nil {
			break
		}
		if err := mut.tuples.put(ctx, k, v); err != nil {
			return err
		}
		if err := uncommittedIter.iterate(ctx); err != nil {
			return err
		}
	}
	mut.uncommitted.Truncate()
	return nil
}

// DiscardPending removes all pending mutations.
func (mut *MutableMap) DiscardPending(context.Context) {
	mut.uncommitted.Truncate()
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

	var memoryIter rangeIter[val.Tuple, val.Tuple]
	if mut.uncommitted.Count() > 0 {
		memoryIter = &mmEditIter{
			committedIter:   memIterFromRange(mut.tuples.edits, rng),
			uncommittedIter: memIterFromRange(mut.uncommitted, rng),
			rng:             rng,
		}
	} else {
		memoryIter = memIterFromRange(mut.tuples.edits, rng)
	}

	return &mutableMapIter[val.Tuple, val.Tuple, val.TupleDesc]{
		memory: memoryIter,
		prolly: treeIter,
		order:  rng.Desc,
	}, nil
}

// HasEdits returns true when the MutableMap has performed at least one Put or Delete operation. This does not indicate
// whether the materialized map contains different values to the contained unedited map.
func (mut MutableMap) HasEdits() bool {
	return mut.uncommitted.Count() > 0 || mut.tuples.edits.Count() > 0
}

// mmEditIter handles iterating over the committed and uncommitted mutations. Returns all keys, including those
// representing deletes (which is a non-nil key with a nil value).
type mmEditIter struct {
	committedIter   *memRangeIter
	uncommittedIter *memRangeIter
	rng             Range
}

var _ rangeIter[val.Tuple, val.Tuple] = &mmEditIter{}

// iterate implements rangeIter. Does not return io.EOF once the end of the range has been reached. Instead, check for
// a nil key from current().
func (it *mmEditIter) iterate(ctx context.Context) error {
	comKey, _ := it.committedIter.current()
	uncomKey, _ := it.uncommittedIter.current()
	if comKey == nil && uncomKey == nil {
		// range is exhausted
		return nil
	}

	cmp := it.compareKeys(comKey, uncomKey)
	if cmp <= 0 {
		if err := it.committedIter.iterate(ctx); err != nil {
			return err
		}
	}
	if cmp >= 0 {
		if err := it.uncommittedIter.iterate(ctx); err != nil {
			return err
		}
	}
	return nil
}

// current implements rangeIter. Returns a nil tuple pair once the end of the range has been reached.
func (it *mmEditIter) current() (key, value val.Tuple) {
	comKey, comValue := it.committedIter.current()
	uncomKey, uncomValue := it.uncommittedIter.current()
	cmp := it.compareKeys(comKey, uncomKey)
	if cmp < 0 {
		return comKey, comValue
	} else /* cmp >= 0 */ {
		// |it.uncommittedIter| wins ties
		return uncomKey, uncomValue
	}
}

// compareKeys compares the given keys. A nil key is treated as the lowest value. If both keys are nil, returns 1.
func (it *mmEditIter) compareKeys(leftKey, rightKey val.Tuple) int {
	if leftKey == nil {
		return 1
	}
	if rightKey == nil {
		return -1
	}
	return it.rng.Desc.Compare(leftKey, rightKey)
}
