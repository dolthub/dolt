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

	"github.com/dolthub/dolt/go/store/skip"
	"github.com/dolthub/dolt/go/store/val"
)

type memoryMap struct {
	list    *skip.List
	keyDesc val.TupleDesc
}

func newMemoryMap(keyDesc val.TupleDesc, tups ...val.Tuple) (mm memoryMap) {
	if len(tups)%2 != 0 {
		panic("tuples must be key-value pairs")
	}

	mm.keyDesc = keyDesc

	// todo(andy): fix allocation for |mm.compare|
	mm.list = skip.NewSkipList(mm.compare)
	for i := 0; i < len(tups); i += 2 {
		mm.list.Put(tups[i], tups[i+1])
	}

	return
}

func (mm memoryMap) compare(left, right []byte) int {
	return int(mm.keyDesc.Compare(left, right))
}

// Count returns the number of entries in the memoryMap.
func (mm memoryMap) Count() uint64 {
	return uint64(mm.list.Count())
}

// Put adds the Tuple pair |key|, |value| to the memoryMap.
func (mm memoryMap) Put(key, val val.Tuple) {
	mm.list.Put(key, val)
}

// Delete deletes the Tuple pair keyed by |key|.
func (mm memoryMap) Delete(key val.Tuple) {
	mm.list.Put(key, nil)
}

// Get fetches the Tuple pair keyed by |key|, if it exists, and passes it to |cb|.
// If the |key| is not present in the memoryMap, a nil Tuple pair is passed to |cb|.
func (mm memoryMap) Get(_ context.Context, key val.Tuple, cb KeyValueFn) error {
	value, ok := mm.list.Get(key)
	if !ok || value == nil {
		key = nil
	}

	// if |ok| is true but |value| is nil, then there
	// is a pending delete of |key| in |mm.list|.
	return cb(key, value)
}

// IterAll returns a MapIterator that iterates over the entire Map.
func (mm memoryMap) IterAll(ctx context.Context) (MapRangeIter, error) {
	rng := Range{
		Start:   RangeCut{Unbound: true},
		Stop:    RangeCut{Unbound: true},
		KeyDesc: mm.keyDesc,
	}
	return mm.IterRange(ctx, rng)
}

// IterValueRange returns a MapRangeIter that iterates over a Range.
func (mm memoryMap) IterRange(ctx context.Context, rng Range) (MapRangeIter, error) {
	memIter := mm.iterFromRange(rng)
	return NewMapRangeIter(memIter, nil, rng), nil
}

func (mm memoryMap) iterFromRange(rng Range) *memRangeIter {
	var iter *skip.ListIter
	if rng.Start.Unbound {
		iter = mm.list.IterAtStart()
	} else {
		vc := valueCmpForRange(rng)
		iter = mm.list.GetIterAtWithFn(rng.Start.Key, vc)
	}

	// enforce range start
	var key val.Tuple
	for {
		key, _ = iter.Current()
		if key == nil || rng.insideStart(key) {
			break // |i| inside |rng|
		}
		iter.Advance()
	}

	// enforce range end
	if key == nil || !rng.insideStop(key) {
		iter = nil
	}

	return &memRangeIter{
		iter: iter,
		rng:  rng,
	}
}

func valueCmpForRange(rng Range) skip.ValueCmp {
	return func(left, right []byte) int {
		l, r := val.Tuple(left), val.Tuple(right)
		return rng.KeyDesc.Compare(l, r)
	}
}

func (mm memoryMap) mutations() mutationIter {
	return &memRangeIter{
		iter: mm.list.IterAtStart(),
		rng: Range{
			Start:   RangeCut{Unbound: true},
			Stop:    RangeCut{Unbound: true},
			KeyDesc: mm.keyDesc,
		},
	}
}

type memRangeIter struct {
	iter *skip.ListIter
	rng  Range
}

var _ rangeIter = &memRangeIter{}
var _ mutationIter = &memRangeIter{}

// current returns the iter's current Tuple pair, or nil Tuples
// if the iter has exhausted its range, it will
func (it *memRangeIter) current() (key, value val.Tuple) {
	// |it.iter| is set to nil when its range is exhausted
	if it.iter != nil {
		key, value = it.iter.Current()
	}
	return
}

// iterate progresses the iter inside its range, skipping
// over pending deletes in the memoryMap.
func (it *memRangeIter) iterate(context.Context) (err error) {
	for {
		it.iter.Advance()

		k, _ := it.current()
		if k == nil || !it.rng.insideStop(k) {
			it.iter = nil // range exhausted
		}

		return
	}
}

func (it *memRangeIter) nextMutation(context.Context) (key, value val.Tuple) {
	key, value = it.iter.Current()
	if key == nil {
		return
	}
	it.iter.Advance()
	return
}

func (it *memRangeIter) close() error {
	return nil
}
