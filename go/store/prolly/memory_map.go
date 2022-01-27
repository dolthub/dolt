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
	var iter *skip.ListIter
	if rng.Start.Unbound {
		iter = mm.list.IterAtStart()
	} else {
		iter = mm.list.IterAt(rng.Start.Key)
	}
	memCur := memTupleCursor{iter: iter}

	return NewMapRangeIter(ctx, memCur, nil, rng)
}

func (mm memoryMap) mutations() mutationIter {
	return memTupleCursor{iter: mm.list.IterAtStart()}
}

type memTupleCursor struct {
	iter    *skip.ListIter
	reverse bool
}

var _ tupleCursor = memTupleCursor{}
var _ mutationIter = memTupleCursor{}

func (it memTupleCursor) nextMutation() (key, value val.Tuple) {
	key, value = it.iter.Current()
	if key == nil {
		return
	}
	it.iter.Advance()
	return
}

func (it memTupleCursor) current() (key, value val.Tuple) {
	return it.iter.Current()
}

func (it memTupleCursor) advance(context.Context) (err error) {
	it.iter.Advance()
	return
}

func (it memTupleCursor) retreat(context.Context) (err error) {
	it.iter.Retreat()
	return
}

func (it memTupleCursor) count() int {
	return it.iter.Count()
}

func (it memTupleCursor) close() error {
	return nil
}
