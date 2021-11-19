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

func newMemoryMap(keyDesc val.TupleDesc, tups ...val.Tuple) (tm memoryMap) {
	if len(tups)%2 != 0 {
		panic("tuples must be key-value pairs")
	}

	tm.keyDesc = keyDesc

	// todo(andy): fix allocation for |tm.compare|
	tm.list = skip.NewSkipList(tm.compare)
	for i := 0; i < len(tups); i += 2 {
		tm.list.Put(tups[i], tups[i+1])
	}

	return
}

func (mm memoryMap) compare(left, right []byte) int {
	return int(mm.keyDesc.Compare(left, right))
}

func (mm memoryMap) Count() uint64 {
	return uint64(mm.list.Count())
}

func (mm memoryMap) Put(key, val val.Tuple) (ok bool) {
	ok = !mm.list.Full()
	if ok {
		mm.list.Put(key, val)
	}
	return
}

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
		Reverse: false,
	}
	return mm.IterValueRange(ctx, rng)
}

// IterValueRange returns a MapIterator that iterates over an ValueRange.
func (mm memoryMap) IterValueRange(ctx context.Context, rng Range) (MapRangeIter, error) {
	var iter *skip.ListIter
	if rng.Start.Unbound {
		if rng.Reverse {
			iter = mm.list.IterAtEnd()
		} else {
			iter = mm.list.IterAtStart()
		}
	} else {
		iter = mm.list.IterAt(rng.Start.Key)
	}

	tc := memTupleCursor{iter: iter}

	err := tc.startInRange(ctx, rng)
	if err != nil {
		return MapRangeIter{}, err
	}

	return MapRangeIter{
		memCur: tc,
		rng:    rng,
	}, nil
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

var skips = 0

func (it memTupleCursor) advance(context.Context) (err error) {
	it.iter.Advance()

	key, value := it.iter.Current()
	// if |value| is nil, it's a pending delete should be skipped
	// if |key| is nil, we're past the end of the list
	for value == nil && key != nil {
		skips++
		it.iter.Advance()
		key, value = it.iter.Current()
	}
	return
}

func (it memTupleCursor) retreat(context.Context) (err error) {
	it.iter.Retreat()

	key, value := it.iter.Current()
	// if |value| is nil, it's a pending delete should be skipped
	// if |key| is nil, we're before the start of the list
	for value == nil && key != nil {
		it.iter.Retreat()
		key, value = it.iter.Current()
	}
	return
}

func (it memTupleCursor) count() int {
	return it.iter.Count()
}

func (it memTupleCursor) close() error {
	return nil
}

// todo(andy) assumes we're no more than one position away from the correct starting position.
func (it memTupleCursor) startInRange(ctx context.Context, r Range) error {

	key, value := it.iter.Current()
	// if |value| is nil, it's a pending delete should be skipped
	// if |key| is nil, we're before the start of the list
	for value == nil && key != nil {
		if r.Reverse {
			it.iter.Retreat()
		} else {
			it.iter.Advance()
		}
		key, value = it.iter.Current()
	}

	if r.Start.Unbound {
		return nil
	}

	key, _ = it.current()
	if key == nil {
		return nil
	}
	cmp := r.KeyDesc.Compare(key, r.Start.Key)

	if cmp == 0 && r.Start.Inclusive {
		return nil
	}

	if r.Reverse && cmp >= 0 {
		return it.retreat(ctx)
	}

	if !r.Reverse && cmp <= 0 {
		return it.advance(ctx)
	}

	return nil
}
