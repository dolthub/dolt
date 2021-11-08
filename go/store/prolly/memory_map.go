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
	"io"

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
	var value val.Tuple
	v, ok := mm.list.Get(key)
	if ok {
		value = v
	} else {
		key = nil
	}

	return cb(key, value)
}

func (mm memoryMap) Has(_ context.Context, key val.Tuple) (ok bool, err error) {
	_, ok = mm.list.Get(key)
	return
}

// IterAll returns a MapIterator that iterates over the entire Map.
func (mm memoryMap) IterAll(_ context.Context) (MapIter, error) {
	panic("unimplemented")
}

// IterValueRange returns a MapIterator that iterates over an ValueRange.
func (mm memoryMap) IterValueRange(_ context.Context, rng ValueRange) (MapIter, error) {
	panic("unimplemented")
}

// IterIndexRange returns a MapIterator that iterates over an IndexRange.
func (mm memoryMap) IterIndexRange(_ context.Context, rng IndexRange) (MapIter, error) {
	panic("unimplemented")
}

func (mm memoryMap) mutations() mutationIter {
	return memIter{ListIter: mm.list.Iter()}
}

type memIter struct {
	*skip.ListIter
	idx int
}

var _ MapIter = memIter{}

func (it memIter) Next(context.Context) (key, val val.Tuple, err error) {
	if it.idx == it.Count() {
		err = io.EOF
		return
	}
	key, val = it.next()
	return
}

func (it memIter) next() (key, value val.Tuple) {
	key, value = it.ListIter.Next()
	it.idx++
	return
}

var _ mutationIter = memIter{}

func (it memIter) count() int {
	return it.ListIter.Count()
}

func (it memIter) close() error {
	return nil
}
