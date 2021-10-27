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
	"github.com/dolthub/dolt/go/store/skip"
	"github.com/dolthub/dolt/go/store/val"
)

type memoryMap struct {
	list    *skip.List
	keyDesc val.TupleDesc
}

func NewTupleMap(keyDesc val.TupleDesc, tups ...val.Tuple) (tm memoryMap) {
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

func (mm memoryMap) Count() int {
	return mm.list.Count()
}

func (mm memoryMap) Get(key val.Tuple) (val val.Tuple, ok bool) {
	return mm.list.Get(key)
}

func (mm memoryMap) Put(key, val val.Tuple) (ok bool) {
	ok = !mm.list.Full()
	if ok {
		mm.list.Put(key, val)
	}
	return
}

func (mm memoryMap) Has(key val.Tuple) (ok bool) {
	_, ok = mm.list.Get(key)
	return
}

func (mm memoryMap) Iter() keyValueIter {
	return keyValueIter{ListIter: mm.list.Iter()}
}

type keyValueIter struct {
	*skip.ListIter
	idx int
}

func (it keyValueIter) Count() int {
	return it.ListIter.Count()
}

func (it keyValueIter) Next() (key, val val.Tuple) {
	key, val = it.ListIter.Next()
	it.idx++
	return
}

func (it keyValueIter) Remaining() int {
	return it.Count() - it.idx
}

func (it keyValueIter) Close() error {
	return nil
}
