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

package val

import "github.com/dolthub/dolt/go/store/skip"

type TupleMap struct {
	list    *skip.List
	keyDesc TupleDesc
}

func NewTupleMap(keyDesc TupleDesc, tups ...Tuple) (tm TupleMap) {
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

func (tm TupleMap) Count() int {
	return tm.list.Count()
}

func (tm TupleMap) Get(key Tuple) (val Tuple) {
	val, _ = tm.list.Get(key)
	return
}

func (tm TupleMap) Put(key, val Tuple) {
	tm.list.Put(key, val)
}

func (tm TupleMap) Has(key Tuple) (ok bool) {
	_, ok = tm.list.Get(key)
	return
}

func (tm TupleMap) compare(left, right []byte) int {
	return int(tm.keyDesc.Compare(left, right))
}
