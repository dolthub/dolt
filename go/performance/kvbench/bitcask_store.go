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

package kvbench

// todo
func newBitcaskStore() keyValStore {
	panic("unimplemented")
}

type BitcaskStore struct{}

var _ keyValStore = BitcaskStore{}

func (nbs BitcaskStore) get(key []byte) (val []byte, ok bool) {
	panic("unimplemented")
}

func (nbs BitcaskStore) put(key, val []byte) {
	panic("unimplemented")
}

func (nbs BitcaskStore) delete(key []byte) {
	panic("unimplemented")
}

func (nbs BitcaskStore) putMany(keys, vals [][]byte) {
	panic("unimplemented")
}
