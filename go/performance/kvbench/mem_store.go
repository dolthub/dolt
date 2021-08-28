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

import "sync"

type keyValStore interface {
	get(key []byte) (val []byte, ok bool)
	put(key, val []byte)
	delete(key []byte)

	// non-atomic put
	load(key, val []byte)
}

type flushingKeyValStore interface {
	keyValStore
	flush()
}

type orderedKeyValStore interface {
	keyValStore
	getRange(low, hi []byte) (vals [][]byte)
	deleteRange(low, hi []byte)
}

func newMemStore() keyValStore {
	return memStore{
		store: make(map[string][]byte),
	}
}

type memStore struct {
	store map[string][]byte
	mu    sync.RWMutex
}

var _ keyValStore = memStore{}

func (m memStore) get(key []byte) (val []byte, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	val, ok = m.store[string(key)]
	return val, ok
}

func (m memStore) put(key, val []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.load(key, val)
}

func (m memStore) delete(key []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.store, string(key))
}

func (m memStore) load(key, val []byte) {
	m.store[string(key)] = val
}
