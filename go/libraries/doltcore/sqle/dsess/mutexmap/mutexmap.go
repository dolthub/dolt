// Copyright 2024 Dolthub, Inc.
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

package mutexmap

import (
	"sync"
)

// MutexMap holds a dynamic number of mutexes identified by keys. When a mutex is no longer needed, it's removed from
// the map.
type MutexMap struct {
	keyedMutexes map[interface{}]*mapMutex
	mu           sync.Mutex
}

type mapMutex struct {
	key      interface{}
	parent   *MutexMap
	refcount int
	mu       sync.Mutex
}

func NewMutexMap() *MutexMap {
	return &MutexMap{keyedMutexes: make(map[interface{}]*mapMutex)}
}

func (mm *MutexMap) Lock(key interface{}) func() {
	mm.mu.Lock()

	var keyedMutex *mapMutex
	func() {
		// We must release the parent lock before attempting to acquire the child lock, otherwise if the child lock
		// is currently held it will never be released.
		defer mm.mu.Unlock()
		var hasKey bool
		keyedMutex, hasKey = mm.keyedMutexes[key]
		if !hasKey {
			keyedMutex = &mapMutex{parent: mm, key: key}
			mm.keyedMutexes[key] = keyedMutex
		}
		keyedMutex.refcount++
	}()

	keyedMutex.mu.Lock()

	return func() {
		keyedMutex.Unlock()
	}
}

func (mm *mapMutex) Unlock() {
	mutexMap := mm.parent
	mutexMap.mu.Lock()
	defer mutexMap.mu.Unlock()

	mm.refcount--
	if mm.refcount < 1 {
		delete(mutexMap.keyedMutexes, mm.key)
	}
	mm.mu.Unlock()
}
