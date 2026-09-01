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
	"context"
	"sync"
)

// MutexMap holds a dynamic number of mutexes identified by keys. When a mutex is no longer needed, it's removed from
// the map.
type MutexMap struct {
	keyedMutexes map[interface{}]*mapMutex
	mu           sync.Mutex // Access to the map itself must be synchronized.
}

type mapMutex struct {
	key      interface{}
	parent   *MutexMap
	refcount int
	// held is a capacity-one channel used as the mutex itself, rather than a sync.Mutex, so
	// that a waiter can give up on a canceled context. Holds a value when locked.
	held chan struct{}
}

func NewMutexMap() *MutexMap {
	return &MutexMap{keyedMutexes: make(map[interface{}]*mapMutex)}
}

// Lock acquires the mutex for |key| and returns a callback that releases it, giving up and
// returning |ctx|'s cause if |ctx| is canceled before the lock can be acquired. It returns
// a nil callback with a non-nil error in that case.
//
// |ctx| governs the wait only: an uncontended lock is granted even to a canceled caller.
func (mm *MutexMap) Lock(ctx context.Context, key interface{}) (func(), error) {
	keyedMutex := mm.ref(key)

	select {
	case keyedMutex.held <- struct{}{}:
		return keyedMutex.Unlock, nil
	default:
	}

	select {
	case keyedMutex.held <- struct{}{}:
		return keyedMutex.Unlock, nil
	case <-ctx.Done():
		keyedMutex.unref()
		return nil, context.Cause(ctx)
	}
}

// ref returns the mutex for |key|, creating it if necessary, with this caller's reference
// counted. The mapMutex refcount is what keeps the mutex in the map, and it is guarded by
// |mm.mu|, just like keyedMutexes itself.
func (mm *MutexMap) ref(key interface{}) *mapMutex {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	keyedMutex, hasKey := mm.keyedMutexes[key]
	if !hasKey {
		keyedMutex = &mapMutex{parent: mm, key: key, held: make(chan struct{}, 1)}
		mm.keyedMutexes[key] = keyedMutex
	}
	keyedMutex.refcount++
	return keyedMutex
}

// unref drops one reference to |mm|, deleting it from |keyedMutex| once nobody holds or
// awaits it.
func (mm *mapMutex) unref() {
	mutexMap := mm.parent
	mutexMap.mu.Lock()
	defer mutexMap.mu.Unlock()

	mm.refcount--
	if mm.refcount < 1 {
		delete(mutexMap.keyedMutexes, mm.key)
	}
}

func (mm *mapMutex) Unlock() {
	select {
	case <-mm.held:
	default:
		// Matches sync.Mutex, which throws on unlock of an unlocked mutex.
		panic("mutexmap: release called for a mutex that is not held")
	}
	mm.unref()
}
