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

package concurrentmap

import (
	"encoding/json"
	"maps"
	"sync"
)

func New[K comparable, V any]() *Map[K, V] {
	return &Map[K, V]{m: make(map[K]V)}
}

type Map[K comparable, V any] struct {
	mu sync.RWMutex
	m  map[K]V
}

// Get returns the value for the given key. If the key does not exist, the zero value for the value type will be returned.
func (cm *Map[K, V]) Get(key K) (V, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if value, found := cm.m[key]; found {
		return value, true
	}
	var zero V
	return zero, false
}

// Set sets the value for the given key. If the key already exists, it will be overwritten.
func (cm *Map[K, V]) Set(key K, value V) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.m[key] = value
}

// Delete removes the key from the map if it exists. If the key does not exist, this is a no-op.
func (cm *Map[K, V]) Delete(key K) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	delete(cm.m, key)
}

// Len returns the number of items in the map at the time of the call.
func (cm *Map[K, V]) Len() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return len(cm.m)
}

// DeepCopy returns a deep copy of the concurrent map.
func (cm *Map[K, V]) DeepCopy() *Map[K, V] {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	newMap := make(map[K]V, len(cm.m))
	maps.Copy(newMap, cm.m)
	return &Map[K, V]{m: newMap}
}

// Iter iterates over the map, calling the provided function for each key/value pair. If the function returns false, the iteration stops.
func (cm *Map[K, V]) Iter(f func(key K, value V) bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	for k, v := range cm.m {
		if !f(k, v) {
			break
		}
	}
}

// Snapshot returns a copy of the internal map at the time of the call. Returns a native map, not a concurrent one.
func (cm *Map[K, V]) Snapshot() map[K]V {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	newMap := make(map[K]V, len(cm.m))
	maps.Copy(newMap, cm.m)
	return newMap
}

func (cm *Map[K, V]) MarshalJSON() ([]byte, error) {
	return json.Marshal(cm.Snapshot())
}

func (cm *Map[K, V]) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &cm.m)
}
