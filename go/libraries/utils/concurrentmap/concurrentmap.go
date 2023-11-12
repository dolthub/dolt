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
	"sync"
)

func New[K comparable, V any]() *Map[K, V] {
	return &Map[K, V]{m: make(map[K]V)}
}

type Map[K comparable, V any] struct {
	mu sync.RWMutex
	m  map[K]V
}

func (cm *Map[K, V]) Get(key K) (V, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if value, found := cm.m[key]; found {
		return value, true
	}
	var zero V
	return zero, false
}

func (cm *Map[K, V]) Set(key K, value V) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.m[key] = value
}

func (cm *Map[K, V]) Delete(key K) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	delete(cm.m, key)
}

func (cm *Map[K, V]) Len() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return len(cm.m)
}

func (cm *Map[K, V]) DeepCopy() *Map[K, V] {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	newMap := make(map[K]V, len(cm.m))
	for k, v := range cm.m {
		newMap[k] = v
	}
	return &Map[K, V]{m: newMap}
}

func (cm *Map[K, V]) Range(f func(key K, value V) bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	for k, v := range cm.m {
		if !f(k, v) {
			break
		}
	}
}

func (cm *Map[K, V]) Snapshot() map[K]V {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	newMap := make(map[K]V, len(cm.m))
	for k, v := range cm.m {
		newMap[k] = v
	}
	return newMap
}
