// Copyright 2026 Dolthub, Inc.
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

package dsess

import "sync"

// SyncMap is a simple generic wrapper around sync.Map, designed for type safety around callsites.
// Using this type instead of sync.Map removes the need for casts.
type SyncMap[Key any, Value any] struct {
	m sync.Map
}

func (a *SyncMap[Key, Value]) Store(key Key, val Value) {
	a.m.Store(key, val)
}

func (a *SyncMap[Key, Value]) Load(key Key) (val Value, ok bool) {
	v, ok := a.m.Load(key)
	if !ok {
		return val, ok
	}
	return v.(Value), ok
}

func (a *SyncMap[Key, Value]) Delete(key Key) {
	a.m.Delete(key)
}

func (a *SyncMap[Key, Value]) LoadOrStore(key Key, val Value) (actual Value, loaded bool) {
	act, loaded := a.m.LoadOrStore(key, val)
	if !loaded {
		return actual, loaded
	}
	return act.(Value), loaded
}

func (a *SyncMap[Key, Value]) CompareAndSwap(key Key, old Value, new Value) (swapped bool) {
	return a.m.CompareAndSwap(key, old, new)
}
