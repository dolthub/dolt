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

import (
	"context"
	"sync"

	"github.com/dolthub/dolt/go/store/types"
)

func newMemoryProllyStore(ctx context.Context) keyValStore {
	return newProllyStore(ctx, types.NewMemoryValueStore())
}

func newProllyStore(ctx context.Context, vrw types.ValueReadWriter) keyValStore {
	m, err := types.NewMap(ctx, vrw)
	if err != nil {
		panic(err)
	}
	return &prollyStore{
		store:  m,
		editor: types.NewMapEditor(m),
	}
}

type prollyStore struct {
	store  types.Map
	editor *types.MapEditor
	mu     sync.RWMutex
}

var _ keyValStore = &prollyStore{}

func (m *prollyStore) get(key []byte) (val []byte, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	m.flush()

	ctx := context.Background()
	v, ok, err := m.store.MaybeGet(ctx, types.String(key))
	if err != nil {
		panic(err)
	}

	val = []byte(v.(types.String))
	return val, ok
}

func (m *prollyStore) put(key, val []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.load(key, val)
	m.flush()
}

func (m *prollyStore) delete(key []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.load(key, nil)
	m.flush()
}

func (m *prollyStore) load(key, val []byte) {
	k := types.String(key)
	v := types.Value(nil)
	if val != nil {
		v = types.String(val)
	}
	m.editor.Set(k, v)
}

func (m *prollyStore) flush() {
	if m.editor.NumEdits() == 0 {
		return
	}

	var err error
	ctx := context.Background()
	m.store, err = m.editor.Map(ctx)
	if err != nil {
		panic(err)
	}
	m.editor = types.NewMapEditor(m.store)
}
