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

package tree

import (
	"context"

	"github.com/dolthub/dolt/go/store/skip"
)

// MutableMap is a mutable prolly Static with ordered elements.
type MutableMap[K, V ~[]byte, O Ordering[K]] struct {
	Edits  *skip.List
	Static StaticMap[K, V, O]
}

func (m MutableMap[K, V, O]) Put(_ context.Context, key K, value V) error {
	m.Edits.Put(key, value)
	return nil
}

func (m MutableMap[K, V, O]) Delete(_ context.Context, key K) error {
	m.Edits.Put(key, nil)
	return nil
}

func (m MutableMap[K, V, O]) Get(ctx context.Context, key K, cb KeyValueFn[K, V]) (err error) {
	value, ok := m.Edits.Get(key)
	if ok {
		if value == nil {
			// there is a pending delete of |key| in |m.Edits|.
			key = nil
		}
		return cb(key, value)
	}

	return m.Static.Get(ctx, key, cb)
}

func (m MutableMap[K, V, O]) Has(ctx context.Context, key K) (present bool, err error) {
	value, ok := m.Edits.Get(key)
	if ok {
		present = value != nil
		return
	}
	return m.Static.Has(ctx, key)
}

func (m MutableMap[K, V, O]) Copy() MutableMap[K, V, O] {
	return MutableMap[K, V, O]{
		Edits:  m.Edits.Copy(),
		Static: m.Static,
	}
}

func (m MutableMap[K, V, O]) Mutations() MutationIter {
	return orderedListIter[K, V]{iter: m.Edits.IterAtStart()}
}

type orderedListIter[K, V ~[]byte] struct {
	iter *skip.ListIter
}

var _ MutationIter = &orderedListIter[Item, Item]{}

func (it orderedListIter[K, V]) NextMutation(context.Context) (Item, Item) {
	k, v := it.iter.Current()
	if k == nil {
		return nil, nil
	}
	it.iter.Advance()
	return k, v
}

func (it orderedListIter[K, V]) Close() error {
	return nil
}
