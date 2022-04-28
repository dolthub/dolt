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

package prolly

import (
	"context"

	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/skip"
)

// orderedMap is a mutable prolly tree with ordered elements.
type orderedMap[K, V ~[]byte, O ordering[K]] struct {
	edits *skip.List
	tree  orderedTree[K, V, O]
}

func (m orderedMap[K, V, O]) put(_ context.Context, key K, value V) error {
	m.edits.Put(key, value)
	return nil
}

func (m orderedMap[K, V, O]) delete(_ context.Context, key K) error {
	m.edits.Put(key, nil)
	return nil
}

func (m orderedMap[K, V, O]) get(ctx context.Context, key K, cb KeyValueFn[K, V]) (err error) {
	value, ok := m.edits.Get(key)
	if ok {
		if value == nil {
			// there is a pending delete of |key| in |m.edits|.
			key = nil
		}
		return cb(key, value)
	}

	return m.tree.get(ctx, key, cb)
}

func (m orderedMap[K, V, O]) has(ctx context.Context, key K) (present bool, err error) {
	value, ok := m.edits.Get(key)
	if ok {
		present = value != nil
		return
	}
	return m.tree.has(ctx, key)
}

func (m orderedMap[K, V, O]) mutations() tree.MutationIter {
	return orderedListIter[K, V]{iter: m.edits.IterAtStart()}
}

type orderedListIter[K, V ~[]byte] struct {
	iter *skip.ListIter
}

var _ tree.MutationIter = &orderedListIter[tree.Item, tree.Item]{}

func (it orderedListIter[K, V]) NextMutation(context.Context) (tree.Item, tree.Item) {
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
