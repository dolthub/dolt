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
	"io"
)

func newOrderedMap[K, V ~[]byte, O ordering[K]](root tree.Node, ns tree.NodeStore, order O) orderedMap[K, V, O] {
	return orderedMap[K, V, O]{
		list: skip.NewSkipList(func(left, right []byte) int {
			return order.Compare(left, right)
		}),
		tree: orderedTree[K, V, O]{
			root:  root,
			ns:    ns,
			order: order,
		},
	}
}

type orderedMap[K, V ~[]byte, O ordering[K]] struct {
	list *skip.List
	tree orderedTree[K, V, O]
}

func (m orderedMap[K, V, O]) flush(ctx context.Context) (orderedTree[K, V, O], error) {
	sfn, cfn := m.tree.searchNode, m.tree.compareItems
	root, err := tree.ApplyMutations(ctx, m.tree.ns, m.tree.root, m.mutations(), sfn, cfn)
	if err != nil {
		return orderedTree[K, V, O]{}, err
	}

	return orderedTree[K, V, O]{
		root:  root,
		ns:    m.tree.ns,
		order: m.tree.order,
	}, nil
}

func (m orderedMap[K, V, O]) put(_ context.Context, key K, value V) error {
	m.list.Put(key, value)
	return nil
}

func (m orderedMap[K, V, O]) delete(_ context.Context, key K) error {
	m.list.Put(key, nil)
	return nil
}

func (m orderedMap[K, V, O]) get(ctx context.Context, key K, cb KeyValueFn[K, V]) (err error) {
	value, ok := m.list.Get(key)
	if ok {
		if value == nil {
			// there is a pending delete of |key| in |m.list|.
			key = nil
		}
		return cb(key, value)
	}

	return m.tree.get(ctx, key, cb)
}

func (m orderedMap[K, V, O]) has(ctx context.Context, key K) (present bool, err error) {
	value, ok := m.list.Get(key)
	if ok {
		present = value == nil
		return
	}

	return m.tree.has(ctx, key)
}

func (m orderedMap[K, V, O]) iterAll(ctx context.Context) (kvIter[K, V], error) {
	iter, err := m.tree.iterAll(ctx)
	if err != nil {
		return nil, err
	}
	list := orderedListIter[K, V]{iter: m.list.IterAtStart()}

	return orderedMapIter[K, V, O]{
		list:  list,
		tree:  iter,
		order: m.tree.order,
	}, nil
}

type orderedMapIter[K, V ~[]byte, O ordering[K]] struct {
	list  orderedListIter[K, V]
	tree  *orderedTreeIter[K, V]
	order O
}

// Next returns the next pair of Tuples in the Range, or io.EOF if the iter is done.
func (it orderedMapIter[K, V, O]) Next(ctx context.Context) (key K, value V, err error) {
	for {
		mk, mv := it.list.current()
		pk, pv := it.tree.current()

		if mk == nil && pk == nil {
			// range is exhausted
			return nil, nil, io.EOF
		}

		cmp := it.order.Compare(pk, mk)
		switch {
		case cmp < 0:
			key, value = pk, pv
			if err = it.tree.iterate(ctx); err != nil {
				return nil, nil, err
			}

		case cmp > 0:
			key, value = mk, mv
			if err = it.list.iterate(ctx); err != nil {
				return nil, nil, err
			}

		case cmp == 0:
			// |it.memory| wins ties
			key, value = mk, mv
			if err = it.list.iterate(ctx); err != nil {
				return nil, nil, err
			}
			if err = it.tree.iterate(ctx); err != nil {
				return nil, nil, err
			}
		}

		if key != nil && value == nil {
			continue // pending delete
		}

		return key, value, nil
	}
}

func (m orderedMap[K, V, O]) mutations() tree.MutationIter {
	return orderedListIter[K, V]{iter: m.list.IterAtStart()}
}

type orderedListIter[K, V ~[]byte] struct {
	iter *skip.ListIter
}

var _ tree.MutationIter = &orderedListIter[tree.Item, tree.Item]{}

func (it orderedListIter[K, V]) current() (K, V) {
	return it.iter.Current()
}

func (it orderedListIter[K, V]) iterate(context.Context) error {
	it.iter.Advance()
	return nil
}

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
