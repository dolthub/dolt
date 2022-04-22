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

	"github.com/dolthub/dolt/go/store/hash"
)

type KeyValueFn[K, V ~[]byte] func(key K, value V) error

func diffOrderedTrees[K, V ~[]byte, O ordering[K]](
	ctx context.Context,
	from, to orderedTree[K, V, O],
	cb DiffFn,
) error {
	differ, err := tree.DifferFromRoots(ctx, from.ns, from.root, to.root, to.compareItems)
	if err != nil {
		return err
	}

	for {
		var diff tree.Diff
		if diff, err = differ.Next(ctx); err != nil {
			break
		}

		if err = cb(ctx, diff); err != nil {
			break
		}
	}
	return err
}

func mergeOrderedTrees[K, V ~[]byte, O ordering[K]](
	ctx context.Context,
	l, r, base orderedTree[K, V, O],
	cb tree.CollisionFn,
) (orderedTree[K, V, O], error) {
	sfn, cfn := base.searchNode, base.compareItems

	root, err := tree.ThreeWayMerge(ctx, base.ns, l.root, r.root, base.root, sfn, cfn, cb)
	if err != nil {
		return orderedTree[K, V, O]{}, err
	}

	return orderedTree[K, V, O]{
		root:  root,
		ns:    base.ns,
		order: base.order,
	}, nil
}

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

type ordering[K ~[]byte] interface {
	Compare(left, right K) int
}

type orderedIter[K, V ~[]byte] interface {
	Next(ctx context.Context) (K, V, error)
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

func (m orderedMap[K, V, O]) mutations() tree.MutationIter {
	return orderedListIter{iter: m.list.IterAtStart()}
}

type orderedListIter struct {
	iter *skip.ListIter
}

var _ tree.MutationIter = &orderedListIter{}

func (it orderedListIter) NextMutation(context.Context) (tree.Item, tree.Item) {
	k, v := it.iter.Current()
	if k == nil {
		return nil, nil
	}
	it.iter.Advance()
	return k, v
}

func (it orderedListIter) Close() error {
	return nil
}

func newOrderedTree[K, V ~[]byte, O ordering[K]](root tree.Node, ns tree.NodeStore, order O) orderedTree[K, V, O] {
	return orderedTree[K, V, O]{
		root:  root,
		ns:    ns,
		order: order,
	}
}

type orderedTree[K, V ~[]byte, O ordering[K]] struct {
	root  tree.Node
	ns    tree.NodeStore
	order O
}

type orderedTreeIter[K, V ~[]byte] struct {
	// current tuple location
	curr *tree.Cursor
	// non-inclusive range stop
	stop *tree.Cursor
}

func (it *orderedTreeIter[K, V]) Next(ctx context.Context) (K, V, error) {
	if it.curr == nil {
		return nil, nil, io.EOF
	}

	k, v := tree.CurrentCursorItems(it.curr)

	if _, err := it.curr.Advance(ctx); err != nil {
		return nil, nil, err
	}
	if it.curr.Compare(it.stop) >= 0 {
		// past the end of the range
		it.curr = nil
	}

	return K(k), V(v), nil
}

func (t orderedTree[K, V, O]) count() int {
	return t.root.TreeCount()
}

func (t orderedTree[K, V, O]) height() int {
	return t.root.Level() + 1
}

func (t orderedTree[K, V, O]) hashOf() hash.Hash {
	return t.root.HashOf()
}

func (t orderedTree[K, V, O]) mutate() orderedMap[K, V, O] {
	return orderedMap[K, V, O]{
		list: skip.NewSkipList(func(left, right []byte) int {
			return t.order.Compare(left, right)
		}),
		tree: t,
	}
}

func (t orderedTree[K, V, O]) walkAddresses(ctx context.Context, cb tree.AddressCb) error {
	return tree.WalkAddresses(ctx, t.root, t.ns, cb)
}

func (t orderedTree[K, V, O]) walkNodes(ctx context.Context, cb tree.NodeCb) error {
	return tree.WalkNodes(ctx, t.root, t.ns, cb)
}

func (t orderedTree[K, V, O]) get(ctx context.Context, query K, cb KeyValueFn[K, V]) (err error) {
	cur, err := tree.NewLeafCursorAtItem(ctx, t.ns, t.root, tree.Item(query), t.searchNode)
	if err != nil {
		return err
	}

	var key K
	var value V

	if cur.Valid() {
		key = K(cur.CurrentKey())
		if t.order.Compare(query, key) == 0 {
			value = V(cur.CurrentValue())
		} else {
			key = nil
		}
	}
	return cb(key, value)
}

func (t orderedTree[K, V, O]) has(ctx context.Context, query K) (ok bool, err error) {
	cur, err := tree.NewLeafCursorAtItem(ctx, t.ns, t.root, tree.Item(query), t.searchNode)
	if err != nil {
		return false, err
	}

	if cur.Valid() {
		ok = t.order.Compare(query, K(cur.CurrentKey())) == 0
	}

	return
}

func (t orderedTree[K, V, O]) last(ctx context.Context) (key K, value V, err error) {
	cur, err := tree.NewCursorAtEnd(ctx, t.ns, t.root)
	if err != nil {
		return nil, nil, err
	}

	if cur.Valid() {
		key, value = K(cur.CurrentKey()), V(cur.CurrentValue())
	}
	return
}

func (t orderedTree[K, V, O]) iterAll(ctx context.Context) (orderedIter[K, V], error) {
	c, err := tree.NewCursorAtStart(ctx, t.ns, t.root)
	if err != nil {
		return nil, err
	}

	s, err := tree.NewCursorPastEnd(ctx, t.ns, t.root)
	if err != nil {
		return nil, err
	}
	return &orderedTreeIter[K, V]{curr: c, stop: s}, nil
}

// searchNode returns the smallest index where nd[i] >= query
// Adapted from search.Sort to inline comparison.
func (t orderedTree[K, V, O]) searchNode(query tree.Item, nd tree.Node) int {
	n := int(nd.Count())
	// Define f(-1) == false and f(n) == true.
	// Invariant: f(i-1) == false, f(j) == true.
	i, j := 0, n
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		less := t.order.Compare(K(query), K(nd.GetKey(h))) <= 0
		// i ≤ h < j
		if !less {
			i = h + 1 // preserves f(i-1) == false
		} else {
			j = h // preserves f(j) == true
		}
	}
	// i == j, f(i-1) == false, and
	// f(j) (= f(i)) == true  =>  answer is i.
	return i
}

func (t orderedTree[K, V, O]) compareItems(left, right tree.Item) int {
	return t.order.Compare(K(left), K(right))
}

//var _ tree.ItemSearchFn = orderedTree[K, V, O]{}.searchNode
//var _ tree.CompareFn = orderedTree[K, V, O]{}.compareItems
