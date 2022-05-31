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
	"fmt"
	"io"

	"github.com/dolthub/dolt/go/store/prolly/message"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/skip"
)

type KeyValueFn[K, V ~[]byte] func(key K, value V) error

type kvIter[K, V ~[]byte] interface {
	Next(ctx context.Context) (K, V, error)
}

type ordering[K ~[]byte] interface {
	Compare(left, right K) int
}

// orderedTree is a static prolly tree with ordered elements.
type orderedTree[K, V ~[]byte, O ordering[K]] struct {
	root  tree.Node
	ns    tree.NodeStore
	order O
}

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

func mergeOrderedTrees[K, V ~[]byte, O ordering[K], S message.Serializer](
	ctx context.Context,
	l, r, base orderedTree[K, V, O],
	cb tree.CollisionFn,
	serializer S,
) (orderedTree[K, V, O], error) {
	cfn := base.compareItems
	root, err := tree.ThreeWayMerge(ctx, base.ns, l.root, r.root, base.root, cfn, cb, serializer)
	if err != nil {
		return orderedTree[K, V, O]{}, err
	}

	return orderedTree[K, V, O]{
		root:  root,
		ns:    base.ns,
		order: base.order,
	}, nil
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
		edits: skip.NewSkipList(func(left, right []byte) int {
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

func (t orderedTree[K, V, O]) iterAll(ctx context.Context) (*orderedTreeIter[K, V], error) {
	c, err := tree.NewCursorAtStart(ctx, t.ns, t.root)
	if err != nil {
		return nil, err
	}

	s, err := tree.NewCursorPastEnd(ctx, t.ns, t.root)
	if err != nil {
		return nil, err
	}

	if c.Compare(s) >= 0 {
		c = nil // empty range
	}

	return &orderedTreeIter[K, V]{curr: c, stop: s}, nil
}

func (t orderedTree[K, V, O]) iterOrdinalRange(ctx context.Context, start, stop uint64) (*orderedTreeIter[K, V], error) {
	if stop == start {
		return &orderedTreeIter[K, V]{curr: nil}, nil
	}
	if stop < start {
		return nil, fmt.Errorf("invalid ordinal bounds (%d, %d)", start, stop)
	} else if stop > uint64(t.count()) {
		return nil, fmt.Errorf("stop index (%d) out of bounds", stop)
	}

	lo, err := tree.NewCursorAtOrdinal(ctx, t.ns, t.root, start)
	if err != nil {
		return nil, err
	}

	hi, err := tree.NewCursorAtOrdinal(ctx, t.ns, t.root, stop)
	if err != nil {
		return nil, err
	}

	return &orderedTreeIter[K, V]{curr: lo, stop: hi}, nil
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

var _ tree.ItemSearchFn = orderedTree[tree.Item, tree.Item, ordering[tree.Item]]{}.searchNode
var _ tree.CompareFn = orderedTree[tree.Item, tree.Item, ordering[tree.Item]]{}.compareItems

type orderedTreeIter[K, V ~[]byte] struct {
	// current tuple location
	curr *tree.Cursor
	// non-inclusive range stop
	stop *tree.Cursor
}

func (it *orderedTreeIter[K, V]) Next(ctx context.Context) (key K, value V, err error) {
	if it.curr == nil {
		return nil, nil, io.EOF
	}

	k, v := tree.CurrentCursorItems(it.curr)
	key, value = K(k), V(v)

	err = it.curr.Advance(ctx)
	if err != nil {
		return nil, nil, err
	}
	if it.curr.Compare(it.stop) >= 0 {
		// past the end of the range
		it.curr = nil
	}

	return
}

func (it *orderedTreeIter[K, V]) current() (key K, value V) {
	// |it.curr| is set to nil when its range is exhausted
	if it.curr != nil && it.curr.Valid() {
		k, v := tree.CurrentCursorItems(it.curr)
		key, value = K(k), V(v)
	}
	return
}

func (it *orderedTreeIter[K, V]) iterate(ctx context.Context) (err error) {
	err = it.curr.Advance(ctx)
	if err != nil {
		return err
	}

	if it.curr.Compare(it.stop) >= 0 {
		// past the end of the range
		it.curr = nil
	}

	return
}
