// Copyright 2022 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/skip"
	"github.com/dolthub/dolt/go/store/val"
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
	cfn := func(left, right tree.Item) int {
		return from.order.Compare(K(left), K(right))
	}
	differ, err := tree.DifferFromRoots(ctx, from.ns, to.ns, from.root, to.root, cfn)
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

func rangeDiffOrderedTrees[K, V ~[]byte, O ordering[K]](
	ctx context.Context,
	from, to orderedTree[K, V, O],
	rng Range,
	cb DiffFn,
) error {
	cfn := func(left, right tree.Item) int {
		return from.order.Compare(K(left), K(right))
	}

	fromStart, err := tree.NewCursorFromSearchFn(ctx, from.ns, from.root, rangeStartSearchFn(rng))
	if err != nil {
		return err
	}
	toStart, err := tree.NewCursorFromSearchFn(ctx, to.ns, to.root, rangeStartSearchFn(rng))
	if err != nil {
		return err
	}

	fromStop, err := tree.NewCursorFromSearchFn(ctx, from.ns, from.root, rangeStopSearchFn(rng))
	if err != nil {
		return err
	}
	toStop, err := tree.NewCursorFromSearchFn(ctx, to.ns, to.root, rangeStopSearchFn(rng))
	if err != nil {
		return err
	}

	differ, err := tree.DifferFromCursors(fromStart, toStart, fromStop, toStop, cfn)
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
	valDesc val.TupleDesc,
) (orderedTree[K, V, O], error) {
	cfn := func(left, right tree.Item) int {
		return base.order.Compare(K(left), K(right))
	}
	root, err := tree.ThreeWayMerge(ctx, base.ns, l.root, r.root, base.root, cfn, cb, serializer, valDesc)
	if err != nil {
		return orderedTree[K, V, O]{}, err
	}

	return orderedTree[K, V, O]{
		root:  root,
		ns:    base.ns,
		order: base.order,
	}, nil
}

func (t orderedTree[K, V, O]) count() (int, error) {
	return t.root.TreeCount()
}

func (t orderedTree[K, V, O]) height() (int, error) {
	l, err := t.root.Level()
	return l + 1, err
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

	stop := func(curr *tree.Cursor) bool {
		return curr.Compare(s) >= 0
	}

	if stop(c) {
		// empty range
		return &orderedTreeIter[K, V]{curr: nil}, nil
	}

	return &orderedTreeIter[K, V]{curr: c, stop: stop, step: c.Advance}, nil
}

func (t orderedTree[K, V, O]) iterAllReverse(ctx context.Context) (*orderedTreeIter[K, V], error) {
	beginning, err := tree.NewCursorAtStart(ctx, t.ns, t.root)
	if err != nil {
		return nil, err
	}
	err = beginning.Retreat(ctx)
	if err != nil {
		return nil, err
	}

	end, err := tree.NewCursorAtEnd(ctx, t.ns, t.root)
	if err != nil {
		return nil, err
	}

	stop := func(curr *tree.Cursor) bool {
		return curr.Compare(beginning) <= 0
	}

	if stop(end) {
		// empty range
		return &orderedTreeIter[K, V]{curr: nil}, nil
	}

	return &orderedTreeIter[K, V]{curr: end, stop: stop, step: end.Retreat}, nil
}

func (t orderedTree[K, V, O]) iterOrdinalRange(ctx context.Context, start, stop uint64) (*orderedTreeIter[K, V], error) {
	if stop == start {
		return &orderedTreeIter[K, V]{curr: nil}, nil
	}
	if stop < start {
		return nil, fmt.Errorf("invalid ordinal bounds (%d, %d)", start, stop)
	} else {
		c, err := t.count()
		if err != nil {
			return nil, err
		}
		if stop > uint64(c) {
			return nil, fmt.Errorf("stop index (%d) out of bounds", stop)
		}
	}

	lo, err := tree.NewCursorAtOrdinal(ctx, t.ns, t.root, start)
	if err != nil {
		return nil, err
	}

	hi, err := tree.NewCursorAtOrdinal(ctx, t.ns, t.root, stop)
	if err != nil {
		return nil, err
	}

	stopF := func(curr *tree.Cursor) bool {
		return curr.Compare(hi) >= 0
	}

	return &orderedTreeIter[K, V]{curr: lo, stop: stopF, step: lo.Advance}, nil
}

func (t orderedTree[K, V, O]) fetchOrdinalRange(ctx context.Context, start, stop uint64) (*orderedLeafSpanIter[K, V], error) {
	if stop == start {
		return &orderedLeafSpanIter[K, V]{}, nil
	}
	if stop < start {
		return nil, fmt.Errorf("invalid ordinal bounds (%d, %d)", start, stop)
	} else {
		c, err := t.count()
		if err != nil {
			return nil, err
		} else if stop > uint64(c) {
			return nil, fmt.Errorf("stop index (%d) out of bounds", stop)
		}
	}

	span, err := tree.FetchLeafNodeSpan(ctx, t.ns, t.root, start, stop)
	if err != nil {
		return nil, err
	}

	nd, leaves := span.Leaves[0], span.Leaves[1:]
	c, s := span.LocalStart, nd.Count()
	if len(leaves) == 0 {
		s = span.LocalStop // one leaf span
	}

	return &orderedLeafSpanIter[K, V]{
		nd:     nd,
		curr:   c,
		stop:   s,
		leaves: leaves,
		final:  span.LocalStop,
	}, nil
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

	// the function called to moved |curr| forward in the direction of iteration.
	step func(context.Context) error
	// should return |true| if the passed in cursor is past the iteration's stopping point.
	stop func(*tree.Cursor) bool
}

func (it *orderedTreeIter[K, V]) Next(ctx context.Context) (key K, value V, err error) {
	if it.curr == nil {
		return nil, nil, io.EOF
	}

	k, v := tree.CurrentCursorItems(it.curr)
	key, value = K(k), V(v)

	err = it.step(ctx)
	if err != nil {
		return nil, nil, err
	}
	if it.stop(it.curr) {
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
	err = it.step(ctx)
	if err != nil {
		return err
	}

	if it.stop(it.curr) {
		// past the end of the range
		it.curr = nil
	}

	return
}

type orderedLeafSpanIter[K, V ~[]byte] struct {
	// in-progress node
	nd tree.Node
	// current index,
	curr int
	// last index for |nd|
	stop int
	// remaining leaves
	leaves []tree.Node
	// stop index in last leaf node
	final int
}

func (s *orderedLeafSpanIter[K, V]) Next(ctx context.Context) (key K, value V, err error) {
	if s.curr >= s.stop {
		// |s.nd| exhausted
		if len(s.leaves) == 0 {
			// span exhausted
			return nil, nil, io.EOF
		}

		s.nd = s.leaves[0]
		s.curr = 0
		s.stop = s.nd.Count()

		s.leaves = s.leaves[1:]
		if len(s.leaves) == 0 {
			// |s.nd| is the last leaf
			s.stop = s.final
		}
	}

	key = K(s.nd.GetKey(s.curr))
	value = V(s.nd.GetValue(s.curr))
	s.curr++
	return
}
