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

package tree

import (
	"context"
	"fmt"
	"io"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/skip"
	"github.com/dolthub/dolt/go/store/val"
)

type KeyValueFn[K, V ~[]byte] func(key K, value V) error

type KvIter[K, V ~[]byte] interface {
	Next(ctx context.Context) (K, V, error)
}

type Ordering[K ~[]byte] interface {
	Compare(left, right K) int
}

// StaticMap is a static prolly Tree with ordered elements.
type StaticMap[K, V ~[]byte, O Ordering[K]] struct {
	Root      Node
	NodeStore NodeStore
	Order     O
}

func DiffOrderedTrees[K, V ~[]byte, O Ordering[K]](
	ctx context.Context,
	from, to StaticMap[K, V, O],
	cb DiffFn,
) error {
	cfn := func(left, right Item) int {
		return from.Order.Compare(K(left), K(right))
	}
	differ, err := DifferFromRoots(ctx, from.NodeStore, to.NodeStore, from.Root, to.Root, cfn)
	if err != nil {
		return err
	}

	for {
		var diff Diff
		if diff, err = differ.Next(ctx); err != nil {
			break
		}

		if err = cb(ctx, diff); err != nil {
			break
		}
	}
	return err
}

func DiffKeyRangeOrderedTrees[K, V ~[]byte, O Ordering[K]](
	ctx context.Context,
	from, to StaticMap[K, V, O],
	start, stop K,
	cb DiffFn,
) error {
	var fromStart, fromStop, toStart, toStop *Cursor
	var err error

	if len(start) == 0 {
		fromStart, err = NewCursorAtStart(ctx, from.NodeStore, from.Root)
		if err != nil {
			return err
		}

		toStart, err = NewCursorAtStart(ctx, to.NodeStore, to.Root)
		if err != nil {
			return err
		}
	} else {
		fromStart, err = NewCursorAtItem(ctx, from.NodeStore, from.Root, Item(start), from.searchNode)
		if err != nil {
			return err
		}

		toStart, err = NewCursorAtItem(ctx, to.NodeStore, to.Root, Item(start), to.searchNode)
		if err != nil {
			return err
		}
	}

	if len(stop) == 0 {
		fromStop, err = NewCursorPastEnd(ctx, from.NodeStore, from.Root)
		if err != nil {
			return err
		}

		toStop, err = NewCursorPastEnd(ctx, to.NodeStore, to.Root)
		if err != nil {
			return err
		}
	} else {
		fromStop, err = NewCursorAtItem(ctx, from.NodeStore, from.Root, Item(stop), from.searchNode)
		if err != nil {
			return err
		}

		toStop, err = NewCursorAtItem(ctx, to.NodeStore, to.Root, Item(stop), to.searchNode)
		if err != nil {
			return err
		}
	}

	cfn := func(left, right Item) int {
		return from.Order.Compare(K(left), K(right))
	}

	differ, err := DifferFromCursors(fromStart, toStart, fromStop, toStop, cfn)
	if err != nil {
		return err
	}

	for {
		var diff Diff
		if diff, err = differ.Next(ctx); err != nil {
			break
		}

		if err = cb(ctx, diff); err != nil {
			break
		}
	}
	return err
}

func MergeOrderedTrees[K, V ~[]byte, O Ordering[K], S message.Serializer](
	ctx context.Context,
	l, r, base StaticMap[K, V, O],
	cb CollisionFn,
	serializer S,
	valDesc val.TupleDesc,
) (StaticMap[K, V, O], error) {
	cfn := func(left, right Item) int {
		return base.Order.Compare(K(left), K(right))
	}
	root, err := ThreeWayMerge(ctx, base.NodeStore, l.Root, r.Root, base.Root, cfn, cb, serializer, valDesc)
	if err != nil {
		return StaticMap[K, V, O]{}, err
	}

	return StaticMap[K, V, O]{
		Root:      root,
		NodeStore: base.NodeStore,
		Order:     base.Order,
	}, nil
}

func (t StaticMap[K, V, O]) Count() (int, error) {
	return t.Root.TreeCount()
}

func (t StaticMap[K, V, O]) Height() (int, error) {
	l, err := t.Root.Level()
	return l + 1, err
}

func (t StaticMap[K, V, O]) HashOf() hash.Hash {
	return t.Root.HashOf()
}

func (t StaticMap[K, V, O]) Mutate() MutableMap[K, V, O] {
	return MutableMap[K, V, O]{
		Edits: skip.NewSkipList(func(left, right []byte) int {
			return t.Order.Compare(left, right)
		}),
		StaticMap: t,
	}
}

func (t StaticMap[K, V, O]) WalkAddresses(ctx context.Context, cb AddressCb) error {
	return WalkAddresses(ctx, t.Root, t.NodeStore, cb)
}

func (t StaticMap[K, V, O]) WalkNodes(ctx context.Context, cb NodeCb) error {
	return WalkNodes(ctx, t.Root, t.NodeStore, cb)
}

func (t StaticMap[K, V, O]) Get(ctx context.Context, query K, cb KeyValueFn[K, V]) (err error) {
	cur, err := NewLeafCursorAtItem(ctx, t.NodeStore, t.Root, Item(query), t.searchNode)
	if err != nil {
		return err
	}

	var key K
	var value V

	if cur.Valid() {
		key = K(cur.CurrentKey())
		if t.Order.Compare(query, key) == 0 {
			value = V(cur.CurrentValue())
		} else {
			key = nil
		}
	}
	return cb(key, value)
}

func (t StaticMap[K, V, O]) Has(ctx context.Context, query K) (ok bool, err error) {
	cur, err := NewLeafCursorAtItem(ctx, t.NodeStore, t.Root, Item(query), t.searchNode)
	if err != nil {
		return false, err
	}

	if cur.Valid() {
		ok = t.Order.Compare(query, K(cur.CurrentKey())) == 0
	}

	return
}

func (t StaticMap[K, V, O]) Last(ctx context.Context) (key K, value V, err error) {
	cur, err := NewCursorAtEnd(ctx, t.NodeStore, t.Root)
	if err != nil {
		return nil, nil, err
	}

	if cur.Valid() {
		key, value = K(cur.CurrentKey()), V(cur.CurrentValue())
	}
	return
}

func (t StaticMap[K, V, O]) IterAll(ctx context.Context) (*OrderedTreeIter[K, V], error) {
	c, err := NewCursorAtStart(ctx, t.NodeStore, t.Root)
	if err != nil {
		return nil, err
	}

	s, err := NewCursorPastEnd(ctx, t.NodeStore, t.Root)
	if err != nil {
		return nil, err
	}

	stop := func(curr *Cursor) bool {
		return curr.Compare(s) >= 0
	}

	if stop(c) {
		// empty range
		return &OrderedTreeIter[K, V]{curr: nil}, nil
	}

	return &OrderedTreeIter[K, V]{curr: c, stop: stop, step: c.Advance}, nil
}

func (t StaticMap[K, V, O]) IterAllReverse(ctx context.Context) (*OrderedTreeIter[K, V], error) {
	beginning, err := NewCursorAtStart(ctx, t.NodeStore, t.Root)
	if err != nil {
		return nil, err
	}
	err = beginning.Retreat(ctx)
	if err != nil {
		return nil, err
	}

	end, err := NewCursorAtEnd(ctx, t.NodeStore, t.Root)
	if err != nil {
		return nil, err
	}

	stop := func(curr *Cursor) bool {
		return curr.Compare(beginning) <= 0
	}

	if stop(end) {
		// empty range
		return &OrderedTreeIter[K, V]{curr: nil}, nil
	}

	return &OrderedTreeIter[K, V]{curr: end, stop: stop, step: end.Retreat}, nil
}

func (t StaticMap[K, V, O]) IterOrdinalRange(ctx context.Context, start, stop uint64) (*OrderedTreeIter[K, V], error) {
	if stop == start {
		return &OrderedTreeIter[K, V]{curr: nil}, nil
	}
	if stop < start {
		return nil, fmt.Errorf("invalid ordinal bounds (%d, %d)", start, stop)
	} else {
		c, err := t.Count()
		if err != nil {
			return nil, err
		}
		if stop > uint64(c) {
			return nil, fmt.Errorf("stop index (%d) out of bounds", stop)
		}
	}

	lo, err := NewCursorAtOrdinal(ctx, t.NodeStore, t.Root, start)
	if err != nil {
		return nil, err
	}

	hi, err := NewCursorAtOrdinal(ctx, t.NodeStore, t.Root, stop)
	if err != nil {
		return nil, err
	}

	stopF := func(curr *Cursor) bool {
		return curr.Compare(hi) >= 0
	}

	return &OrderedTreeIter[K, V]{curr: lo, stop: stopF, step: lo.Advance}, nil
}

func (t StaticMap[K, V, O]) FetchOrdinalRange(ctx context.Context, start, stop uint64) (*orderedLeafSpanIter[K, V], error) {
	if stop == start {
		return &orderedLeafSpanIter[K, V]{}, nil
	}
	if stop < start {
		return nil, fmt.Errorf("invalid ordinal bounds (%d, %d)", start, stop)
	} else {
		c, err := t.Count()
		if err != nil {
			return nil, err
		} else if stop > uint64(c) {
			return nil, fmt.Errorf("stop index (%d) out of bounds", stop)
		}
	}

	span, err := FetchLeafNodeSpan(ctx, t.NodeStore, t.Root, start, stop)
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

func (t StaticMap[K, V, O]) IterKeyRange(ctx context.Context, start, stop K) (*OrderedTreeIter[K, V], error) {
	lo, hi, err := t.getKeyRangeCursors(ctx, start, stop)
	if err != nil {
		return nil, err
	}

	stopF := func(curr *Cursor) bool {
		return curr.Compare(hi) >= 0
	}

	if stopF(lo) {
		return &OrderedTreeIter[K, V]{curr: nil}, nil
	}

	return &OrderedTreeIter[K, V]{curr: lo, stop: stopF, step: lo.Advance}, nil
}

func (t StaticMap[K, V, O]) GetKeyRangeCardinality(ctx context.Context, start, stop K) (uint64, error) {
	lo, hi, err := t.getKeyRangeCursors(ctx, start, stop)
	if err != nil {
		return 0, err
	}

	startOrd, err := GetOrdinalOfCursor(lo)
	if err != nil {
		return 0, err
	}

	endOrd, err := GetOrdinalOfCursor(hi)
	if err != nil {
		return 0, err
	}

	if startOrd > endOrd {
		return 0, nil
	}

	return endOrd - startOrd, nil
}

func (t StaticMap[K, V, O]) getKeyRangeCursors(ctx context.Context, startInclusive, stopExclusive K) (lo, hi *Cursor, err error) {
	if len(startInclusive) == 0 {
		lo, err = NewCursorAtStart(ctx, t.NodeStore, t.Root)
		if err != nil {
			return nil, nil, err
		}
	} else {
		lo, err = NewCursorAtItem(ctx, t.NodeStore, t.Root, Item(startInclusive), t.searchNode)
		if err != nil {
			return nil, nil, err
		}
	}

	if len(stopExclusive) == 0 {
		hi, err = NewCursorPastEnd(ctx, t.NodeStore, t.Root)
		if err != nil {
			return nil, nil, err
		}
	} else {
		hi, err = NewCursorAtItem(ctx, t.NodeStore, t.Root, Item(stopExclusive), t.searchNode)
		if err != nil {
			return nil, nil, err
		}
	}

	return
}

// searchNode returns the smallest index where nd[i] >= query
// Adapted from search.Sort to inline comparison.
func (t StaticMap[K, V, O]) searchNode(query Item, nd Node) int {
	n := int(nd.Count())
	// Define f(-1) == false and f(n) == true.
	// Invariant: f(i-1) == false, f(j) == true.
	i, j := 0, n
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		less := t.Order.Compare(K(query), K(nd.GetKey(h))) <= 0
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

func (t StaticMap[K, V, O]) CompareItems(left, right Item) int {
	return t.Order.Compare(K(left), K(right))
}

// getOrdinalForKey returns the smallest ordinal position at which the key >= |query|.
func (t StaticMap[K, V, O]) GetOrdinalForKey(ctx context.Context, query K) (uint64, error) {
	cur, err := NewCursorAtItem(ctx, t.NodeStore, t.Root, Item(query), t.searchNode)
	if err != nil {
		return 0, err
	}

	return GetOrdinalOfCursor(cur)
}

var _ ItemSearchFn = StaticMap[Item, Item, Ordering[Item]]{}.searchNode
var _ CompareFn = StaticMap[Item, Item, Ordering[Item]]{}.CompareItems

type OrderedTreeIter[K, V ~[]byte] struct {
	// current tuple location
	curr *Cursor

	// the function called to moved |curr| forward in the direction of iteration.
	step func(context.Context) error
	// should return |true| if the passed in cursor is past the iteration's stopping point.
	stop func(*Cursor) bool
}

func OrderedTreeIterFromCursors[K, V ~[]byte](start, stop *Cursor) *OrderedTreeIter[K, V] {
	stopF := func(curr *Cursor) bool {
		return curr.Compare(stop) >= 0
	}

	if stopF(start) {
		start = nil // empty range
	}

	return &OrderedTreeIter[K, V]{curr: start, stop: stopF, step: start.Advance}
}

func (it *OrderedTreeIter[K, V]) Next(ctx context.Context) (key K, value V, err error) {
	if it.curr == nil {
		return nil, nil, io.EOF
	}

	k, v := CurrentCursorItems(it.curr)
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

func (it *OrderedTreeIter[K, V]) Current() (key K, value V) {
	// |it.curr| is set to nil when its range is exhausted
	if it.curr != nil && it.curr.Valid() {
		k, v := CurrentCursorItems(it.curr)
		key, value = K(k), V(v)
	}
	return
}

func (it *OrderedTreeIter[K, V]) Iterate(ctx context.Context) (err error) {
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
	nd Node
	// current index,
	curr int
	// last index for |nd|
	stop int
	// remaining leaves
	leaves []Node
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
