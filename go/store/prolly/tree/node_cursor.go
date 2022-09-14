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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package tree

import (
	"context"
	"errors"
	"fmt"

	"github.com/dolthub/dolt/go/store/hash"
)

// Cursor explores a tree of Nodes.
type Cursor struct {
	nd     Node
	idx    int
	parent *Cursor
	nrw    NodeStore
}

type SearchFn func(nd Node) (idx int)

type Ordering[K ~[]byte] interface {
	Compare(left, right K) int
}

func NewCursorAtStart(ctx context.Context, ns NodeStore, nd Node) (cur *Cursor, err error) {
	cur = &Cursor{nd: nd, nrw: ns}
	var leaf bool
	leaf, err = cur.isLeaf()
	if err != nil {
		return nil, err
	}
	for !leaf {
		nd, err = fetchChild(ctx, ns, cur.CurrentRef())
		if err != nil {
			return nil, err
		}

		parent := cur
		cur = &Cursor{nd: nd, parent: parent, nrw: ns}
		leaf, err = cur.isLeaf()
		if err != nil {
			return nil, err
		}
	}
	return
}

func NewCursorAtEnd(ctx context.Context, ns NodeStore, nd Node) (cur *Cursor, err error) {
	cur = &Cursor{nd: nd, nrw: ns}
	cur.skipToNodeEnd()

	var leaf bool
	leaf, err = cur.isLeaf()
	if err != nil {
		return nil, err
	}
	for !leaf {
		nd, err = fetchChild(ctx, ns, cur.CurrentRef())
		if err != nil {
			return nil, err
		}

		parent := cur
		cur = &Cursor{nd: nd, parent: parent, nrw: ns}
		cur.skipToNodeEnd()
		leaf, err = cur.isLeaf()
		if err != nil {
			return nil, err
		}
	}
	return
}

func NewCursorPastEnd(ctx context.Context, ns NodeStore, nd Node) (cur *Cursor, err error) {
	cur, err = NewCursorAtEnd(ctx, ns, nd)
	if err != nil {
		return nil, err
	}

	// Advance |cur| past the end
	err = cur.Advance(ctx)
	if err != nil {
		return nil, err
	}
	if cur.idx != int(cur.nd.count) {
		panic("expected |ok| to be  false")
	}

	return
}

func NewCursorAtOrdinal(ctx context.Context, ns NodeStore, nd Node, ord uint64) (cur *Cursor, err error) {
	cnt, err := nd.TreeCount()
	if err != nil {
		return nil, err
	}
	if ord >= uint64(cnt) {
		return NewCursorPastEnd(ctx, ns, nd)
	}

	distance := int64(ord)
	return NewCursorFromSearchFn(ctx, ns, nd, func(nd Node) (idx int) {
		if nd.IsLeaf() {
			return int(distance)
		}
		nd, _ = nd.loadSubtrees()

		for idx = 0; idx < nd.Count(); idx++ {
			cnt, _ := nd.getSubtreeCount(idx)
			card := int64(cnt)
			if (distance - card) < 0 {
				break
			}
			distance -= card
		}
		return
	})
}

// GetOrdinalOfCursor returns the ordinal position of a Cursor.
func GetOrdinalOfCursor(curr *Cursor) (ord uint64, err error) {
	leaf, err := curr.isLeaf()
	if err != nil {
		return 0, err
	}
	if !leaf {
		return 0, fmt.Errorf("|cur| must be at a leaf")
	}

	ord += uint64(curr.idx)

	for curr.parent != nil {
		curr = curr.parent

		// If a parent has been invalidated past end, act like we were at the
		// last subtree.
		if curr.idx >= curr.nd.Count() {
			curr.skipToNodeEnd()
		} else if curr.idx < 0 {
			return 0, fmt.Errorf("found invalid parent cursor behind node start")
		}

		curr.nd, err = curr.nd.loadSubtrees()
		if err != nil {
			return 0, err
		}

		for idx := curr.idx - 1; idx >= 0; idx-- {
			cnt, err := curr.nd.getSubtreeCount(idx)
			if err != nil {
				return 0, err
			}
			ord += cnt
		}
	}

	return ord, nil
}

func NewCursorAtKey[K ~[]byte, O Ordering[K]](ctx context.Context, ns NodeStore, nd Node, key K, order O) (cur *Cursor, err error) {
	return NewCursorFromSearchFn(ctx, ns, nd, SearchForKey(key, order))
}

func NewCursorFromSearchFn(ctx context.Context, ns NodeStore, nd Node, search SearchFn) (cur *Cursor, err error) {
	cur = &Cursor{nd: nd, nrw: ns}

	cur.idx = search(cur.nd)
	var leaf bool
	leaf, err = cur.isLeaf()
	if err != nil {
		return nil, err
	}
	for !leaf {

		// stay in bounds for internal nodes
		cur.keepInBounds()

		nd, err = fetchChild(ctx, ns, cur.CurrentRef())
		if err != nil {
			return cur, err
		}

		parent := cur
		cur = &Cursor{nd: nd, parent: parent, nrw: ns}

		cur.idx = search(cur.nd)
		leaf, err = cur.isLeaf()
		if err != nil {
			return nil, err
		}
	}

	return
}

func NewLeafCursorAtKey[K ~[]byte, O Ordering[K]](ctx context.Context, ns NodeStore, nd Node, key K, order O) (Cursor, error) {
	cur := Cursor{nd: nd, nrw: ns}
	for {
		// binary search |cur.nd| for |key|
		i, j := 0, cur.nd.Count()
		for i < j {
			h := int(uint(i+j) >> 1)
			cmp := order.Compare(key, K(cur.nd.GetKey(h)))
			if cmp > 0 {
				i = h + 1
			} else {
				j = h
			}
		}
		cur.idx = i

		leaf, err := cur.isLeaf()
		if err != nil {
			return cur, err
		} else if leaf {
			break // done
		}

		// stay in bounds for internal nodes
		cur.keepInBounds()

		// reuse |cur| object to keep stack alloc'd
		cur.nd, err = fetchChild(ctx, ns, cur.CurrentRef())
		if err != nil {
			return cur, err
		}
	}
	return cur, nil
}

// SearchForKey returns a SearchFn for |key|.
func SearchForKey[K ~[]byte, O Ordering[K]](key K, order O) SearchFn {
	return func(nd Node) (idx int) {
		n := int(nd.Count())
		// Define f(-1) == false and f(n) == true.
		// Invariant: f(i-1) == false, f(j) == true.
		i, j := 0, n
		for i < j {
			h := int(uint(i+j) >> 1) // avoid overflow when computing h
			less := order.Compare(key, K(nd.GetKey(h))) <= 0
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
}

type LeafSpan struct {
	Leaves     []Node
	LocalStart int
	LocalStop  int
}

// FetchLeafNodeSpan returns the leaf Node span for the ordinal range [start, stop). It fetches the span using
// an eager breadth-first search and makes batch read calls to the persistence layer via NodeStore.ReadMany.
func FetchLeafNodeSpan(ctx context.Context, ns NodeStore, root Node, start, stop uint64) (LeafSpan, error) {
	leaves, localStart, err := fetchLeafNodeSpan(ctx, ns, []Node{root}, start, stop)
	if err != nil {
		return LeafSpan{}, err
	}

	localStop := (stop - start) + localStart
	for i := 0; i < len(leaves)-1; i++ {
		localStop -= uint64(leaves[i].Count())
	}

	return LeafSpan{
		Leaves:     leaves,
		LocalStart: int(localStart),
		LocalStop:  int(localStop),
	}, nil
}

func fetchLeafNodeSpan(ctx context.Context, ns NodeStore, nodes []Node, start, stop uint64) ([]Node, uint64, error) {
	if nodes[0].IsLeaf() {
		// verify leaf homogeneity
		for i := range nodes {
			if !nodes[i].IsLeaf() {
				return nil, 0, errors.New("mixed leaf/non-leaf set")
			}
		}
		return nodes, start, nil
	}

	gets := make(hash.HashSlice, 0, len(nodes)*nodes[0].Count())
	acc := uint64(0)

	var err error
	for _, nd := range nodes {
		if nd, err = nd.loadSubtrees(); err != nil {
			return nil, 0, err
		}

		for i := 0; i < nd.Count(); i++ {
			card, err := nd.getSubtreeCount(i)
			if err != nil {
				return nil, 0, err
			}

			if acc == 0 && card < start {
				start -= card
				stop -= card
				continue
			}

			gets = append(gets, hash.New(nd.GetValue(i)))
			acc += card
			if acc >= stop {
				break
			}
		}
	}

	children, err := ns.ReadMany(ctx, gets)
	if err != nil {
		return nil, 0, err
	}
	return fetchLeafNodeSpan(ctx, ns, children, start, stop)
}

func CurrentCursorItems(cur *Cursor) (key, value Item) {
	key = cur.nd.keys.GetItem(cur.idx, cur.nd.msg)
	value = cur.nd.values.GetItem(cur.idx, cur.nd.msg)
	return
}

// Seek updates the cursor's node to one whose range spans the key's value, or the last
// node if the key is greater than all existing keys.
// If a node does not contain the key, we recurse upwards to the parent cursor. If the
// node contains a key, we recurse downwards into child nodes.
func Seek[K ~[]byte, O Ordering[K]](ctx context.Context, cur *Cursor, key K, order O) (err error) {
	inBounds := true
	if cur.parent != nil {
		inBounds = inBounds && order.Compare(key, K(cur.firstKey())) >= 0
		inBounds = inBounds && order.Compare(key, K(cur.lastKey())) <= 0
	}

	if !inBounds {
		// |item| is outside the bounds of |cur.nd|, search up the tree
		err = Seek(ctx, cur.parent, key, order)
		if err != nil {
			return err
		}
		// stay in bounds for internal nodes
		cur.parent.keepInBounds()

		cur.nd, err = fetchChild(ctx, cur.nrw, cur.parent.CurrentRef())
		if err != nil {
			return err
		}
	}

	cur.idx = SearchForKey(key, order)(cur.nd)

	return
}

func (cur *Cursor) Valid() bool {
	return cur.nd.count != 0 &&
		cur.nd.bytes() != nil &&
		cur.idx >= 0 &&
		cur.idx < int(cur.nd.count)
}

func (cur *Cursor) CurrentKey() Item {
	return cur.nd.GetKey(cur.idx)
}

func (cur *Cursor) CurrentValue() Item {
	return cur.nd.GetValue(cur.idx)
}

func (cur *Cursor) CurrentRef() hash.Hash {
	return cur.nd.getAddress(cur.idx)
}

func (cur *Cursor) currentSubtreeSize() (uint64, error) {
	leaf, err := cur.isLeaf()
	if err != nil {
		return 0, err
	}
	if leaf {
		return 1, nil
	}
	cur.nd, err = cur.nd.loadSubtrees()
	if err != nil {
		return 0, err
	}
	return cur.nd.getSubtreeCount(cur.idx)
}

func (cur *Cursor) firstKey() Item {
	return cur.nd.GetKey(0)
}

func (cur *Cursor) lastKey() Item {
	lastKeyIdx := int(cur.nd.count) - 1
	return cur.nd.GetKey(lastKeyIdx)
}

func (cur *Cursor) skipToNodeStart() {
	cur.idx = 0
}

func (cur *Cursor) skipToNodeEnd() {
	lastKeyIdx := int(cur.nd.count) - 1
	cur.idx = lastKeyIdx
}

func (cur *Cursor) keepInBounds() {
	if cur.idx < 0 {
		cur.skipToNodeStart()
	}
	lastKeyIdx := int(cur.nd.count) - 1
	if cur.idx > lastKeyIdx {
		cur.skipToNodeEnd()
	}
}

func (cur *Cursor) atNodeStart() bool {
	return cur.idx == 0
}

// atNodeEnd returns true if the cursor's current |idx|
// points to the last node item
func (cur *Cursor) atNodeEnd() bool {
	lastKeyIdx := int(cur.nd.count) - 1
	return cur.idx == lastKeyIdx
}

func (cur *Cursor) isLeaf() (bool, error) {
	// todo(andy): cache Level
	lvl, err := cur.level()
	if err != nil {
		return false, err
	}
	return lvl == 0, nil
}

func (cur *Cursor) level() (uint64, error) {
	return uint64(cur.nd.level), nil
}

// invalidateAtEnd sets the cursor's index to the node count.
func (cur *Cursor) invalidateAtEnd() {
	cur.idx = int(cur.nd.count)
}

// invalidateAtStart sets the cursor's index to -1.
func (cur *Cursor) invalidateAtStart() {
	cur.idx = -1
}

// hasNext returns true if we do not need to recursively
// check the parent to know that the current cursor
// has more keys. hasNext can be false even if parent
// cursors are not exhausted.
func (cur *Cursor) hasNext() bool {
	return cur.idx < int(cur.nd.count)-1
}

// hasPrev returns true if the current node has preceding
// keys. hasPrev can be false even in a parent node has
// preceding keys.
func (cur *Cursor) hasPrev() bool {
	return cur.idx > 0
}

// outOfBounds returns true if the current cursor and
// all parents are exhausted.
func (cur *Cursor) outOfBounds() bool {
	return cur.idx < 0 || cur.idx >= int(cur.nd.count)
}

// Advance either increments the current key index by one,
// or has reached the end of the current node and skips to the next
// child of the parent cursor, recursively if necessary, returning
// either an error or nil.
//
// More specifically, one of three things happens:
//
// 1) The current chunk still has keys, iterate to
// the next |idx|;
//
// 2) We've exhausted the current cursor, but there is at least
// one |parent| cursor with more keys. We find that |parent| recursively,
// perform step (1), and then have every child initialize itself
// using the new |parent|.
//
// 3) We've exhausted the current cursor and every |parent|. Jump
// to an end state (idx = node.count).
func (cur *Cursor) Advance(ctx context.Context) error {
	if cur.hasNext() {
		cur.idx++
		return nil
	}

	if cur.parent == nil {
		cur.invalidateAtEnd()
		return nil
	}

	// recursively increment the parent
	err := cur.parent.Advance(ctx)
	if err != nil {
		return err
	}

	if cur.parent.outOfBounds() {
		// exhausted every parent cursor
		cur.invalidateAtEnd()
		return nil
	}

	// new parent cursor points to new cur node
	err = cur.fetchNode(ctx)
	if err != nil {
		return err
	}

	cur.skipToNodeStart()
	return nil
}

// Retreat decrements to the previous key, if necessary by
// recursively decrementing parent nodes.
func (cur *Cursor) Retreat(ctx context.Context) error {
	if cur.hasPrev() {
		cur.idx--
		return nil
	}

	if cur.parent == nil {
		cur.invalidateAtStart()
		return nil
	}

	// recursively decrement the parent
	err := cur.parent.Retreat(ctx)
	if err != nil {
		return err
	}

	if cur.parent.outOfBounds() {
		// exhausted every parent cursor
		cur.invalidateAtStart()
		return nil
	}

	// new parent cursor points to new cur node
	err = cur.fetchNode(ctx)
	if err != nil {
		return err
	}

	cur.skipToNodeEnd()
	return nil
}

// fetchNode loads the Node that the cursor index points to.
// It's called whenever the cursor advances/retreats to a different chunk.
func (cur *Cursor) fetchNode(ctx context.Context) (err error) {
	assertTrue(cur.parent != nil, "cannot fetch node for cursor with nil parent")
	cur.nd, err = fetchChild(ctx, cur.nrw, cur.parent.CurrentRef())
	cur.idx = -1 // caller must set
	return err
}

// Compare returns the highest relative index difference
// between two cursor trees. A parent has a higher precedence
// than its child.
//
// Ex:
//
// cur:   L3 -> 4, L2 -> 2, L1 -> 5, L0 -> 2
// other: L3 -> 4, L2 -> 2, L1 -> 5, L0 -> 4
//
//	res => -2 (from level 0)
//
// cur:   L3 -> 4, L2 -> 2, L1 -> 5, L0 -> 2
// other: L3 -> 4, L2 -> 3, L1 -> 5, L0 -> 4
//
//	res => +1 (from level 2)
func (cur *Cursor) Compare(other *Cursor) int {
	return compareCursors(cur, other)
}

func (cur *Cursor) Clone() *Cursor {
	cln := Cursor{
		nd:  cur.nd,
		idx: cur.idx,
		nrw: cur.nrw,
	}

	if cur.parent != nil {
		cln.parent = cur.parent.Clone()
	}

	return &cln
}

func (cur *Cursor) copy(other *Cursor) {
	cur.nd = other.nd
	cur.idx = other.idx
	cur.nrw = other.nrw

	if cur.parent != nil {
		assertTrue(other.parent != nil, "cursors must be of equal height to call copy()")
		cur.parent.copy(other.parent)
	} else {
		assertTrue(other.parent == nil, "cursors must be of equal height to call copy()")
	}
}

func compareCursors(left, right *Cursor) (diff int) {
	diff = 0
	for {
		d := left.idx - right.idx
		if d != 0 {
			diff = d
		}

		if left.parent == nil || right.parent == nil {
			break
		}
		left, right = left.parent, right.parent
	}
	return
}

func fetchChild(ctx context.Context, ns NodeStore, ref hash.Hash) (Node, error) {
	// todo(andy) handle nil Node, dangling ref
	return ns.Read(ctx, ref)
}

func assertTrue(b bool, msg string) {
	if !b {
		panic("assertion failed: " + msg)
	}
}
