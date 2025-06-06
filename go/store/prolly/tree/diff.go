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
	"bytes"
	"context"
	"io"
)

type DiffType byte

const (
	NoDiff       DiffType = 0
	AddedDiff    DiffType = 1
	ModifiedDiff DiffType = 2
	RemovedDiff  DiffType = 3
	RangeDiff    DiffType = 4
)

type Diff struct {
	Type DiffType
	From Item
	Mutation
}

func newDiff(diffType DiffType, previousKey, key Item, from, to Item, subtreeCount uint64, level int) Diff {
	return Diff{
		Type: diffType,
		From: from,
		Mutation: Mutation{
			PreviousKey:  previousKey,
			Key:          key,
			To:           to,
			SubtreeCount: subtreeCount,
			Level:        level,
		},
	}
}

type DiffFn func(context.Context, Diff) error

// Differ computes the diff between two prolly trees.
// If `considerAllRowsModified` is true, it will consider every leaf to be modified and generate a diff for every leaf. (This
// is useful in cases where the schema has changed and we want to consider a leaf changed even if the byte representation
// of the leaf is the same.
type Differ[K ~[]byte, O Ordering[K]] struct {
	previousKey             Item
	from, to                *cursor
	fromStop, toStop        *cursor
	order                   O
	considerAllRowsModified bool
	previousDiffType        DiffType
	emitRanges              bool
}

func RangeDifferFromRoots[K ~[]byte, O Ordering[K]](
	ctx context.Context,
	fromNs NodeStore, toNs NodeStore,
	from, to Node,
	order O,
	considerAllRowsModified bool,
) (Differ[K, O], error) {
	differ, err := DifferFromRoots(ctx, fromNs, toNs, from, to, order, considerAllRowsModified)
	differ.emitRanges = true
	return differ, err
}

func DifferFromRoots[K ~[]byte, O Ordering[K]](
	ctx context.Context,
	fromNs, toNs NodeStore,
	from, to Node,
	order O,
	considerAllRowsModified bool,
) (Differ[K, O], error) {
	var fc, tc *cursor
	var err error

	if !from.empty() {
		fc = newCursorAtRoot(ctx, fromNs, from)
	} else {
		fc = &cursor{}
	}

	if !to.empty() {
		tc = newCursorAtRoot(ctx, toNs, to)
	} else {
		tc = &cursor{}
	}

	// If the diffed maps have different heights, then cursors will be at different levels.
	for fc.nd.level > tc.nd.level {
		nd, err := fetchChild(ctx, fromNs, fc.currentRef())
		if err != nil {
			return Differ[K, O]{}, err
		}

		parent := fc
		fc = &cursor{nd: nd, parent: parent, nrw: fromNs}
	}

	for tc.nd.level > fc.nd.level {
		nd, err := fetchChild(ctx, toNs, tc.currentRef())
		if err != nil {
			return Differ[K, O]{}, err
		}

		parent := tc
		tc = &cursor{nd: nd, parent: parent, nrw: toNs}
	}

	fs, err := newCursorPastEnd(ctx, fromNs, from)
	if err != nil {
		return Differ[K, O]{}, err
	}

	ts, err := newCursorPastEnd(ctx, toNs, to)
	if err != nil {
		return Differ[K, O]{}, err
	}

	return Differ[K, O]{
		from:                    fc,
		to:                      tc,
		fromStop:                fs,
		toStop:                  ts,
		order:                   order,
		considerAllRowsModified: considerAllRowsModified,
	}, nil
}

func DifferFromCursors[K ~[]byte, O Ordering[K]](
	ctx context.Context,
	fromRoot, toRoot Node,
	findStart, findStop SearchFn,
	fromStore, toStore NodeStore,
	order O,
) (Differ[K, O], error) {
	fromStart, err := newCursorFromSearchFn(ctx, fromStore, fromRoot, findStart)
	if err != nil {
		return Differ[K, O]{}, err
	}
	toStart, err := newCursorFromSearchFn(ctx, toStore, toRoot, findStart)
	if err != nil {
		return Differ[K, O]{}, err
	}
	fromStop, err := newCursorFromSearchFn(ctx, fromStore, fromRoot, findStop)
	if err != nil {
		return Differ[K, O]{}, err
	}
	toStop, err := newCursorFromSearchFn(ctx, toStore, toRoot, findStop)
	if err != nil {
		return Differ[K, O]{}, err
	}
	return Differ[K, O]{
		from:       fromStart,
		to:         toStart,
		fromStop:   fromStop,
		toStop:     toStop,
		order:      order,
		emitRanges: false,
	}, nil
}

func (td *Differ[K, O]) Next(ctx context.Context) (diff Diff, err error) {
	diff, err = td.NextRange(ctx)
	if err != nil {
		return Diff{}, err
	}
	if !td.emitRanges {
		for diff.Level > 0 {
			diff, err = td.split(ctx)
			if err != nil {
				return Diff{}, err
			}
		}
	}
	return diff, nil
}

func compareWithEnd(cur, end *cursor) int {
	// We can't just compare the cursors because |end| is always a cursor to a leaf node,
	// but |cur| may not be.
	// Assume that we're checking to see if we've reached the end.
	// A cursor at a higher level hasn't reached the end yet.
	if cur.nd.level > end.nd.level {
		cmp := compareWithEnd(cur, end.parent)
		if cmp == 0 {
			return -1
		}
		return cmp
	}
	return compareCursors(cur, end)
}

func (td *Differ[K, O]) NextRange(ctx context.Context) (diff Diff, err error) {
	switch td.previousDiffType {
	case RemovedDiff:
		err = td.from.advance(ctx)
		if err != nil {
			return Diff{}, err
		}
	case AddedDiff:
		err = td.to.advance(ctx)
		if err != nil {
			return diff, err
		}
	case ModifiedDiff:
		err = td.from.advance(ctx)
		if err != nil {
			return Diff{}, err
		}
		err = td.to.advance(ctx)
		if err != nil {
			return diff, err
		}
	case RangeDiff:
		err = td.to.advance(ctx)
		if err != nil {
			return diff, err
		}
		// Everything less than or equal to the key of the last emitted range has been covered.
		// Skip to the first node greater than that key. This may require us to split.
		// If the last to block was small we may not advance from at all.
		ck := td.from.CurrentKey()
		_ = ck
		cmp := td.order.Compare(ctx, K(td.from.CurrentKey()), K(td.previousKey))

		for cmp != 0 {
			if cmp > 0 {
				// The current from node contains additional rows that overlap with the new to node.
				// We can encode this as another range.
				return td.sendRange()
			}
			// Every value in the from node was covered by the previous diff. Advance it and check again.
			err = td.from.advance(ctx)
			if err != nil {
				return diff, err
			}
			cmp = td.order.Compare(ctx, K(td.from.CurrentKey()), K(td.previousKey))
		}
		// At this point, the from cursor lines up with the max key emitted by the previous range diff.
		// Advancing the from cursor one more time guarantees that both cursors reference chunks with the same start range.
		err = td.to.advance(ctx)
		if err != nil {
			return diff, err
		}
	}

	for td.from.Valid() && compareWithEnd(td.from, td.fromStop) < 0 && td.to.Valid() && compareWithEnd(td.to, td.toStop) < 0 {
		level, err := td.from.level()
		if err != nil {
			return Diff{}, err
		}
		f := td.from.CurrentKey()
		t := td.to.CurrentKey()
		cmp := td.order.Compare(ctx, K(f), K(t))

		// If we're not at the leaf, we want to emit a single diff for the total range.
		// This means that unless both keys and values are equal, we want to emit a single range diff.
		// At the start of the next run, we can advance the from iterator.

		if cmp == 0 {
			// If the cursor schema has changed, then all rows should be considered modified.
			// If the cursor schema hasn't changed, rows are modified iff their bytes have changed.
			if td.considerAllRowsModified || !equalcursorValues(td.from, td.to) {
				if level > 0 {
					return td.sendRange()
				} else {
					return td.sendModified()
				}
			}

			// advance both cursors since we have already determined that they are equal. This needs to be done because
			// skipCommon will not advance the cursors if they are equal in a collation sensitive comparison but differ
			// in a byte comparison.
			td.previousKey = f
			if err = td.from.advance(ctx); err != nil {
				return Diff{}, err
			}
			if err = td.to.advance(ctx); err != nil {
				return Diff{}, err
			}

			// Can we just call skipCommon here? Why do we do the first check?
			// seek ahead to the next diff and loop again
			var lastSeenKey Item
			var err error
			lastSeenKey, td.from, td.to, err = skipCommon(ctx, td.from, td.to)
			if err != nil {
				return Diff{}, err
			}
			if lastSeenKey != nil {
				td.previousKey = lastSeenKey
			}
		} else if level > 0 {
			return td.sendRange()
		} else if cmp < 0 {
			return td.sendRemoved()
		} else {
			return td.sendAdded()
		}
	}

	// Compare Cursors need to be at the same level.
	// What if the stop cursor is in the middle? Do we need to recurse back down to the child?
	if td.from.Valid() && td.from.compare(td.fromStop) < 0 {
		return td.sendRemoved()
	}
	if td.to.Valid() && td.to.compare(td.toStop) < 0 {
		return td.sendAdded()
	}

	return Diff{}, io.EOF
}

// split iterates through the children of the current nodes to find the first change.
// We only call this if both nodes are non-leaf nodes with different hashes, so we're guaranteed to find one.
func (td *Differ[K, O]) split(ctx context.Context) (diff Diff, err error) {
	if !td.to.Valid() {
		// A corner case: we're splitting the RemoveDiff that appears at the end.
		fromChild, err := fetchChild(ctx, td.from.nrw, td.from.currentRef())
		if err != nil {
			return Diff{}, err
		}
		td.from = &cursor{
			nd:     fromChild,
			idx:    0,
			parent: td.from,
			nrw:    td.from.nrw,
		}
		return td.sendRemoved()
	}

	fromChild, err := fetchChild(ctx, td.from.nrw, td.from.currentRef())
	if err != nil {
		return Diff{}, err
	}
	toChild, err := fetchChild(ctx, td.to.nrw, td.to.currentRef())
	if err != nil {
		return Diff{}, err
	}
	toChild, err = toChild.loadSubtrees()
	if err != nil {
		return Diff{}, err
	}

	if fromChild.level == 0 {
		// TODO: Does this also work for splitting into a non-leaf node?
		// The current split implementation fast-tracks based on address, but Next might already do that.
		td.from = &cursor{
			nd:     fromChild,
			idx:    0,
			parent: td.from,
			nrw:    td.from.nrw,
		}
		td.to = &cursor{
			nd:     toChild,
			idx:    0,
			parent: td.to,
			nrw:    td.to.nrw,
		}
		td.previousDiffType = NoDiff
		return td.Next(ctx)
	}

	for i := 0; i < min(fromChild.Count(), toChild.Count()); i++ {
		fromAddress := fromChild.getAddress(i)
		toAddress := toChild.getAddress(i)
		if fromAddress == toAddress {
			td.previousKey = fromChild.GetKey(i)
			continue
		}
		// Can we assume that both chunks have the same start position?
		// Case 1: to.upper < from.lower: An entire chunk was added
		// Case 2: from.upper < to.lower: An entire chunk was removed
		// Case 3: A chunk was edited
		// In all cases, return the current to chunk with the previous key.
		td.from = &cursor{
			nd:     fromChild,
			idx:    i,
			parent: td.from,
			nrw:    td.from.nrw,
		}
		td.to = &cursor{
			nd:     toChild,
			idx:    i,
			parent: td.to,
			nrw:    td.to.nrw,
		}
		return td.sendRange()
	}
	// both chunks start the same, but one has additional children.
	// the only way this is possible is if we've reached the end of the tree on one side.
	// if |to| has added rows, emit an added range diff?.
	// if |to| has removed rows, emit a deleted range diff with no end range?
	panic("reached end of tree")
}

func (td *Differ[K, O]) sendRemoved() (diff Diff, err error) {
	level, err := td.from.level()
	if err != nil {
		return Diff{}, err
	}

	k := td.from.CurrentKey()
	_ = k
	diff = newDiff(RemovedDiff, td.previousKey, td.from.CurrentKey(), td.from.currentValue(), nil, 1, int(level))

	td.previousDiffType = RemovedDiff
	td.previousKey = td.from.CurrentKey()
	return diff, nil
}

func (td *Differ[K, O]) sendRange() (diff Diff, err error) {
	var subtreeCount uint64
	subtreeCount, err = td.to.currentSubtreeSize()
	if err != nil {
		return Diff{}, err
	}
	level, err := td.from.level()
	if err != nil {
		return Diff{}, err
	}
	diff = newDiff(RangeDiff, td.previousKey, td.to.CurrentKey(), td.from.currentValue(), td.to.currentValue(), subtreeCount, int(level))

	td.previousDiffType = RangeDiff
	td.previousKey = td.to.CurrentKey()
	return diff, nil
}

func (td *Differ[K, O]) sendAdded() (diff Diff, err error) {
	subtreeCount, err := td.to.currentSubtreeSize()
	if err != nil {
		return Diff{}, err
	}
	level, err := td.from.level()
	if err != nil {
		return Diff{}, err
	}

	k := td.to.CurrentKey()
	_ = k

	diff = newDiff(AddedDiff, td.previousKey, td.to.CurrentKey(), nil, td.to.currentValue(), subtreeCount, int(level))

	td.previousDiffType = AddedDiff
	td.previousKey = td.to.CurrentKey()
	return diff, nil
}

func (td *Differ[K, O]) sendModified() (diff Diff, err error) {
	subtreeCount, err := td.to.currentSubtreeSize()
	if err != nil {
		return Diff{}, err
	}
	level, err := td.from.level()
	if err != nil {
		return Diff{}, err
	}

	k := td.from.CurrentKey()
	_ = k

	diff = newDiff(ModifiedDiff, td.previousKey, td.to.CurrentKey(), td.from.currentValue(), td.to.currentValue(), subtreeCount, int(level))

	td.previousDiffType = ModifiedDiff
	td.previousKey = td.to.CurrentKey()

	return diff, nil
}

func skipCommon(ctx context.Context, from, to *cursor) (lastSeenKey Item, newFrom, newTo *cursor, err error) {
	// track when |from.parent| and |to.parent| change
	// to avoid unnecessary comparisons.
	parentsAreNew := true

	for from.Valid() && to.Valid() {
		if !equalItems(from, to) {
			// found the next difference
			return lastSeenKey, from, to, nil
		}

		if parentsAreNew {
			if equalParents(from, to) {
				// if our parents are equal, we can search for differences
				// faster at the next highest tree Level.
				return skipCommon(ctx, from.parent, to.parent)
			}
			parentsAreNew = false
		}

		// if one of the cursors is at the end of its node, it will
		// need to Advance its parent and fetch a new node. In this
		// case we need to Compare parents again.
		parentsAreNew = from.atNodeEnd() || to.atNodeEnd()

		lastSeenKey = from.CurrentKey()
		if err = from.advance(ctx); err != nil {
			return lastSeenKey, from, to, err
		}
		if err = to.advance(ctx); err != nil {
			return lastSeenKey, from, to, err
		}
	}

	return lastSeenKey, from, to, err
}

// todo(andy): assumes equal byte representations
func equalItems(from, to *cursor) bool {
	return bytes.Equal(from.CurrentKey(), to.CurrentKey()) &&
		bytes.Equal(from.currentValue(), to.currentValue())
}

func equalParents(from, to *cursor) (eq bool) {
	if from.parent != nil && to.parent != nil {
		eq = equalItems(from.parent, to.parent)
	}
	return
}

func equalcursorValues(from, to *cursor) bool {
	return bytes.Equal(from.currentValue(), to.currentValue())
}
