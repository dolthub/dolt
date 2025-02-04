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
	AddedDiff    DiffType = 0
	ModifiedDiff DiffType = 1
	RemovedDiff  DiffType = 2
	RangeDiff    DiffType = 3
)

type Diff struct {
	PreviousKey Item
	Key         Item
	From        Item
	toCur       *cursor
	Type        DiffType
}

func (d Diff) To() Item {
	return d.toCur.currentValue()
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
}

func DifferFromRoots[K ~[]byte, O Ordering[K]](
	ctx context.Context,
	fromNs NodeStore, toNs NodeStore,
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
		from:     fromStart,
		to:       toStart,
		fromStop: fromStop,
		toStop:   toStop,
		order:    order,
	}, nil
}

func (td Differ[K, O]) Next(ctx context.Context) (diff Diff, err error) {
	return td.next(ctx, true)
}

// next finds the next diff and then conditionally advances the cursors past the modified chunks.
// In most cases, we want to advance the cursors, but in some circumstances the caller may want to access the cursors
// and then advance them manually.
func (td Differ[K, O]) next(ctx context.Context, advanceCursors bool) (diff Diff, err error) {
	for td.from.Valid() && td.from.compare(td.fromStop) < 0 && td.to.Valid() && td.to.compare(td.toStop) < 0 {

		f := td.from.CurrentKey()
		t := td.to.CurrentKey()
		cmp := td.order.Compare(ctx, K(f), K(t))

		switch {
		case cmp < 0:
			return td.sendRemoved(ctx, advanceCursors)

		case cmp > 0:
			return td.sendAdded(ctx, td.to, advanceCursors)

		case cmp == 0:
			// If the cursor schema has changed, then all rows should be considered modified.
			// If the cursor schema hasn't changed, rows are modified iff their bytes have changed.
			if td.considerAllRowsModified || !equalcursorValues(td.from, td.to) {
				return td.sendModified(ctx, td.from, td.to, advanceCursors)
			}

			// advance both cursors since we have already determined that they are equal. This needs to be done because
			// skipCommon will not advance the cursors if they are equal in a collation sensitive comparison but differ
			// in a byte comparison.
			if err = td.from.advance(ctx); err != nil {
				return Diff{}, err
			}
			if err = td.to.advance(ctx); err != nil {
				return Diff{}, err
			}

			// seek ahead to the next diff and loop again
			if err = skipCommon(ctx, td.from, td.to); err != nil {
				return Diff{}, err
			}
		}
	}

	if td.from.Valid() && td.from.compare(td.fromStop) < 0 {
		return td.sendRemoved(ctx, td.from, advanceCursors)
	}
	if td.to.Valid() && td.to.compare(td.toStop) < 0 {
		return td.sendAdded(ctx, td.to, advanceCursors)
	}

	return Diff{}, io.EOF
}

// split iterates through the children of the current nodes to find the first change.
// We only call this if both nodes are non-leaf nodes with different hashes, so we're guaranteed to find one.
func (td Differ[K, O]) split(ctx context.Context) (diff Diff, err error) {
	// the cursors may exist at different levels: this indicates the we're recursing into just one for a Range add or delete. (not true?)
	// potential problem: due to moving chunk boundaries a claimed added or deleted range might actually exist later in the other cursor?
	//  it's fine, keep cursors in the same level
	// So we may need to advance that cursor?
	// TODO: Also need to handle leaf nodes.
	fromChild, err := fetchChild(ctx, td.from.nrw, td.from.currentRef())
	if err != nil {
		return Diff{}, err
	}
	toChild, err := fetchChild(ctx, td.to.nrw, td.to.currentRef())
	if err != nil {
		return Diff{}, err
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
		result := Diff{
			PreviousKey: td.previousKey,
			Key:         td.to.CurrentKey(),
			toCur:       td.to,
			Type:        RangeDiff,
		}
		// when do we advance the from? Walk through some examples.
		err = td.to.advance(ctx)
		return result, err
	}
	// both chunks start the same, but one has additional children.
	// the only way this is possible is if we've reached the end of the tree on one side.
	// if |to| has added rows, emit an added range diff?.
	// if |to| has removed rows, emit a deleted range diff with no end range?
	panic("reached end of tree")
}

// Advance the cursors to find the next diff. When reaching the end of a block, go up a level instead of retching the cousin node
func (td Differ[K, O]) sibling(ctx context.Context) (diff Diff, err error) {
	for td.from.Valid() && td.from.compare(td.fromStop) < 0 && td.to.Valid() && td.to.compare(td.toStop) < 0 {

		// If the ends of blocks line up, go up a level. Otherwise stay on the current level.
		// Do we implement this:
		// - By looking at node indexes?
		//     When both are at index 0/past the end, go up a level (but only if we hit a boundary here?)
		// - By looking to see when keys line up?
		//     When keys are equal, we hit a boundary in the level below. We can stop emitting blocks but we don't go up a level just yet.
		f := td.from.CurrentKey()
		t := td.to.CurrentKey()
		cmp := td.order.Compare(K(f), K(t))

		// If both cursors are at the end, go up a level?
		switch {
		case cmp < 0:
			// from block ends before to block
			// we don't care about emitting from blocks, advance the from block until we catch up
			err = td.from.advance(ctx)
			if err != nil {
				return Diff{}, err
			}
			continue

		case cmp > 0:
			// I think the behavior is different for leaf nodes and other nodes.
			// to block ends before from block. Advance to block and emit the next one
			td.previousKey = td.to.CurrentKey()
			if err = td.to.advance(ctx); err != nil {
				return Diff{}, err
			}
			var diffType DiffType
			if td.from.isLeaf() {
				diffType = AddedDiff
			} else {
				diffType = RangeDiff
			}
			diff = Diff{
				PreviousKey: td.previousKey,
				Type:        diffType,
				Key:         td.to.CurrentKey(),
				toCur:       td.to.clone(),
			}

			return diff, err
		case cmp == 0:
			// If the cursor schema has changed, then all rows should be considered modified.
			// If the cursor schema hasn't changed, rows are modified iff their bytes have changed.
			if td.considerAllRowsModified || !equalcursorValues(td.from, td.to) {
				return sendModified(ctx, td.from, td.to, advanceCursors)
			}

			// advance both cursors since we have already determined that they are equal. This needs to be done because
			// skipCommon will not advance the cursors if they are equal in a collation sensitive comparison but differ
			// in a byte comparison.
			if err = td.from.advance(ctx); err != nil {
				return Diff{}, err
			}
			if err = td.to.advance(ctx); err != nil {
				return Diff{}, err
			}

			// seek ahead to the next diff and loop again
			if err = skipCommon(ctx, td.from, td.to); err != nil {
				return Diff{}, err
			}
		}
	}

	if td.from.Valid() && td.from.compare(td.fromStop) < 0 {
		return td.sendRemoved(ctx, td.from, advanceCursors)
	}
	if td.to.Valid() && td.to.compare(td.toStop) < 0 {
		return sendAdded(ctx, td.to, advanceCursors)
	}

	return Diff{}, io.EOF
}

func (td Differ[K, O]) sendRemoved(ctx context.Context, advanceCursors bool) (diff Diff, err error) {
	var diffType DiffType
	if td.from.isLeaf() {
		diffType = RemovedDiff
	} else {
		diffType = RangeDiff
	}
	diff = Diff{
		PreviousKey: td.previousKey,
		Type:        diffType,
		Key:         td.from.CurrentKey(),
		From:        td.from.currentValue(),
		toCur:       td.to,
	}

	if advanceCursors {
		if err = td.from.advance(ctx); err != nil {
			return Diff{}, err
		}
	}
	return
}

func (td Differ[K, O]) sendAdded(ctx context.Context, to *cursor, advanceCursors bool) (diff Diff, err error) {
	var diffType DiffType
	if td.from.isLeaf() {
		diffType = AddedDiff
	} else {
		diffType = RangeDiff
	}
	diff = Diff{
		PreviousKey: td.previousKey,
		Type:        diffType,
		Key:         to.CurrentKey(),
		toCur:       to.clone(),
	}

	if advanceCursors {
		if err = to.advance(ctx); err != nil {
			return Diff{}, err
		}
	}
	return
}

func (td Differ[K, O]) sendModified(ctx context.Context, from, to *cursor, advanceCursors bool) (diff Diff, err error) {
	var diffType DiffType
	if td.from.isLeaf() {
		diffType = ModifiedDiff
	} else {
		diffType = RangeDiff
	}
	diff = Diff{
		PreviousKey: td.previousKey,
		Type:        diffType,
		Key:         from.CurrentKey(),
		From:        from.currentValue(),
		toCur:       to.clone(),
	}

	if advanceCursors {
		if err = from.advance(ctx); err != nil {
			return Diff{}, err
		}
		if err = to.advance(ctx); err != nil {
			return Diff{}, err
		}
	}
	return
}

func skipCommon(ctx context.Context, from, to *cursor) (err error) {
	// track when |from.parent| and |to.parent| change
	// to avoid unnecessary comparisons.
	parentsAreNew := true

	for from.Valid() && to.Valid() {
		if !equalItems(from, to) {
			// found the next difference
			return nil
		}

		if parentsAreNew {
			if equalParents(from, to) {
				// if our parents are equal, we can search for differences
				// faster at the next highest tree Level.
				if err = skipCommonParents(ctx, from, to); err != nil {
					return err
				}
				continue
			}
			parentsAreNew = false
		}

		// if one of the cursors is at the end of its node, it will
		// need to Advance its parent and fetch a new node. In this
		// case we need to Compare parents again.
		parentsAreNew = from.atNodeEnd() || to.atNodeEnd()

		if err = from.advance(ctx); err != nil {
			return err
		}
		if err = to.advance(ctx); err != nil {
			return err
		}
	}

	return err
}

func skipCommonParents(ctx context.Context, from, to *cursor) (err error) {
	err = skipCommon(ctx, from.parent, to.parent)
	if err != nil {
		return err
	}

	if from.parent.Valid() {
		if err = from.fetchNode(ctx); err != nil {
			return err
		}
		from.skipToNodeStart()
	} else {
		from.invalidateAtEnd()
	}

	if to.parent.Valid() {
		if err = to.fetchNode(ctx); err != nil {
			return err
		}
		to.skipToNodeStart()
	} else {
		to.invalidateAtEnd()
	}

	return
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
