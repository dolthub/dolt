// Copyright 2025 Dolthub, Inc.
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
)

// A Patch represents a single change being applied to a tree.
// If Level == 0, then this is a change to a single key, and |KeyBelowStart| and |SubtreeCount| are ignored.
// If Level > 0, then To is either an address or nil, and this patch represents a change to the range (KeyBelowStart, EndKey].
// An address indicates that the every row in the provided range should be replaced by the rows found by loading the address.
// A nil address indicates that every row in the provided range has been removed.
type Patch struct {
	From          Item
	KeyBelowStart Item
	EndKey        Item
	To            Item
	SubtreeCount  uint64
	Level         int
}

func newAddedPatch(previousKey, key Item, to Item, subtreeCount uint64, level int) Patch {
	return Patch{
		From:          nil,
		KeyBelowStart: previousKey,
		EndKey:        key,
		To:            to,
		SubtreeCount:  subtreeCount,
		Level:         level,
	}
}

func newModifiedPatch(previousKey, key Item, from, to Item, subtreeCount uint64, level int) Patch {
	return Patch{
		From:          from,
		KeyBelowStart: previousKey,
		EndKey:        key,
		To:            to,
		SubtreeCount:  subtreeCount,
		Level:         level,
	}
}

func newRemovedPatch(previousKey, key Item, from Item, level int) Patch {
	return Patch{
		From:          from,
		KeyBelowStart: previousKey,
		EndKey:        key,
		To:            nil,
		SubtreeCount:  0,
		Level:         level,
	}
}

func newLeafPatch(key Item, from, to Item) Patch {
	return Patch{
		From:   from,
		EndKey: key,
		To:     to,
		Level:  0,
	}
}

type PatchIter interface {
	NextPatch(ctx context.Context) (Patch, error)
	Close() error
}

// PatchBuffer implements PatchIter. It consumes Patches
// from the parallel treeDiffers and transforms them into
// patches for the chunker to apply.
type PatchBuffer struct {
	buf chan Patch
}

var _ PatchIter = PatchBuffer{}

func NewPatchBuffer(sz int) PatchBuffer {
	return PatchBuffer{buf: make(chan Patch, sz)}
}

func (ps PatchBuffer) SendPatch(ctx context.Context, patch Patch) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case ps.buf <- patch:
		return nil
	}
}

func (ps PatchBuffer) SendKV(ctx context.Context, key, value Item) error {
	return ps.SendPatch(ctx, Patch{
		EndKey: key,
		To:     value,
		Level:  0,
	})
}

func (ps PatchBuffer) SendDone(ctx context.Context) error {
	return ps.Close()
}

// NextPatch implements PatchIter.
func (ps PatchBuffer) NextPatch(ctx context.Context) (patch Patch, err error) {
	select {
	case patch = <-ps.buf:
		return patch, nil
	case <-ctx.Done():
		return Patch{}, context.Cause(ctx)
	}
}

func (ps PatchBuffer) Close() error {
	close(ps.buf)
	return nil
}

// PatchGenerator takes two cursors and produces a set of Patches that describe the difference between them.
type PatchGenerator[K ~[]byte, O Ordering[K]] struct {
	previousKey        Item
	from, to           *cursor
	order              O
	previousDiffType   DiffType
	previousPatchLevel int
}

func PatchGeneratorFromRoots[K ~[]byte, O Ordering[K]](ctx context.Context, fromNs, toNs NodeStore, from, to Node, order O) (PatchGenerator[K, O], error) {
	var fc, tc *cursor

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

	for fc.nd.level > tc.nd.level {
		fromChild, err := fetchChild(ctx, fc.nrw, fc.currentRef())
		if err != nil {
			return PatchGenerator[K, O]{}, err
		}

		fc = &cursor{
			nd:     fromChild,
			idx:    0,
			parent: fc,
			nrw:    fc.nrw,
		}
	}

	return PatchGenerator[K, O]{
		from:  fc,
		to:    tc,
		order: order,
	}, nil
}

func (td *PatchGenerator[K, O]) advanceToNextDiff(ctx context.Context) (err error) {
	// advance both cursors even if we previously determined they are equal. This needs to be done because
	// skipCommon will not advance the cursors if they are equal in a collation sensitive comparison but differ
	// in a byte comparison.
	if td.to.Valid() {
		td.previousKey = td.to.CurrentKey()
	}
	err = td.from.advance(ctx)
	if err != nil {
		return err
	}
	err = td.to.advance(ctx)
	if err != nil {
		return err
	}
	var lastSeenKey Item
	lastSeenKey, td.from, td.to, err = skipCommonVisitingParents(ctx, td.from, td.to)
	if err != nil {
		return err
	}
	if lastSeenKey != nil {
		td.previousKey = lastSeenKey
	}
	return nil
}

// advanceFromPreviousPatch advances the cursors past the end of the previous patches.
// In the event that the chunk boundaries have shifted between the two versions of the tree,
// this process might produce additional patches until the boundaries line up again.
func (td *PatchGenerator[K, O]) advanceFromPreviousPatch(ctx context.Context) (patch Patch, diffType DiffType, err error) {
	if td.previousPatchLevel > 0 {
		switch td.previousDiffType {
		case AddedDiff:
			td.previousKey = td.to.CurrentKey()
			// This can only happen if we've exhausted the |from| iterator.
			// If we're at the end of a node in |to|, we go up a level to avoid continuing into the next sibling.
			for td.to.atNodeEnd() && td.to.parent != nil {
				td.to = td.to.parent
			}
			err = td.to.advance(ctx)
			if err != nil {
				return Patch{}, NoDiff, err
			}
		case RemovedDiff:
			td.previousKey = td.from.CurrentKey()
			// This can only happen if we've exhausted the |to| iterator.
			// If we're at the end of a node in |from|, we go up a level to avoid continuing into the next sibling.
			for td.from.atNodeEnd() && td.from.parent != nil {
				td.from = td.from.parent
			}
			err = td.from.advance(ctx)
			if err != nil {
				return Patch{}, NoDiff, err
			}
		case ModifiedDiff:
			td.previousKey = td.to.CurrentKey()
			err = td.to.advance(ctx)
			if err != nil {
				return Patch{}, NoDiff, err
			}
			// Everything less than or equal to the key of the last emitted range has been covered.
			// Advance the |from| cursor to the first node that contains keys greater than that key.
			// If the last to block was small we may not advance from at all.
			currentKey := td.from.CurrentKey()
			if currentKey != nil {
				cmp := compareWithNilAsMin(ctx, td.order, K(currentKey), K(td.previousKey))

				for cmp != 0 {
					if cmp > 0 {
						// If this was the very end of the |to| tree, all remaining ranges in |from| have been removed.
						if !td.to.Valid() {
							return td.sendRemovedRange(), RemovedDiff, nil
						}
						// The current |from| node contains additional rows that overlap with the new |to| node.
						// We can encode this as another range.
						return td.sendModifiedRange()
					}
					// Every value in the from node was covered by the previous diff. Advance it and check again.
					err = td.from.advance(ctx)
					if err != nil {
						return Patch{}, NoDiff, err
					}
					// If we reach the end of the |from| tree, all remaining nodes in |to| have been added.
					if !td.from.Valid() {
						if !td.to.Valid() {
							return Patch{}, NoDiff, io.EOF
						}
						return td.sendAddedRange()
					}
					cmp = td.order.Compare(ctx, K(td.from.CurrentKey()), K(td.previousKey))
				}
				// At this point, the from cursor lines up with the max key emitted by the previous range diff.
				// Advancing the from cursor one more time guarantees that both cursors reference chunks with the same start range.
				err = td.from.advance(ctx)
				if err != nil {
					return Patch{}, NoDiff, err
				}
			}
		}
	} else {
		switch td.previousDiffType {
		case RemovedDiff:
			td.previousKey = td.from.CurrentKey()
			// If we've already exhausted the |to| iterator, then returning to the parent
			// at the end of the block lets us avoid visiting leaf nodes unnecessarily.
			for td.from.atNodeEnd() && td.from.parent != nil && !td.to.Valid() {
				td.from = td.from.parent
			}
			err = td.from.advance(ctx)
			if err != nil {
				return Patch{}, NoDiff, err
			}
		case AddedDiff:
			td.previousKey = td.to.CurrentKey()
			// If we've already exhausted the |from| iterator, then returning to the parent
			// at the end of the block lets us avoid visiting leaf nodes unnecessarily.
			for td.to.atNodeEnd() && td.to.parent != nil && !td.from.Valid() {
				td.to = td.to.parent
			}
			err = td.to.advance(ctx)
			if err != nil {
				return Patch{}, NoDiff, err
			}
		case ModifiedDiff:
			err = td.advanceToNextDiff(ctx)
			if err != nil {
				return Patch{}, NoDiff, err
			}
		}
	}
	return Patch{}, NoDiff, nil
}

func (td *PatchGenerator[K, O]) Next(ctx context.Context) (patch Patch, diffType DiffType, err error) {
	if td.previousDiffType != NoDiff {
		patch, diffType, err = td.advanceFromPreviousPatch(ctx)
	}
	if err != nil || diffType != NoDiff {
		return patch, diffType, err
	}
	return td.findNextPatch(ctx)
}

func (td *PatchGenerator[K, O]) findNextPatch(ctx context.Context) (patch Patch, diffType DiffType, err error) {
	for td.from.Valid() && td.to.Valid() {
		level, err := td.to.level()
		if err != nil {
			return Patch{}, NoDiff, err
		}
		f := td.from.CurrentKey()
		t := td.to.CurrentKey()
		cmp := td.order.Compare(ctx, K(f), K(t))

		if cmp == 0 {
			if !equalcursorValues(td.from, td.to) {
				if level > 0 {
					return td.sendModifiedRange()
				} else {
					return td.sendModifiedKey(), ModifiedDiff, nil
				}
			}

			err = td.advanceToNextDiff(ctx)
			if err != nil {
				return Patch{}, NoDiff, err
			}
		} else if level > 0 {
			// There is a corner case where this *seems* incorrect, but is actually correct.
			// If the |from| cursor is pointing to a node whose range is entire before the node pointed to by |to|,
			// we don't know whether the |to| node still exists in the |from| tree. Given that, it may seem weird to
			// emit a Patch on the |to| node. However, if the |to| node does exist in the |from| tree, then every node
			// between the current |from| node and that node has been deleted. This is encoded as a Patch whose node
			// is the |to| node and whose key range is the entire range of the removed nodes.
			return td.sendModifiedRange()
		} else if cmp < 0 {
			return td.sendRemovedKey(), RemovedDiff, nil
		} else {
			return td.sendAddedKey(), AddedDiff, nil
		}
	}

	if td.from.Valid() {
		if td.from.nd.level > 0 {
			return td.sendRemovedRange(), RemovedDiff, nil
		}
		return td.sendRemovedKey(), RemovedDiff, nil
	}
	if td.to.Valid() {
		if td.to.nd.level > 0 {
			return td.sendAddedRange()
		}
		return td.sendAddedKey(), AddedDiff, nil
	}

	return Patch{}, NoDiff, io.EOF
}

// split iterates through the children of the current nodes to find the first change.
// We only call this if both nodes are non-leaf nodes with different hashes, so we're guaranteed to find one.
func (td *PatchGenerator[K, O]) split(ctx context.Context) (patch Patch, diffType DiffType, err error) {
	if td.previousPatchLevel == 0 {
		return Patch{}, NoDiff, fmt.Errorf("can't split a patch that's already at the leaf level")
	}
	switch td.previousDiffType {
	case RemovedDiff:
		fromChild, err := fetchChild(ctx, td.from.nrw, td.from.currentRef())
		if err != nil {
			return Patch{}, NoDiff, err
		}
		td.from = &cursor{
			nd:     fromChild,
			idx:    0,
			parent: td.from,
			nrw:    td.from.nrw,
		}
		if td.from.nd.level > 0 {
			return td.sendRemovedRange(), RemovedDiff, nil
		} else {
			return td.sendRemovedKey(), RemovedDiff, nil
		}
	case AddedDiff:
		toChild, err := fetchChild(ctx, td.to.nrw, td.to.currentRef())
		if err != nil {
			return Patch{}, NoDiff, err
		}
		td.to = &cursor{
			nd:     toChild,
			idx:    0,
			parent: td.to,
			nrw:    td.to.nrw,
		}
		if td.to.nd.level > 0 {
			return td.sendAddedRange()
		} else {
			return td.sendAddedKey(), AddedDiff, nil
		}
	case ModifiedDiff:

		toChild, err := fetchChild(ctx, td.to.nrw, td.to.currentRef())
		if err != nil {
			return Patch{}, NoDiff, err
		}
		toChild, err = toChild.loadSubtrees()
		if err != nil {
			return Patch{}, NoDiff, err
		}

		// Maintain invariant that the |from| cursor is never at a higher level than the |to| cursor.
		if td.from.nd.level == td.to.nd.level {
			fromChild, err := fetchChild(ctx, td.from.nrw, td.from.currentRef())
			if err != nil {
				return Patch{}, NoDiff, err
			}

			td.from = &cursor{
				nd:     fromChild,
				idx:    0,
				parent: td.from,
				nrw:    td.from.nrw,
			}
			for compareWithNilAsMin(ctx, td.order, K(td.from.CurrentKey()), K(td.previousKey)) <= 0 {
				err = td.from.advance(ctx)
				if err != nil {
					return Patch{}, NoDiff, err
				}
			}
		}

		td.to = &cursor{
			nd:     toChild,
			idx:    0,
			parent: td.to,
			nrw:    td.to.nrw,
		}
		return td.findNextPatch(ctx)
	default:
		return Patch{}, NoDiff, fmt.Errorf("unexpected Diff type: this shouldn't be possible")
	}
}

func (td *PatchGenerator[K, O]) sendRemovedKey() (patch Patch) {
	patch = newLeafPatch(td.from.CurrentKey(), td.from.currentValue(), nil)
	td.previousDiffType = RemovedDiff
	td.previousPatchLevel = 0
	return patch
}

func (td *PatchGenerator[K, O]) sendAddedKey() (patch Patch) {
	patch = newLeafPatch(td.to.CurrentKey(), nil, td.to.currentValue())
	td.previousDiffType = AddedDiff
	td.previousPatchLevel = 0
	return patch
}

func (td *PatchGenerator[K, O]) sendModifiedKey() (patch Patch) {
	patch = newLeafPatch(td.to.CurrentKey(), td.from.currentValue(), td.to.currentValue())
	td.previousDiffType = ModifiedDiff
	td.previousPatchLevel = 0
	return patch
}

func (td *PatchGenerator[K, O]) sendModifiedRange() (patch Patch, diffType DiffType, err error) {
	var subtreeCount uint64
	level := td.to.nd.Level()
	var fromValue Item
	if td.from.Valid() {
		fromValue = td.from.currentValue()
	}
	subtreeCount, err = td.to.currentSubtreeSize()
	if err != nil {
		return Patch{}, NoDiff, err
	}

	patch = newModifiedPatch(td.previousKey, td.to.CurrentKey(), fromValue, td.to.currentValue(), subtreeCount, level)

	td.previousDiffType = ModifiedDiff
	td.previousPatchLevel = level
	return patch, ModifiedDiff, nil
}

func (td *PatchGenerator[K, O]) sendAddedRange() (patch Patch, diffType DiffType, err error) {
	level := td.to.nd.Level()
	subtreeCount, err := td.to.currentSubtreeSize()
	if err != nil {
		return Patch{}, NoDiff, err
	}
	patch = newAddedPatch(td.previousKey, td.to.CurrentKey(), td.to.currentValue(), subtreeCount, level)
	td.previousDiffType = AddedDiff
	td.previousPatchLevel = level
	return patch, AddedDiff, nil
}

func (td *PatchGenerator[K, O]) sendRemovedRange() (patch Patch) {
	level := td.from.nd.Level()
	patch = newRemovedPatch(td.previousKey, td.from.CurrentKey(), td.from.currentValue(), level)
	td.previousDiffType = RemovedDiff
	td.previousPatchLevel = level
	return patch
}

// skipCommonVisitingParents advances the cursors past any elements they have in common.
// Unlike skipCommon, if both cursors reach the end of a node together, they move up a level in the tree.
func skipCommonVisitingParents(ctx context.Context, from, to *cursor) (lastSeenKey Item, newFrom, newTo *cursor, err error) {
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
				return skipCommonVisitingParents(ctx, from.parent, to.parent)
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
