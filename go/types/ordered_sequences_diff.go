// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/util/functions"
)

type DiffChangeType uint8

const (
	DiffChangeAdded DiffChangeType = iota
	DiffChangeRemoved
	DiffChangeModified
)

type ValueChanged struct {
	ChangeType DiffChangeType
	V          Value
}

func sendChange(changes chan<- ValueChanged, closeChan <-chan struct{}, change ValueChanged) bool {
	select {
	case changes <- change:
		return true
	case <-closeChan:
		return false
	}
}

// Streams the diff from |last| to |current| into |changes|, using both left-right and top-down approach in parallel.
// The left-right diff is expected to return results earlier, whereas the top-down approach is faster overall. This "best" algorithm runs both:
// - early results from left-right are sent to |changes|.
// - if/when top-down catches up, left-right is stopped and the rest of the changes are streamed from top-down.
func orderedSequenceDiffBest(last orderedSequence, current orderedSequence, changes chan<- ValueChanged, closeChan <-chan struct{}) bool {
	lrChanges := make(chan ValueChanged)
	tdChanges := make(chan ValueChanged)
	// Give the close channels a buffer size of 1 so that they won't block, see call sites.
	lrCloseChan := make(chan struct{}, 1)
	tdCloseChan := make(chan struct{}, 1)

	go func() {
		orderedSequenceDiffLeftRight(last, current, lrChanges, lrCloseChan)
		close(lrChanges)
	}()
	go func() {
		orderedSequenceDiffTopDown(last, current, tdChanges, tdCloseChan)
		close(tdChanges)
	}()

	// Stream left-right changes while the top-down diff algorithm catches up.
	var lrChangeCount, tdChangeCount int

	for multiplexing := true; multiplexing; {
		select {
		case <-closeChan:
			// The buffer size of 1 makes these non-blocking.
			lrCloseChan <- struct{}{}
			tdCloseChan <- struct{}{}
			return false
		case c, ok := <-lrChanges:
			if !ok {
				// Left-right diff completed, try to stop top-down. The close chan has a buffer size of 1, so it won't block if it already finished.
				tdCloseChan <- struct{}{}
				return true
			}
			lrChangeCount++
			if !sendChange(changes, closeChan, c) {
				return false
			}
		case c, ok := <-tdChanges:
			if !ok {
				// Top-down diff completed, try to stop left-right. The close chan has a buffer size of 1, so it won't block if it already finished.
				lrCloseChan <- struct{}{}
				return true
			}
			tdChangeCount++
			if tdChangeCount > lrChangeCount {
				// Top-down changes have overtaken left-right changes.
				if !sendChange(changes, closeChan, c) {
					return false
				}
				lrCloseChan <- struct{}{}
				multiplexing = false
			}
		}
	}

	for c := range tdChanges {
		if !sendChange(changes, closeChan, c) {
			return false
		}
	}
	return true
}

// Streams the diff from |last| to |current| into |changes|, using a top-down approach.
// Top-down is parallel and efficiently returns the complete diff, but compared to left-right it's slow to start streaming changes.
func orderedSequenceDiffTopDown(last orderedSequence, current orderedSequence, changes chan<- ValueChanged, closeChan <-chan struct{}) bool {
	var lastHeight, currentHeight int
	functions.All(
		func() { lastHeight = newCursorAt(last, emptyKey, false, false).depth() },
		func() { currentHeight = newCursorAt(current, emptyKey, false, false).depth() },
	)
	return orderedSequenceDiffInternalNodes(last, current, changes, closeChan, lastHeight, currentHeight)
}

// TODO - something other than the literal edit-distance, which is way too much cpu work for this case - https://github.com/attic-labs/noms/issues/2027
func orderedSequenceDiffInternalNodes(last orderedSequence, current orderedSequence, changes chan<- ValueChanged, closeChan <-chan struct{}, lastHeight, currentHeight int) bool {
	if lastHeight > currentHeight {
		lastChild := last.(orderedMetaSequence).getCompositeChildSequence(0, uint64(last.seqLen())).(orderedSequence)
		return orderedSequenceDiffInternalNodes(lastChild, current, changes, closeChan, lastHeight-1, currentHeight)
	}

	if currentHeight > lastHeight {
		currentChild := current.(orderedMetaSequence).getCompositeChildSequence(0, uint64(current.seqLen())).(orderedSequence)
		return orderedSequenceDiffInternalNodes(last, currentChild, changes, closeChan, lastHeight, currentHeight-1)
	}

	if !isMetaSequence(last) && !isMetaSequence(current) {
		return orderedSequenceDiffLeftRight(last, current, changes, closeChan)
	}

	compareFn := last.getCompareFn(current)
	initialSplices := calcSplices(uint64(last.seqLen()), uint64(current.seqLen()), DEFAULT_MAX_SPLICE_MATRIX_SIZE,
		func(i uint64, j uint64) bool { return compareFn(int(i), int(j)) })

	for _, splice := range initialSplices {
		var lastChild, currentChild orderedSequence
		functions.All(
			func() {
				lastChild = last.(orderedMetaSequence).getCompositeChildSequence(splice.SpAt, splice.SpRemoved).(orderedSequence)
			},
			func() {
				currentChild = current.(orderedMetaSequence).getCompositeChildSequence(splice.SpFrom, splice.SpAdded).(orderedSequence)
			},
		)
		if ok := orderedSequenceDiffInternalNodes(lastChild, currentChild, changes, closeChan, lastHeight-1, currentHeight-1); !ok {
			return false
		}
	}

	return true
}

// Streams the diff from |last| to |current| into |changes|, using a left-right approach.
// Left-right immediately descends to the first change and starts streaming changes, but compared to top-down it's serial and much slower to calculate the full diff.
func orderedSequenceDiffLeftRight(last orderedSequence, current orderedSequence, changes chan<- ValueChanged, closeChan <-chan struct{}) bool {
	lastCur := newCursorAt(last, emptyKey, false, false)
	currentCur := newCursorAt(current, emptyKey, false, false)

	for lastCur.valid() && currentCur.valid() {
		fastForward(lastCur, currentCur)

		for lastCur.valid() && currentCur.valid() &&
			!lastCur.seq.getCompareFn(currentCur.seq)(lastCur.idx, currentCur.idx) {
			lastKey := getCurrentKey(lastCur)
			currentKey := getCurrentKey(currentCur)
			if currentKey.Less(lastKey) {
				if ok := sendChange(changes, closeChan, ValueChanged{DiffChangeAdded, currentKey.v}); !ok {
					return false
				}
				currentCur.advance()
			} else if lastKey.Less(currentKey) {
				if ok := sendChange(changes, closeChan, ValueChanged{DiffChangeRemoved, lastKey.v}); !ok {
					return false
				}
				lastCur.advance()
			} else {
				if ok := sendChange(changes, closeChan, ValueChanged{DiffChangeModified, lastKey.v}); !ok {
					return false
				}
				lastCur.advance()
				currentCur.advance()
			}
		}
	}

	for lastCur.valid() {
		if ok := sendChange(changes, closeChan, ValueChanged{DiffChangeRemoved, getCurrentKey(lastCur).v}); !ok {
			return false
		}
		lastCur.advance()
	}
	for currentCur.valid() {
		if ok := sendChange(changes, closeChan, ValueChanged{DiffChangeAdded, getCurrentKey(currentCur).v}); !ok {
			return false
		}
		currentCur.advance()
	}

	return true
}

/**
 * Advances |a| and |b| past their common sequence of equal values.
 */
func fastForward(a *sequenceCursor, b *sequenceCursor) {
	if a.valid() && b.valid() {
		doFastForward(true, a, b)
	}
}

func syncWithIdx(cur *sequenceCursor, hasMore bool, allowPastEnd bool) {
	cur.sync()
	if hasMore {
		cur.idx = 0
	} else if allowPastEnd {
		cur.idx = cur.length()
	} else {
		cur.idx = cur.length() - 1
	}
}

/*
 * Returns an array matching |a| and |b| respectively to whether that cursor has more values.
 */
func doFastForward(allowPastEnd bool, a *sequenceCursor, b *sequenceCursor) (aHasMore bool, bHasMore bool) {
	d.Chk.True(a.valid())
	d.Chk.True(b.valid())
	aHasMore = true
	bHasMore = true

	for aHasMore && bHasMore && isCurrentEqual(a, b) {
		if nil != a.parent && nil != b.parent && isCurrentEqual(a.parent, b.parent) {
			// Key optimisation: if the sequences have common parents, then entire chunks can be
			// fast-forwarded without reading unnecessary data.
			aHasMore, bHasMore = doFastForward(false, a.parent, b.parent)
			syncWithIdx(a, aHasMore, allowPastEnd)
			syncWithIdx(b, bHasMore, allowPastEnd)
		} else {
			aHasMore = a.advanceMaybeAllowPastEnd(allowPastEnd)
			bHasMore = b.advanceMaybeAllowPastEnd(allowPastEnd)
		}
	}
	return aHasMore, bHasMore
}

func isCurrentEqual(a *sequenceCursor, b *sequenceCursor) bool {
	return a.seq.getCompareFn(b.seq)(a.idx, b.idx)
}
