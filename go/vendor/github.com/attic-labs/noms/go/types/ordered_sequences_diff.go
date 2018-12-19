// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"sync"

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
	ChangeType              DiffChangeType
	Key, OldValue, NewValue Value
}

func sendChange(changes chan<- ValueChanged, stopChan <-chan struct{}, change ValueChanged) bool {
	select {
	case changes <- change:
		return true
	case <-stopChan:
		return false
	}
}

// Streams the diff from |last| to |current| into |changes|, using both left-right and top-down approach in parallel.
// The left-right diff is expected to return results earlier, whereas the top-down approach is faster overall. This "best" algorithm runs both:
// - early results from left-right are sent to |changes|.
// - if/when top-down catches up, left-right is stopped and the rest of the changes are streamed from top-down.
func orderedSequenceDiffBest(last orderedSequence, current orderedSequence, changes chan<- ValueChanged, stopChan <-chan struct{}) bool {
	lrChanges := make(chan ValueChanged)
	tdChanges := make(chan ValueChanged)
	// Give the stop channels a buffer size of 1 so that they won't block (see below).
	lrStopChan := make(chan struct{}, 1)
	tdStopChan := make(chan struct{}, 1)

	// Ensure all diff functions have finished doing work by the time this function returns, otherwise database reads might cause deadlock - e.g. https://github.com/attic-labs/noms/issues/2165.
	wg := &sync.WaitGroup{}

	defer func() {
		// Stop diffing. The left-right or top-down diff might have already finished, but sending to the stop channels won't block due to the buffer.
		lrStopChan <- struct{}{}
		tdStopChan <- struct{}{}
		wg.Wait()
	}()

	wg.Add(2)
	go func() {
		defer wg.Done()
		orderedSequenceDiffLeftRight(last, current, lrChanges, lrStopChan)
		close(lrChanges)
	}()
	go func() {
		defer wg.Done()
		orderedSequenceDiffTopDown(last, current, tdChanges, tdStopChan)
		close(tdChanges)
	}()

	// Stream left-right changes while the top-down diff algorithm catches up.
	var lrChangeCount, tdChangeCount int

	for multiplexing := true; multiplexing; {
		select {
		case <-stopChan:
			return false
		case c, ok := <-lrChanges:
			if !ok {
				// Left-right diff completed.
				return true
			}
			lrChangeCount++
			if !sendChange(changes, stopChan, c) {
				return false
			}
		case c, ok := <-tdChanges:
			if !ok {
				// Top-down diff completed.
				return true
			}
			tdChangeCount++
			if tdChangeCount > lrChangeCount {
				// Top-down diff has overtaken left-right diff.
				if !sendChange(changes, stopChan, c) {
					return false
				}
				lrStopChan <- struct{}{}
				multiplexing = false
			}
		}
	}

	for c := range tdChanges {
		if !sendChange(changes, stopChan, c) {
			return false
		}
	}
	return true
}

// Streams the diff from |last| to |current| into |changes|, using a top-down approach.
// Top-down is parallel and efficiently returns the complete diff, but compared to left-right it's slow to start streaming changes.
func orderedSequenceDiffTopDown(last orderedSequence, current orderedSequence, changes chan<- ValueChanged, stopChan <-chan struct{}) bool {
	return orderedSequenceDiffInternalNodes(last, current, changes, stopChan)
}

// TODO - something other than the literal edit-distance, which is way too much cpu work for this case - https://github.com/attic-labs/noms/issues/2027
func orderedSequenceDiffInternalNodes(last orderedSequence, current orderedSequence, changes chan<- ValueChanged, stopChan <-chan struct{}) bool {
	if last.treeLevel() > current.treeLevel() {
		lastChild := last.getCompositeChildSequence(0, uint64(last.seqLen())).(orderedSequence)
		return orderedSequenceDiffInternalNodes(lastChild, current, changes, stopChan)
	}

	if current.treeLevel() > last.treeLevel() {
		currentChild := current.getCompositeChildSequence(0, uint64(current.seqLen())).(orderedSequence)
		return orderedSequenceDiffInternalNodes(last, currentChild, changes, stopChan)
	}

	if last.isLeaf() && current.isLeaf() {
		return orderedSequenceDiffLeftRight(last, current, changes, stopChan)
	}

	compareFn := last.getCompareFn(current)
	initialSplices := calcSplices(uint64(last.seqLen()), uint64(current.seqLen()), DEFAULT_MAX_SPLICE_MATRIX_SIZE,
		func(i uint64, j uint64) bool { return compareFn(int(i), int(j)) })

	for _, splice := range initialSplices {
		var lastChild, currentChild orderedSequence
		functions.All(
			func() {
				lastChild = last.getCompositeChildSequence(splice.SpAt, splice.SpRemoved).(orderedSequence)
			},
			func() {
				currentChild = current.getCompositeChildSequence(splice.SpFrom, splice.SpAdded).(orderedSequence)
			},
		)
		if ok := orderedSequenceDiffInternalNodes(lastChild, currentChild, changes, stopChan); !ok {
			return false
		}
	}

	return true
}

// Streams the diff from |last| to |current| into |changes|, using a left-right approach.
// Left-right immediately descends to the first change and starts streaming changes, but compared to top-down it's serial and much slower to calculate the full diff.
func orderedSequenceDiffLeftRight(last orderedSequence, current orderedSequence, changes chan<- ValueChanged, stopChan <-chan struct{}) bool {
	lastCur := newCursorAt(last, emptyKey, false, false)
	currentCur := newCursorAt(current, emptyKey, false, false)

	for lastCur.valid() && currentCur.valid() {
		fastForward(lastCur, currentCur)

		for lastCur.valid() && currentCur.valid() &&
			!lastCur.seq.getCompareFn(currentCur.seq)(lastCur.idx, currentCur.idx) {
			lastKey := getCurrentKey(lastCur)
			currentKey := getCurrentKey(currentCur)
			if currentKey.Less(lastKey) {
				if !sendChange(changes, stopChan, ValueChanged{DiffChangeAdded, currentKey.v, nil, getMapValue(currentCur)}) {
					return false
				}
				currentCur.advance()
			} else if lastKey.Less(currentKey) {
				if !sendChange(changes, stopChan, ValueChanged{DiffChangeRemoved, lastKey.v, getMapValue(lastCur), nil}) {
					return false
				}
				lastCur.advance()
			} else {
				if !sendChange(changes, stopChan, ValueChanged{DiffChangeModified, lastKey.v, getMapValue(lastCur), getMapValue(currentCur)}) {
					return false
				}
				lastCur.advance()
				currentCur.advance()
			}
		}
	}

	for lastCur.valid() {
		if !sendChange(changes, stopChan, ValueChanged{DiffChangeRemoved, getCurrentKey(lastCur).v, getMapValue(lastCur), nil}) {
			return false
		}
		lastCur.advance()
	}
	for currentCur.valid() {
		if !sendChange(changes, stopChan, ValueChanged{DiffChangeAdded, getCurrentKey(currentCur).v, nil, getMapValue(currentCur)}) {
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
	d.PanicIfFalse(a.valid())
	d.PanicIfFalse(b.valid())
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
