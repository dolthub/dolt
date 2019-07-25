// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/store/nbs"
	"sync"

	"github.com/liquidata-inc/ld/dolt/go/store/d"
	"github.com/liquidata-inc/ld/dolt/go/store/util/functions"
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
func orderedSequenceDiffBest(ctx context.Context, last orderedSequence, current orderedSequence, ae *nbs.AtomicError, changes chan<- ValueChanged, stopChan <-chan struct{}) bool {
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
		defer close(lrChanges)

		orderedSequenceDiffLeftRight(ctx, last, current, ae, lrChanges, lrStopChan)
	}()
	go func() {
		defer wg.Done()
		defer close(tdChanges)

		orderedSequenceDiffTopDown(ctx, last, current, ae, tdChanges, tdStopChan)
	}()

	// Stream left-right changes while the top-down diff algorithm catches up.
	var lrChangeCount, tdChangeCount int

	for multiplexing := true; multiplexing; {
		if ae.IsSet() {
			return false
		}

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
func orderedSequenceDiffTopDown(ctx context.Context, last orderedSequence, current orderedSequence, ae *nbs.AtomicError, changes chan<- ValueChanged, stopChan <-chan struct{}) bool {
	return orderedSequenceDiffInternalNodes(ctx, last, current, ae, changes, stopChan)
}

// TODO - something other than the literal edit-distance, which is way too much cpu work for this case - https://github.com/attic-labs/noms/issues/2027
func orderedSequenceDiffInternalNodes(ctx context.Context, last orderedSequence, current orderedSequence, ae *nbs.AtomicError, changes chan<- ValueChanged, stopChan <-chan struct{}) bool {
	if last.treeLevel() > current.treeLevel() && !ae.IsSet() {
		lastChild, err := last.getCompositeChildSequence(ctx, 0, uint64(last.seqLen()))

		if ae.SetIfErrAndCheck(err) {
			return false
		}

		return orderedSequenceDiffInternalNodes(ctx, lastChild.(orderedSequence), current, ae, changes, stopChan)
	}

	if current.treeLevel() > last.treeLevel() && !ae.IsSet() {
		currentChild, err := current.getCompositeChildSequence(ctx, 0, uint64(current.seqLen()))

		if ae.SetIfErrAndCheck(err) {
			return false
		}

		return orderedSequenceDiffInternalNodes(ctx, last, currentChild.(orderedSequence), ae, changes, stopChan)
	}

	if last.isLeaf() && current.isLeaf() && !ae.IsSet() {
		return orderedSequenceDiffLeftRight(ctx, last, current, ae, changes, stopChan)
	}

	if ae.IsSet() {
		return false
	}

	compareFn := last.getCompareFn(current)
	initialSplices, err := calcSplices(uint64(last.seqLen()), uint64(current.seqLen()), DEFAULT_MAX_SPLICE_MATRIX_SIZE,
		func(i uint64, j uint64) (bool, error) { return compareFn(int(i), int(j)) })

	if ae.SetIfErrAndCheck(err) {
		return false
	}

	for _, splice := range initialSplices {
		if ae.IsSet() {
			return false
		}

		var lastChild, currentChild orderedSequence
		functions.All(
			func() {
				seq, err := last.getCompositeChildSequence(ctx, splice.SpAt, splice.SpRemoved)

				if !ae.SetIfError(err) {
					lastChild = seq.(orderedSequence)
				}
			},
			func() {
				seq, err := current.getCompositeChildSequence(ctx, splice.SpFrom, splice.SpAdded)

				if !ae.SetIfError(err) {
					currentChild = seq.(orderedSequence)
				}
			},
		)

		if !orderedSequenceDiffInternalNodes(ctx, lastChild, currentChild, ae, changes, stopChan) {
			return false
		}
	}

	return true
}

// Streams the diff from |last| to |current| into |changes|, using a left-right approach.
// Left-right immediately descends to the first change and starts streaming changes, but compared to top-down it's serial and much slower to calculate the full diff.
func orderedSequenceDiffLeftRight(ctx context.Context, last orderedSequence, current orderedSequence, ae *nbs.AtomicError, changes chan<- ValueChanged, stopChan <-chan struct{}) bool {
	lastCur, err := newCursorAt(ctx, last, emptyKey, false, false)

	if ae.SetIfErrAndCheck(err) {
		return false
	}

	currentCur, err := newCursorAt(ctx, current, emptyKey, false, false)

	if ae.SetIfErrAndCheck(err) {
		return false
	}

	for lastCur.valid() && currentCur.valid() {
		if ae.IsSet() {
			return false
		}

		err := fastForward(ctx, lastCur, currentCur)

		if ae.SetIfErrAndCheck(err) {
			return false
		}

		equals, err := lastCur.seq.getCompareFn(currentCur.seq)(lastCur.idx, currentCur.idx)

		if ae.SetIfErrAndCheck(err) {
			return false
		}

		for lastCur.valid() && currentCur.valid() && !equals {
			if ae.IsSet() {
				return false
			}

			lastKey, err := getCurrentKey(lastCur)

			if ae.SetIfErrAndCheck(err) {
				return false
			}

			currentKey, err := getCurrentKey(currentCur)

			if ae.SetIfErrAndCheck(err) {
				return false
			}

			if currentKey.Less(last.format(), lastKey) {
				mv, err := getMapValue(currentCur)

				if ae.SetIfErrAndCheck(err) {
					return false
				}

				if !sendChange(changes, stopChan, ValueChanged{DiffChangeAdded, currentKey.v, nil, mv}) {
					return false
				}

				_, err = currentCur.advance(ctx)

				if ae.SetIfErrAndCheck(err) {
					return false
				}
			} else if lastKey.Less(last.format(), currentKey) {
				mv, err := getMapValue(lastCur)

				if ae.SetIfErrAndCheck(err) {
					return false
				}

				if !sendChange(changes, stopChan, ValueChanged{DiffChangeRemoved, lastKey.v, mv, nil}) {
					return false
				}

				_, err = lastCur.advance(ctx)

				if ae.SetIfErrAndCheck(err) {
					return false
				}
			} else {
				lmv, err := getMapValue(currentCur)

				if ae.SetIfErrAndCheck(err) {
					return false
				}

				cmv, err := getMapValue(currentCur)

				if ae.SetIfErrAndCheck(err) {
					return false
				}

				if !sendChange(changes, stopChan, ValueChanged{DiffChangeModified, lastKey.v, lmv, cmv}) {
					return false
				}

				_, err = lastCur.advance(ctx)

				if ae.SetIfErrAndCheck(err) {
					return false
				}

				_, err = currentCur.advance(ctx)

				if ae.SetIfErrAndCheck(err) {
					return false
				}
			}
		}
	}

	for lastCur.valid() && !ae.IsSet() {
		currKey, err := getCurrentKey(lastCur)

		if ae.SetIfErrAndCheck(err) {
			return false
		}

		mv, err := getMapValue(lastCur)
		if ae.SetIfErrAndCheck(err) {
			return false
		}

		if !sendChange(changes, stopChan, ValueChanged{DiffChangeRemoved, currKey.v, mv, nil}) {
			return false
		}

		_, err = lastCur.advance(ctx)

		if ae.SetIfErrAndCheck(err) {
			return false
		}
	}

	for currentCur.valid() && !ae.IsSet() {
		currKey, err := getCurrentKey(lastCur)

		if ae.SetIfErrAndCheck(err) {
			return false
		}

		mv, err := getMapValue(currentCur)

		if ae.SetIfErrAndCheck(err) {
			return false
		}

		if !sendChange(changes, stopChan, ValueChanged{DiffChangeAdded, currKey.v, nil, mv}) {
			return false
		}

		_, err = currentCur.advance(ctx)

		if ae.SetIfErrAndCheck(err) {
			return false
		}
	}

	return true
}

/**
 * Advances |a| and |b| past their common sequence of equal values.
 */
func fastForward(ctx context.Context, a *sequenceCursor, b *sequenceCursor) error {
	if a.valid() && b.valid() {
		_, _, err := doFastForward(ctx, true, a, b)

		if err != nil {
			return err
		}
	}

	return nil
}

func syncWithIdx(ctx context.Context, cur *sequenceCursor, hasMore bool, allowPastEnd bool) error {
	err := cur.sync(ctx)

	if err != nil {
		return err
	}

	if hasMore {
		cur.idx = 0
	} else if allowPastEnd {
		cur.idx = cur.length()
	} else {
		cur.idx = cur.length() - 1
	}

	return nil
}

/*
 * Returns an array matching |a| and |b| respectively to whether that cursor has more values.
 */
func doFastForward(ctx context.Context, allowPastEnd bool, a *sequenceCursor, b *sequenceCursor) (aHasMore bool, bHasMore bool, err error) {
	d.PanicIfFalse(a.valid())
	d.PanicIfFalse(b.valid())
	aHasMore = true
	bHasMore = true

	equals, err := isCurrentEqual(a, b)

	if err != nil {
		return false, false, err
	}

	for aHasMore && bHasMore && equals {
		parentsEqual, err := isCurrentEqual(a.parent, b.parent)

		if err != nil {
			return false, false, err
		}

		if nil != a.parent && nil != b.parent && parentsEqual {
			// Key optimisation: if the sequences have common parents, then entire chunks can be
			// fast-forwarded without reading unnecessary data.
			aHasMore, bHasMore, err = doFastForward(ctx, false, a.parent, b.parent)

			if err != nil {
				return false, false, err
			}

			err := syncWithIdx(ctx, a, aHasMore, allowPastEnd)

			if err != nil {
				return false, false, err
			}

			err = syncWithIdx(ctx, b, bHasMore, allowPastEnd)

			if err != nil {
				return false, false, err
			}
		} else {
			aHasMore, err = a.advanceMaybeAllowPastEnd(ctx, allowPastEnd)

			if err != nil {
				return false, false, err
			}

			bHasMore, err = b.advanceMaybeAllowPastEnd(ctx, allowPastEnd)

			if err != nil {
				return false, false, err
			}
		}
	}
	return aHasMore, bHasMore, nil
}

func isCurrentEqual(a *sequenceCursor, b *sequenceCursor) (bool, error) {
	return a.seq.getCompareFn(b.seq)(a.idx, b.idx)
}
