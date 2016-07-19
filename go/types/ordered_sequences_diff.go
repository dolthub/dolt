// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"sync"

	"github.com/attic-labs/noms/go/d"
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

func sendChange(changes chan<- ValueChanged, closeChan <-chan struct{}, change ValueChanged) error {
	select {
	case changes <- change:
	case <-closeChan:
		close(changes)
		return ChangeChanClosedErr
	}
	return nil
}

// TODO - something other than the literal edit-distance, which is way too much cpu work for this case - https://github.com/attic-labs/noms/issues/2027
func orderedSequenceDiff(last orderedSequence, current orderedSequence, changes chan<- ValueChanged, closeChan <-chan struct{}) error {
	var lastHeight, currentHeight int
	wg := &sync.WaitGroup{}
	wg.Add(2)
	getHeight := func(seq orderedSequence, out *int) {
		cur := newCursorAt(seq, emptyKey, false, false)
		*out = cur.depth()
		wg.Done()
	}
	go getHeight(last, &lastHeight)
	go getHeight(current, &currentHeight)
	wg.Wait()
	return orderedSequenceDiffInternalNodes(last, current, changes, closeChan, lastHeight, currentHeight)
}

func orderedSequenceDiffInternalNodes(last orderedSequence, current orderedSequence, changes chan<- ValueChanged, closeChan <-chan struct{}, lastHeight, currentHeight int) error {
	if lastHeight > currentHeight {
		lastChild := last.(orderedMetaSequence).getCompositeChildSequence(0, uint64(last.seqLen())).(orderedSequence)
		return orderedSequenceDiffInternalNodes(lastChild, current, changes, closeChan, lastHeight-1, currentHeight)
	}

	if currentHeight > lastHeight {
		currentChild := current.(orderedMetaSequence).getCompositeChildSequence(0, uint64(current.seqLen())).(orderedSequence)
		return orderedSequenceDiffInternalNodes(last, currentChild, changes, closeChan, lastHeight, currentHeight-1)
	}

	if !isMetaSequence(last) && !isMetaSequence(current) {
		return orderedSequenceDiffLeafItems(last, current, changes, closeChan)
	} else {
		compareFn := last.getCompareFn(current)
		initialSplices := calcSplices(uint64(last.seqLen()), uint64(current.seqLen()), DEFAULT_MAX_SPLICE_MATRIX_SIZE,
			func(i uint64, j uint64) bool { return compareFn(int(i), int(j)) })

		for _, splice := range initialSplices {
			var lastChild, currentChild orderedSequence
			wg := &sync.WaitGroup{}
			wg.Add(2)
			go func() {
				lastChild = last.(orderedMetaSequence).getCompositeChildSequence(splice.SpAt, splice.SpRemoved).(orderedSequence)
				wg.Done()
			}()
			go func() {
				currentChild = current.(orderedMetaSequence).getCompositeChildSequence(splice.SpFrom, splice.SpAdded).(orderedSequence)
				wg.Done()
			}()
			wg.Wait()
			err := orderedSequenceDiffInternalNodes(lastChild, currentChild, changes, closeChan, lastHeight-1, currentHeight-1)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func orderedSequenceDiffLeafItems(last orderedSequence, current orderedSequence, changes chan<- ValueChanged, closeChan <-chan struct{}) error {
	lastCur := newCursorAt(last, emptyKey, false, false)
	currentCur := newCursorAt(current, emptyKey, false, false)

	for lastCur.valid() && currentCur.valid() {
		fastForward(lastCur, currentCur)

		for lastCur.valid() && currentCur.valid() &&
			!lastCur.seq.getCompareFn(currentCur.seq)(lastCur.idx, currentCur.idx) {
			lastKey := getCurrentKey(lastCur)
			currentKey := getCurrentKey(currentCur)
			if currentKey.Less(lastKey) {
				if err := sendChange(changes, closeChan, ValueChanged{DiffChangeAdded, currentKey.v}); err != nil {
					return err
				}
				currentCur.advance()
			} else if lastKey.Less(currentKey) {
				if err := sendChange(changes, closeChan, ValueChanged{DiffChangeRemoved, lastKey.v}); err != nil {
					return err
				}
				lastCur.advance()
			} else {
				if err := sendChange(changes, closeChan, ValueChanged{DiffChangeModified, lastKey.v}); err != nil {
					return err
				}
				lastCur.advance()
				currentCur.advance()
			}
		}
	}

	for lastCur.valid() {
		if err := sendChange(changes, closeChan, ValueChanged{DiffChangeRemoved, getCurrentKey(lastCur).v}); err != nil {
			return err
		}
		lastCur.advance()
	}
	for currentCur.valid() {
		if err := sendChange(changes, closeChan, ValueChanged{DiffChangeAdded, getCurrentKey(currentCur).v}); err != nil {
			return err
		}
		currentCur.advance()
	}
	return nil
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
