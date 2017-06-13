// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

func sendSpliceChange(changes chan<- Splice, closeChan <-chan struct{}, splice Splice) bool {
	select {
	case changes <- splice:
	case <-closeChan:
		return false
	}
	return true
}

func indexedSequenceDiff(last sequence, lastOffset uint64, current sequence, currentOffset uint64, changes chan<- Splice, closeChan <-chan struct{}, maxSpliceMatrixSize uint64) bool {
	if last.treeLevel() > current.treeLevel() {
		lastChild := last.getCompositeChildSequence(0, uint64(last.seqLen()))
		return indexedSequenceDiff(lastChild, lastOffset, current, currentOffset, changes, closeChan, maxSpliceMatrixSize)
	}

	if current.treeLevel() > last.treeLevel() {
		currentChild := current.getCompositeChildSequence(0, uint64(current.seqLen()))
		return indexedSequenceDiff(last, lastOffset, currentChild, currentOffset, changes, closeChan, maxSpliceMatrixSize)
	}

	compareFn := last.getCompareFn(current)
	initialSplices := calcSplices(uint64(last.seqLen()), uint64(current.seqLen()), maxSpliceMatrixSize,
		func(i uint64, j uint64) bool { return compareFn(int(i), int(j)) })

	for _, splice := range initialSplices {
		if last.isLeaf() {
			// This is a leaf sequence, we can just report the splice, but it's indices must be offset.
			splice.SpAt += lastOffset
			if splice.SpAdded > 0 {
				splice.SpFrom += currentOffset
			}

			if !sendSpliceChange(changes, closeChan, splice) {
				return false
			}
			continue
		}

		if splice.SpRemoved == 0 || splice.SpAdded == 0 {
			// An entire subtree was removed at a meta level. We must do some math to map the splice from the meta level into the leaf coordinates.
			beginRemoveIndex := uint64(0)
			if splice.SpAt > 0 {
				beginRemoveIndex = last.cumulativeNumberOfLeaves(int(splice.SpAt) - 1)
			}
			endRemoveIndex := uint64(0)
			if splice.SpAt+splice.SpRemoved > 0 {
				endRemoveIndex = last.cumulativeNumberOfLeaves(int(splice.SpAt+splice.SpRemoved) - 1)
			}
			beginAddIndex := uint64(0)
			if splice.SpFrom > 0 {
				beginAddIndex = current.cumulativeNumberOfLeaves(int(splice.SpFrom) - 1)
			}
			endAddIndex := uint64(0)
			if splice.SpFrom+splice.SpAdded > 0 {
				endAddIndex = current.cumulativeNumberOfLeaves(int(splice.SpFrom+splice.SpAdded) - 1)
			}

			splice.SpAt = lastOffset + beginRemoveIndex
			splice.SpRemoved = endRemoveIndex - beginRemoveIndex

			splice.SpAdded = endAddIndex - beginAddIndex
			if splice.SpAdded > 0 {
				splice.SpFrom = currentOffset + beginAddIndex
			}

			if !sendSpliceChange(changes, closeChan, splice) {
				return false
			}
			continue
		}

		// Meta sequence splice which includes removed & added sub-sequences. Must recurse down.
		lastChild := last.getCompositeChildSequence(splice.SpAt, splice.SpRemoved)
		currentChild := current.getCompositeChildSequence(splice.SpFrom, splice.SpAdded)
		lastChildOffset := lastOffset
		if splice.SpAt > 0 {
			lastChildOffset += last.cumulativeNumberOfLeaves(int(splice.SpAt) - 1)
		}
		currentChildOffset := currentOffset
		if splice.SpFrom > 0 {
			currentChildOffset += current.cumulativeNumberOfLeaves(int(splice.SpFrom) - 1)
		}
		if ok := indexedSequenceDiff(lastChild, lastChildOffset, currentChild, currentChildOffset, changes, closeChan, maxSpliceMatrixSize); !ok {
			return false
		}
	}

	return true
}
