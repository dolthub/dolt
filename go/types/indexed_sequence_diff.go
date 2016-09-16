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

func indexedSequenceDiff(last sequence, lastHeight int, lastOffset uint64, current sequence, currentHeight int, currentOffset uint64, changes chan<- Splice, closeChan <-chan struct{}, maxSpliceMatrixSize uint64) bool {
	if lastHeight > currentHeight {
		lastChild := last.(metaSequence).getCompositeChildSequence(0, uint64(last.seqLen()))
		return indexedSequenceDiff(lastChild, lastHeight-1, lastOffset, current, currentHeight, currentOffset, changes, closeChan, maxSpliceMatrixSize)
	}

	if currentHeight > lastHeight {
		currentChild := current.(metaSequence).getCompositeChildSequence(0, uint64(current.seqLen()))
		return indexedSequenceDiff(last, lastHeight, lastOffset, currentChild, currentHeight-1, currentOffset, changes, closeChan, maxSpliceMatrixSize)
	}

	compareFn := last.getCompareFn(current)
	initialSplices := calcSplices(uint64(last.seqLen()), uint64(current.seqLen()), maxSpliceMatrixSize,
		func(i uint64, j uint64) bool { return compareFn(int(i), int(j)) })

	for _, splice := range initialSplices {
		if !isMetaSequence(last) {
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

		lastMeta := last.(metaSequence)
		currentMeta := current.(metaSequence)

		if splice.SpRemoved == 0 || splice.SpAdded == 0 {
			// An entire subtree was removed at a meta level. We must do some math to map the splice from the meta level into the leaf coordinates.
			beginRemoveIndex := uint64(0)
			if splice.SpAt > 0 {
				beginRemoveIndex = lastMeta.cumulativeNumberOfLeaves(int(splice.SpAt) - 1)
			}
			endRemoveIndex := uint64(0)
			if splice.SpAt+splice.SpRemoved > 0 {
				endRemoveIndex = lastMeta.cumulativeNumberOfLeaves(int(splice.SpAt+splice.SpRemoved) - 1)
			}
			beginAddIndex := uint64(0)
			if splice.SpFrom > 0 {
				beginAddIndex = currentMeta.cumulativeNumberOfLeaves(int(splice.SpFrom) - 1)
			}
			endAddIndex := uint64(0)
			if splice.SpFrom+splice.SpAdded > 0 {
				endAddIndex = currentMeta.cumulativeNumberOfLeaves(int(splice.SpFrom+splice.SpAdded) - 1)
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
		lastChild := lastMeta.getCompositeChildSequence(splice.SpAt, splice.SpRemoved)
		currentChild := currentMeta.getCompositeChildSequence(splice.SpFrom, splice.SpAdded)
		lastChildOffset := lastOffset
		if splice.SpAt > 0 {
			lastChildOffset += lastMeta.cumulativeNumberOfLeaves(int(splice.SpAt) - 1)
		}
		currentChildOffset := currentOffset
		if splice.SpFrom > 0 {
			currentChildOffset += currentMeta.cumulativeNumberOfLeaves(int(splice.SpFrom) - 1)
		}
		if ok := indexedSequenceDiff(lastChild, lastHeight-1, lastChildOffset, currentChild, currentHeight-1, currentChildOffset, changes, closeChan, maxSpliceMatrixSize); !ok {
			return false
		}
	}

	return true
}
