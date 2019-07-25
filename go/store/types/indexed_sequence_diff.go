// Copyright 2019 Liquidata, Inc.
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

package types

import "context"

func sendSpliceChange(changes chan<- Splice, closeChan <-chan struct{}, splice Splice) bool {
	select {
	case changes <- splice:
	case <-closeChan:
		return false
	}
	return true
}

func indexedSequenceDiff(ctx context.Context, last sequence, lastOffset uint64, current sequence, currentOffset uint64, changes chan<- Splice, closeChan <-chan struct{}, maxSpliceMatrixSize uint64) (bool, error) {
	if last.treeLevel() > current.treeLevel() {
		lastChild, err := last.getCompositeChildSequence(ctx, 0, uint64(last.seqLen()))

		if err != nil {
			return false, err
		}

		return indexedSequenceDiff(ctx, lastChild, lastOffset, current, currentOffset, changes, closeChan, maxSpliceMatrixSize)
	}

	if current.treeLevel() > last.treeLevel() {
		currentChild, err := current.getCompositeChildSequence(ctx, 0, uint64(current.seqLen()))

		if err != nil {
			return false, err
		}

		return indexedSequenceDiff(ctx, last, lastOffset, currentChild, currentOffset, changes, closeChan, maxSpliceMatrixSize)
	}

	compareFn := last.getCompareFn(current)
	initialSplices, err := calcSplices(uint64(last.seqLen()), uint64(current.seqLen()), maxSpliceMatrixSize,
		func(i uint64, j uint64) (bool, error) { return compareFn(int(i), int(j)) })

	if err != nil {
		return false, err
	}

	for _, splice := range initialSplices {
		if last.isLeaf() {
			// This is a leaf sequence, we can just report the splice, but it's indices must be offset.
			splice.SpAt += lastOffset
			if splice.SpAdded > 0 {
				splice.SpFrom += currentOffset
			}

			if !sendSpliceChange(changes, closeChan, splice) {
				return false, nil
			}
			continue
		}

		if splice.SpRemoved == 0 || splice.SpAdded == 0 {
			var err error
			// An entire subtree was removed at a meta level. We must do some math to map the splice from the meta level into the leaf coordinates.
			beginRemoveIndex := uint64(0)
			if splice.SpAt > 0 {
				beginRemoveIndex, err = last.cumulativeNumberOfLeaves(int(splice.SpAt) - 1)

				if err != nil {
					return false, err
				}
			}
			endRemoveIndex := uint64(0)
			if splice.SpAt+splice.SpRemoved > 0 {
				endRemoveIndex, err = last.cumulativeNumberOfLeaves(int(splice.SpAt+splice.SpRemoved) - 1)

				if err != nil {
					return false, err
				}
			}
			beginAddIndex := uint64(0)
			if splice.SpFrom > 0 {
				beginAddIndex, err = current.cumulativeNumberOfLeaves(int(splice.SpFrom) - 1)

				if err != nil {
					return false, err
				}
			}
			endAddIndex := uint64(0)
			if splice.SpFrom+splice.SpAdded > 0 {
				endAddIndex, err = current.cumulativeNumberOfLeaves(int(splice.SpFrom+splice.SpAdded) - 1)

				if err != nil {
					return false, err
				}
			}

			splice.SpAt = lastOffset + beginRemoveIndex
			splice.SpRemoved = endRemoveIndex - beginRemoveIndex

			splice.SpAdded = endAddIndex - beginAddIndex
			if splice.SpAdded > 0 {
				splice.SpFrom = currentOffset + beginAddIndex
			}

			if !sendSpliceChange(changes, closeChan, splice) {
				return false, nil
			}
			continue
		}

		// Meta sequence splice which includes removed & added sub-sequences. Must recurse down.
		lastChild, err := last.getCompositeChildSequence(ctx, splice.SpAt, splice.SpRemoved)

		if err != nil {
			return false, err
		}

		currentChild, err := current.getCompositeChildSequence(ctx, splice.SpFrom, splice.SpAdded)

		if err != nil {
			return false, err
		}

		lastChildOffset := lastOffset
		if splice.SpAt > 0 {
			cnt, err := last.cumulativeNumberOfLeaves(int(splice.SpAt) - 1)

			if err != nil {
				return false, err
			}

			lastChildOffset += cnt
		}
		currentChildOffset := currentOffset
		if splice.SpFrom > 0 {
			cnt, err := current.cumulativeNumberOfLeaves(int(splice.SpFrom) - 1)

			if err != nil {
				return false, err
			}

			currentChildOffset += cnt
		}

		if ok, err := indexedSequenceDiff(ctx, lastChild, lastChildOffset, currentChild, currentChildOffset, changes, closeChan, maxSpliceMatrixSize); err != nil {
			return false, err
		} else if !ok {
			return false, nil
		}
	}

	return true, nil
}
