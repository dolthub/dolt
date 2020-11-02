// Copyright 2019 Dolthub, Inc.
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

func sendSpliceChange(ctx context.Context, changes chan<- Splice, splice Splice) error {
	select {
	case changes <- splice:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func indexedSequenceDiff(ctx context.Context, last sequence, lastOffset uint64, current sequence, currentOffset uint64, changes chan<- Splice, maxSpliceMatrixSize uint64) error {
	if last.treeLevel() > current.treeLevel() {
		lastChild, err := last.getCompositeChildSequence(ctx, 0, uint64(last.seqLen()))
		if err != nil {
			return err
		}

		return indexedSequenceDiff(ctx, lastChild, lastOffset, current, currentOffset, changes, maxSpliceMatrixSize)
	}

	if current.treeLevel() > last.treeLevel() {
		currentChild, err := current.getCompositeChildSequence(ctx, 0, uint64(current.seqLen()))
		if err != nil {
			return err
		}

		return indexedSequenceDiff(ctx, last, lastOffset, currentChild, currentOffset, changes, maxSpliceMatrixSize)
	}

	compareFn := last.getCompareFn(current)
	initialSplices, err := calcSplices(uint64(last.seqLen()), uint64(current.seqLen()), maxSpliceMatrixSize,
		func(i uint64, j uint64) (bool, error) { return compareFn(int(i), int(j)) })
	if err != nil {
		return err
	}

	for _, splice := range initialSplices {
		if last.isLeaf() {
			// This is a leaf sequence, we can just report the splice, but it's indices must be offset.
			splice.SpAt += lastOffset
			if splice.SpAdded > 0 {
				splice.SpFrom += currentOffset
			}
			if err := sendSpliceChange(ctx, changes, splice); err != nil {
				return err
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
					return err
				}
			}
			endRemoveIndex := uint64(0)
			if splice.SpAt+splice.SpRemoved > 0 {
				endRemoveIndex, err = last.cumulativeNumberOfLeaves(int(splice.SpAt+splice.SpRemoved) - 1)
				if err != nil {
					return err
				}
			}
			beginAddIndex := uint64(0)
			if splice.SpFrom > 0 {
				beginAddIndex, err = current.cumulativeNumberOfLeaves(int(splice.SpFrom) - 1)
				if err != nil {
					return err
				}
			}
			endAddIndex := uint64(0)
			if splice.SpFrom+splice.SpAdded > 0 {
				endAddIndex, err = current.cumulativeNumberOfLeaves(int(splice.SpFrom+splice.SpAdded) - 1)
				if err != nil {
					return err
				}
			}

			splice.SpAt = lastOffset + beginRemoveIndex
			splice.SpRemoved = endRemoveIndex - beginRemoveIndex

			splice.SpAdded = endAddIndex - beginAddIndex
			if splice.SpAdded > 0 {
				splice.SpFrom = currentOffset + beginAddIndex
			}

			if err := sendSpliceChange(ctx, changes, splice); err != nil {
				return err
			}
			continue
		}

		// Meta sequence splice which includes removed & added sub-sequences. Must recurse down.
		lastChild, err := last.getCompositeChildSequence(ctx, splice.SpAt, splice.SpRemoved)
		if err != nil {
			return err
		}

		currentChild, err := current.getCompositeChildSequence(ctx, splice.SpFrom, splice.SpAdded)
		if err != nil {
			return err
		}

		lastChildOffset := lastOffset
		if splice.SpAt > 0 {
			cnt, err := last.cumulativeNumberOfLeaves(int(splice.SpAt) - 1)
			if err != nil {
				return err
			}

			lastChildOffset += cnt
		}
		currentChildOffset := currentOffset
		if splice.SpFrom > 0 {
			cnt, err := current.cumulativeNumberOfLeaves(int(splice.SpFrom) - 1)
			if err != nil {
				return err
			}

			currentChildOffset += cnt
		}

		if err := indexedSequenceDiff(ctx, lastChild, lastChildOffset, currentChild, currentChildOffset, changes, maxSpliceMatrixSize); err != nil {
			return err
		}
	}

	return nil
}
