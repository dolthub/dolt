// Copyright 2023 Dolthub, Inc.
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

package nbs

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// errUnsafeIteration is returned when iterating through a ring buffer too slowly and new, inserted data is detected
// as wrapping around into the iteration range.
var errUnsafeIteration = errors.New(
	"unable to finish iteration: insertion index has wrapped around into iteration range")

// reflogRootHashEntry is a data container for a root hash update that was recorded to the chunk journal. It contains
// the root and the time at which it was written.
type reflogRootHashEntry struct {
	timestamp time.Time
	root      string
}

// reflogRingBuffer is a fixed size circular buffer that allows the most recent N entries to be iterated over (where
// N is equal to the size requested when this ring buffer is constructed. Its locking strategy assumes that
// only new entries are written to the head (through Push) and that existing entries will never need to be
// updated. Internally, it allocates a slice that is twice as large as the requested size, so that less locking
// is needed when iterating over entries to read them.
type reflogRingBuffer struct {
	mu            *sync.Mutex
	items         []reflogRootHashEntry
	requestedSize int
	totalSize     int
	insertIndex   int
	itemCount     int
	epoch         uint
}

// newReflogRingBuffer creates a new reflogRingBuffer that allows the reflog to query up to |size| records.
// Internally, the ring buffer allocates extra storage so that |size| records can be read while new root entries
// are still being recorded.
func newReflogRingBuffer(size int) *reflogRingBuffer {
	if size < 0 {
		panic(fmt.Sprintf("invalid size specified in newReflogRingBuffer construction: %d", size))
	}

	return &reflogRingBuffer{
		requestedSize: size,
		totalSize:     size * 2,
		items:         make([]reflogRootHashEntry, size*2),
		mu:            &sync.Mutex{},
		insertIndex:   0,
		itemCount:     0,
		epoch:         1,
	}
}

// Push pushes |newItem| onto this ring buffer, replacing the oldest entry in this ring buffer once the buffer
// is fully populated.
func (rb *reflogRingBuffer) Push(newItem reflogRootHashEntry) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.items[rb.insertIndex] = newItem
	rb.insertIndex = (rb.insertIndex + 1) % len(rb.items)
	if rb.insertIndex == 0 {
		rb.epoch++
	}

	if rb.itemCount < rb.requestedSize {
		rb.itemCount++
	}
}

// Iterate traverses the entries in this ring buffer and invokes the specified callback function, |f|, on each
// entry. Iteration starts with the oldest entries inserted into this ring buffer and ends with the most recent
// entry. This function will iterate over at most N entries, where N is the requested size the caller specified
// when constructing this ring buffer.
func (rb *reflogRingBuffer) Iterate(f func(item reflogRootHashEntry) error) error {
	startPosition, endPosition, startingEpoch := rb.getIterationIndexes()
	if startPosition == endPosition {
		return nil
	}

	for idx := startPosition; ; {
		// The ring buffer holds twice as many entries as we ever expose through the Iterate function, so that
		// entries can still be inserted without having to lock the whole ring buffer during iteration. However,
		// as a sanity check, before we look at an index, we make sure the current insertion index hasn't
		// gone into the range we're iterating.
		if rb.insertionIndexIsInRange(startPosition, endPosition, startingEpoch) {
			return errUnsafeIteration
		}

		err := f(rb.items[idx])
		if err != nil {
			return err
		}

		// Move to next spot
		idx = (idx + 1) % rb.totalSize
		if idx == endPosition {
			break
		}
	}

	return nil
}

// Truncate resets this ring buffer so that it is empty.
func (rb *reflogRingBuffer) Truncate() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.itemCount = 0
}

// getIterationIndexes returns the start (inclusive) and end (exclusive) positions for iterating over the
// entries in this ring buffer, as well as the current epoch, or generation of the ring buffer for the starting
// position. Note that the end position may be less than the start position, which indicates that iteration
// wraps around the ring buffer.
func (rb *reflogRingBuffer) getIterationIndexes() (int, int, uint) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	// If the buffer is empty, return the start position equal to the end position so that iteration is a no-op
	if rb.itemCount == 0 || rb.totalSize == 0 {
		return rb.insertIndex, rb.insertIndex, rb.epoch
	}

	// When the ring buffer isn't fully populated yet, we need to be careful to limit iteration to the number
	// of items that have actually been inserted. Once more entries have been inserted than the requested size
	// of this ring buffer, we will iterate over only the most recent entries and limit to the requested size.
	itemCount := rb.itemCount
	if itemCount > rb.requestedSize {
		itemCount = rb.requestedSize
	}

	endPosition := rb.insertIndex
	startPosition := (endPosition - itemCount) % rb.totalSize
	epoch := rb.epoch
	if startPosition < 0 {
		startPosition = rb.totalSize + startPosition
		epoch--
	}

	return startPosition, endPosition, epoch
}

// insertionIndexIsInRange returns true if the current insertion pointer for this ring buffer is within the
// specified |rangeStart| and |rangeEnd| indexes. The |startingEpoch| parameter is used to determine if the
// current insertion index has wrapped around the ring buffer, possibly multiple times.
func (rb *reflogRingBuffer) insertionIndexIsInRange(rangeStart, rangeEnd int, startingEpoch uint) bool {
	rb.mu.Lock()
	currentInsertIndex := rb.insertIndex
	currentEpoch := rb.epoch
	rb.mu.Unlock()

	// When the epoch value overflows and wraps around to 0 again, adjust the starting epoch accordingly
	epochDelta := currentEpoch - startingEpoch
	if epochDelta < 0 {
		maxUint := ^uint(0)
		epochDelta += maxUint
	}

	// If the range wraps around the ring buffer, adjust currentInsertIndex and rangeEnd
	// so that we can use the same logic for an in range check.
	if rangeStart > rangeEnd {
		currentInsertIndex += rb.totalSize
		rangeEnd += rb.totalSize
		epochDelta--
	}

	switch epochDelta {
	case 0:
		// same epoch
		return currentInsertIndex >= rangeStart && currentInsertIndex < rangeEnd
	case 1:
		return currentInsertIndex >= rangeStart
	default:
		return true
	}
}
