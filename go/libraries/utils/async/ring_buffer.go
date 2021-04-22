// Copyright 2021 Dolthub, Inc.
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

package async

import (
	"errors"
	"io"
	"os"
	"sync"
)

// RingBuffer is a dynamically sized ring buffer that is thread safe
type RingBuffer struct {
	cond      *sync.Cond
	allocSize int

	closed    bool
	epoch     int
	headPos   int
	tailPos   int
	headSlice int
	tailSlice int

	items [][]interface{}
}

// Returned from Push() when the supplied epoch does not match the
// buffer's current epoch.
var ErrWrongEpoch = errors.New("ring buffer: wrong epoch")

// NewRingBuffer creates a new RingBuffer instance
func NewRingBuffer(allocSize int) *RingBuffer {
	itemBuffer := make([]interface{}, allocSize*2)
	items := [][]interface{}{itemBuffer[:allocSize], itemBuffer[allocSize:]}

	return &RingBuffer{
		cond:      sync.NewCond(&sync.Mutex{}),
		allocSize: allocSize,
		items:     items,
	}
}

// Reset clears a ring buffer so that it can be reused
func (rb *RingBuffer) Reset() int {
	rb.cond.L.Lock()
	defer rb.cond.L.Unlock()

	rb.closed = false
	rb.headPos = 0
	rb.tailPos = 0
	rb.headSlice = 0
	rb.tailSlice = 0
	rb.epoch += 1

	for i := 0; i < len(rb.items); i++ {
		for j := 0; j < len(rb.items[i]); j++ {
			rb.items[i][j] = nil
		}
	}

	return rb.epoch
}

// Close closes a RingBuffer so that no new items can be pushed onto it.  Items that are already in the buffer can still
// be read via Pop and TryPop.  Close will broadcast to all go routines waiting inside Pop
func (rb *RingBuffer) Close() error {
	rb.cond.L.Lock()
	defer rb.cond.L.Unlock()

	if rb.closed {
		return os.ErrClosed
	}

	rb.closed = true
	rb.cond.Broadcast()

	return nil
}

// Push will add a new item to the RingBuffer and signal a go routine waiting inside Pop that new data is available
func (rb *RingBuffer) Push(item interface{}, epoch int) error {
	rb.cond.L.Lock()
	defer rb.cond.L.Unlock()

	if rb.closed {
		return os.ErrClosed
	}
	if epoch != rb.epoch {
		return ErrWrongEpoch
	}

	rb.items[rb.headSlice][rb.headPos] = item
	rb.headPos++

	if rb.headPos == rb.allocSize {
		numSlices := len(rb.items)
		nextSlice := (rb.headSlice + 1) % numSlices

		if nextSlice == rb.tailSlice {
			newItems := make([][]interface{}, numSlices+1)

			j := 0
			for i := rb.tailSlice; i < numSlices; i, j = i+1, j+1 {
				newItems[j] = rb.items[i]
			}

			for i := 0; i < rb.tailSlice; i, j = i+1, j+1 {
				newItems[j] = rb.items[i]
			}

			newItems[numSlices] = make([]interface{}, rb.allocSize)

			rb.items = newItems
			rb.tailSlice = 0
			rb.headSlice = numSlices
		} else {
			rb.headSlice = nextSlice
		}

		rb.headPos = 0
	}

	rb.cond.Signal()

	return nil
}

// noLockPop is used internally by methods that already hold a lock on the RingBuffer and should never be called directly
func (rb *RingBuffer) noLockPop() (interface{}, bool) {
	if rb.tailPos == rb.headPos && rb.tailSlice == rb.headSlice {
		return nil, false
	}

	item := rb.items[rb.tailSlice][rb.tailPos]
	rb.tailPos++

	if rb.tailPos == rb.allocSize {
		rb.tailPos = 0
		rb.tailSlice = (rb.tailSlice + 1) % len(rb.items)
	}

	return item, true
}

// TryPop will return the next item in the RingBuffer.  If there are no items available TryPop will return immediately
// with with `ok` set to false.
func (rb *RingBuffer) TryPop() (item interface{}, ok bool) {
	rb.cond.L.Lock()
	defer rb.cond.L.Unlock()

	return rb.noLockPop()
}

// Pop will return the next item in the RingBuffer. If there are no items available, Pop will wait until a new item is
// pushed, or the RingBuffer is closed.
func (rb *RingBuffer) Pop() (item interface{}, err error) {
	rb.cond.L.Lock()
	defer rb.cond.L.Unlock()

	for {
		item, ok := rb.noLockPop()

		if ok {
			return item, nil
		}

		if rb.closed {
			return nil, io.EOF
		}

		rb.cond.Wait()
	}
}
