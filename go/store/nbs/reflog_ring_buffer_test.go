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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIteration asserts that we can iterate over the contents of a reflog ring buffer as the ring buffer grows.
func TestIteration(t *testing.T) {
	buffer := newReflogRingBuffer(5)

	// Assert that Iterate returns the correct items in the correct order when the buffer
	// contains fewer items than the requested buffer size.
	insertTestRecord(buffer, "aaaa")
	insertTestRecord(buffer, "bbbb")
	insertTestRecord(buffer, "cccc")
	assertExpectedIterationOrder(t, buffer, []string{"aaaa", "bbbb", "cccc"})

	// Assert that Iterate returns the correct items in the correct order when the buffer
	// contains the same number of items as the requested buffer size.
	insertTestRecord(buffer, "dddd")
	insertTestRecord(buffer, "eeee")
	assertExpectedIterationOrder(t, buffer, []string{"aaaa", "bbbb", "cccc", "dddd", "eeee"})

	// Insert two new records that cause the buffer to exclude the first two records
	insertTestRecord(buffer, "ffff")
	insertTestRecord(buffer, "gggg")
	assertExpectedIterationOrder(t, buffer, []string{"cccc", "dddd", "eeee", "ffff", "gggg"})

	// Insert three records to fill up the buffer's internal capacity
	insertTestRecord(buffer, "hhhh")
	insertTestRecord(buffer, "iiii")
	insertTestRecord(buffer, "jjjj")
	assertExpectedIterationOrder(t, buffer, []string{"ffff", "gggg", "hhhh", "iiii", "jjjj"})

	// Insert four records to test the buffer wrapping around for the first time
	insertTestRecord(buffer, "kkkk")
	insertTestRecord(buffer, "llll")
	insertTestRecord(buffer, "mmmm")
	insertTestRecord(buffer, "nnnn")
	assertExpectedIterationOrder(t, buffer, []string{"jjjj", "kkkk", "llll", "mmmm", "nnnn"})

	// Insert 10 records to test the buffer wrapping around a second time
	insertTestRecord(buffer, "oooo")
	insertTestRecord(buffer, "pppp")
	insertTestRecord(buffer, "qqqq")
	insertTestRecord(buffer, "rrrr")
	insertTestRecord(buffer, "ssss")
	insertTestRecord(buffer, "tttt")
	insertTestRecord(buffer, "uuuu")
	insertTestRecord(buffer, "vvvv")
	insertTestRecord(buffer, "wwww")
	insertTestRecord(buffer, "xxxx")
	assertExpectedIterationOrder(t, buffer, []string{"tttt", "uuuu", "vvvv", "wwww", "xxxx"})
}

// TestTruncate asserts that the Truncate works correctly regardless of how much data
// is currently stored in the buffer.
func TestTruncate(t *testing.T) {
	buffer := newReflogRingBuffer(5)

	// When the buffer is empty, Truncate is a no-op
	buffer.Truncate()
	assertExpectedIterationOrder(t, buffer, []string{})
	buffer.Truncate()
	assertExpectedIterationOrder(t, buffer, []string{})

	// When the buffer contains a single item
	insertTestRecord(buffer, "aaaa")
	buffer.Truncate()
	assertExpectedIterationOrder(t, buffer, []string{})
	buffer.Truncate()
	assertExpectedIterationOrder(t, buffer, []string{})

	// When the buffer is not full, Truncate empties the buffer
	insertTestRecord(buffer, "bbbb")
	insertTestRecord(buffer, "cccc")
	insertTestRecord(buffer, "dddd")
	buffer.Truncate()
	assertExpectedIterationOrder(t, buffer, []string{})

	// When the buffer is full, Truncate empties the buffer
	insertTestRecord(buffer, "aaaa")
	insertTestRecord(buffer, "bbbb")
	insertTestRecord(buffer, "cccc")
	insertTestRecord(buffer, "dddd")
	insertTestRecord(buffer, "eeee")
	insertTestRecord(buffer, "ffff")
	insertTestRecord(buffer, "gggg")
	insertTestRecord(buffer, "hhhh")
	insertTestRecord(buffer, "iiii")
	insertTestRecord(buffer, "jjjj")
	insertTestRecord(buffer, "kkkk")
	insertTestRecord(buffer, "llll")
	insertTestRecord(buffer, "mmmm")
	buffer.Truncate()
	assertExpectedIterationOrder(t, buffer, []string{})
}

// TestIterationConflict asserts that when iterating through a reflog ring buffer and new items are written to the
// buffer and wrap around into the iteration range, that iteration stops early and an error is returned.
func TestIterationConflict(t *testing.T) {
	buffer := newReflogRingBuffer(5)
	buffer.Push(reflogRootHashEntry{root: "aaaa", timestamp: time.Now()})
	buffer.Push(reflogRootHashEntry{root: "bbbb", timestamp: time.Now()})
	buffer.Push(reflogRootHashEntry{root: "cccc", timestamp: time.Now()})
	buffer.Push(reflogRootHashEntry{root: "dddd", timestamp: time.Now()})
	buffer.Push(reflogRootHashEntry{root: "eeee", timestamp: time.Now()})

	iterationCount := 0
	err := buffer.Iterate(func(item reflogRootHashEntry) error {
		for i := 0; i < 100; i++ {
			buffer.Push(reflogRootHashEntry{root: fmt.Sprintf("i-%d", i), timestamp: time.Now()})
		}
		iterationCount++
		return nil
	})
	require.Error(t, err)
	require.Equal(t, errUnsafeIteration, err)
	require.True(t, iterationCount < 5)
}

func insertTestRecord(buffer *reflogRingBuffer, root string) {
	buffer.Push(reflogRootHashEntry{
		root:      root,
		timestamp: time.Now(),
	})
}

func assertExpectedIterationOrder(t *testing.T, buffer *reflogRingBuffer, expectedRoots []string) {
	i := 0
	err := buffer.Iterate(func(item reflogRootHashEntry) error {
		assert.Equal(t, expectedRoots[i], item.root)
		assert.False(t, time.Time.IsZero(item.timestamp))
		i++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, len(expectedRoots), i)
}
