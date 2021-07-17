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

package editor

import "github.com/dolthub/dolt/go/store/types"

// indexOperationStack is a limited-size stack, intended for usage with the index editor and its undo functionality.
// As operations are added, the internal array is filled up. Once it is full, new operations replace the oldest ones.
// This reduces memory usage compared to a traditional stack with an unbounded size, as undo should always come
// immediately after an operation is added.
type indexOperationStack struct {
	// entries has a length of 4 as an UPDATE on a table is a Delete & Insert on the index, so we double it for safety.
	entries [4]indexOperation
	// This is the index of the next item we are adding. Add at this index, then increment.
	currentIndex uint64
	// Represents the number of items relative to the "stack size".
	numOfItems uint64
}

// indexOperation is an operation performed by the index editor, along with the key used.
type indexOperation struct {
	isInsert   bool
	fullKey    types.Tuple
	partialKey types.Tuple
	value types.Tuple
}

// Push adds the given keys to the top of the stack.
func (ios *indexOperationStack) Push(isInsert bool, fullKey, partialKey types.Tuple, value types.Tuple) {
	ios.entries[ios.currentIndex].isInsert = isInsert
	ios.entries[ios.currentIndex].fullKey = fullKey
	ios.entries[ios.currentIndex].partialKey = partialKey
	ios.entries[ios.currentIndex].value = value
	ios.currentIndex = (ios.currentIndex + 1) % uint64(len(ios.entries))
	ios.numOfItems++
	if ios.numOfItems > uint64(len(ios.entries)) {
		ios.numOfItems = uint64(len(ios.entries))
	}
}

// Pop removes and returns the keys from the top of the stack. Returns false if the stack is empty.
func (ios *indexOperationStack) Pop() (indexOperation, bool) {
	if ios.numOfItems == 0 {
		return indexOperation{}, false
	}
	ios.numOfItems--
	ios.currentIndex = (ios.currentIndex - 1) % uint64(len(ios.entries))
	return ios.entries[ios.currentIndex], true
}
