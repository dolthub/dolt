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

package edits

import (
	"context"
	"io"

	"github.com/dolthub/dolt/go/store/types"
)

// SortedEditItr is a KVPIterator implementation that takes two KVPCollItr and merges them as it iterates
type SortedEditItr struct {
	leftItr  *KVPCollItr
	rightItr *KVPCollItr
	done     bool
	read     int64
}

// NewSortedEditItr creates an iterator from two KVPCollection references.  As the iterator iterates it
// merges the collections and iterates in order
func NewSortedEditItr(vr types.ValueReader, left, right *KVPCollection) *SortedEditItr {
	leftItr := NewItr(vr, left)
	rightItr := NewItr(vr, right)

	return &SortedEditItr{leftItr: leftItr, rightItr: rightItr}
}

// Next returns the next KVP representing the next edit to be applied.  Next will always return KVPs
// in key sorted order.  Once all KVPs have been read io.EOF will be returned.
func (itr *SortedEditItr) Next(ctx context.Context) (*types.KVP, error) {
	if itr.done {
		return nil, io.EOF
	}

	lesser := itr.leftItr
	isLess, err := itr.rightItr.Less(ctx, itr.leftItr)

	if err != nil {
		return nil, err
	}

	if isLess {
		lesser = itr.rightItr
	}

	kvp, err := lesser.Next(ctx)

	if err != nil {
		return nil, err
	}

	itr.done = kvp == nil

	if itr.done {
		return nil, io.EOF
	}

	itr.read++

	return kvp, nil
}

// ReachedEOF returns true once all data is exhausted.  If ReachedEOF returns false that does not mean that there
// is more data, only that io.EOF has not been returned previously.  If ReachedEOF returns true then all edits have
// been read
func (itr *SortedEditItr) ReachedEOF() bool {
	return itr.leftItr.ReachedEOF() && itr.rightItr.ReachedEOF()
}

// Peek returns the next KVP without advancing
func (itr *SortedEditItr) Peek(ctx context.Context) (*types.KVP, error) {
	if itr.done {
		return nil, nil
	}

	lesser := itr.leftItr
	isLess, err := itr.rightItr.Less(ctx, itr.leftItr)

	if err != nil {
		return nil, err
	}

	if isLess {
		lesser = itr.rightItr
	}

	return lesser.Peek(), nil
}

// Size returns the total number of elements this iterator will iterate over.
func (itr *SortedEditItr) Size() int64 {
	return itr.leftItr.coll.totalSize + itr.rightItr.coll.totalSize
}

func (itr *SortedEditItr) Close(ctx context.Context) error {
	return nil
}
