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

package types

import (
	"context"
	"io"
)

// DumbEditAccumulator is a simple EditAccumulator and EditProvider implementation that allows for more complex
// implementations to be put into other packages. It is fine for small edits, and tests, but edits.AsyncSortedEdits
// performs much better for large amounts of data
type DumbEditAccumulator struct {
	vr         ValueReader
	edits      KVPSlice
	pos        int
	reachedEOF bool
}

// NewDumbEditAccumulator is a factory method for creation of DumbEditAccumulators
func NewDumbEditAccumulator(vr ValueReader) EditAccumulator {
	return &DumbEditAccumulator{vr: vr}
}

// EditsAdded returns the number of edits that have been added to this EditAccumulator
func (dumb *DumbEditAccumulator) EditsAdded() int {
	return len(dumb.edits)
}

// AddEdit adds an edit to the list of edits
func (dumb *DumbEditAccumulator) AddEdit(k LesserValuable, v Valuable) {
	dumb.edits = append(dumb.edits, KVP{k, v})
}

// FinishEditing should be called when all edits have been added to get an EditProvider which provides the
// edits in sorted order. Adding more edits after calling FinishedEditing is an error
func (dumb *DumbEditAccumulator) FinishedEditing(ctx context.Context) (EditProvider, error) {
	err := SortWithErroringLess(ctx, dumb.vr.Format(), KVPSort{dumb.edits})

	if err != nil {
		return nil, err
	}

	return dumb, nil
}

// Close satisfies the EditAccumulator interface
func (dumb *DumbEditAccumulator) Close(ctx context.Context) error {
	return nil
}

// Next returns the next KVP representing the next edit to be applied.  Next will always return KVPs
// in key sorted order. Once all KVPs have been read io.EOF will be returned.
func (dumb *DumbEditAccumulator) Next(ctx context.Context) (*KVP, error) {
	if dumb.pos < len(dumb.edits) {
		curr := &dumb.edits[dumb.pos]
		dumb.pos++

		return curr, nil
	}

	dumb.reachedEOF = true
	return nil, io.EOF
}

// ReachedEOF returns true once all data is exhausted.  If ReachedEOF returns false that does not mean that there
// is more data, only that io.EOF has not been returned previously.  If ReachedEOF returns true then all edits have
// been read
func (dumb *DumbEditAccumulator) ReachedEOF() bool {
	return dumb.reachedEOF
}
