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

package types

// DumbEditAccumulator is a simple EditAccumulator and EditProvider implementation that allows for more complex
// implementations to be put into other packages. It is fine for small edits, and tests, but edits.AsyncSortedEdits
// performs much better for large amounts of data
type DumbEditAccumulator struct {
	pos   int
	edits KVPSlice
	nbf   *NomsBinFormat
}

// NewDumbEditAccumulator is a factory method for creation of DumbEditAccumulators
func NewDumbEditAccumulator(nbf *NomsBinFormat) EditAccumulator {
	return &DumbEditAccumulator{0, nil, nbf}
}

// AddEdit adds an edit to the list of edits
func (dumb *DumbEditAccumulator) AddEdit(k LesserValuable, v Valuable) {
	dumb.edits = append(dumb.edits, KVP{k, v})
}

// FinishEditing should be called when all edits have been added to get an EditProvider which provides the
// edits in sorted order.  Adding more edits after calling FinishedEditing is an error
func (dumb *DumbEditAccumulator) FinishedEditing() (EditProvider, error) {
	err := SortWithErroringLess(KVPSort{dumb.edits, dumb.nbf})

	if err != nil {
		return nil, err
	}

	return dumb, nil
}

// Next returns the next KVP representing the next edit to be applied.  Next will always return KVPs
// in key sorted order
func (dumb *DumbEditAccumulator) Next() (*KVP, error) {
	var curr *KVP
	if dumb.pos < len(dumb.edits) {
		curr = &dumb.edits[dumb.pos]
		dumb.pos++
	}

	return curr, nil
}

// NumEdits returns the number of KVPs representing the edits that will be provided when calling next
func (dumb *DumbEditAccumulator) NumEdits() int64 {
	return int64(len(dumb.edits))
}
