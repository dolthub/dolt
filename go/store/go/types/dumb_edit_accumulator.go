package types

import "sort"

// DumbEditAccumulator is a simple EditAccumulator and EditProvider implementation that allows for more complex
// implementations to be put into other packages. It is fine for small edits, and tests, but edits.AsyncSortedEdits
// performs much better for large amounts of data
type DumbEditAccumulator struct {
	pos   int
	edits KVPSlice
}

// NewDumbEditAccumulator is a factory method for creation of DumbEditAccumulators
func NewDumbEditAccumulator() EditAccumulator {
	return &DumbEditAccumulator{}
}

// AddEdit adds an edit to the list of edits
func (dumb *DumbEditAccumulator) AddEdit(k LesserValuable, v Valuable) {
	dumb.edits = append(dumb.edits, KVP{k, v})
}

// FinishEditing should be called when all edits have been added to get an EditProvider which provides the
// edits in sorted order.  Adding more edits after calling FinishedEditing is an error
func (dumb *DumbEditAccumulator) FinishedEditing() EditProvider {
	sort.Stable(dumb.edits)
	return dumb
}

// Next returns the next KVP representing the next edit to be applied.  Next will always return KVPs
// in key sorted order
func (dumb *DumbEditAccumulator) Next() *KVP {
	var curr *KVP
	if dumb.pos < len(dumb.edits) {
		curr = &dumb.edits[dumb.pos]
		dumb.pos++
	}

	return curr
}

// NumEdits returns the number of KVPs representing the edits that will be provided when calling next
func (dumb *DumbEditAccumulator) NumEdits() int64 {
	return int64(len(dumb.edits))
}
