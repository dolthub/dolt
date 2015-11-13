package types

import (
	"sort"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
)

// metaSequenceCursor allows traversal of a tree of metaSequence nodes.

type metaSequenceCursor struct {
	parent   *metaSequenceCursor
	sequence metaSequence
	idx      int
	cs       chunks.ChunkSource
}

func newMetaSequenceCursor(root metaSequence, cs chunks.ChunkSource) *metaSequenceCursor {
	cursor := &metaSequenceCursor{nil, root, 0, cs}
	parent := cursor
	child := ReadValue(cursor.current().ref, cs)
	if ms, ok := child.(metaSequence); ok {
		cursor = newMetaSequenceCursor(ms, cs)
		cursor.parent = parent
	}

	return cursor
}

func (ms *metaSequenceCursor) advance() bool {
	newIdx := ms.idx + 1

	if newIdx < ms.sequence.tupleCount() {
		ms.idx = newIdx
		return true
	}

	if ms.parent != nil && ms.parent.advance() {
		ms.readSequence()
		ms.idx = 0
		return true
	}

	return false
}

func (ms *metaSequenceCursor) readSequence() {
	if ms.sequence.Ref() == ms.parent.current().ref {
		return
	}

	ms.sequence = ReadValue(ms.parent.current().ref, ms.cs).(metaSequence)
	d.Chk.NotNil(ms.sequence)
}

func (ms *metaSequenceCursor) current() metaTuple {
	d.Chk.NotNil(ms.sequence)
	d.Chk.True(ms.idx >= 0 && ms.idx < ms.sequence.tupleCount())
	return ms.sequence.tupleAt(ms.idx)
}

type metaSequenceSeekFn func(v, parent Value) bool
type seekParentValueFn func(parent, prev, curr Value) Value

// |seek| will never advance the cursor beyond the final tuple in the cursor, even if seekFn never returns true
func (ms *metaSequenceCursor) seek(seekFn metaSequenceSeekFn, parentValueFn seekParentValueFn, parentValue Value) Value {
	d.Chk.NotNil(seekFn) // parentValueFn is allowed to be nil

	if ms.parent != nil {
		parentValue = ms.parent.seek(seekFn, parentValueFn, parentValue)
		ms.readSequence()
	}

	ms.idx = sort.Search(ms.sequence.tupleCount(), func(i int) bool {
		return seekFn(ms.sequence.tupleAt(i).value, parentValue)
	})

	if ms.idx >= ms.sequence.tupleCount() {
		ms.idx = ms.sequence.tupleCount() - 1
	}

	if parentValueFn == nil {
		return nil
	}

	var prev Value
	if ms.idx > 0 {
		prev = ms.sequence.tupleAt(ms.idx - 1).value
	}

	return parentValueFn(parentValue, prev, ms.sequence.tupleAt(ms.idx).value)
}
