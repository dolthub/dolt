package types

import (
	"sort"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

// metaSequenceCursor allows traversal of a tree of metaSequence nodes.

type metaSequenceCursor struct {
	parent   *metaSequenceCursor
	sequence metaSequence
	idx      int
	cs       chunks.ChunkSource
}

func newMetaSequenceCursor(root metaSequence, cs chunks.ChunkSource) (cursor *metaSequenceCursor, leaf Value) {
	cursors := []*metaSequenceCursor{&metaSequenceCursor{nil, root, 0, cs}}
	for {
		cursor = cursors[len(cursors)-1]
		leaf = cursor.currentVal()
		if ms, ok := leaf.(metaSequence); ok {
			cursors = append(cursors, &metaSequenceCursor{cursor, ms, 0, cs})
		} else {
			return
		}
	}

	panic("not reachable")
}

type cursorIterFn func(v Value) bool

func iterateMetaSequenceLeaf(root metaSequence, cs chunks.ChunkSource, cb cursorIterFn) {
	cursor, v := newMetaSequenceCursor(root, cs)
	for {
		if cb(v) || !cursor.advance() {
			return
		}

		v = cursor.currentVal()
	}

	panic("not reachable")
}

func (ms *metaSequenceCursor) clone() sequenceCursor {
	var parent *metaSequenceCursor
	if ms.parent != nil {
		parent = ms.parent.clone().(*metaSequenceCursor)
	}

	return &metaSequenceCursor{parent, ms.sequence, ms.idx, ms.cs}
}

func (ms *metaSequenceCursor) getParent() sequenceCursor {
	if ms.parent == nil {
		return nil
	}

	return ms.parent
}

func (ms *metaSequenceCursor) advance() bool {
	newIdx := ms.idx + 1

	if newIdx < ms.sequence.tupleCount() {
		ms.idx = newIdx
		return true
	}

	if ms.parent != nil && ms.parent.advance() {
		ms.syncToParent()
		ms.idx = 0
		return true
	}

	return false
}

func (ms *metaSequenceCursor) retreat() bool {
	newIdx := ms.idx - 1

	if newIdx >= 0 {
		ms.idx = newIdx
		return true
	}

	if ms.parent != nil && ms.parent.retreat() {
		ms.syncToParent()
		ms.idx = ms.sequence.tupleCount() - 1
		return true
	}

	return false
}

func (ms *metaSequenceCursor) syncToParent() {
	if ms.sequence.Ref() == ms.parent.currentRef() {
		return
	}

	ms.sequence = ms.parent.currentVal().(metaSequence)
	d.Chk.NotNil(ms.sequence)
}

func (ms *metaSequenceCursor) indexInChunk() int {
	return ms.idx
}

func (ms *metaSequenceCursor) current() sequenceItem {
	d.Chk.NotNil(ms.sequence)
	d.Chk.True(ms.idx >= 0 && ms.idx < ms.sequence.tupleCount())
	return ms.sequence.tupleAt(ms.idx)
}

func (ms *metaSequenceCursor) currentVal() Value {
	return ReadValue(ms.currentRef(), ms.cs)
}

func (ms *metaSequenceCursor) currentRef() ref.Ref {
	return ms.current().(metaTuple).ref
}

type metaSequenceSeekFn func(v, parent Value) bool
type seekParentValueFn func(parent, prev, curr Value) Value

// |seek| will never advance the cursor beyond the final tuple in the cursor, even if seekFn never returns true
func (ms *metaSequenceCursor) seek(seekFn metaSequenceSeekFn, parentValueFn seekParentValueFn, parentValue Value) Value {
	d.Chk.NotNil(seekFn) // parentValueFn is allowed to be nil

	if ms.parent != nil {
		parentValue = ms.parent.seek(seekFn, parentValueFn, parentValue)
		ms.syncToParent()
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
