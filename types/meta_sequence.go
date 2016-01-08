package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

const (
	objectWindowSize          = 8
	orderedSequenceWindowSize = 1
	objectPattern             = uint32(1<<6 - 1) // Average size of 64 elements
)

// metaSequence is a logical abstraction, but has no concrete "base" implementation. A Meta Sequence is a non-leaf (internal) node of a "probably" tree, which results from the chunking of an ordered or unordered sequence of values.
type metaSequence interface {
	Value
	data() metaSequenceData
	tupleAt(idx int) metaTuple
	tupleSlice(to int) []metaTuple
	tupleCount() int
}

// metaTuple is a node in a "probably" tree, consisting of data in the node (either tree leaves or other metaSequences), and a Value annotation for exploring the tree (e.g. the largest item if this an ordered sequence).
type metaTuple struct {
	child    Value // may be nil if the child data hasn't been read yet
	childRef ref.Ref
	value    Value
}

func (mt metaTuple) uint64Value() uint64 {
	return uint64(mt.value.(Uint64))
}

type metaSequenceData []metaTuple

func (msd metaSequenceData) uint64ValuesSum() (sum uint64) {
	for _, mt := range msd {
		sum += mt.uint64Value()
	}
	return
}

func (msd metaSequenceData) last() metaTuple {
	return msd[len(msd)-1]
}

type metaSequenceObject struct {
	tuples metaSequenceData
	t      Type
}

func (ms metaSequenceObject) tupleAt(idx int) metaTuple {
	return ms.tuples[idx]
}

func (ms metaSequenceObject) tupleSlice(to int) []metaTuple {
	return ms.tuples[:to]
}

func (ms metaSequenceObject) tupleCount() int {
	return len(ms.tuples)
}

func (ms metaSequenceObject) data() metaSequenceData {
	return ms.tuples
}

func (ms metaSequenceObject) ChildValues() []Value {
	leafType := ms.t.Desc.(CompoundDesc).ElemTypes[0]
	refOfLeafType := MakeCompoundType(RefKind, leafType)
	res := make([]Value, len(ms.tuples))
	for i, t := range ms.tuples {
		res[i] = refFromType(t.childRef, refOfLeafType)
	}
	return res
}

func (ms metaSequenceObject) Chunks() (chunks []ref.Ref) {
	for _, tuple := range ms.tuples {
		chunks = append(chunks, tuple.childRef)
	}
	return
}

func (ms metaSequenceObject) Type() Type {
	return ms.t
}

type metaBuilderFunc func(tuples metaSequenceData, t Type, cs chunks.ChunkStore) Value

var (
	metaFuncMap map[NomsKind]metaBuilderFunc = map[NomsKind]metaBuilderFunc{}
)

func registerMetaValue(k NomsKind, bf metaBuilderFunc) {
	metaFuncMap[k] = bf
}

func newMetaSequenceFromData(tuples metaSequenceData, t Type, cs chunks.ChunkStore) Value {
	if bf, ok := metaFuncMap[t.Kind()]; ok {
		return bf(tuples, t, cs)
	}

	panic("not reachable")
}

// Creates a sequenceCursor pointing to the first metaTuple in a metaSequence, and returns that cursor plus the leaf Value referenced from that metaTuple.
func newMetaSequenceCursor(root metaSequence, cs chunks.ChunkStore) (*sequenceCursor, Value) {
	d.Chk.NotNil(root)

	newCursor := func(parent *sequenceCursor, ms metaSequence) *sequenceCursor {
		return &sequenceCursor{parent, ms, 0, ms.tupleCount(), func(otherMs sequenceItem, idx int) sequenceItem {
			return otherMs.(metaSequence).tupleAt(idx)
		}, func(item sequenceItem) (sequenceItem, int) {
			otherMs := readMetaTupleValue(item, cs).(metaSequence)
			return otherMs, otherMs.tupleCount()
		}}
	}

	cursors := []*sequenceCursor{newCursor(nil, root)}
	for {
		cursor := cursors[len(cursors)-1]
		val := readMetaTupleValue(cursor.current(), cs)
		if ms, ok := val.(metaSequence); ok {
			cursors = append(cursors, newCursor(cursor, ms))
		} else {
			return cursor, val
		}
	}

	panic("not reachable")
}

func readMetaTupleValue(item sequenceItem, cs chunks.ChunkStore) Value {
	mt := item.(metaTuple)
	if mt.child == nil {
		mt.child = ReadValue(mt.childRef, cs)
		d.Chk.NotNil(mt.child)
	}
	return internalValueFromType(mt.child, mt.child.Type())
}

func iterateMetaSequenceLeaf(ms metaSequence, cs chunks.ChunkStore, cb func(Value) bool) {
	cursor, v := newMetaSequenceCursor(ms, cs)
	for {
		if cb(v) || !cursor.advance() {
			return
		}

		v = readMetaTupleValue(cursor.current(), cs)
	}

	panic("not reachable")
}
