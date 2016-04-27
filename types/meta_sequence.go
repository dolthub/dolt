package types

import "github.com/attic-labs/noms/d"

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

func newMetaTuple(value, child Value, childRef Ref, numLeaves uint64) metaTuple {
	d.Chk.True((child != nil) != (childRef != Ref{}), "Either child or childRef can be set, but not both")
	return metaTuple{child, childRef, value, numLeaves}
}

// metaTuple is a node in a Prolly Tree, consisting of data in the node (either tree leaves or other metaSequences), and a Value annotation for exploring the tree (e.g. the largest item if this an ordered sequence).
type metaTuple struct {
	child     Value   // nil if the child data hasn't been read, or has already been written
	childRef  Ref // may be empty if |child| is non-nil; call ChildRef() instead of accessing |childRef| directly
	value     Value
	numLeaves uint64
}

func (mt metaTuple) ChildRef() Ref {
	if mt.child != nil {
		return NewTypedRef(MakeRefType(mt.child.Type()), mt.child.Ref())
	}
	d.Chk.False(mt.childRef.TargetRef().IsEmpty())
	return mt.childRef
}

func (mt metaTuple) uint64Value() uint64 {
	return uint64(mt.value.(Number))
}

type metaSequenceData []metaTuple

func (msd metaSequenceData) uint64ValuesSum() (sum uint64) {
	for _, mt := range msd {
		sum += mt.uint64Value()
	}
	return
}

func (msd metaSequenceData) numLeavesSum() (sum uint64) {
	for _, mt := range msd {
		sum += mt.numLeaves
	}
	return
}

func (msd metaSequenceData) last() metaTuple {
	return msd[len(msd)-1]
}

type metaSequenceObject struct {
	tuples metaSequenceData
	t      *Type
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
	refOfLeafType := MakeRefType(leafType)
	res := make([]Value, len(ms.tuples))
	for i, t := range ms.tuples {
		res[i] = NewTypedRef(refOfLeafType, t.ChildRef().TargetRef())
	}
	return res
}

func (ms metaSequenceObject) Chunks() (chunks []Ref) {
	for _, tuple := range ms.tuples {
		chunks = append(chunks, tuple.ChildRef())
	}
	return
}

func (ms metaSequenceObject) Type() *Type {
	return ms.t
}

type metaBuilderFunc func(tuples metaSequenceData, t *Type, vr ValueReader) Value

var metaFuncMap = map[NomsKind]metaBuilderFunc{}

func registerMetaValue(k NomsKind, bf metaBuilderFunc) {
	metaFuncMap[k] = bf
}

func newMetaSequenceFromData(tuples metaSequenceData, t *Type, vr ValueReader) Value {
	if bf, ok := metaFuncMap[t.Kind()]; ok {
		return bf(tuples, t, vr)
	}

	panic("not reachable")
}

// Creates a sequenceCursor pointing to the first metaTuple in a metaSequence, and returns that cursor plus the leaf Value referenced from that metaTuple.
func newMetaSequenceCursor(root metaSequence, vr ValueReader) (*sequenceCursor, Value) {
	d.Chk.NotNil(root)

	newCursor := func(parent *sequenceCursor, ms metaSequence) *sequenceCursor {
		return &sequenceCursor{parent, ms, 0, ms.tupleCount(), func(otherMs sequenceItem, idx int) sequenceItem {
			return otherMs.(metaSequence).tupleAt(idx)
		}, func(item sequenceItem) (sequenceItem, int) {
			otherMs := readMetaTupleValue(item, vr).(metaSequence)
			return otherMs, otherMs.tupleCount()
		}}
	}

	cursors := []*sequenceCursor{newCursor(nil, root)}
	for {
		cursor := cursors[len(cursors)-1]
		val := readMetaTupleValue(cursor.current(), vr)
		if ms, ok := val.(metaSequence); ok {
			cursors = append(cursors, newCursor(cursor, ms))
		} else {
			return cursor, val
		}
	}
}

func readMetaTupleValue(item sequenceItem, vr ValueReader) Value {
	mt := item.(metaTuple)
	if mt.child != nil {
		return mt.child
	}

	r := mt.childRef.TargetRef()
	d.Chk.False(r.IsEmpty())
	return vr.ReadValue(r)
}

func iterateMetaSequenceLeaf(ms metaSequence, vr ValueReader, cb func(Value) bool) {
	cursor, v := newMetaSequenceCursor(ms, vr)
	for {
		if cb(v) || !cursor.advance() {
			return
		}

		v = readMetaTupleValue(cursor.current(), vr)
	}
}
