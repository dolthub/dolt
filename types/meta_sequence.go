package types

import (
	"crypto/sha1"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

const (
	objectWindowSize = 8
	objectPattern    = uint32(1<<6 - 1) // Average size of 64 elements
)

// metaSequence is a logical abstraction, but has no concrete "base" implementation. A Meta Sequence is a non-leaf (internal) node of a "probably" tree, which results from the chunking of an ordered or unordered sequence of values.

type metaSequence interface {
	Value
	data() metaSequenceData
	tupleAt(idx int) metaTuple
	tupleSlice(to int) []metaTuple
	lastTuple() metaTuple
	tupleCount() int
}

type metaTuple struct {
	ref   ref.Ref
	value Value
}

func (mt metaTuple) uint64Value() uint64 {
	return uint64(mt.value.(Uint64))
}

type metaSequenceData []metaTuple

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

func (ms metaSequenceObject) lastTuple() metaTuple {
	return ms.tuples[len(ms.tuples)-1]
}

func (ms metaSequenceObject) ChildValues() []Value {
	leafType := ms.t.Desc.(CompoundDesc).ElemTypes[0]
	refOfLeafType := MakeCompoundType(RefKind, leafType)
	res := make([]Value, len(ms.tuples))
	for i, t := range ms.tuples {
		res[i] = refFromType(t.ref, refOfLeafType)
	}
	return res
}

func (ms metaSequenceObject) Chunks() (chunks []ref.Ref) {
	for _, tuple := range ms.tuples {
		chunks = append(chunks, tuple.ref)
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

func newMetaSequenceBoundaryChecker() boundaryChecker {
	return newBuzHashBoundaryChecker(objectWindowSize, sha1.Size, objectPattern, func(item sequenceItem) []byte {
		digest := item.(metaTuple).ref.Digest()
		return digest[:]
	})
}

func newMetaSequenceChunkFn(t Type, cs chunks.ChunkStore) makeChunkFn {
	return func(items []sequenceItem) (sequenceItem, Value) {
		tuples := make(metaSequenceData, len(items))
		offsetSum := uint64(0)

		for i, v := range items {
			mt := v.(metaTuple)
			offsetSum += mt.uint64Value()
			tuples[i] = metaTuple{mt.ref, Uint64(offsetSum)}
		}

		meta := newMetaSequenceFromData(tuples, t, cs)
		ref := WriteValue(meta, cs)
		return metaTuple{ref, Uint64(offsetSum)}, meta
	}
}

func normalizeMetaSequenceChunk(in []sequenceItem) (out []sequenceItem) {
	offset := uint64(0)
	for _, v := range in {
		mt := v.(metaTuple)
		out = append(out, metaTuple{mt.ref, Uint64(mt.uint64Value() - offset)})
		offset = mt.uint64Value()
	}
	return
}

// Creates a sequenceCursor pointing to the first metaTuple in a metaSequence, and returns that cursor plus the leaf Value referenced from that metaTuple.
func newMetaSequenceCursor(root metaSequence, cs chunks.ChunkStore) (*sequenceCursor, Value) {
	d.Chk.NotNil(root)

	newCursor := func(parent *sequenceCursor, ms metaSequence) *sequenceCursor {
		return &sequenceCursor{parent, ms, 0, ms.tupleCount(), func(item sequenceItem, idx int) sequenceItem {
			return item.(metaSequence).tupleAt(idx)
		}, func(item sequenceItem) (sequenceItem, int) {
			ms := readMetaTupleValue(item, cs).(metaSequence)
			return ms, ms.tupleCount()
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
	return ReadValue(item.(metaTuple).ref, cs)
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
