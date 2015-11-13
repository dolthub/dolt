package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

// metaSequence is a logical abstraction, but has no concrete "base" implementation. A Meta Sequence is a non-leaf (internal) node of a "probably" tree, which results from the chunking of an ordered or unordered sequence of values.

type metaSequence interface {
	tupleAt(idx int) metaTuple
	lastTuple() metaTuple
	tupleCount() int
	Ref() ref.Ref
}

type metaTuple struct {
	ref   ref.Ref
	value Value
}

func (mt metaTuple) uint64Value() uint64 {
	return uint64(mt.value.(UInt64))
}

type metaSequenceData []metaTuple

type metaBuilderFunc func(tuples metaSequenceData, t Type, cs chunks.ChunkSource) Value
type metaReaderFunc func(v Value) metaSequenceData

type metaSequenceFuncs struct {
	builder metaBuilderFunc
	reader  metaReaderFunc
}

var (
	metaFuncMap map[NomsKind]metaSequenceFuncs = map[NomsKind]metaSequenceFuncs{}
)

func registerMetaValue(k NomsKind, bf metaBuilderFunc, rf metaReaderFunc) {
	metaFuncMap[k] = metaSequenceFuncs{bf, rf}
}

func newMetaSequenceFromData(tuples metaSequenceData, t Type, cs chunks.ChunkSource) Value {
	concreteType := t.Desc.(CompoundDesc).ElemTypes[0]

	if s, ok := metaFuncMap[concreteType.Kind()]; ok {
		return s.builder(tuples, t, cs)
	}

	panic("not reached")
}

func getDataFromMetaSequence(v Value) metaSequenceData {
	concreteType := v.Type().Desc.(CompoundDesc).ElemTypes[0]

	if s, ok := metaFuncMap[concreteType.Kind()]; ok {
		return s.reader(v)
	}

	panic("not reached")
}
