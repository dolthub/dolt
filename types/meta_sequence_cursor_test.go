package types

import (
	"testing"

	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/assert"
)

func TestMeta(t *testing.T) {
	assert := assert.New(t)

	vs := NewTestValueStore()

	flatList := []Value{Uint32(0), Uint32(1), Uint32(2), Uint32(3), Uint32(4), Uint32(5), Uint32(6), Uint32(7)}
	typeForRefOfListOfValue := MakeRefType(MakeCompoundType(ListKind, MakePrimitiveType(ValueKind)))
	l0 := NewList(flatList[0])
	lr0 := newRef(vs.WriteValue(l0), typeForRefOfListOfValue)
	l1 := NewList(flatList[1])
	lr1 := newRef(vs.WriteValue(l1), typeForRefOfListOfValue)
	l2 := NewList(flatList[2])
	lr2 := newRef(vs.WriteValue(l2), typeForRefOfListOfValue)
	l3 := NewList(flatList[3])
	lr3 := newRef(vs.WriteValue(l3), typeForRefOfListOfValue)
	l4 := NewList(flatList[4])
	lr4 := newRef(vs.WriteValue(l4), typeForRefOfListOfValue)
	l5 := NewList(flatList[5])
	lr5 := newRef(vs.WriteValue(l5), typeForRefOfListOfValue)
	l6 := NewList(flatList[6])
	lr6 := newRef(vs.WriteValue(l6), typeForRefOfListOfValue)
	l7 := NewList(flatList[7])
	lr7 := newRef(vs.WriteValue(l7), typeForRefOfListOfValue)

	mtr := l0.Type()

	m0 := compoundList{metaSequenceObject{metaSequenceData{{l0, lr0, Uint64(1)}, {l1, lr1, Uint64(2)}}, mtr}, 0, &ref.Ref{}, vs}
	lm0 := newRef(vs.WriteValue(m0), typeForRefOfListOfValue)
	m1 := compoundList{metaSequenceObject{metaSequenceData{{l2, lr2, Uint64(1)}, {l3, lr3, Uint64(2)}}, mtr}, 0, &ref.Ref{}, vs}
	lm1 := newRef(vs.WriteValue(m1), typeForRefOfListOfValue)
	m2 := compoundList{metaSequenceObject{metaSequenceData{{l4, lr4, Uint64(1)}, {l5, lr5, Uint64(2)}}, mtr}, 0, &ref.Ref{}, vs}
	lm2 := newRef(vs.WriteValue(m2), typeForRefOfListOfValue)
	m3 := compoundList{metaSequenceObject{metaSequenceData{{l6, lr6, Uint64(1)}, {l7, lr7, Uint64(2)}}, mtr}, 0, &ref.Ref{}, vs}
	lm3 := newRef(vs.WriteValue(m3), typeForRefOfListOfValue)

	m00 := compoundList{metaSequenceObject{metaSequenceData{{m0, lm0, Uint64(2)}, {m1, lm1, Uint64(4)}}, mtr}, 0, &ref.Ref{}, vs}
	lm00 := newRef(vs.WriteValue(m00), typeForRefOfListOfValue)
	m01 := compoundList{metaSequenceObject{metaSequenceData{{m2, lm2, Uint64(2)}, {m3, lm3, Uint64(4)}}, mtr}, 0, &ref.Ref{}, vs}
	lm01 := newRef(vs.WriteValue(m01), typeForRefOfListOfValue)

	rootList := compoundList{metaSequenceObject{metaSequenceData{{m00, lm00, Uint64(4)}, {m01, lm01, Uint64(8)}}, mtr}, 0, &ref.Ref{}, vs}
	rootRef := vs.WriteValue(rootList)

	rootList = vs.ReadValue(rootRef).(compoundList)

	rootList.IterAll(func(v Value, index uint64) {
		assert.Equal(flatList[index], v)
	})
}
