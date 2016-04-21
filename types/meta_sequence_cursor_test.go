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
	l0 := NewList(flatList[0])
	lr0 := vs.WriteValue(l0)
	l1 := NewList(flatList[1])
	lr1 := vs.WriteValue(l1)
	l2 := NewList(flatList[2])
	lr2 := vs.WriteValue(l2)
	l3 := NewList(flatList[3])
	lr3 := vs.WriteValue(l3)
	l4 := NewList(flatList[4])
	lr4 := vs.WriteValue(l4)
	l5 := NewList(flatList[5])
	lr5 := vs.WriteValue(l5)
	l6 := NewList(flatList[6])
	lr6 := vs.WriteValue(l6)
	l7 := NewList(flatList[7])
	lr7 := vs.WriteValue(l7)

	mtr := l0.Type()

	m0 := compoundList{metaSequenceObject{metaSequenceData{{l0, lr0, Uint64(1), 1}, {l1, lr1, Uint64(2), 2}}, mtr}, 0, &ref.Ref{}, vs}
	lm0 := vs.WriteValue(m0)
	m1 := compoundList{metaSequenceObject{metaSequenceData{{l2, lr2, Uint64(1), 1}, {l3, lr3, Uint64(2), 2}}, mtr}, 0, &ref.Ref{}, vs}
	lm1 := vs.WriteValue(m1)
	m2 := compoundList{metaSequenceObject{metaSequenceData{{l4, lr4, Uint64(1), 1}, {l5, lr5, Uint64(2), 2}}, mtr}, 0, &ref.Ref{}, vs}
	lm2 := vs.WriteValue(m2)
	m3 := compoundList{metaSequenceObject{metaSequenceData{{l6, lr6, Uint64(1), 1}, {l7, lr7, Uint64(2), 2}}, mtr}, 0, &ref.Ref{}, vs}
	lm3 := vs.WriteValue(m3)

	m00 := compoundList{metaSequenceObject{metaSequenceData{{m0, lm0, Uint64(2), 2}, {m1, lm1, Uint64(4), 4}}, mtr}, 0, &ref.Ref{}, vs}
	lm00 := vs.WriteValue(m00)
	m01 := compoundList{metaSequenceObject{metaSequenceData{{m2, lm2, Uint64(2), 2}, {m3, lm3, Uint64(4), 4}}, mtr}, 0, &ref.Ref{}, vs}
	lm01 := vs.WriteValue(m01)

	rootList := compoundList{metaSequenceObject{metaSequenceData{{m00, lm00, Uint64(4), 4}, {m01, lm01, Uint64(8), 8}}, mtr}, 0, &ref.Ref{}, vs}
	rootRef := vs.WriteValue(rootList).TargetRef()

	rootList = vs.ReadValue(rootRef).(compoundList)

	rootList.IterAll(func(v Value, index uint64) {
		assert.Equal(flatList[index], v)
	})
}
