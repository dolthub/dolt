package types

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

func TestMeta(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()

	flatList := []Value{Uint32(0), Uint32(1), Uint32(2), Uint32(3), Uint32(4), Uint32(5), Uint32(6), Uint32(7)}
	l0 := NewList(cs, flatList[0])
	lr0 := WriteValue(l0, cs)
	l1 := NewList(cs, flatList[1])
	lr1 := WriteValue(l1, cs)
	l2 := NewList(cs, flatList[2])
	lr2 := WriteValue(l2, cs)
	l3 := NewList(cs, flatList[3])
	lr3 := WriteValue(l3, cs)
	l4 := NewList(cs, flatList[4])
	lr4 := WriteValue(l4, cs)
	l5 := NewList(cs, flatList[5])
	lr5 := WriteValue(l5, cs)
	l6 := NewList(cs, flatList[6])
	lr6 := WriteValue(l6, cs)
	l7 := NewList(cs, flatList[7])
	lr7 := WriteValue(l7, cs)

	mtr := l0.Type()

	m0 := compoundList{metaSequenceObject{metaSequenceData{{lr0, Uint64(1)}, {lr1, Uint64(2)}}, mtr}, &ref.Ref{}, cs}
	lm0 := WriteValue(m0, cs)
	m1 := compoundList{metaSequenceObject{metaSequenceData{{lr2, Uint64(1)}, {lr3, Uint64(2)}}, mtr}, &ref.Ref{}, cs}
	lm1 := WriteValue(m1, cs)
	m2 := compoundList{metaSequenceObject{metaSequenceData{{lr4, Uint64(1)}, {lr5, Uint64(2)}}, mtr}, &ref.Ref{}, cs}
	lm2 := WriteValue(m2, cs)
	m3 := compoundList{metaSequenceObject{metaSequenceData{{lr6, Uint64(1)}, {lr7, Uint64(2)}}, mtr}, &ref.Ref{}, cs}
	lm3 := WriteValue(m3, cs)

	m00 := compoundList{metaSequenceObject{metaSequenceData{{lm0, Uint64(2)}, {lm1, Uint64(4)}}, mtr}, &ref.Ref{}, cs}
	lm00 := WriteValue(m00, cs)
	m01 := compoundList{metaSequenceObject{metaSequenceData{{lm2, Uint64(2)}, {lm3, Uint64(4)}}, mtr}, &ref.Ref{}, cs}
	lm01 := WriteValue(m01, cs)

	rootList := compoundList{metaSequenceObject{metaSequenceData{{lm00, Uint64(4)}, {lm01, Uint64(8)}}, mtr}, &ref.Ref{}, cs}
	rootRef := WriteValue(rootList, cs)

	rootList = ReadValue(rootRef, cs).(compoundList)

	rootList.IterAll(func(v Value, index uint64) {
		assert.Equal(flatList[index], v)
	})
}
