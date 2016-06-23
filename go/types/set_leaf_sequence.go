// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

type setLeafSequence struct {
	data []Value // sorted by Hash()
	t    *Type
	vr   ValueReader
}

func newSetLeafSequence(vr ValueReader, v ...Value) orderedSequence {
	ts := make([]*Type, len(v))
	for i, v := range v {
		ts[i] = v.Type()
	}
	t := MakeSetType(MakeUnionType(ts...))
	return setLeafSequence{v, t, vr}
}

// sequence interface
func (sl setLeafSequence) getItem(idx int) sequenceItem {
	return sl.data[idx]
}

func (sl setLeafSequence) seqLen() int {
	return len(sl.data)
}

func (sl setLeafSequence) numLeaves() uint64 {
	return uint64(len(sl.data))
}

func (sl setLeafSequence) valueReader() ValueReader {
	return sl.vr
}

func (sl setLeafSequence) Chunks() (chunks []Ref) {
	for _, v := range sl.data {
		chunks = append(chunks, v.Chunks()...)
	}
	return
}

func (sl setLeafSequence) Type() *Type {
	return sl.t
}

// orderedSequence interface
func (sl setLeafSequence) getKey(idx int) orderedKey {
	return newOrderedKey(sl.data[idx])
}

func (sl setLeafSequence) getCompareFn(other sequence) compareFn {
	osl := other.(setLeafSequence)
	return func(idx, otherIdx int) bool {
		entry := sl.data[idx]
		return entry.Equals(osl.data[otherIdx])
	}
}
