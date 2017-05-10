// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

type setLeafSequence struct {
	leafSequence
	data []Value // sorted by Hash()
}

func newSetLeafSequence(vr ValueReader, v ...Value) orderedSequence {
	return setLeafSequence{leafSequence{vr, len(v), SetKind}, v}
}

// sequence interface

func (sl setLeafSequence) getItem(idx int) sequenceItem {
	return sl.data[idx]
}

func (sl setLeafSequence) WalkRefs(cb RefCallback) {
	for _, v := range sl.data {
		v.WalkRefs(cb)
	}
}

func (sl setLeafSequence) getCompareFn(other sequence) compareFn {
	osl := other.(setLeafSequence)
	return func(idx, otherIdx int) bool {
		entry := sl.data[idx]
		return entry.Equals(osl.data[otherIdx])
	}
}

func (sl setLeafSequence) typeOf() *Type {
	ts := make([]*Type, len(sl.data))
	for i, v := range sl.data {
		ts[i] = v.typeOf()
	}
	return makeCompoundType(SetKind, makeCompoundType(UnionKind, ts...))
}

// orderedSequence interface

func (sl setLeafSequence) getKey(idx int) orderedKey {
	return newOrderedKey(sl.data[idx])
}

func (sl setLeafSequence) Kind() NomsKind {
	return SetKind
}
