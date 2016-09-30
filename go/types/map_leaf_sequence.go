// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

type mapLeafSequence struct {
	leafSequence
	data []mapEntry // sorted by entry.key.Hash()
}

type mapEntry struct {
	key   Value
	value Value
}

type mapEntrySlice []mapEntry

func (mes mapEntrySlice) Len() int           { return len(mes) }
func (mes mapEntrySlice) Swap(i, j int)      { mes[i], mes[j] = mes[j], mes[i] }
func (mes mapEntrySlice) Less(i, j int) bool { return mes[i].key.Less(mes[j].key) }
func (mes mapEntrySlice) Equals(other mapEntrySlice) bool {
	if mes.Len() != other.Len() {
		return false
	}

	for i, v := range mes {
		if !v.key.Equals(other[i].key) || !v.value.Equals(other[i].value) {
			return false
		}
	}

	return true
}

func newMapLeafSequence(vr ValueReader, data ...mapEntry) orderedSequence {
	kts := make([]*Type, len(data))
	vts := make([]*Type, len(data))
	for i, e := range data {
		kts[i] = e.key.Type()
		vts[i] = e.value.Type()
	}
	t := MakeMapType(MakeUnionType(kts...), MakeUnionType(vts...))
	return mapLeafSequence{leafSequence{vr, len(data), t}, data}
}

// sequence interface

func (ml mapLeafSequence) getItem(idx int) sequenceItem {
	return ml.data[idx]
}

func (ml mapLeafSequence) WalkRefs(cb RefCallback) {
	for _, entry := range ml.data {
		entry.key.WalkRefs(cb)
		entry.value.WalkRefs(cb)
	}
}

func (ml mapLeafSequence) getCompareFn(other sequence) compareFn {
	oml := other.(mapLeafSequence)
	return func(idx, otherIdx int) bool {
		entry := ml.data[idx]
		otherEntry := oml.data[otherIdx]
		return entry.key.Equals(otherEntry.key) && entry.value.Equals(otherEntry.value)
	}
}

// orderedSequence interface

func (ml mapLeafSequence) getKey(idx int) orderedKey {
	return newOrderedKey(ml.data[idx].key)
}

// Collection interface
func (ml mapLeafSequence) Len() uint64 {
	return uint64(len(ml.data))
}

func (ml mapLeafSequence) Empty() bool {
	return ml.Len() == uint64(0)
}
