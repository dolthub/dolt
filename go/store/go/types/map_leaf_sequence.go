// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"sort"

	"github.com/attic-labs/noms/go/d"
)

type mapLeafSequence struct {
	leafSequence
}

type mapEntry struct {
	key   Value
	value Value
}

func (entry mapEntry) writeTo(w nomsWriter) {
	entry.key.writeTo(w)
	entry.value.writeTo(w)
}

func readMapEntry(r *valueDecoder) mapEntry {
	return mapEntry{r.readValue(), r.readValue()}
}

func (entry mapEntry) equals(other mapEntry) bool {
	return entry.key.Equals(other.key) && entry.value.Equals(other.value)
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
		if !v.equals(other[i]) {
			return false
		}
	}

	return true
}

func newMapLeafSequence(vrw ValueReadWriter, data ...mapEntry) orderedSequence {
	d.PanicIfTrue(vrw == nil)
	offsets := make([]uint32, len(data)+sequencePartValues+1)
	w := newBinaryNomsWriter()
	offsets[sequencePartKind] = w.offset
	MapKind.writeTo(&w)
	offsets[sequencePartLevel] = w.offset
	w.writeCount(0) // level
	offsets[sequencePartCount] = w.offset
	count := uint64(len(data))
	w.writeCount(count)
	offsets[sequencePartValues] = w.offset
	for i, me := range data {
		me.writeTo(&w)
		offsets[i+sequencePartValues+1] = w.offset
	}
	return mapLeafSequence{newLeafSequence(vrw, w.data(), offsets, count)}
}

func (ml mapLeafSequence) writeTo(w nomsWriter) {
	w.writeRaw(ml.buff)
}

// sequence interface

func (ml mapLeafSequence) getItem(idx int) sequenceItem {
	dec := ml.decoderSkipToIndex(idx)
	return readMapEntry(&dec)
}

func (ml mapLeafSequence) WalkRefs(cb RefCallback) {
	walkRefs(ml.valueBytes(), cb)
}

func (ml mapLeafSequence) entries() mapEntrySlice {
	dec, count := ml.decoderSkipToValues()
	entries := make(mapEntrySlice, count)
	for i := uint64(0); i < count; i++ {
		entries[i] = mapEntry{dec.readValue(), dec.readValue()}
	}
	return entries
}

func (ml mapLeafSequence) getCompareFn(other sequence) compareFn {
	dec1 := ml.decoder()
	ml2 := other.(mapLeafSequence)
	dec2 := ml2.decoder()
	return func(idx, otherIdx int) bool {
		dec1.offset = uint32(ml.getItemOffset(idx))
		dec2.offset = uint32(ml2.getItemOffset(otherIdx))
		k1 := dec1.readValue()
		k2 := dec2.readValue()
		if !k1.Equals(k2) {
			return false
		}
		v1 := dec1.readValue()
		v2 := dec2.readValue()
		return v1.Equals(v2)
	}
}

func (ml mapLeafSequence) typeOf() *Type {
	dec, count := ml.decoderSkipToValues()
	kts := make(typeSlice, 0, count)
	vts := make(typeSlice, 0, count)
	var lastKeyType, lastValueType *Type
	for i := uint64(0); i < count; i++ {
		if lastKeyType != nil && lastValueType != nil {
			offset := dec.offset
			if dec.isValueSameTypeForSure(lastKeyType) && dec.isValueSameTypeForSure(lastValueType) {
				continue
			}
			dec.offset = offset

		}

		lastKeyType = dec.readTypeOfValue()
		kts = append(kts, lastKeyType)
		lastValueType = dec.readTypeOfValue()
		vts = append(vts, lastValueType)
	}

	return makeCompoundType(MapKind, makeUnionType(kts...), makeUnionType(vts...))
}

// orderedSequence interface

func (ml mapLeafSequence) decoderSkipToIndex(idx int) valueDecoder {
	offset := ml.getItemOffset(idx)
	return ml.decoderAtOffset(offset)
}

func (ml mapLeafSequence) getKey(idx int) orderedKey {
	dec := ml.decoderSkipToIndex(idx)
	return newOrderedKey(dec.readValue())
}

func (ml mapLeafSequence) search(key orderedKey) int {
	return sort.Search(int(ml.Len()), func(i int) bool {
		return !ml.getKey(i).Less(key)
	})
}

func (ml mapLeafSequence) getValue(idx int) Value {
	dec := ml.decoderSkipToIndex(idx)
	dec.skipValue()
	return dec.readValue()
}
