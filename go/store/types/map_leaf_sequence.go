// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"sort"

	"github.com/liquidata-inc/ld/dolt/go/store/d"
)

type mapLeafSequence struct {
	leafSequence
}

type mapEntry struct {
	key   Value
	value Value
}

func (entry mapEntry) writeTo(w nomsWriter, f *format) {
	entry.key.writeTo(w, f)
	entry.value.writeTo(w, f)
}

func readMapEntry(r *valueDecoder) mapEntry {
	// TODO(binformat)
	return mapEntry{r.readValue(Format_7_18), r.readValue(Format_7_18)}
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
	// TODO(binformat)
	MapKind.writeTo(&w, Format_7_18)
	offsets[sequencePartLevel] = w.offset
	w.writeCount(0) // level
	offsets[sequencePartCount] = w.offset
	count := uint64(len(data))
	w.writeCount(count)
	offsets[sequencePartValues] = w.offset
	for i, me := range data {
		// TODO(binformat)
		me.writeTo(&w, Format_7_18)
		offsets[i+sequencePartValues+1] = w.offset
	}
	return mapLeafSequence{newLeafSequence(vrw, w.data(), offsets, count)}
}

func (ml mapLeafSequence) writeTo(w nomsWriter, f *format) {
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
		// TODO(binformat)
		entries[i] = mapEntry{dec.readValue(Format_7_18), dec.readValue(Format_7_18)}
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
		// TODO(binformat)
		k1 := dec1.readValue(Format_7_18)
		k2 := dec2.readValue(Format_7_18)
		if !k1.Equals(k2) {
			return false
		}
		// TODO(binformat)
		v1 := dec1.readValue(Format_7_18)
		v2 := dec2.readValue(Format_7_18)
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
	// TODO(binformat)
	return newOrderedKey(dec.readValue(Format_7_18))
}

func (ml mapLeafSequence) search(key orderedKey) int {
	return sort.Search(int(ml.Len()), func(i int) bool {
		return !ml.getKey(i).Less(key)
	})
}

func (ml mapLeafSequence) getValue(idx int) Value {
	dec := ml.decoderSkipToIndex(idx)
	// TODO(binformat)
	dec.skipValue(Format_7_18)
	// TODO(binformat)
	return dec.readValue(Format_7_18)
}
