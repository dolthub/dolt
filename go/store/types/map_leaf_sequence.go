// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
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

func (entry mapEntry) writeTo(w nomsWriter, nbf *NomsBinFormat) {
	entry.key.writeTo(w, nbf)
	entry.value.writeTo(w, nbf)
}

func readMapEntry(r *valueDecoder, nbf *NomsBinFormat) mapEntry {
	return mapEntry{r.readValue(nbf), r.readValue(nbf)}
}

func (entry mapEntry) equals(other mapEntry) bool {
	return entry.key.Equals(other.key) && entry.value.Equals(other.value)
}

type mapEntrySlice struct {
	entries []mapEntry
	nbf     *NomsBinFormat
}

func (mes mapEntrySlice) Len() int { return len(mes.entries) }
func (mes mapEntrySlice) Swap(i, j int) {
	mes.entries[i], mes.entries[j] = mes.entries[j], mes.entries[i]
}
func (mes mapEntrySlice) Less(i, j int) bool {
	return mes.entries[i].key.Less(mes.nbf, mes.entries[j].key)
}
func (mes mapEntrySlice) Equals(other mapEntrySlice) bool {
	if mes.Len() != other.Len() {
		return false
	}

	for i, v := range mes.entries {
		if !v.equals(other.entries[i]) {
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
	MapKind.writeTo(&w, vrw.Format())
	offsets[sequencePartLevel] = w.offset
	w.writeCount(0) // level
	offsets[sequencePartCount] = w.offset
	count := uint64(len(data))
	w.writeCount(count)
	offsets[sequencePartValues] = w.offset
	for i, me := range data {
		me.writeTo(&w, vrw.Format())
		offsets[i+sequencePartValues+1] = w.offset
	}
	return mapLeafSequence{newLeafSequence(vrw, w.data(), offsets, count)}
}

func (ml mapLeafSequence) writeTo(w nomsWriter, nbf *NomsBinFormat) {
	w.writeRaw(ml.buff)
}

// sequence interface

func (ml mapLeafSequence) getItem(idx int) sequenceItem {
	dec := ml.decoderSkipToIndex(idx)
	return readMapEntry(&dec, ml.format())
}

func (ml mapLeafSequence) WalkRefs(nbf *NomsBinFormat, cb RefCallback) {
	walkRefs(ml.valueBytes(ml.format()), ml.format(), cb)
}

func (ml mapLeafSequence) entries() mapEntrySlice {
	dec, count := ml.decoderSkipToValues()
	entries := mapEntrySlice{
		make([]mapEntry, count),
		ml.format(),
	}
	for i := uint64(0); i < count; i++ {
		entries.entries[i] = mapEntry{dec.readValue(ml.format()), dec.readValue(ml.format())}
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
		k1 := dec1.readValue(ml.format())
		k2 := dec2.readValue(ml2.format())
		if !k1.Equals(k2) {
			return false
		}
		v1 := dec1.readValue(ml.format())
		v2 := dec2.readValue(ml2.format())
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
			if dec.isValueSameTypeForSure(ml.format(), lastKeyType) && dec.isValueSameTypeForSure(ml.format(), lastValueType) {
				continue
			}
			dec.offset = offset

		}

		lastKeyType = dec.readTypeOfValue(ml.format())
		kts = append(kts, lastKeyType)
		lastValueType = dec.readTypeOfValue(ml.format())
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
	return newOrderedKey(dec.readValue(ml.format()), ml.format())
}

func (ml mapLeafSequence) search(key orderedKey) int {
	return sort.Search(int(ml.Len()), func(i int) bool {
		return !ml.getKey(i).Less(ml.format(), key)
	})
}

func (ml mapLeafSequence) getValue(idx int) Value {
	dec := ml.decoderSkipToIndex(idx)
	dec.skipValue(ml.format())
	return dec.readValue(ml.format())
}
