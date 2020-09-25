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
	"github.com/dolthub/dolt/go/store/d"
)

type mapLeafSequence struct {
	leafSequence
}

type mapEntry struct {
	key   Value
	value Value
}

func (entry mapEntry) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	err := entry.key.writeTo(w, nbf)

	if err != nil {
		return err
	}

	return entry.value.writeTo(w, nbf)
}

func readMapEntry(r *valueDecoder, nbf *NomsBinFormat) (mapEntry, error) {
	k, err := r.readValue(nbf)

	if err != nil {
		return mapEntry{}, err
	}

	v, err := r.readValue(nbf)

	if err != nil {
		return mapEntry{}, err
	}

	return mapEntry{k, v}, nil
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
func (mes mapEntrySlice) Less(i, j int) (bool, error) {
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

func newMapLeafSequence(vrw ValueReadWriter, data ...mapEntry) (orderedSequence, error) {
	d.PanicIfTrue(vrw == nil)
	offsets := make([]uint32, len(data)+sequencePartValues+1)
	w := newBinaryNomsWriter()
	offsets[sequencePartKind] = w.offset
	err := MapKind.writeTo(&w, vrw.Format())

	if err != nil {
		return nil, err
	}

	offsets[sequencePartLevel] = w.offset
	w.writeCount(0) // level
	offsets[sequencePartCount] = w.offset
	count := uint64(len(data))
	w.writeCount(count)
	offsets[sequencePartValues] = w.offset
	for i, me := range data {
		err := me.writeTo(&w, vrw.Format())

		if err != nil {
			return nil, err
		}

		offsets[i+sequencePartValues+1] = w.offset
	}
	return mapLeafSequence{newLeafSequence(vrw, w.data(), offsets, count)}, nil
}

func (ml mapLeafSequence) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	w.writeRaw(ml.buff)
	return nil
}

// sequence interface

func (ml mapLeafSequence) getItem(idx int) (sequenceItem, error) {
	dec := ml.decoderSkipToIndex(idx)
	return readMapEntry(&dec, ml.format())
}

func (ml mapLeafSequence) WalkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	w := binaryNomsWriter{make([]byte, 4), 0}
	err := ml.writeTo(&w, ml.format())

	if err != nil {
		return err
	}

	return walkRefs(w.buff[:w.offset], ml.format(), cb)
}

func (ml mapLeafSequence) entries() (mapEntrySlice, error) {
	dec, count := ml.decoderSkipToValues()
	entries := mapEntrySlice{
		make([]mapEntry, count),
		ml.format(),
	}
	for i := uint64(0); i < count; i++ {
		k, err := dec.readValue(ml.format())

		if err != nil {
			return mapEntrySlice{}, err
		}

		v, err := dec.readValue(ml.format())

		if err != nil {
			return mapEntrySlice{}, err
		}

		entries.entries[i] = mapEntry{k, v}
	}

	return entries, nil
}

func (ml mapLeafSequence) getCompareFn(other sequence) compareFn {
	dec1 := ml.decoder()
	ml2 := other.(mapLeafSequence)
	dec2 := ml2.decoder()
	return func(idx, otherIdx int) (bool, error) {
		dec1.offset = uint32(ml.getItemOffset(idx))
		dec2.offset = uint32(ml2.getItemOffset(otherIdx))
		k1, err := dec1.readValue(ml.format())

		if err != nil {
			return false, err
		}

		k2, err := dec2.readValue(ml2.format())

		if err != nil {
			return false, err
		}

		if !k1.Equals(k2) {
			return false, nil
		}

		v1, err := dec1.readValue(ml.format())

		if err != nil {
			return false, err
		}

		v2, err := dec2.readValue(ml2.format())

		if err != nil {
			return false, err
		}

		return v1.Equals(v2), nil
	}
}

func (ml mapLeafSequence) typeOf() (*Type, error) {
	dec, count := ml.decoderSkipToValues()
	kts := make(typeSlice, 0, count)
	vts := make(typeSlice, 0, count)
	var lastKeyType, lastValueType *Type
	for i := uint64(0); i < count; i++ {
		if lastKeyType != nil && lastValueType != nil {
			offset := dec.offset

			sameKeyTypes, err := dec.isValueSameTypeForSure(ml.format(), lastKeyType)

			if err != nil {
				return nil, err
			}

			sameValTypes, err := dec.isValueSameTypeForSure(ml.format(), lastValueType)

			if err != nil {
				return nil, err
			}

			if sameKeyTypes && sameValTypes {
				continue
			}

			dec.offset = offset
		}

		var err error
		lastKeyType, err = dec.readTypeOfValue(ml.format())

		if err != nil {
			return nil, err
		}

		if lastKeyType.Kind() == UnknownKind {
			// if any of the elements are unknown, return unknown
			return nil, ErrUnknownType
		}

		kts = append(kts, lastKeyType)
		lastValueType, err = dec.readTypeOfValue(ml.format())

		if err != nil {
			return nil, err
		}

		if lastValueType.Kind() == UnknownKind {
			// if any of the elements are unknown, return unknown
			return nil, ErrUnknownType
		}

		vts = append(vts, lastValueType)
	}

	unionOfKTypes, err := makeUnionType(kts...)

	if err != nil {
		return nil, err
	}

	uninionOfVTypes, err := makeUnionType(vts...)

	if err != nil {
		return nil, err
	}

	return makeCompoundType(MapKind, unionOfKTypes, uninionOfVTypes)
}

// orderedSequence interface

func (ml mapLeafSequence) decoderSkipToIndex(idx int) valueDecoder {
	offset := ml.getItemOffset(idx)
	return ml.decoderAtOffset(offset)
}

func (ml mapLeafSequence) getKey(idx int) (orderedKey, error) {
	dec := ml.decoderSkipToIndex(idx)
	v, err := dec.readValue(ml.format())

	if err != nil {
		return orderedKey{}, err
	}

	return newOrderedKey(v, ml.format())
}

func (ml mapLeafSequence) search(key orderedKey) (int, error) {
	n, err := SearchWithErroringLess(int(ml.Len()), func(i int) (bool, error) {
		k, err := ml.getKey(i)

		if err != nil {
			return false, err
		}

		isLess, err := k.Less(ml.format(), key)

		if err != nil {
			return false, nil
		}

		return !isLess, nil
	})

	return n, err
}

func (ml mapLeafSequence) getValue(idx int) (Value, error) {
	dec := ml.decoderSkipToIndex(idx)
	err := dec.skipValue(ml.format())

	if err != nil {
		return nil, err
	}

	return dec.readValue(ml.format())
}
