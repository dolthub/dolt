// Copyright 2019 Dolthub, Inc.
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
	"context"
	"errors"

	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
)

var ErrNotAMapOfTuples = errors.New("type error: not a map of tuples")

type mapLeafSequence struct {
	leafSequence
}

type tupleMapEntry struct {
	key   Tuple
	value Tuple
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

func readTupleMapEntry(r *valueDecoder, nbf *NomsBinFormat) (tupleMapEntry, error) {
	k, err := r.readTuple(nbf)

	if err != nil {
		return tupleMapEntry{}, err
	}

	v, err := r.readTuple(nbf)

	if err != nil {
		return tupleMapEntry{}, err
	}

	return tupleMapEntry{k, v}, nil
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
	dataLen := len(data)
	offsets := make([]uint32, dataLen+sequencePartValues+1)
	w := newBinaryNomsWriterWithSizeHint(uint64(dataLen) * 16)
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

func (ml mapLeafSequence) getTupleMapEntry(idx int) (tupleMapEntry, error) {
	dec := ml.decoderSkipToIndex(idx)
	return readTupleMapEntry(&dec, ml.format())
}

func (ml mapLeafSequence) walkRefs(nbf *NomsBinFormat, cb RefCallback) error {
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
	if mes, ok := other.(mapEntrySequence); ok {
		// use mapEntrySequence comparison func rather than implementing the logic 2x
		cmpFn := mes.getCompareFn(ml)
		return func(idx, otherIdx int) (bool, error) {
			// need to use otherIdx as first param and idx as second since we are using other's comparison func
			return cmpFn(otherIdx, idx)
		}
	} else {

		ml2 := other.(mapLeafSequence)
		dec1 := ml.decoder()
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
	err := dec.SkipValue(ml.format())

	if err != nil {
		return nil, err
	}

	return dec.readValue(ml.format())
}

var _ sequence = (*mapEntrySequence)(nil)
var _ orderedSequence = (*mapEntrySequence)(nil)

type mapEntrySequence struct {
	nbf     *NomsBinFormat
	vrw     ValueReadWriter
	entries []mapEntry
}

func newMapEntrySequence(vrw ValueReadWriter, data ...mapEntry) (sequence, error) {
	return mapEntrySequence{nbf: vrw.Format(), vrw: vrw, entries: data}, nil
}

func (mes mapEntrySequence) getKey(idx int) (orderedKey, error) {
	return newOrderedKey(mes.entries[idx].key, mes.nbf)
}

func (mes mapEntrySequence) getValue(idx int) (Value, error) {
	return mes.entries[idx].value, nil
}

func (mes mapEntrySequence) search(key orderedKey) (int, error) {
	n, err := SearchWithErroringLess(len(mes.entries), func(i int) (bool, error) {
		ordKey, err := mes.getKey(i)

		if err != nil {
			return false, err
		}

		isLess, err := ordKey.Less(mes.nbf, key)

		if err != nil {
			return false, nil
		}

		return !isLess, nil
	})

	return n, err
}

func (mes mapEntrySequence) cumulativeNumberOfLeaves(idx int) (uint64, error) {
	return uint64(idx) + 1, nil
}

func (mes mapEntrySequence) Empty() bool {
	return len(mes.entries) == 0
}

func (mes mapEntrySequence) format() *NomsBinFormat {
	return mes.vrw.Format()
}

func (mes mapEntrySequence) getChildSequence(ctx context.Context, idx int) (sequence, error) {
	return nil, nil
}

func (mes mapEntrySequence) getItem(idx int) (sequenceItem, error) {
	return mes.entries[idx], nil
}

func (mes mapEntrySequence) isLeaf() bool {
	return true
}

func (mes mapEntrySequence) Kind() NomsKind {
	return MapKind
}

func (mes mapEntrySequence) Len() uint64 {
	return uint64(len(mes.entries))
}

func (mes mapEntrySequence) numLeaves() uint64 {
	return uint64(len(mes.entries))
}

func (mes mapEntrySequence) seqLen() int {
	return len(mes.entries)
}

func (mes mapEntrySequence) treeLevel() uint64 {
	return 0
}

func (mes mapEntrySequence) valueReadWriter() ValueReadWriter {
	return mes.vrw
}

func (mes mapEntrySequence) valuesSlice(from, to uint64) ([]Value, error) {
	if l := mes.Len(); to > l {
		to = l
	}

	numPairs := (to - from)
	numTuples := numPairs * 2 // 1 key, 1 value interleaved
	dest := make([]Value, numTuples)

	dest = dest[:numTuples]
	for i := uint64(0); i < numPairs; i++ {
		entry := mes.entries[i]
		dest[i*2] = entry.key
		dest[i*2+1] = entry.value
	}
	return dest, nil
}

func (mes mapEntrySequence) kvTuples(from, to uint64, dest []Tuple) ([]Tuple, error) {
	if l := mes.Len(); to > l {
		to = l
	}

	numPairs := (to - from)
	numTuples := numPairs * 2 // 1 key, 1 value interleaved

	if uint64(cap(dest)) < numTuples {
		dest = make([]Tuple, numTuples)
	}

	dest = dest[:numTuples]
	for i := uint64(0); i < numPairs; i++ {
		entry := mes.entries[i]

		keyTuple, ok := entry.key.(Tuple)

		if !ok {
			return nil, ErrNotAMapOfTuples
		}

		valTuple, ok := entry.value.(Tuple)

		if !ok {
			return nil, ErrNotAMapOfTuples
		}

		dest[i*2] = keyTuple
		dest[i*2+1] = valTuple
	}
	return dest, nil
}

func (mes mapEntrySequence) getCompareFn(other sequence) compareFn {
	if otherCMLS, ok := other.(mapEntrySequence); ok {
		return func(idx, otherIdx int) (bool, error) {
			entry1 := mes.entries[idx]
			entry2 := otherCMLS.entries[otherIdx]

			if !entry1.key.Equals(entry2.key) {
				return false, nil
			}

			return entry1.value.Equals(entry2.value), nil
		}
	} else if ml, ok := other.(mapLeafSequence); ok {
		dec1 := ml.decoder()
		return func(mesIdx, mlsIdx int) (bool, error) {
			entry := mes.entries[mesIdx]
			dec1.offset = uint32(ml.getItemOffset(mlsIdx))
			mlk, err := dec1.readValue(ml.format())

			if err != nil {
				return false, err
			}

			if !entry.key.Equals(mlk) {
				return false, nil
			}

			mlv, err := dec1.readValue(ml.format())

			if err != nil {
				return false, err
			}

			return entry.value.Equals(mlv), nil
		}
	}

	panic("unsupported")
}

func (mes mapEntrySequence) typeOf() (*Type, error) {
	panic("not implemented")
}

func (mes mapEntrySequence) walkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	panic("not implemented")
}

func (mes mapEntrySequence) writeTo(writer nomsWriter, format *NomsBinFormat) error {
	panic("not implemented")
}

func (mes mapEntrySequence) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	panic("not implemented")
}

func (mes mapEntrySequence) Compare(nbf *NomsBinFormat, other LesserValuable) (int, error) {
	panic("not implemented")
}

func (mes mapEntrySequence) Hash(format *NomsBinFormat) (hash.Hash, error) {
	panic("not implemented")
}

func (mes mapEntrySequence) Equals(other Value) bool {
	panic("not implemented")
}

func (mes mapEntrySequence) asValueImpl() valueImpl {
	panic("not implemented")
}

func (mes mapEntrySequence) getCompositeChildSequence(ctx context.Context, start uint64, length uint64) (sequence, error) {
	panic("getCompositeChildSequence called on a leaf sequence")
}
