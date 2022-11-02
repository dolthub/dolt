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
	"math"

	"github.com/dolthub/dolt/go/store/d"
)

type leafSequence struct {
	sequenceImpl
}

func newLeafSequence(nbf *NomsBinFormat, vrw ValueReadWriter, buff []byte, offsets []uint32, len uint64) leafSequence {
	return leafSequence{newSequenceImpl(nbf, vrw, buff, offsets, len)}
}

func newLeafSequenceFromValues(kind NomsKind, vrw ValueReadWriter, vs ...Value) (leafSequence, error) {
	d.PanicIfTrue(vrw == nil)
	w := newBinaryNomsWriter()
	offsets := make([]uint32, len(vs)+sequencePartValues+1)
	offsets[sequencePartKind] = w.offset
	err := kind.writeTo(&w, vrw.Format())

	if err != nil {
		return leafSequence{}, err
	}

	offsets[sequencePartLevel] = w.offset
	w.writeCount(0) // level
	offsets[sequencePartCount] = w.offset
	count := uint64(len(vs))
	w.writeCount(count)
	offsets[sequencePartValues] = w.offset
	for i, v := range vs {
		err := v.writeTo(&w, vrw.Format())

		if err != nil {
			return leafSequence{}, err
		}

		offsets[i+sequencePartValues+1] = w.offset
	}
	return newLeafSequence(vrw.Format(), vrw, w.data(), offsets, count), nil
}

func (seq leafSequence) values() ([]Value, error) {
	return seq.valuesSlice(0, math.MaxUint64)
}

func (seq leafSequence) valuesSlice(from, to uint64) ([]Value, error) {
	if l := seq.Len(); to > l {
		to = l
	}

	dec := seq.decoderSkipToIndex(int(from))
	vs := make([]Value, (to-from)*uint64(getValuesPerIdx(seq.Kind())))
	for i := range vs {
		var err error
		vs[i], err = dec.readValue(seq.format())

		if err != nil {
			return nil, err
		}
	}
	return vs, nil
}

func (seq leafSequence) kvTuples(from, to uint64, dest []Tuple) ([]Tuple, error) {
	if l := seq.Len(); to > l {
		to = l
	}

	dec := seq.decoderSkipToIndex(int(from))
	numTuples := (to - from) * uint64(getValuesPerIdx(seq.Kind()))

	if uint64(cap(dest)) < numTuples {
		dest = make([]Tuple, numTuples)
	}

	dest = dest[:numTuples]

	nbf := seq.format()
	for i := uint64(0); i < numTuples; i++ {
		var err error
		dest[i], err = dec.readTuple(nbf)

		if err != nil {
			return nil, err
		}
	}
	return dest, nil
}

func (seq leafSequence) getCompareFnHelper(other leafSequence) compareFn {
	dec := seq.decoder()
	otherDec := other.decoder()

	return func(idx, otherIdx int) (bool, error) {
		dec.offset = uint32(seq.getItemOffset(idx))
		otherDec.offset = uint32(other.getItemOffset(otherIdx))
		val, err := dec.readValue(seq.format())

		if err != nil {
			return false, err
		}

		otherVal, err := otherDec.readValue(seq.format())

		if err != nil {
			return false, err
		}

		return val.Equals(otherVal), nil
	}
}

func (seq leafSequence) getCompareFn(other sequence) compareFn {
	panic("unreachable")
}

func (seq leafSequence) typeOf() (*Type, error) {
	dec := seq.decoder()
	kind := dec.ReadKind()
	dec.skipCount() // level
	count := dec.readCount()
	ts := make(typeSlice, 0, count)
	var lastType *Type
	for i := uint64(0); i < count; i++ {
		if lastType != nil {
			offset := dec.offset
			sameType, err := dec.isValueSameTypeForSure(seq.format(), lastType)

			if err != nil {
				return nil, err
			}

			if sameType {
				continue
			}
			dec.offset = offset
		}

		var err error
		lastType, err = dec.readTypeOfValue(seq.format())

		if err != nil {
			return nil, err
		}

		if lastType.Kind() == UnknownKind {
			// if any of the elements are unknown, return unknown
			return nil, ErrUnknownType
		}

		ts = append(ts, lastType)
	}

	t, err := makeUnionType(ts...)

	if err != nil {
		return nil, err
	}

	return makeCompoundType(kind, t)
}

func (seq leafSequence) numLeaves() uint64 {
	return seq.len
}

func (seq leafSequence) getChildSequence(ctx context.Context, idx int) (sequence, error) {
	return nil, nil
}

func (seq leafSequence) treeLevel() uint64 {
	return 0
}

func (seq leafSequence) isLeaf() bool {
	return true
}

func (seq leafSequence) cumulativeNumberOfLeaves(idx int) (uint64, error) {
	return uint64(idx) + 1, nil
}

func (seq leafSequence) getCompositeChildSequence(ctx context.Context, start uint64, length uint64) (sequence, error) {
	panic("getCompositeChildSequence called on a leaf sequence")
}

func (seq leafSequence) getItem(idx int) (sequenceItem, error) {
	dec := seq.decoderSkipToIndex(idx)
	return dec.readValue(seq.format())
}

func getValuesPerIdx(kind NomsKind) int {
	if kind == MapKind {
		return 2
	}
	return 1
}
