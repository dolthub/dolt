// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"math"

	"github.com/liquidata-inc/ld/dolt/go/store/d"
)

type leafSequence struct {
	sequenceImpl
	format *Format
}

func newLeafSequence(format *Format, vrw ValueReadWriter, buff []byte, offsets []uint32, len uint64) leafSequence {
	return leafSequence{newSequenceImpl(vrw, buff, offsets, len), format}
}

func newLeafSequenceFromValues(kind NomsKind, vrw ValueReadWriter, f *Format, vs ...Value) leafSequence {
	d.PanicIfTrue(vrw == nil)
	w := newBinaryNomsWriter()
	offsets := make([]uint32, len(vs)+sequencePartValues+1)
	offsets[sequencePartKind] = w.offset
	kind.writeTo(&w, f)
	offsets[sequencePartLevel] = w.offset
	w.writeCount(0) // level
	offsets[sequencePartCount] = w.offset
	count := uint64(len(vs))
	w.writeCount(count)
	offsets[sequencePartValues] = w.offset
	for i, v := range vs {
		v.writeTo(&w, f)
		offsets[i+sequencePartValues+1] = w.offset
	}
	return newLeafSequence(f, vrw, w.data(), offsets, count)
}

func (seq leafSequence) values(f *Format) []Value {
	return seq.valuesSlice(f, 0, math.MaxUint64)
}

func (seq leafSequence) valuesSlice(f *Format, from, to uint64) []Value {
	if len := seq.Len(); to > len {
		to = len
	}

	dec := seq.decoderSkipToIndex(int(from))
	vs := make([]Value, (to-from)*uint64(getValuesPerIdx(seq.Kind())))
	for i := range vs {
		vs[i] = dec.readValue(f)
	}
	return vs
}

func (seq leafSequence) getCompareFnHelper(f *Format, other leafSequence) compareFn {
	dec := seq.decoder()
	otherDec := other.decoder()

	return func(idx, otherIdx int) bool {
		dec.offset = uint32(seq.getItemOffset(idx))
		otherDec.offset = uint32(other.getItemOffset(otherIdx))
		return dec.readValue(f).Equals(f, otherDec.readValue(f))
	}
}

func (seq leafSequence) getCompareFn(f *Format, other sequence) compareFn {
	panic("unreachable")
}

func (seq leafSequence) typeOf() *Type {
	dec := seq.decoder()
	kind := dec.readKind()
	dec.skipCount() // level
	count := dec.readCount()
	ts := make(typeSlice, 0, count)
	var lastType *Type
	for i := uint64(0); i < count; i++ {
		if lastType != nil {
			offset := dec.offset
			if dec.isValueSameTypeForSure(seq.format, lastType) {
				continue
			}
			dec.offset = offset
		}

		lastType = dec.readTypeOfValue(seq.format)
		ts = append(ts, lastType)
	}

	return makeCompoundType(kind, makeUnionType(ts...))
}

func (seq leafSequence) numLeaves() uint64 {
	return seq.len
}

func (seq leafSequence) getChildSequence(ctx context.Context, idx int) sequence {
	return nil
}

func (seq leafSequence) treeLevel() uint64 {
	return 0
}

func (seq leafSequence) isLeaf() bool {
	return true
}

func (seq leafSequence) cumulativeNumberOfLeaves(idx int) uint64 {
	return uint64(idx) + 1
}

func (seq leafSequence) getCompositeChildSequence(ctx context.Context, start uint64, length uint64) sequence {
	panic("getCompositeChildSequence called on a leaf sequence")
}

func (seq leafSequence) getItem(idx int, f *Format) sequenceItem {
	dec := seq.decoderSkipToIndex(idx)
	return dec.readValue(f)
}

func getValuesPerIdx(kind NomsKind) int {
	if kind == MapKind {
		return 2
	}
	return 1
}
