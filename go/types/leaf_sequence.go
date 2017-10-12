// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"math"

	"github.com/attic-labs/noms/go/d"
)

type leafSequence struct {
	sequenceImpl
}

func newLeafSequence(vrw ValueReadWriter, buff []byte, offsets []uint32) leafSequence {
	return leafSequence{newSequenceImpl(vrw, buff, offsets)}
}

func newLeafSequenceFromValues(kind NomsKind, vrw ValueReadWriter, vs ...Value) leafSequence {
	d.PanicIfTrue(vrw == nil)
	w := newBinaryNomsWriter()
	offsets := make([]uint32, len(vs)+sequencePartValues+1)
	offsets[sequencePartKind] = w.offset
	kind.writeTo(&w)
	offsets[sequencePartLevel] = w.offset
	w.writeCount(0) // level
	offsets[sequencePartCount] = w.offset
	count := uint64(len(vs))
	w.writeCount(count)
	offsets[sequencePartValues] = w.offset
	for i, v := range vs {
		v.writeTo(&w)
		offsets[i+sequencePartValues+1] = w.offset
	}
	return newLeafSequence(vrw, w.data(), offsets)
}

// readLeafSequence reads the data provided by a decoder and moves the decoder forward.
func readLeafSequence(dec *valueDecoder) leafSequence {
	start := dec.pos()
	offsets := skipLeafSequence(dec)
	end := dec.pos()
	return newLeafSequence(dec.vrw, dec.byteSlice(start, end), offsets)
}

func skipLeafSequence(dec *valueDecoder) []uint32 {
	kindPos := dec.pos()
	dec.skipKind()
	levelPos := dec.pos()
	dec.skipCount() // level
	countPos := dec.pos()
	count := dec.readCount()
	offsets := make([]uint32, count+sequencePartValues+1)
	offsets[sequencePartKind] = kindPos
	offsets[sequencePartLevel] = levelPos
	offsets[sequencePartCount] = countPos
	offsets[sequencePartValues] = dec.pos()
	for i := uint64(0); i < count; i++ {
		dec.skipValue()
		offsets[i+sequencePartValues+1] = dec.pos()
	}
	return offsets
}

func (seq leafSequence) values() []Value {
	return seq.valuesSlice(0, math.MaxUint64)
}

func (seq leafSequence) valuesSlice(from, to uint64) []Value {
	if len := seq.Len(); to > len {
		to = len
	}

	dec := seq.decoderSkipToIndex(int(from))
	vs := make([]Value, (to-from)*getValuesPerIdx(seq))
	for i := range vs {
		vs[i] = dec.readValue()
	}
	return vs
}

func (seq leafSequence) getCompareFnHelper(other leafSequence) compareFn {
	dec := seq.decoder()
	otherDec := other.decoder()

	return func(idx, otherIdx int) bool {
		dec.offset = uint32(seq.getItemOffset(idx))
		otherDec.offset = uint32(other.getItemOffset(otherIdx))
		return dec.readValue().Equals(otherDec.readValue())
	}
}

func (seq leafSequence) getCompareFn(other sequence) compareFn {
	panic("unreachable")
}

func (seq leafSequence) typeOf() *Type {
	dec := seq.decoder()
	kind := dec.readKind()
	dec.skipCount() // level
	count := dec.readCount()
	ts := make([]*Type, count)
	for i := uint64(0); i < count; i++ {
		ts[i] = dec.readTypeOfValue()
	}
	return makeCompoundType(kind, makeCompoundType(UnionKind, ts...))
}

func (seq leafSequence) numLeaves() uint64 {
	_, count := seq.decoderSkipToValues()
	return count
}

func (seq leafSequence) getChildSequence(idx int) sequence {
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

func (seq leafSequence) getCompositeChildSequence(start uint64, length uint64) sequence {
	panic("getCompositeChildSequence called on a leaf sequence")
}

func (seq leafSequence) getItem(idx int) sequenceItem {
	dec := seq.decoderSkipToIndex(idx)
	return dec.readValue()
}

func (seq leafSequence) Len() uint64 {
	return seq.numLeaves()
}

func getValuesPerIdx(seq sequence) uint64 {
	if seq.Kind() == MapKind {
		return 2
	}
	return 1
}
