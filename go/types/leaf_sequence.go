// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

type leafSequence struct {
	vrw     ValueReadWriter
	buff    []byte
	offsets []uint32
}

func newLeafSequence(kind NomsKind, count uint64, vrw ValueReadWriter, vs ...Value) leafSequence {
	d.PanicIfTrue(vrw == nil)
	w := newBinaryNomsWriter()
	offsets := make([]uint32, len(vs)+sequencePartValues+1)
	offsets[sequencePartKind] = w.offset
	kind.writeTo(&w)
	offsets[sequencePartLevel] = w.offset
	w.writeCount(0) // level
	offsets[sequencePartCount] = w.offset
	w.writeCount(count)
	offsets[sequencePartValues] = w.offset
	for i, v := range vs {
		v.writeTo(&w)
		offsets[i+sequencePartValues+1] = w.offset
	}
	return leafSequence{vrw, w.data(), offsets}
}

// readLeafSequence reads the data provided by a decoder and moves the decoder forward.
func readLeafSequence(dec *valueDecoder) leafSequence {
	start := dec.pos()
	offsets := skipLeafSequence(dec)
	end := dec.pos()
	return leafSequence{dec.vrw, dec.byteSlice(start, end), offsets}
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

func (seq leafSequence) decoder() valueDecoder {
	return newValueDecoder(seq.buff, seq.vrw)
}

func (seq leafSequence) decoderAtOffset(offset int) valueDecoder {
	return newValueDecoder(seq.buff[offset:], seq.vrw)
}

func (seq leafSequence) decoderAtPart(part uint32) valueDecoder {
	offset := seq.offsets[part] - seq.offsets[sequencePartKind]
	return newValueDecoder(seq.buff[offset:], seq.vrw)
}

func (seq leafSequence) decoderSkipToValues() (valueDecoder, uint64) {
	dec := seq.decoderAtPart(sequencePartCount)
	count := dec.readCount()
	return dec, count
}

func (seq leafSequence) decoderSkipToIndex(idx int) valueDecoder {
	offset := seq.getItemOffset(idx)
	return seq.decoderAtOffset(offset)
}

func (seq leafSequence) writeTo(w nomsWriter) {
	w.writeRaw(seq.buff)
}

func (seq leafSequence) values() []Value {
	dec, count := seq.decoderSkipToValues()
	vs := make([]Value, count)
	for i := uint64(0); i < count; i++ {
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

func (seq leafSequence) seqLen() int {
	return int(seq.numLeaves())
}

func (seq leafSequence) numLeaves() uint64 {
	_, count := seq.decoderSkipToValues()
	return count
}

func (seq leafSequence) valueReadWriter() ValueReadWriter {
	return seq.vrw
}

func (seq leafSequence) getChildSequence(idx int) sequence {
	return nil
}

func (seq leafSequence) Kind() NomsKind {
	dec := seq.decoder()
	return dec.readKind()
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

func (seq leafSequence) getItemOffset(idx int) int {
	// kind, level, count, elements...
	// 0     1      2      3          n+1
	d.PanicIfTrue(idx+sequencePartValues+1 > len(seq.offsets))
	return int(seq.offsets[idx+sequencePartValues] - seq.offsets[sequencePartKind])
}

func (seq leafSequence) getItem(idx int) sequenceItem {
	dec := seq.decoderSkipToIndex(idx)
	return dec.readValue()
}

func (seq leafSequence) WalkRefs(cb RefCallback) {
	dec, count := seq.decoderSkipToValues()
	for i := uint64(0); i < count; i++ {
		dec.readValue().WalkRefs(cb)
	}
}

// Collection interface

func (seq leafSequence) Len() uint64 {
	_, count := seq.decoderSkipToValues()
	return count
}

func (seq leafSequence) Empty() bool {
	return seq.Len() == uint64(0)
}

func (seq leafSequence) hash() hash.Hash {
	return hash.Of(seq.buff)
}

func (seq leafSequence) equals(other sequence) bool {
	return bytes.Equal(seq.bytes(), other.bytes())
}

func (seq leafSequence) bytes() []byte {
	return seq.buff
}
