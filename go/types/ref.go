// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"

	"github.com/attic-labs/noms/go/hash"
)

type Ref struct {
	buff    []byte
	offsets refOffsets
}

type refPart uint32

const (
	refPartKind refPart = iota
	refPartTargetHash
	refPartTargetType
	refPartHeight
	refPartEnd
)

type refOffsets [refPartEnd]uint32

func NewRef(v Value) Ref {
	// TODO: Taking the hash will duplicate the work of computing the type
	return constructRef(v.Hash(), TypeOf(v), maxChunkHeight(v)+1)
}

// ToRefOfValue returns a new Ref that points to the same target as |r|, but
// with the type 'Ref<Value>'.
func ToRefOfValue(r Ref) Ref {
	return constructRef(r.TargetHash(), ValueType, r.Height())
}

func constructRef(targetHash hash.Hash, targetType *Type, height uint64) Ref {
	w := newBinaryNomsWriter()

	var offsets refOffsets
	offsets[refPartKind] = w.offset
	RefKind.writeTo(&w)
	offsets[refPartTargetHash] = w.offset
	w.writeHash(targetHash)
	offsets[refPartTargetType] = w.offset
	targetType.writeToAsType(&w, map[string]*Type{})
	offsets[refPartHeight] = w.offset
	w.writeCount(height)

	return Ref{w.data(), offsets}
}

func writeRefPartsTo(w nomsWriter, targetHash hash.Hash, targetType *Type, height uint64) {
	RefKind.writeTo(w)
	w.writeHash(targetHash)
	targetType.writeToAsType(w, map[string]*Type{})
	w.writeCount(height)
}

// readRef reads the data provided by a decoder and moves the decoder forward.
func readRef(dec *valueDecoder) Ref {
	start := dec.pos()
	offsets := skipRef(dec)
	end := dec.pos()
	return Ref{dec.byteSlice(start, end), offsets}
}

// readRef reads the data provided by a decoder and moves the decoder forward.
func skipRef(dec *valueDecoder) refOffsets {
	var offsets refOffsets
	offsets[refPartKind] = dec.pos()
	dec.skipKind()
	offsets[refPartTargetHash] = dec.pos()
	dec.skipHash() // targetHash
	offsets[refPartTargetType] = dec.pos()
	dec.skipType() // targetType
	offsets[refPartHeight] = dec.pos()
	dec.skipCount() // height
	return offsets
}

func (r Ref) writeTo(w nomsWriter) {
	w.writeRaw(r.buff)
}

func (r Ref) valueBytes() []byte {
	return r.buff
}

func maxChunkHeight(v Value) (max uint64) {
	v.WalkRefs(func(r Ref) {
		if height := r.Height(); height > max {
			max = height
		}
	})
	return
}

func (r Ref) decoder() valueDecoder {
	return newValueDecoder(r.buff, nil)
}

func (r Ref) decoderAtPart(part refPart) valueDecoder {
	offset := r.offsets[part] - r.offsets[refPartKind]
	return newValueDecoder(r.buff[offset:], nil)
}

func (r Ref) TargetHash() hash.Hash {
	dec := r.decoderAtPart(refPartTargetHash)
	return dec.readHash()
}

func (r Ref) Height() uint64 {
	dec := r.decoderAtPart(refPartHeight)
	return dec.readCount()
}

func (r Ref) TargetValue(vr ValueReader) Value {
	return vr.ReadValue(r.TargetHash())
}

func (r Ref) TargetType() *Type {
	dec := r.decoderAtPart(refPartTargetType)
	return dec.readType()
}

// Value interface
func (r Ref) Value() Value {
	return r
}

func (r Ref) Equals(other Value) bool {
	if otherRef, ok := other.(Ref); ok {
		return bytes.Equal(r.buff, otherRef.buff)
	}
	return false
}

func (r Ref) Less(other Value) bool {
	return valueLess(r, other)
}

func (r Ref) Hash() hash.Hash {
	return hash.Of(r.buff)
}

func (r Ref) WalkValues(cb ValueCallback) {
}

func (r Ref) WalkRefs(cb RefCallback) {
	cb(r)
}

func (r Ref) typeOf() *Type {
	return makeCompoundType(RefKind, r.TargetType())
}

func (r Ref) Kind() NomsKind {
	return RefKind
}

func (r Ref) valueReadWriter() ValueReadWriter {
	return nil
}
