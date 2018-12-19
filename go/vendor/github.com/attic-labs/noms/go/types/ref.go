// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"

	"github.com/attic-labs/noms/go/hash"
)

type Ref struct {
	valueImpl
}

type refPart uint32

const (
	refPartKind refPart = iota
	refPartTargetHash
	refPartTargetType
	refPartHeight
	refPartEnd
)

func NewRef(v Value) Ref {
	return constructRef(v.Hash(), TypeOf(v), maxChunkHeight(v)+1)
}

// ToRefOfValue returns a new Ref that points to the same target as |r|, but
// with the type 'Ref<Value>'.
func ToRefOfValue(r Ref) Ref {
	return constructRef(r.TargetHash(), ValueType, r.Height())
}

func constructRef(targetHash hash.Hash, targetType *Type, height uint64) Ref {
	w := newBinaryNomsWriter()

	offsets := make([]uint32, refPartEnd)
	offsets[refPartKind] = w.offset
	RefKind.writeTo(&w)
	offsets[refPartTargetHash] = w.offset
	w.writeHash(targetHash)
	offsets[refPartTargetType] = w.offset
	targetType.writeToAsType(&w, map[string]*Type{})
	offsets[refPartHeight] = w.offset
	w.writeCount(height)

	return Ref{valueImpl{nil, w.data(), offsets}}
}

func writeRefPartsTo(w nomsWriter, targetHash hash.Hash, targetType *Type, height uint64) {
	RefKind.writeTo(w)
	w.writeHash(targetHash)
	targetType.writeToAsType(w, map[string]*Type{})
	w.writeCount(height)
}

// readRef reads the data provided by a reader and moves the reader forward.
func readRef(dec *typedBinaryNomsReader) Ref {
	start := dec.pos()
	offsets := skipRef(dec)
	end := dec.pos()
	return Ref{valueImpl{nil, dec.byteSlice(start, end), offsets}}
}

// skipRef moves the reader forward, past the data representing the Ref, and returns the offsets of the component parts.
func skipRef(dec *typedBinaryNomsReader) []uint32 {
	offsets := make([]uint32, refPartEnd)
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

func maxChunkHeight(v Value) (max uint64) {
	v.WalkRefs(func(r Ref) {
		if height := r.Height(); height > max {
			max = height
		}
	})
	return
}

func (r Ref) offsetAtPart(part refPart) uint32 {
	return r.offsets[part] - r.offsets[refPartKind]
}

func (r Ref) decoderAtPart(part refPart) valueDecoder {
	offset := r.offsetAtPart(part)
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

func (r Ref) WalkValues(cb ValueCallback) {
}

func (r Ref) typeOf() *Type {
	return makeCompoundType(RefKind, r.TargetType())
}

func (r Ref) isSameTargetType(other Ref) bool {
	targetTypeBytes := r.buff[r.offsetAtPart(refPartTargetType):r.offsetAtPart(refPartHeight)]
	otherTargetTypeBytes := other.buff[other.offsetAtPart(refPartTargetType):other.offsetAtPart(refPartHeight)]
	return bytes.Equal(targetTypeBytes, otherTargetTypeBytes)
}
