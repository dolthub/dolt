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
	"bytes"
	"context"
	"fmt"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
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

func NewRef(v Value, nbf *NomsBinFormat) (Ref, error) {
	h, err := v.Hash(nbf)

	if err != nil {
		return Ref{}, err
	}

	mch, err := maxChunkHeight(nbf, v)

	if err != nil {
		return Ref{}, err
	}

	t, err := TypeOf(v)

	if err != nil {
		return Ref{}, err
	}

	return constructRef(nbf, h, t, mch+1)
}

// ToRefOfValue returns a new Ref that points to the same target as |r|, but
// with the type 'Ref<Value>'.
func ToRefOfValue(r Ref, nbf *NomsBinFormat) (Ref, error) {
	return constructRef(nbf, r.TargetHash(), PrimitiveTypeMap[ValueKind], r.Height())
}

func constructRef(nbf *NomsBinFormat, targetHash hash.Hash, targetType *Type, height uint64) (Ref, error) {
	w := newBinaryNomsWriter()

	offsets := make([]uint32, refPartEnd)
	offsets[refPartKind] = w.offset
	err := RefKind.writeTo(&w, nbf)

	if err != nil {
		return Ref{}, err
	}

	offsets[refPartTargetHash] = w.offset
	w.writeHash(targetHash)
	offsets[refPartTargetType] = w.offset
	err = targetType.writeToAsType(&w, map[string]*Type{}, nbf)

	if err != nil {
		return Ref{}, err
	}

	offsets[refPartHeight] = w.offset
	w.writeCount(height)

	return Ref{valueImpl{nil, nbf, w.data(), offsets}}, nil
}

// readRef reads the data provided by a reader and moves the reader forward.
func readRef(nbf *NomsBinFormat, dec *typedBinaryNomsReader) (Ref, error) {
	start := dec.pos()
	offsets, err := skipRef(dec)

	if err != nil {
		return Ref{}, err
	}

	end := dec.pos()
	return Ref{valueImpl{nil, nbf, dec.byteSlice(start, end), offsets}}, nil
}

// skipRef moves the reader forward, past the data representing the Ref, and returns the offsets of the component parts.
func skipRef(dec *typedBinaryNomsReader) ([]uint32, error) {
	offsets := make([]uint32, refPartEnd)
	offsets[refPartKind] = dec.pos()
	dec.skipKind()
	offsets[refPartTargetHash] = dec.pos()
	dec.skipHash() // targetHash
	offsets[refPartTargetType] = dec.pos()
	err := dec.skipType() // targetType

	if err != nil {
		return nil, err
	}

	offsets[refPartHeight] = dec.pos()
	dec.skipCount() // height
	return offsets, nil
}

func maxChunkHeight(nbf *NomsBinFormat, v Value) (max uint64, err error) {
	err = v.WalkRefs(nbf, func(r Ref) error {
		if height := r.Height(); height > max {
			max = height
		}

		return nil
	})

	if err != nil {
		return 0, err
	}

	return max, nil
}

func (r Ref) offsetAtPart(part refPart) uint32 {
	return r.offsets[part] - r.offsets[refPartKind]
}

func (r Ref) decoderAtPart(part refPart) valueDecoder {
	offset := r.offsetAtPart(part)
	return newValueDecoder(r.buff[offset:], nil)
}

func (r Ref) Format() *NomsBinFormat {
	return r.format()
}

func (r Ref) TargetHash() hash.Hash {
	dec := r.decoderAtPart(refPartTargetHash)
	return dec.readHash()
}

func (r Ref) Height() uint64 {
	dec := r.decoderAtPart(refPartHeight)
	return dec.readCount()
}

func (r Ref) TargetValue(ctx context.Context, vr ValueReader) (Value, error) {
	return vr.ReadValue(ctx, r.TargetHash())
}

func (r Ref) TargetType() (*Type, error) {
	dec := r.decoderAtPart(refPartTargetType)
	return dec.readType()
}

// Value interface
func (r Ref) isPrimitive() bool {
	return false
}

func (r Ref) Value(ctx context.Context) (Value, error) {
	return r, nil
}

func (r Ref) WalkValues(ctx context.Context, cb ValueCallback) error {
	return nil
}

func (r Ref) typeOf() (*Type, error) {
	t, err := r.TargetType()

	if err != nil {
		return nil, err
	}

	return makeCompoundType(RefKind, t)
}

func (r Ref) isSameTargetType(other Ref) bool {
	targetTypeBytes := r.buff[r.offsetAtPart(refPartTargetType):r.offsetAtPart(refPartHeight)]
	otherTargetTypeBytes := other.buff[other.offsetAtPart(refPartTargetType):other.offsetAtPart(refPartHeight)]
	return bytes.Equal(targetTypeBytes, otherTargetTypeBytes)
}

func (r Ref) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	panic("unreachable")
}

func (r Ref) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	panic("unreachable")
}

func (r Ref) String() string {
	panic("unreachable")
}

func (r Ref) HumanReadableString() string {
	panic("unreachable")
}

// Returns a function that can be used to walk the hash and height of all the
// Refs of a given Chunk.  This function is meant to decouple callers from the
// types package itself, and so the callback itself does not take |types.Ref|
// values.
func WalkRefsForChunkStore(cs chunks.ChunkStore) (func(chunks.Chunk, func(h hash.Hash, height uint64) error) error, error) {
	nbf, err := GetFormatForVersionString(cs.Version())
	if err != nil {
		return nil, fmt.Errorf("could not find binary format corresponding to %s. try upgrading dolt.", cs.Version())
	}
	return func(c chunks.Chunk, cb func(h hash.Hash, height uint64) error) error {
		return WalkRefs(c, nbf, func(r Ref) error {
			return cb(r.TargetHash(), r.Height())
		})
	}, nil
}
