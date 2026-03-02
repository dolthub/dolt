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

type RefSlice []Ref

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

func maxChunkHeight(nbf *NomsBinFormat, v Value) (max uint64, err error) {
	if _, ok := v.(SerialMessage); ok {
		// Refs in SerialMessage do not have height. This should be taller than
		// any true Ref height we expect to see in a RootValue.
		return SerialMessageRefHeight, nil
	}

	err = v.walkRefs(nbf, func(r Ref) error {
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

func (r Ref) readerAtPart(part refPart) binaryNomsReader {
	offset := r.offsetAtPart(part)
	return binaryNomsReader{r.buff[offset:], 0}
}

func (r Ref) Format() *NomsBinFormat {
	return r.format()
}

func (r Ref) TargetHash() hash.Hash {
	rdr := r.readerAtPart(refPartTargetHash)
	return rdr.readHash()
}

func (r Ref) Height() uint64 {
	rdr := r.readerAtPart(refPartHeight)
	return rdr.readCount()
}

// TargetValue retrieves the value pointed to by the Ref from the provided ValueReader. It can return a nil Value
// and a nil error if the target value is not found.
func (r Ref) TargetValue(ctx context.Context, vr ValueReader) (Value, error) {
	val, err := vr.ReadValue(ctx, r.TargetHash())
	if err != nil {
		return nil, err
	}
	return val, nil
}

// Value interface
func (r Ref) isPrimitive() bool {
	return false
}

func (r Ref) Value(ctx context.Context) (Value, error) {
	return r, nil
}

func (r Ref) typeOf() (*Type, error) {
	return makeCompoundType(RefKind, PrimitiveTypeMap[ValueKind])
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

// Returns a function that can be used to walk the hashes of all the
// Refs of a given Chunk. The callback also takes a boolean parameter |isleaf|,
// which is true when the ref points to a known leaf chunk. This function is
// meant to decouple callers from the types package itself, and so the callback
// itself does not take |types.Ref| values.
func WalkAddrsForChunkStore(cs chunks.ChunkStore) (func(chunks.Chunk, func(h hash.Hash, isleaf bool) error) error, error) {
	nbf, err := GetFormatForVersionString(cs.Version())
	if err != nil {
		return nil, fmt.Errorf("could not find binary format corresponding to %s. try upgrading dolt.", cs.Version())
	}
	return WalkAddrsForNBF(nbf, nil), nil
}

func WalkAddrsForNBF(nbf *NomsBinFormat, skipAddrs hash.HashSet) func(chunks.Chunk, func(h hash.Hash, isleaf bool) error) error {
	return func(c chunks.Chunk, cb func(h hash.Hash, isleaf bool) error) error {
		return walkRefs(c.Data(), nbf, func(r Ref) error {
			if skipAddrs != nil && skipAddrs.Has(r.TargetHash()) {
				return nil
			}

			return cb(r.TargetHash(), r.Height() == 1)
		})
	}
}

func WalkAddrs(v Value, nbf *NomsBinFormat, cb func(h hash.Hash, isleaf bool) error) error {
	return v.walkRefs(nbf, func(r Ref) error {
		return cb(r.TargetHash(), r.Height() == 1)
	})
}
