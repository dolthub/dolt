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
	"fmt"
	"github.com/dolthub/dolt/go/store/d"

	"github.com/dolthub/dolt/go/store/hash"
)

type Geometry struct {
	valueImpl
}

// NewGeometryObj wraps value in a Geometry value.
func NewGeometryObj(nbf *NomsBinFormat, vrw ValueReadWriter, value Value) (Geometry, error) {
	w := newBinaryNomsWriter()
	if err := GeometryKind.writeTo(&w, nbf); err != nil {
		return EmptyGeometryObj(nbf), err
	}

	if err := value.writeTo(&w, nbf); err != nil {
		return EmptyGeometryObj(nbf), err
	}

	return Geometry{valueImpl{vrw, nbf, w.data(), nil}}, nil
}

// EmptyGeometryObj creates an empty Geometry value.
func EmptyGeometryObj(nbf *NomsBinFormat) Geometry {
	w := newBinaryNomsWriter()
	if err := GeometryKind.writeTo(&w, nbf); err != nil {
		d.PanicIfError(err)
	}

	return Geometry{valueImpl{nil, nbf, w.data(), nil}}
}

// readGeometry reads the data provided by a decoder and moves the decoder forward.
func readGeometry(nbf *NomsBinFormat, dec *valueDecoder) (Geometry, error) {
	start := dec.pos()

	k := dec.PeekKind()
	if k == NullKind {
		dec.skipKind()
		return EmptyGeometryObj(nbf), nil
	}
	if k != GeometryKind {
		return Geometry{}, errors.New("current value is not a Geometry")
	}

	if err := skipJSON(nbf, dec); err != nil {
		return Geometry{}, err
	}

	end := dec.pos()
	return Geometry{valueImpl{dec.vrw, nbf, dec.byteSlice(start, end), nil}}, nil
}

// Value implements the Value interface
func (v Geometry) Value(ctx context.Context) (Value, error) {
	return v, nil
}

// Inner returns the Geometry value's inner value.
func (v Geometry) Inner() (Value, error) {
	dec := newValueDecoder(v.buff, v.vrw)
	dec.skipKind()
	return dec.readValue(v.nbf)
}

// Equals implements the Value interface
func (v Geometry) Equals(other Value) bool {
	return other == nil || other.Kind() == GeometryKind
}

// Hash implements the Value interface
func (v Geometry) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(NullValue, nbf)
}

// isPrimitive implements the Value interface
func (v Geometry) isPrimitive() bool {
	return false
}

// WalkValues implements the Value interface
func (v Geometry) WalkValues(ctx context.Context, cb ValueCallback) error {
	return nil
}

// WalkRefs implements the Value interface
func (v Geometry) WalkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func compareGeometry(a, b Value) (int, error) {
	aNull := a.Kind() == NullKind
	bNull := b.Kind() == NullKind
	if aNull && bNull {
		return 0, nil
	} else if aNull && !bNull {
		return -1, nil
	} else if !aNull && bNull {
		return 1, nil
	}

	switch a := a.(type) {
	default:
		return 0, fmt.Errorf("unexpected type: %v", a)
	}
}

// Compare implements MySQL Geometry type compare semantics.
func (t Geometry) Compare(other Geometry) (int, error) {
	left, err := t.Inner()
	if err != nil {
		return 0, err
	}

	right, err := other.Inner()
	if err != nil {
		return 0, err
	}

	return compareGeometry(left, right)
}

// HumanReadableString implements the Value interface
func (v Geometry) HumanReadableString() string {
	val, err := v.Inner()
	if err != nil {
		d.PanicIfError(err)
	}
	h, err := val.Hash(v.nbf)
	if err != nil {
		d.PanicIfError(err)
	}
	return fmt.Sprintf("Geometry(%s)", h.String())
}

func (v Geometry) typeOf() (*Type, error) {
	return PrimitiveTypeMap[GeometryKind], nil
}

// Kind implements the Valuable interface.
func (v Geometry) Kind() NomsKind {
	return GeometryKind
}

func (v Geometry) valueReadWriter() ValueReadWriter {
	return nil
}

func (v Geometry) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	return GeometryKind.writeTo(w, nbf)
}

func (v Geometry) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	return NullValue, nil
}

func (v Geometry) skip(nbf *NomsBinFormat, b *binaryNomsReader) {}
