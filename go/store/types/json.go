// Copyright 2021 Dolthub, Inc.
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

package types

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/dolthub/dolt/go/store/d"
)

type JSON struct {
	valueImpl
}

// NewJSONDoc wraps |value| in a JSON value.
func NewJSONDoc(nbf *NomsBinFormat, vrw ValueReadWriter, value Value) (JSON, error) {
	w := newBinaryNomsWriter()
	if err := JSONKind.writeTo(&w, nbf); err != nil {
		return emptyJSONDoc(nbf), err
	}

	if err := value.writeTo(&w, nbf); err != nil {
		return emptyJSONDoc(nbf), err
	}

	return JSON{valueImpl{vrw, nbf, w.data(), nil}}, nil
}

func NewTestJSONDoc(nbf *NomsBinFormat, vrw ValueReadWriter, buf []byte) (JSON, error) {
	w := newBinaryNomsWriter()
	if err := JSONKind.writeTo(&w, nbf); err != nil {
		return emptyJSONDoc(nbf), err
	}

	w.writeString(string(buf))
	return JSON{valueImpl{vrw, nbf, w.data(), nil}}, nil
}

// emptyJSONDoc creates and empty JSON value.
func emptyJSONDoc(nbf *NomsBinFormat) JSON {
	w := newBinaryNomsWriter()
	if err := JSONKind.writeTo(&w, nbf); err != nil {
		d.PanicIfError(err)
	}

	return JSON{valueImpl{nil, nbf, w.data(), nil}}
}

// readJSON reads the data provided by a decoder and moves the decoder forward.
func readJSON(nbf *NomsBinFormat, dec *valueDecoder) (JSON, error) {
	start := dec.pos()

	k := dec.PeekKind()
	if k == NullKind {
		dec.skipKind()
		return emptyJSONDoc(nbf), nil
	}
	if k != JSONKind {
		return JSON{}, errors.New("current value is not a JSON")
	}

	if err := skipJSON(nbf, dec); err != nil {
		return JSON{}, err
	}

	end := dec.pos()
	return JSON{valueImpl{dec.vrw, nbf, dec.byteSlice(start, end), nil}}, nil
}

func skipJSON(nbf *NomsBinFormat, dec *valueDecoder) error {
	dec.skipKind()
	return dec.SkipValue(nbf)
}

func walkJSON(nbf *NomsBinFormat, r *refWalker, cb RefCallback) error {
	r.skipKind()
	return r.walkValue(nbf, cb)
}

// CopyOf creates a copy of a JSON.  This is necessary in cases where keeping a reference to the original JSON is
// preventing larger objects from being collected.
func (t JSON) CopyOf(vrw ValueReadWriter) JSON {
	buff := make([]byte, len(t.buff))
	offsets := make([]uint32, len(t.offsets))

	copy(buff, t.buff)
	copy(offsets, t.offsets)

	return JSON{
		valueImpl{
			buff:    buff,
			offsets: offsets,
			vrw:     vrw,
			nbf:     t.nbf,
		},
	}
}

// Empty implements the Emptyable interface.
func (t JSON) Empty() bool {
	return t.Len() == 0
}

// Format returns this values NomsBinFormat.
func (t JSON) Format() *NomsBinFormat {
	return t.format()
}

// Value implements the Value interface.
func (t JSON) Value(ctx context.Context) (Value, error) {
	return t, nil
}

// Inner returns the JSON value's inner value.
func (t JSON) Inner() (Value, error) {
	dec := newValueDecoder(t.buff, t.vrw)
	dec.skipKind()
	return dec.readValue(t.nbf)
}

// typeOf implements the Value interface.
func (t JSON) typeOf() (*Type, error) {
	val, err := t.Inner()
	if err != nil {
		return nil, err
	}
	return val.typeOf()
}

// Kind implements the Valuable interface.
func (t JSON) Kind() NomsKind {
	return JSONKind
}

func (t JSON) decoderSkipToFields() (valueDecoder, uint64) {
	dec := t.decoder()
	dec.skipKind()
	return dec, uint64(1)
}

// Len implements the Value interface.
func (t JSON) Len() uint64 {
	// TODO(andy): is this ever 0?
	return 1
}

func (t JSON) isPrimitive() bool {
	return false
}

// Less implements the LesserValuable interface.
func (t JSON) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	otherJSONDoc, ok := other.(JSON)
	if !ok {
		return JSONKind < other.Kind(), nil
	}

	cmp, err := t.Compare(otherJSONDoc)
	if err != nil {
		return false, err
	}

	return cmp == -1, nil
}

// Compare implements MySQL JSON type compare semantics.
func (t JSON) Compare(other JSON) (int, error) {
	left, err := t.Inner()
	if err != nil {
		return 0, err
	}

	right, err := other.Inner()
	if err != nil {
		return 0, err
	}

	return compareJSON(left, right)
}

func (t JSON) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	panic("unreachable")
}

func (t JSON) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	panic("unreachable")
}

// HumanReadableString implements the Value interface.
func (t JSON) HumanReadableString() string {
	val, err := t.Inner()
	if err != nil {
		d.PanicIfError(err)
	}
	h, err := val.Hash(t.nbf)
	if err != nil {
		d.PanicIfError(err)
	}
	return fmt.Sprintf("JSON(%s)", h.String())
}

func compareJSON(a, b Value) (int, error) {
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
	case Bool:
		return compareJSONBool(a, b)
	case List:
		return compareJSONArray(a, b)
	case Map:
		return compareJSONObject(a, b)
	case String:
		return compareJSONString(a, b)
	case Float:
		return compareJSONNumber(a, b)
	default:
		return 0, fmt.Errorf("unexpected type: %v", a)
	}
}

func compareJSONBool(a Bool, b Value) (int, error) {
	switch b := b.(type) {
	case Bool:
		// The JSON false literal is less than the JSON true literal.
		if a == b {
			return 0, nil
		}
		if a {
			// a > b
			return 1, nil
		} else {
			// a < b
			return -1, nil
		}

	default:
		// a is higher precedence
		return 1, nil
	}
}

func compareJSONArray(a List, b Value) (int, error) {
	switch b := b.(type) {
	case Bool:
		// a is lower precedence
		return -1, nil

	case List:
		// Two JSON arrays are equal if they have the same length and values in corresponding positions in the arrays
		// are equal. If the arrays are not equal, their order is determined by the elements in the first position
		// where there is a difference. The array with the smaller value in that position is ordered first.

		// TODO(andy): this diverges from GMS
		aLess, err := a.Less(a.format(), b)
		if err != nil {
			return 0, err
		}
		if aLess {
			return -1, nil
		}

		bLess, err := b.Less(b.format(), a)
		if err != nil {
			return 0, err
		}
		if bLess {
			return 1, nil
		}

		return 0, nil

	default:
		// a is higher precedence
		return 1, nil
	}
}

func compareJSONObject(a Map, b Value) (int, error) {
	switch b := b.(type) {
	case
		Bool,
		List:
		// a is lower precedence
		return -1, nil

	case Map:
		// Two JSON objects are equal if they have the same set of keys, and each key has the same value in both
		// objects. The order of two objects that are not equal is unspecified but deterministic.

		// TODO(andy): this diverges from GMS
		aLess, err := a.Less(a.format(), b)
		if err != nil {
			return 0, err
		}
		if aLess {
			return -1, nil
		}

		bLess, err := b.Less(b.format(), a)
		if err != nil {
			return 0, err
		}
		if bLess {
			return 1, nil
		}

		return 0, nil

	default:
		// a is higher precedence
		return 1, nil
	}
}

func compareJSONString(a String, b Value) (int, error) {
	switch b := b.(type) {
	case
		Bool,
		List,
		Map:
		// a is lower precedence
		return -1, nil

	case String:
		return strings.Compare(string(a), string(b)), nil

	default:
		// a is higher precedence
		return 1, nil
	}
}

func compareJSONNumber(a Float, b Value) (int, error) {
	switch b := b.(type) {
	case
		Bool,
		List,
		Map,
		String:
		// a is lower precedence
		return -1, nil

	case Float:
		if a > b {
			return 1, nil
		} else if a < b {
			return -1, nil
		}
		return 0, nil

	default:
		// a is higher precedence
		return 1, nil
	}
}
