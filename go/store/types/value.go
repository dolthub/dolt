// Copyright 2019 Liquidata, Inc.
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
	"github.com/liquidata-inc/dolt/go/store/hash"
)

type ValueCallback func(v Value) error
type RefCallback func(ref Ref) error

// Valuable is an interface from which a Value can be retrieved.
type Valuable interface {
	// Kind is the NomsKind describing the kind of value this is.
	Kind() NomsKind

	Value(ctx context.Context) (Value, error)
}

type LesserValuable interface {
	Valuable
	// Less determines if this Noms value is less than another Noms value.
	// When comparing two Noms values and both are comparable and the same type (Bool, Float or
	// String) then the natural ordering is used. For other Noms values the Hash of the value is
	// used. When comparing Noms values of different type the following ordering is used:
	// Bool < Float < String < everything else.
	Less(nbf *NomsBinFormat, other LesserValuable) (bool, error)
}

// Emptyable is an interface for Values which may or may not be empty
type Emptyable interface {
	Empty() bool
}

// Value is the interface all Noms values implement.
type Value interface {
	LesserValuable

	// Equals determines if two different Noms values represents the same underlying value.
	Equals(other Value) bool

	// Hash is the hash of the value. All Noms values have a unique hash and if two values have the
	// same hash they must be equal.
	Hash(*NomsBinFormat) (hash.Hash, error)

	// WalkValues iterates over the immediate children of this value in the DAG, if any, not including
	// Type()
	WalkValues(context.Context, ValueCallback) error

	// WalkRefs iterates over the refs to the underlying chunks. If this value is a collection that has been
	// chunked then this will return the refs of th sub trees of the prolly-tree.
	WalkRefs(*NomsBinFormat, RefCallback) error

	// typeOf is the internal implementation of types.TypeOf. It is not normalized
	// and unions might have a single element, duplicates and be in the wrong
	// order.
	typeOf() (*Type, error)

	// writeTo writes the encoded version of the value to a nomsWriter.
	writeTo(nomsWriter, *NomsBinFormat) error
}

type ValueSlice []Value

func (vs ValueSlice) Equals(other ValueSlice) bool {
	if len(vs) != len(other) {
		return false
	}

	for i, v := range vs {
		if !v.Equals(other[i]) {
			return false
		}
	}

	return true
}

func (vs ValueSlice) Contains(nbf *NomsBinFormat, v Value) bool {
	for _, v := range vs {
		if v.Equals(v) {
			return true
		}
	}
	return false
}

type ValueSort struct {
	values []Value
	nbf    *NomsBinFormat
}

func (vs ValueSort) Len() int      { return len(vs.values) }
func (vs ValueSort) Swap(i, j int) { vs.values[i], vs.values[j] = vs.values[j], vs.values[i] }
func (vs ValueSort) Less(i, j int) (bool, error) {
	return vs.values[i].Less(vs.nbf, vs.values[j])
}

func (vs ValueSort) Equals(other ValueSort) bool {
	return ValueSlice(vs.values).Equals(ValueSlice(other.values))
}

func (vs ValueSort) Contains(v Value) bool {
	return ValueSlice(vs.values).Contains(vs.nbf, v)
}

type valueReadWriter interface {
	valueReadWriter() ValueReadWriter
}

type valueImpl struct {
	vrw     ValueReadWriter
	nbf     *NomsBinFormat
	buff    []byte
	offsets []uint32
}

func (v valueImpl) valueReadWriter() ValueReadWriter {
	return v.vrw
}

func (v valueImpl) writeTo(enc nomsWriter, nbf *NomsBinFormat) error {
	enc.writeRaw(v.buff)
	return nil
}

func (v valueImpl) valueBytes(nbf *NomsBinFormat) ([]byte, error) {
	return v.buff, nil
}

// IsZeroValue can be used to test if a Value is the same as T{}.
func (v valueImpl) IsZeroValue() bool {
	return v.buff == nil
}

func (v valueImpl) Hash(*NomsBinFormat) (hash.Hash, error) {
	return hash.Of(v.buff), nil
}

func (v valueImpl) decoder() valueDecoder {
	return newValueDecoder(v.buff, v.vrw)
}

func (v valueImpl) format() *NomsBinFormat {
	return v.nbf
}

func (v valueImpl) decoderAtOffset(offset int) valueDecoder {
	return newValueDecoder(v.buff[offset:], v.vrw)
}

func (v valueImpl) asValueImpl() valueImpl {
	return v
}

func (v valueImpl) Equals(other Value) bool {
	if otherValueImpl, ok := other.(asValueImpl); ok {
		return bytes.Equal(v.buff, otherValueImpl.asValueImpl().buff)
	}
	return false
}

func (v valueImpl) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	return valueLess(nbf, v, other.(Value))
}

func (v valueImpl) WalkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	bts, err := v.valueBytes(nbf)

	if err != nil {
		return err
	}

	return walkRefs(bts, nbf, cb)
}

type asValueImpl interface {
	asValueImpl() valueImpl
}

func (v valueImpl) Kind() NomsKind {
	return NomsKind(v.buff[0])
}
