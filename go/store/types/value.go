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

	"github.com/dolthub/dolt/go/store/hash"
)

type ValueCallback func(v Value) error
type RefCallback func(ref Ref) error
type MarshalCallback func(val Value) (Value, error)

var MaxPrimitiveKind int
var PrimitiveKindMask []bool

func init() {
	for _, value := range KindToType {
		if value != nil && value.isPrimitive() {
			nomsKind := value.Kind()
			PrimitiveTypeMap[nomsKind] = makePrimitiveType(nomsKind)
		}
	}
	for k := range PrimitiveTypeMap {
		if int(k) > MaxPrimitiveKind {
			MaxPrimitiveKind = int(k)
		}
	}
	PrimitiveKindMask = make([]bool, MaxPrimitiveKind+1)
	for k := range PrimitiveTypeMap {
		PrimitiveKindMask[int(k)] = true
	}

	maxKindInKindToType := 0
	for k := range KindToType {
		if int(k) > maxKindInKindToType {
			maxKindInKindToType = int(k)
		}
	}
	KindToTypeSlice = make([]Value, maxKindInKindToType+1)
	for k, v := range KindToType {
		KindToTypeSlice[int(k)] = v
	}

}

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

	// isPrimitive returns whether the Value is a primitive type
	isPrimitive() bool

	// WalkValues iterates over the immediate children of this value in the DAG, if any, not including
	// Type()
	WalkValues(context.Context, ValueCallback) error

	// HumanReadableString returns a human-readable string version of this Value (not meant for re-parsing)
	HumanReadableString() string

	// walkRefs iterates over the refs to the underlying chunks. If this value is a collection that has been
	// chunked then this will return the refs of th sub trees of the prolly-tree.
	walkRefs(*NomsBinFormat, RefCallback) error

	// typeOf is the internal implementation of types.TypeOf. It is not normalized
	// and unions might have a single element, duplicates and be in the wrong
	// order.
	typeOf() (*Type, error)

	// writeTo writes the encoded version of the value to a nomsWriter.
	writeTo(nomsWriter, *NomsBinFormat) error

	// readFrom reads the encoded version of the value from a binaryNomsReader
	readFrom(*NomsBinFormat, *binaryNomsReader) (Value, error)

	// skip takes in a binaryNomsReader and skips the encoded version of the value
	skip(*NomsBinFormat, *binaryNomsReader)
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

func (vs ValueSlice) Contains(nbf *NomsBinFormat, val Value) bool {
	for _, v := range vs {
		if v.Equals(val) {
			return true
		}
	}
	return false
}

type ValueSort struct {
	Values []Value
	Nbf    *NomsBinFormat
}

func (vs ValueSort) Len() int      { return len(vs.Values) }
func (vs ValueSort) Swap(i, j int) { vs.Values[i], vs.Values[j] = vs.Values[j], vs.Values[i] }
func (vs ValueSort) Less(i, j int) (bool, error) {
	return vs.Values[i].Less(vs.Nbf, vs.Values[j])
}

func (vs ValueSort) Equals(other ValueSort) bool {
	return ValueSlice(vs.Values).Equals(ValueSlice(other.Values))
}

func (vs ValueSort) Contains(v Value) bool {
	return ValueSlice(vs.Values).Contains(vs.Nbf, v)
}

type valueReadWriter interface {
	valueReadWriter() ValueReadWriter
}

type TupleSlice []Tuple

func (vs TupleSlice) Equals(other TupleSlice) bool {
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

func (vs TupleSlice) Contains(nbf *NomsBinFormat, v Tuple) bool {
	for _, v := range vs {
		if v.Equals(v) {
			return true
		}
	}
	return false
}

type TupleSort struct {
	Tuples []Tuple
	Nbf    *NomsBinFormat
}

func (vs TupleSort) Len() int {
	return len(vs.Tuples)
}

func (vs TupleSort) Swap(i, j int) {
	vs.Tuples[i], vs.Tuples[j] = vs.Tuples[j], vs.Tuples[i]
}

func (vs TupleSort) Less(i, j int) (bool, error) {
	res, err := vs.Tuples[i].TupleCompare(vs.Nbf, vs.Tuples[j])
	if err != nil {
		return false, err
	}

	return res < 0, nil
}

func (vs TupleSort) Equals(other TupleSort) bool {
	return TupleSlice(vs.Tuples).Equals(other.Tuples)
}

func (vs TupleSort) Contains(v Tuple) bool {
	return TupleSlice(vs.Tuples).Contains(vs.Nbf, v)
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
	res, err := valueCompare(nbf, v, other.(Value))
	if err != nil {
		return false, nil
	}

	isLess := res < 0
	return isLess, nil
}

func (v valueImpl) Compare(nbf *NomsBinFormat, other LesserValuable) (int, error) {
	return valueCompare(nbf, v, other.(Value))
}

func (v valueImpl) walkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	w := binaryNomsWriter{make([]byte, len(v.buff)+1), 0}
	err := v.writeTo(&w, nbf)

	if err != nil {
		return err
	}

	return walkRefs(w.buff[:w.offset], nbf, cb)
}

type asValueImpl interface {
	asValueImpl() valueImpl
}

func (v valueImpl) Kind() NomsKind {
	return NomsKind(v.buff[0])
}
