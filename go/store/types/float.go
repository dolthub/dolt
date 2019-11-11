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
	"context"
	"strconv"
	"time"

	"github.com/liquidata-inc/dolt/go/store/hash"
)

// Float is a Noms Value wrapper around the primitive float64 type.
type Float float64

// Value interface
func (v Float) Value(ctx context.Context) (Value, error) {
	return v, nil
}

func (v Float) Equals(other Value) bool {
	return v == other
}

func (v Float) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	if v2, ok := other.(Float); ok {
		return v < v2, nil
	}
	return FloatKind < other.Kind(), nil
}

func (v Float) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(v, nbf)
}

func (v Float) isPrimitive() bool {
	return true
}

func (v Float) WalkValues(ctx context.Context, cb ValueCallback) error {
	return nil
}

func (v Float) WalkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (v Float) typeOf() (*Type, error) {
	return PrimitiveTypeMap[FloatKind], nil
}

func (v Float) Kind() NomsKind {
	return FloatKind
}

func (v Float) valueReadWriter() ValueReadWriter {
	return nil
}

func (v Float) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	err := FloatKind.writeTo(w, nbf)

	if err != nil {
		return err
	}

	w.writeFloat(v, nbf)
	return nil
}

func (v Float) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	return Float(b.readFloat(nbf)), nil
}

func (v Float) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	b.skipFloat(nbf)
}

func (Float) GetMarshalFunc(targetKind NomsKind) (MarshalCallback, error) {
	switch targetKind {
	case BoolKind:
		return func(val Value) (Value, error) {
			if val == nil {
				return nil, nil
			}
			fl := float64(val.(Float))
			return Bool(fl != 0), nil
		}, nil
	case FloatKind:
		return func(val Value) (Value, error) {
			return val, nil
		}, nil
	case IntKind:
		return func(val Value) (Value, error) {
			if val == nil {
				return nil, nil
			}
			fl := float64(val.(Float))
			return Int(int(fl)), nil
		}, nil
	case NullKind:
		return func(Value) (Value, error) {
			return NullValue, nil
		}, nil
	case StringKind:
		return func(val Value) (Value, error) {
			if val == nil {
				return nil, nil
			}
			fl := float64(val.(Float))
			str := strconv.FormatFloat(fl, 'f', -1, 64)
			return String(str), nil
		}, nil
	case TimestampKind:
		return func(val Value) (Value, error) {
			if val == nil {
				return nil, nil
			}
			fl := float64(val.(Float))
			// If Float is too large, we'll clamp it to the max time representable
			// There are comparison issues for times too large, so "200000000-12-31 23:59:59 UTC" seems like a reasonable maximum.
			if fl > 6311328264403199 {
				fl = 6311328264403199
				// I could not find anything pointing to a minimum allowed time, so "-200000000-01-01 00:00:00 UTC" seems reasonable
			} else if fl < -6311452567219200 {
				fl = -6311452567219200
			}
			// We treat a Float as seconds and nanoseconds, unlike integers which are just seconds
			seconds := int64(fl)
			nanoseconds := int64((fl - float64(seconds)) * float64(time.Second/time.Nanosecond))
			return Timestamp(time.Unix(seconds, nanoseconds).UTC()), nil
		}, nil
	case UintKind:
		return func(val Value) (Value, error) {
			if val == nil {
				return nil, nil
			}
			fl := float64(val.(Float))
			return Uint(uint64(fl)), nil
		}, nil
	}

	return nil, CreateNoConversionError(FloatKind, targetKind)
}

func (v Float) HumanReadableString() string {
	return strconv.FormatFloat(float64(v), 'g', -1, 64)
}
