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
	"github.com/liquidata-inc/dolt/go/store/hash"
	"strconv"
)

// Int is a Noms Value wrapper around the primitive int32 type.
type Uint uint64

// Value interface
func (v Uint) Value(ctx context.Context) (Value, error) {
	return v, nil
}

func (v Uint) Equals(other Value) bool {
	return v == other
}

func (v Uint) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	if v2, ok := other.(Uint); ok {
		return v < v2, nil
	}

	return UintKind < other.Kind(), nil
}

func (v Uint) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(v, nbf)
}

func (v Uint) IsPrimitive() bool {
	return true
}

func (v Uint) WalkValues(ctx context.Context, cb ValueCallback) error {
	return nil
}

func (v Uint) WalkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (v Uint) typeOf() (*Type, error) {
	return PrimitiveTypeMap[UintKind], nil
}

func (v Uint) Kind() NomsKind {
	return UintKind
}

func (v Uint) valueReadWriter() ValueReadWriter {
	return nil
}

func (v Uint) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	err := UintKind.writeTo(w, nbf)

	if err != nil {
		return err
	}

	w.writeUint(v)

	return nil
}

func (v Uint) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	return Uint(b.readUint()), nil
}

func (v Uint) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	b.skipUint()
}

func (Uint) MarshalToKind(targetKind NomsKind) MarshalCallback {
	switch targetKind {
	case BoolKind:
		return func(val Value) (Value, error) {
			if val == nil {
				return nil, nil
			}
			n := uint64(val.(Uint))
			return Bool(n != 0), nil
		}
	case FloatKind:
		return func(val Value) (Value, error) {
			if val == nil {
				return nil, nil
			}
			n := uint64(val.(Uint))
			return Float(float64(n)), nil
		}
	case IntKind:
		return func(val Value) (Value, error) {
			if val == nil {
				return nil, nil
			}
			n := uint64(val.(Uint))
			return Int(int64(n)), nil
		}
	case NullKind:
		return func(Value) (Value, error) {
			return NullValue, nil
		}
	case StringKind:
		return func(val Value) (Value, error) {
			if val == nil {
				return nil, nil
			}
			n := uint64(val.(Uint))
			str := strconv.FormatUint(n, 10)
			return String(str), nil
		}
	case UintKind:
		return func(val Value) (Value, error) {
			return val, nil
		}
	}

	return nil
}

func (v Uint) HumanReadableString() string {
	return strconv.FormatUint(uint64(v), 10)
}

