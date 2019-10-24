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
	"encoding/base64"
	"github.com/google/uuid"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"math"
	"strconv"
	"strings"
)

// String is a Noms Value wrapper around the primitive string type.
type String string

// Value interface
func (s String) Value(ctx context.Context) (Value, error) {
	return s, nil
}

func (s String) Equals(other Value) bool {
	return s == other
}

func (s String) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	if s2, ok := other.(String); ok {
		return s < s2, nil
	}
	return StringKind < other.Kind(), nil
}

func (s String) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(s, nbf)
}

func (s String) IsPrimitive() bool {
	return true
}

func (s String) WalkValues(ctx context.Context, cb ValueCallback) error {
	return nil
}

func (s String) WalkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (s String) typeOf() (*Type, error) {
	return PrimitiveTypeMap[StringKind], nil
}

func (s String) Kind() NomsKind {
	return StringKind
}

func (s String) valueReadWriter() ValueReadWriter {
	return nil
}

func (s String) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	err := StringKind.writeTo(w, nbf)

	if err != nil {
		return err
	}

	w.writeString(string(s))

	return nil
}

func (s String) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	return String(b.readString()), nil
}

func (s String) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	b.skipString()
}

func (String) MarshalToKind(targetKind NomsKind) MarshalCallback {
	switch targetKind {
	case BoolKind:
		return func(val Value) (Value, error) {
			if val == nil {
				return nil, nil
			}
			s := val.(String)
			if len(s) == 0 {
				return NullValue, nil
			}
			b, err := strconv.ParseBool(strings.ToLower(string(s)))
			if err != nil {
				return Bool(false), CreateConversionError(s, BoolKind, err)
			}
			return Bool(b), nil
		}
	case FloatKind:
		return func(val Value) (Value, error) {
			if val == nil {
				return nil, nil
			}
			s := val.(String)
			if len(s) == 0 {
				return NullValue, nil
			}
			f, err := strconv.ParseFloat(string(s), 64)
			if err != nil {
				return Float(math.NaN()), CreateConversionError(s, FloatKind, err)
			}
			return Float(f), nil
		}
	case InlineBlobKind:
		return func(val Value) (Value, error) {
			if val == nil {
				return nil, nil
			}
			s := val.(String)
			if len(s) == 0 {
				return NullValue, nil
			}
			data, err := base64.RawURLEncoding.DecodeString(string(s))
			if err != nil {
				return InlineBlob{}, CreateConversionError(s, InlineBlobKind, err)
			}
			return InlineBlob(data), nil
		}
	case IntKind:
		return func(val Value) (Value, error) {
			if val == nil {
				return nil, nil
			}
			s := val.(String)
			if len(s) == 0 {
				return NullValue, nil
			}
			n, err := strconv.ParseInt(string(s), 10, 64)
			if err != nil {
				return Int(0), CreateConversionError(s, IntKind, err)
			}
			return Int(n), nil
		}
	case NullKind:
		return func(Value) (Value, error) {
			return NullValue, nil
		}
	case StringKind:
		return func(val Value) (Value, error) {
			return val, nil
		}
	case UintKind:
		return func(val Value) (Value, error) {
			if val == nil {
				return nil, nil
			}
			s := val.(String)
			if len(s) == 0 {
				return NullValue, nil
			}
			n, err := strconv.ParseUint(string(s), 10, 64)
			if err != nil {
				return Uint(0), CreateConversionError(s, UintKind, err)
			}
			return Uint(n), nil
		}
	case UUIDKind:
		return func(val Value) (Value, error) {
			if val == nil {
				return nil, nil
			}
			s := val.(String)
			if len(s) == 0 {
				return NullValue, nil
			}
			u, err := uuid.Parse(string(s))
			if err != nil {
				return UUID(u), CreateConversionError(s, UUIDKind, err)
			}
			return UUID(u), nil
		}
	}

	return nil
}

func (s String) HumanReadableString() string {
	return strconv.Quote(string(s))
}
