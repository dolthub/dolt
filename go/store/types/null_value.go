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

	"github.com/dolthub/dolt/go/store/hash"
)

var NullValue Null

// IsNull returns true if the value is nil, or if the value is of kind NULLKind
func IsNull(val Value) bool {
	return val == nil || val.Kind() == NullKind
}

// Int is a Noms Value wrapper around the primitive int32 type.
type Null byte

// Value interface
func (v Null) Value(ctx context.Context) (Value, error) {
	return v, nil
}

func (v Null) Equals(other Value) bool {
	return other == nil || other.Kind() == NullKind
}

func (v Null) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	return NullKind < other.Kind(), nil
}

func (v Null) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(NullValue, nbf)
}

func (v Null) isPrimitive() bool {
	return true
}

func (v Null) WalkValues(ctx context.Context, cb ValueCallback) error {
	return nil
}

func (v Null) walkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (v Null) typeOf() (*Type, error) {
	return PrimitiveTypeMap[NullKind], nil
}

func (v Null) Kind() NomsKind {
	return NullKind
}

func (v Null) valueReadWriter() ValueReadWriter {
	return nil
}

func (v Null) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	return NullKind.writeTo(w, nbf)
}

func (v Null) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	return NullValue, nil
}

func (v Null) skip(nbf *NomsBinFormat, b *binaryNomsReader) {}

func (v Null) HumanReadableString() string {
	return "null_value"
}
