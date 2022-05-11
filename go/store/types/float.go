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
	"strconv"

	"github.com/dolthub/dolt/go/store/hash"
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

func (v Float) walkRefs(nbf *NomsBinFormat, cb RefCallback) error {
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
	return Float(b.ReadFloat(nbf)), nil
}

func (v Float) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	b.skipFloat(nbf)
}

func (v Float) HumanReadableString() string {
	return strconv.FormatFloat(float64(v), 'g', -1, 64)
}
