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

// Polygon is a Noms Value wrapper around a string.
type Polygon string

// Value interface
func (v Polygon) Value(ctx context.Context) (Value, error) {
	return v, nil
}

func (v Polygon) Equals(other Value) bool {
	return v == other
}

func (v Polygon) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	if v2, ok := other.(Polygon); ok {
		return v < v2, nil
	}
	return PolygonKind < other.Kind(), nil
}

func (v Polygon) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(v, nbf)
}

func (v Polygon) isPrimitive() bool {
	return true
}

func (v Polygon) WalkValues(ctx context.Context, cb ValueCallback) error {
	return nil
}

func (v Polygon) WalkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (v Polygon) typeOf() (*Type, error) {
	return PrimitiveTypeMap[PolygonKind], nil
}

func (v Polygon) Kind() NomsKind {
	return PolygonKind
}

func (v Polygon) valueReadWriter() ValueReadWriter {
	return nil
}

func (v Polygon) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	err := PolygonKind.writeTo(w, nbf)
	if err != nil {
		return err
	}

	w.writeString(string(v))
	return nil
}

func (v Polygon) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	return Polygon(b.ReadString()), nil
}

func (v Polygon) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	b.skipString()
}

func (v Polygon) HumanReadableString() string {
	return strconv.Quote(string(v))
}
