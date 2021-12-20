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
	"strings"

	"github.com/dolthub/dolt/go/store/hash"
)

// Polygon is a Noms Value wrapper around an array of Point.
type Polygon []Linestring

// Value interface
func (v Polygon) Value(ctx context.Context) (Value, error) {
	return v, nil
}

func (v Polygon) Equals(other Value) bool {
	// Cast other to LineString
	_other, ok := other.(Polygon)
	if !ok {
		return false
	}
	// Check that they have same length
	if len(v) != len(_other) {
		return false
	}

	// Check that every point is equal
	for i := 0; i < len(v); i++ {
		if !v[i].Equals(_other[i]) {
			return false
		}
	}
	return true
}

func (v Polygon) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	if v2, ok := other.(Polygon); ok {
		return v[0].Less(nbf, v2[0])
	}
	return PolygonKind < other.Kind(), nil
}

func (v Polygon) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(v, nbf)
}

func (v Polygon) isPrimitive() bool {
	return false
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

	// TODO: might have to combine and comma separate
	for _, l := range v {
		l.writeTo(w, nbf)
	}

	return nil
}

func (v Polygon) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	// TODO: convert b.ReadString to []Point somehow
	//return Linestring(b.ReadString()), nil
	return Polygon([]Linestring{}), nil
}

func (v Polygon) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	b.skipString()
}

func (v Polygon) HumanReadableString() string {
	var res []string
	for _, l := range v {
		res = append(res, l.HumanReadableString())
	}
	return strconv.Quote(strings.Join(res, ","))
}
