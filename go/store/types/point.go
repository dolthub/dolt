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

// Point is a Noms Value wrapper around the primitive string type (for now).
//TODO: type Point sql.PointValue
type Point string

// Value interface
func (v Point) Value(ctx context.Context) (Value, error) {
	return v, nil
}

func (v Point) Equals(other Value) bool {
	return v == other
}

func (v Point) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	if v2, ok := other.(Point); ok {
		return v < v2, nil
	}
	return GeometryKind < other.Kind(), nil
}

func (v Point) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(v, nbf)
}

func (v Point) isPrimitive() bool {
	return true
}

func (v Point) WalkValues(ctx context.Context, cb ValueCallback) error {
	return nil
}

func (v Point) WalkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (v Point) typeOf() (*Type, error) {
	return PrimitiveTypeMap[GeometryKind], nil
}

func (v Point) Kind() NomsKind {
	return GeometryKind
}

func (v Point) valueReadWriter() ValueReadWriter {
	return nil
}

func (v Point) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	err := GeometryKind.writeTo(w, nbf)

	if err != nil {
		return err
	}

	w.writeString(string(v))
	return nil
}

func (v Point) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	return Point(b.ReadString()), nil
}

func (v Point) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	b.skipString()
}

func (v Point) HumanReadableString() string {
	return strconv.Quote(string(v))
}
