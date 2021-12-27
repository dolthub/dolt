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
	"strconv"
)

// Linestring is a Noms Value wrapper around a string.
type Linestring string

// Value interface
func (v Linestring) Value(ctx context.Context) (Value, error) {
	return v, nil
}

func (v Linestring) Equals(other Value) bool {
	return v == other
}

func (v Linestring) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	if v2, ok := other.(Linestring); ok {
		return v < v2, nil
	}
	return LinestringKind < other.Kind(), nil
}

func (v Linestring) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(v, nbf)
}

func (v Linestring) isPrimitive() bool {
	return true
}

func (v Linestring) WalkValues(ctx context.Context, cb ValueCallback) error {
	return nil
}

func (v Linestring) WalkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (v Linestring) typeOf() (*Type, error) {
	return PrimitiveTypeMap[LinestringKind], nil
}

func (v Linestring) Kind() NomsKind {
	return LinestringKind
}

func (v Linestring) valueReadWriter() ValueReadWriter {
	return nil
}

func (v Linestring) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	err := LinestringKind.writeTo(w, nbf)
	if err != nil {
		return err
	}

	w.writeString(string(v))
	return nil
}

func (v Linestring) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	return Point(b.ReadString()), nil
}

func (v Linestring) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	b.skipString()
}

func (v Linestring) HumanReadableString() string {
	return strconv.Quote(string(v))
}
