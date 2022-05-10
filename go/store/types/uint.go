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

func (v Uint) isPrimitive() bool {
	return true
}

func (v Uint) walkRefs(nbf *NomsBinFormat, cb RefCallback) error {
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
	return Uint(b.ReadUint()), nil
}

func (v Uint) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	b.skipUint()
}

func (v Uint) HumanReadableString() string {
	return strconv.FormatUint(uint64(v), 10)
}
