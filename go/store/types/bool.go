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

// Bool is a Noms Value wrapper around the primitive bool type.
type Bool bool

// Value interface
func (b Bool) Value(ctx context.Context) (Value, error) {
	return b, nil
}

func (b Bool) Equals(other Value) bool {
	return b == other
}

func (b Bool) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	if b2, ok := other.(Bool); ok {
		return !bool(b) && bool(b2), nil
	}
	return true, nil
}

func (b Bool) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(b, nbf)
}

func (b Bool) isPrimitive() bool {
	return true
}

func (b Bool) walkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (b Bool) typeOf() (*Type, error) {
	return PrimitiveTypeMap[BoolKind], nil
}

func (b Bool) Kind() NomsKind {
	return BoolKind
}

func (b Bool) valueReadWriter() ValueReadWriter {
	return nil
}

func (b Bool) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	err := BoolKind.writeTo(w, nbf)

	if err != nil {
		return err
	}

	w.writeBool(bool(b))

	return nil
}

func (b Bool) readFrom(nbf *NomsBinFormat, bnr *binaryNomsReader) (Value, error) {
	return Bool(bnr.ReadBool()), nil
}

func (b Bool) skip(nbf *NomsBinFormat, bnr *binaryNomsReader) {
	bnr.skipUint8()
}

func (b Bool) HumanReadableString() string {
	return strconv.FormatBool(bool(b))
}
