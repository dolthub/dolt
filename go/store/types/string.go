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
	"errors"
	"fmt"
	"strconv"

	"github.com/dolthub/dolt/go/store/hash"
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

func (s String) isPrimitive() bool {
	return true
}

func (s String) WalkValues(ctx context.Context, cb ValueCallback) error {
	return nil
}

func (s String) walkRefs(nbf *NomsBinFormat, cb RefCallback) error {
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
	return String(b.ReadString()), nil
}

func (s String) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	b.skipString()
}

func parseNumber(s String) (isNegative bool, decPos int, err error) {
	decPos = -1
	for i, c := range s {
		if i == 0 && c == '-' {
			isNegative = true
		} else if c == '.' {
			if decPos != -1 {
				return false, -1, errors.New("not a valid number.  multiple decimal points found.")
			}

			decPos = i
		} else if c > '9' || c < '0' {
			return false, -1, fmt.Errorf("for the string '%s' found invalid character '%s' at pos %d", s, string(c), i)
		}
	}

	return isNegative, decPos, nil
}

func (s String) HumanReadableString() string {
	return strconv.Quote(string(s))
}
