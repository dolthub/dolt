// Copyright 2024 Dolthub, Inc.
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

package types

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"strings"

	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/store/hash"
)

type CustomInline []byte

func (v CustomInline) Value(ctx context.Context) (Value, error) {
	return v, nil
}

func (v CustomInline) Equals(other Value) bool {
	res, _ := v.Compare(other)
	return res == 0
}

func (v CustomInline) Less(ctx context.Context, nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	res, err := v.Compare(other)
	return res == -1, err
}

func (v CustomInline) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(v, nbf)
}

func (v CustomInline) Kind() NomsKind {
	return CustomInlineKind
}

func (v CustomInline) HumanReadableString() string {
	return strings.ToUpper(hex.EncodeToString(v))
}

func (v CustomInline) Compare(other LesserValuable) (int, error) {
	v2, ok := other.(CustomInline)
	if !ok || len(v) == 0 || len(v2) == 0 {
		if CustomInlineKind < other.Kind() {
			return -1, nil
		} else if CustomInlineKind > other.Kind() {
			return 1, nil
		} else {
			return 0, fmt.Errorf("non-custom type uses custom type's kind")
		}
	}
	if v[0] != v2[0] {
		return 0, fmt.Errorf("differing custom types during comparison: `%d` and `%d`", v[0], v2[0])
	}
	c, ok := types.DeserializeCustomType(v[0])
	if !ok {
		return 0, fmt.Errorf("cannot find the associated custom type for `%d`", v[0])
	}
	a, err := c.DeserializeValue(v[1:])
	if err != nil {
		return 0, err
	}
	b, err := c.DeserializeValue(v2[1:])
	if err != nil {
		return 0, err
	}
	return c.Compare(a, b)
}

func (v CustomInline) GetType() (types.Custom, bool) {
	if len(v) == 0 {
		return nil, false
	}
	return types.DeserializeCustomType(v[0])
}

func (v CustomInline) isPrimitive() bool {
	return true
}

func (v CustomInline) walkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (v CustomInline) typeOf() (*Type, error) {
	return PrimitiveTypeMap[CustomInlineKind], nil
}

func (v CustomInline) valueReadWriter() ValueReadWriter {
	return nil
}

func (v CustomInline) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	byteLen := len(v)
	if byteLen > math.MaxUint16 {
		return fmt.Errorf("CustomInline has length %v when max is %v", byteLen, math.MaxUint16)
	}

	err := CustomInlineKind.writeTo(w, nbf)
	if err != nil {
		return err
	}

	w.writeUint16(uint16(byteLen))
	w.writeRaw(v)
	return nil
}

func (v CustomInline) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	bytes := b.ReadInlineBlob()
	return CustomInline(bytes), nil
}

func (v CustomInline) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	size := uint32(b.readUint16())
	b.skipBytes(size)
}
