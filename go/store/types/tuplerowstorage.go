// Copyright 2022 Dolthub, Inc.
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
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"strings"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/message"
)

// TupleRowStorage is a clone of InlineBlob. It only exists to be able to easily differentiate these two very different
// use cases during the migration from the old storage format to the new one.
type TupleRowStorage []byte

func (v TupleRowStorage) Value(ctx context.Context) (Value, error) {
	return v, nil
}

func (v TupleRowStorage) Equals(other Value) bool {
	v2, ok := other.(TupleRowStorage)
	if !ok {
		return false
	}

	return bytes.Equal(v, v2)
}

func (v TupleRowStorage) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	if v2, ok := other.(TupleRowStorage); ok {
		return bytes.Compare(v, v2) == -1, nil
	}
	return TupleRowStorageKind < other.Kind(), nil
}

func (v TupleRowStorage) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(v, nbf)
}

func (v TupleRowStorage) isPrimitive() bool {
	return true
}

func (v TupleRowStorage) walkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return message.WalkAddresses(context.TODO(), message.Message([]byte(v)), func(ctx context.Context, addr hash.Hash) error {
		r, err := constructRef(nbf, addr, PrimitiveTypeMap[ValueKind], SerialMessageRefHeight)
		if err != nil {
			return err
		}
		return cb(r)
	})
}

func (v TupleRowStorage) typeOf() (*Type, error) {
	return PrimitiveTypeMap[TupleRowStorageKind], nil
}

func (v TupleRowStorage) Kind() NomsKind {
	return TupleRowStorageKind
}

func (v TupleRowStorage) valueReadWriter() ValueReadWriter {
	return nil
}

func (v TupleRowStorage) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	byteLen := len(v)
	if byteLen > math.MaxUint16 {
		return fmt.Errorf("TupleRowStorage has length %v when max is %v", byteLen, math.MaxUint16)
	}

	err := TupleRowStorageKind.writeTo(w, nbf)
	if err != nil {
		return err
	}

	w.writeUint16(uint16(byteLen))
	w.writeRaw(v)
	return nil
}

func (v TupleRowStorage) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	bytes := b.ReadInlineBlob()
	return TupleRowStorage(bytes), nil
}

func (v TupleRowStorage) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	size := uint32(b.readUint16())
	b.skipBytes(size)
}

func (v TupleRowStorage) HumanReadableString() string {
	return strings.ToUpper(hex.EncodeToString(v))
}
