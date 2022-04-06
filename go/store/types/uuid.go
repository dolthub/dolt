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

package types

import (
	"bytes"
	"context"

	"github.com/google/uuid"

	"github.com/dolthub/dolt/go/store/hash"
)

const (
	uuidNumBytes = 16
)

// UUIDHashedFromValues generates a UUID from the first 16 byes of the hash.Hash
// generated from serialized |vals|.
func UUIDHashedFromValues(nbf *NomsBinFormat, vals ...Value) (UUID, error) {
	w := binaryNomsWriter{make([]byte, 4), 0}
	for _, v := range vals {
		if v == nil || v.Kind() == NullKind {
			continue
		}
		if err := v.writeTo(&w, nbf); err != nil {
			return [16]byte{}, err
		}
	}

	h := hash.Of(w.data())
	id, err := uuid.FromBytes(h[:uuidNumBytes])
	return UUID(id), err
}

type UUID uuid.UUID

func (v UUID) Value(ctx context.Context) (Value, error) {
	return v, nil
}

func (v UUID) Equals(other Value) bool {
	return v == other
}

func (v UUID) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	if v2, ok := other.(UUID); ok {
		return bytes.Compare(v[:], v2[:]) < 0, nil
	}
	return UUIDKind < other.Kind(), nil
}

func (v UUID) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(v, nbf)
}

func (v UUID) isPrimitive() bool {
	return true
}

func (v UUID) WalkValues(ctx context.Context, cb ValueCallback) error {
	return nil
}

func (v UUID) walkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (v UUID) typeOf() (*Type, error) {
	return PrimitiveTypeMap[UUIDKind], nil
}

func (v UUID) Kind() NomsKind {
	return UUIDKind
}

func (v UUID) valueReadWriter() ValueReadWriter {
	return nil
}

func (v UUID) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	id := UUID(v)
	byteSl := id[:]
	err := UUIDKind.writeTo(w, nbf)

	if err != nil {
		return err
	}

	w.writeRaw(byteSl)
	return nil
}

func (v UUID) readFrom(_ *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	id := b.ReadUUID()
	return UUID(id), nil
}

func (v UUID) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	b.skipBytes(uuidNumBytes)
}

func (v UUID) String() string {
	return uuid.UUID(v).String()
}

func (v UUID) HumanReadableString() string {
	return v.String()
}
