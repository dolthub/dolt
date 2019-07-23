// Copyright 2019 Liquidata, Inc.
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

	"github.com/google/uuid"

	"github.com/liquidata-inc/ld/dolt/go/store/hash"
)

const (
	uuidNumBytes = 16
)

type UUID uuid.UUID

func (v UUID) Value(ctx context.Context) Value {
	return v
}

func (v UUID) Equals(other Value) bool {
	return v == other
}

func (v UUID) Less(nbf *NomsBinFormat, other LesserValuable) bool {
	if v2, ok := other.(UUID); ok {
		for i := 0; i < uuidNumBytes; i++ {
			b1 := v[i]
			b2 := v2[i]

			if b1 != b2 {
				return b1 < b2
			}
		}

		return false
	}
	return UUIDKind < other.Kind()
}

func (v UUID) Hash(nbf *NomsBinFormat) hash.Hash {
	return getHash(v, nbf)
}

func (v UUID) WalkValues(ctx context.Context, cb ValueCallback) {
}

func (v UUID) WalkRefs(nbf *NomsBinFormat, cb RefCallback) {
}

func (v UUID) typeOf() *Type {
	return UUIDType
}

func (v UUID) Kind() NomsKind {
	return UUIDKind
}

func (v UUID) valueReadWriter() ValueReadWriter {
	return nil
}

func (v UUID) writeTo(w nomsWriter, nbf *NomsBinFormat) {
	id := UUID(v)
	byteSl := id[:]
	UUIDKind.writeTo(w, nbf)
	w.writeBytes(byteSl)
}

func (v UUID) valueBytes(nbf *NomsBinFormat) []byte {
	return v[:]
}

func (v UUID) String() string {
	return uuid.UUID(v).String()
}
