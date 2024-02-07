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
	"errors"

	"github.com/dolthub/dolt/go/store/hash"
)

type Extended []byte

func (v Extended) Value(ctx context.Context) (Value, error) {
	return v, errors.New("extended is invalid in the old format")
}

func (v Extended) Equals(other Value) bool {
	return true
}

func (v Extended) Less(ctx context.Context, nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	return false, errors.New("extended is invalid in the old format")
}

func (v Extended) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return hash.Hash{}, errors.New("extended is invalid in the old format")
}

func (v Extended) Kind() NomsKind {
	return ExtendedKind
}

func (v Extended) HumanReadableString() string {
	return "INVALID"
}

func (v Extended) Compare(other LesserValuable) (int, error) {
	return 0, errors.New("extended is invalid in the old format")
}

func (v Extended) isPrimitive() bool {
	return true
}

func (v Extended) walkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return errors.New("extended is invalid in the old format")
}

func (v Extended) typeOf() (*Type, error) {
	return PrimitiveTypeMap[ExtendedKind], nil
}

func (v Extended) valueReadWriter() ValueReadWriter {
	return nil
}

func (v Extended) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	return errors.New("extended is invalid in the old format")
}

func (v Extended) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	return Extended{}, errors.New("extended is invalid in the old format")
}

func (v Extended) skip(nbf *NomsBinFormat, b *binaryNomsReader) {}
