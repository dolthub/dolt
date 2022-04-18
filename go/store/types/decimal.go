// Copyright 2020 Dolthub, Inc.
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

	"github.com/shopspring/decimal"

	"github.com/dolthub/dolt/go/store/hash"
)

type Decimal decimal.Decimal

func (v Decimal) Value(ctx context.Context) (Value, error) {
	return v, nil
}

func (v Decimal) Equals(other Value) bool {
	v2, ok := other.(Decimal)
	if !ok {
		return false
	}

	return decimal.Decimal(v).Equal(decimal.Decimal(v2))
}

func (v Decimal) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	if v2, ok := other.(Decimal); ok {
		return decimal.Decimal(v).LessThan(decimal.Decimal(v2)), nil
	}
	return DecimalKind < other.Kind(), nil
}

func (v Decimal) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(v, nbf)
}

func (v Decimal) isPrimitive() bool {
	return true
}

func (v Decimal) WalkValues(ctx context.Context, cb ValueCallback) error {
	return nil
}

func (v Decimal) walkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (v Decimal) typeOf() (*Type, error) {
	return PrimitiveTypeMap[DecimalKind], nil
}

func (v Decimal) Kind() NomsKind {
	return DecimalKind
}

func (v Decimal) valueReadWriter() ValueReadWriter {
	return nil
}

func (v Decimal) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	encodedDecimal, err := decimal.Decimal(v).GobEncode()
	if err != nil {
		return err
	}

	err = DecimalKind.writeTo(w, nbf)
	if err != nil {
		return err
	}

	w.writeUint16(uint16(len(encodedDecimal)))
	w.writeRaw(encodedDecimal)
	return nil
}

func (v Decimal) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	dec, err := b.ReadDecimal()
	if err != nil {
		return nil, err
	}
	return Decimal(dec), nil
}

func (v Decimal) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	size := uint32(b.readUint16())
	b.skipBytes(size)
}

func (v Decimal) HumanReadableString() string {
	return decimal.Decimal(v).String()
}
