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
	"context"
	"time"

	"github.com/dolthub/dolt/go/store/hash"
)

const (
	timestampNumBytes = 15
	timestampFormat   = "2006-01-02 15:04:05.999999"
)

type Timestamp time.Time

func (v Timestamp) Value(ctx context.Context) (Value, error) {
	return v, nil
}

func (v Timestamp) Equals(other Value) bool {
	v2, ok := other.(Timestamp)
	if !ok {
		return false
	}

	return time.Time(v).Equal(time.Time(v2))
}

func (v Timestamp) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	if v2, ok := other.(Timestamp); ok {
		return time.Time(v).Before(time.Time(v2)), nil
	}
	return TimestampKind < other.Kind(), nil
}

func (v Timestamp) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(v, nbf)
}

func (v Timestamp) isPrimitive() bool {
	return true
}

func (v Timestamp) walkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (v Timestamp) typeOf() (*Type, error) {
	return PrimitiveTypeMap[TimestampKind], nil
}

func (v Timestamp) Kind() NomsKind {
	return TimestampKind
}

func (v Timestamp) valueReadWriter() ValueReadWriter {
	return nil
}

func (v Timestamp) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	data, err := time.Time(v).MarshalBinary()
	if err != nil {
		return err
	}

	err = TimestampKind.writeTo(w, nbf)
	if err != nil {
		return err
	}

	w.writeRaw(data)
	return nil
}

func (v Timestamp) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	t, err := b.ReadTimestamp()
	if err != nil {
		return nil, err
	}
	return Timestamp(t), nil

}

func (v Timestamp) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	b.skipBytes(timestampNumBytes)
}

func (v Timestamp) String() string {
	return time.Time(v).UTC().Format(timestampFormat)
}

func (v Timestamp) HumanReadableString() string {
	return v.String()
}
