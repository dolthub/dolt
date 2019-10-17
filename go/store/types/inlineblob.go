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
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"math"

	"github.com/liquidata-inc/dolt/go/store/hash"
)

type InlineBlob []byte

func (v InlineBlob) Value(ctx context.Context) (Value, error) {
	return v, nil
}

func (v InlineBlob) Equals(other Value) bool {
	v2, ok := other.(InlineBlob)
	if !ok {
		return false
	}

	return bytes.Equal(v, v2)
}

func (v InlineBlob) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	if v2, ok := other.(InlineBlob); ok {
		return bytes.Compare(v, v2) == -1, nil
	}
	return InlineBlobKind < other.Kind(), nil
}

func (v InlineBlob) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(v, nbf)
}

func (v InlineBlob) WalkValues(ctx context.Context, cb ValueCallback) error {
	return nil
}

func (v InlineBlob) WalkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (v InlineBlob) typeOf() (*Type, error) {
	return InlineBlobType, nil
}

func (v InlineBlob) Kind() NomsKind {
	return InlineBlobKind
}

func (v InlineBlob) valueReadWriter() ValueReadWriter {
	return nil
}

func (v InlineBlob) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	byteLen := len(v)
	if byteLen > math.MaxUint16 {
		return fmt.Errorf("InlineBlob has length %v when max is %v", byteLen, math.MaxUint16)
	}

	err := InlineBlobKind.writeTo(w, nbf)
	if err != nil {
		return err
	}

	w.writeUint16(uint16(byteLen))
	w.writeBytes(v)
	return nil
}

func (v InlineBlob) valueBytes(nbf *NomsBinFormat) ([]byte, error) {
	// Length is uint8(InlineBlobKind) + uint16(length_prefix) + data
	buff := make([]byte, 3+len(v))
	w := binaryNomsWriter{buff, 0}
	err := v.writeTo(&w, nbf)

	if err != nil {
		return nil, err
	}

	return buff[:w.offset], err
}

func (v InlineBlob) String() string {
	return base64.RawURLEncoding.EncodeToString(v)
}
