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
	"encoding/binary"
	"fmt"
	"math"

	"github.com/liquidata-inc/dolt/go/store/hash"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dolttypes"
)

type UnderlyingArray []byte

func (v UnderlyingArray) Value(ctx context.Context) (Value, error) {
	return v, nil
}

func (v UnderlyingArray) Equals(other Value) bool {
	if v2, ok := other.(UnderlyingArray); ok {
		vlen := len(v)
		if vlen == len(v2) {
			for i := 0; i < vlen; i++ {
				if v[i] != v2[i] {
					return false
				}
			}
			return true
		}
	}
	return false
}

func (v UnderlyingArray) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	if v2, ok := other.(UnderlyingArray); ok {
		vlen := len(v)
		v2len := len(v2)
		if vlen == v2len {
			for i := 0; i < vlen; i++ {
				b1 := v[i]
				b2 := v2[i]

				if b1 != b2 {
					return b1 < b2, nil
				}
			}
			return false, nil
		}
		return vlen < v2len, nil
	}
	return UnderlyingArrayKind < other.Kind(), nil
}

func (v UnderlyingArray) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(v, nbf)
}

func (v UnderlyingArray) WalkValues(ctx context.Context, cb ValueCallback) error {
	return nil
}

func (v UnderlyingArray) WalkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (v UnderlyingArray) typeOf() (*Type, error) {
	return UnderlyingArrayType, nil
}

func (v UnderlyingArray) Kind() NomsKind {
	return UnderlyingArrayKind
}

func (v UnderlyingArray) valueReadWriter() ValueReadWriter {
	return nil
}

func (v UnderlyingArray) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	byteLen := len(v)
	if byteLen > math.MaxUint16 {
		return fmt.Errorf("UnderlyingArray has length %v when max is %v", byteLen, math.MaxUint16)
	}
	byteLenSl := make([]byte, 2)
	binary.BigEndian.PutUint16(byteLenSl, uint16(byteLen))

	err := UnderlyingArrayKind.writeTo(w, nbf)
	if err != nil {
		return err
	}

	w.writeBytes(byteLenSl)
	w.writeBytes(v)
	return nil
}

func (v UnderlyingArray) valueBytes(nbf *NomsBinFormat) ([]byte, error) {
	byteLen := len(v)
	if byteLen > math.MaxUint16 {
		return nil, fmt.Errorf("UnderlyingArray has length %v when max is %v", byteLen, math.MaxUint16)
	}
	byteLenSl := make([]byte, 2)
	binary.BigEndian.PutUint16(byteLenSl, uint16(byteLen))
	return append(byteLenSl, v...), nil
}

func (v UnderlyingArray) String() string {
	// This returns Unknown on errors, so we can ignore for a String call
	dt, _ := dolttypes.DecodeDoltType(v)
	return dt.String()
}

func (v UnderlyingArray) DoltKind() dolttypes.DoltKind {
	if len(v) > 0 {
		doltKind := dolttypes.DoltKind(v[0])
		if _, ok := dolttypes.KindName[doltKind]; ok {
			return doltKind
		}
	}
	return dolttypes.UnknownKind
}
