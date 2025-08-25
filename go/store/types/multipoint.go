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
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/dolthub/dolt/go/store/hash"
)

// MultiPoint is a Noms Value wrapper around a string.
type MultiPoint struct {
	Points []Point
	SRID   uint32
}

// Value interface
func (v MultiPoint) Value(ctx context.Context) (Value, error) {
	return v, nil
}

func (v MultiPoint) Equals(other Value) bool {
	v2, ok := other.(MultiPoint)
	if !ok {
		return false
	}
	if v.SRID != v2.SRID {
		return false
	}
	if len(v.Points) != len(v2.Points) {
		return false
	}
	for i := 0; i < len(v.Points); i++ {
		if !v.Points[i].Equals(v2.Points[i]) {
			return false
		}
	}
	return true
}

func (v MultiPoint) Less(ctx context.Context, nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	v2, ok := other.(MultiPoint)
	if !ok {
		return MultiPointKind < other.Kind(), nil
	}
	if v.SRID != v2.SRID {
		return v.SRID < v2.SRID, nil
	}
	var n int
	len1 := len(v.Points)
	len2 := len(v2.Points)
	if len1 < len2 {
		n = len1
	} else {
		n = len2
	}

	for i := 0; i < n; i++ {
		if !v.Points[i].Equals(v2.Points[i]) {
			return v.Points[i].Less(ctx, nbf, v2.Points[i])
		}
	}

	return len1 < len2, nil
}

func (v MultiPoint) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(v, nbf)
}

func (v MultiPoint) isPrimitive() bool {
	return true
}

func (v MultiPoint) walkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (v MultiPoint) typeOf() (*Type, error) {
	return PrimitiveTypeMap[MultiPointKind], nil
}

func (v MultiPoint) Kind() NomsKind {
	return MultiPointKind
}

func (v MultiPoint) valueReadWriter() ValueReadWriter {
	return nil
}

func (v MultiPoint) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	err := MultiPointKind.writeTo(w, nbf)
	if err != nil {
		return err
	}
	w.writeString(string(SerializeMultiPoint(v)))
	return nil
}

func readMultiPoint(nbf *NomsBinFormat, b *valueDecoder) (MultiPoint, error) {
	buf := []byte(b.ReadString())
	srid, _, geomType, err := DeserializeEWKBHeader(buf)
	if err != nil {
		return MultiPoint{}, nil
	}
	if geomType != WKBMultiPointID {
		return MultiPoint{}, errors.New("not a multipoint")
	}
	buf = buf[EWKBHeaderSize:]
	return DeserializeTypesMPoint(buf, false, srid), nil
}

func (v MultiPoint) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	buf := []byte(b.ReadString())
	srid, _, geomType, err := DeserializeEWKBHeader(buf)
	if err != nil {
		return MultiPoint{}, nil
	}
	if geomType != WKBMultiPointID {
		return MultiPoint{}, errors.New("not a multipoint")
	}
	return DeserializeTypesMPoint(buf, false, srid), nil
}

func (v MultiPoint) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	b.skipString()
}

func (v MultiPoint) HumanReadableString() string {
	points := make([]string, len(v.Points))
	for i, p := range v.Points {
		points[i] = p.HumanReadableString()
	}
	s := fmt.Sprintf("SRID: %d MULTIPOINT(%s)", v.SRID, strings.Join(points, ","))
	return strconv.Quote(s)
}
