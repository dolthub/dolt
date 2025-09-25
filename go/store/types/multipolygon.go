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

// MultiPolygon is a Noms Value wrapper around a string.
type MultiPolygon struct {
	Polygons []Polygon
	SRID     uint32
}

// Value interface
func (v MultiPolygon) Value(ctx context.Context) (Value, error) {
	return v, nil
}

func (v MultiPolygon) Equals(other Value) bool {
	// Compare types
	v2, ok := other.(MultiPolygon)
	if !ok {
		return false
	}
	// Compare SRID
	if v.SRID != v2.SRID {
		return false
	}
	// Compare lengths of polygons
	if len(v.Polygons) != len(v2.Polygons) {
		return false
	}
	// Compare each polygon
	for i := 0; i < len(v.Polygons); i++ {
		if !v.Polygons[i].Equals(v2.Polygons[i]) {
			return false
		}
	}
	return true
}

func (v MultiPolygon) Less(ctx context.Context, nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	// Compare types
	v2, ok := other.(MultiPolygon)
	if !ok {
		return MultiPolygonKind < other.Kind(), nil
	}
	// Compare SRID
	if v.SRID != v2.SRID {
		return v.SRID < v2.SRID, nil
	}
	// Get shorter length
	var n int
	len1 := len(v.Polygons)
	len2 := len(v2.Polygons)
	if len1 < len2 {
		n = len1
	} else {
		n = len2
	}
	// Compare each polygon until there is one that is less
	for i := 0; i < n; i++ {
		if !v.Polygons[i].Equals(v2.Polygons[i]) {
			return v.Polygons[i].Less(ctx, nbf, v2.Polygons[i])
		}
	}
	// Determine based off length
	return len1 < len2, nil
}

func (v MultiPolygon) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(v, nbf)
}

func (v MultiPolygon) isPrimitive() bool {
	return true
}

func (v MultiPolygon) walkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (v MultiPolygon) typeOf() (*Type, error) {
	return PrimitiveTypeMap[MultiPolygonKind], nil
}

func (v MultiPolygon) Kind() NomsKind {
	return MultiPolygonKind
}

func (v MultiPolygon) valueReadWriter() ValueReadWriter {
	return nil
}

func (v MultiPolygon) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	err := MultiPolygonKind.writeTo(w, nbf)
	if err != nil {
		return err
	}

	w.writeString(string(SerializeMultiPolygon(v)))
	return nil
}

func readMultiPolygon(nbf *NomsBinFormat, b *valueDecoder) (MultiPolygon, error) {
	buf := []byte(b.ReadString())
	srid, _, geomType, err := DeserializeEWKBHeader(buf)
	if err != nil {
		return MultiPolygon{}, err
	}
	if geomType != WKBMultiPolyID {
		return MultiPolygon{}, errors.New("not a multipolygon")
	}
	buf = buf[EWKBHeaderSize:]
	return DeserializeTypesMPoly(buf, false, srid), nil
}

func (v MultiPolygon) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	buf := []byte(b.ReadString())
	srid, _, geomType, err := DeserializeEWKBHeader(buf)
	if err != nil {
		return MultiPolygon{}, err
	}
	if geomType != WKBMultiPolyID {
		return MultiPolygon{}, errors.New("not a multipolygon")
	}
	buf = buf[EWKBHeaderSize:]
	return DeserializeTypesMPoly(buf, false, srid), nil
}

func (v MultiPolygon) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	b.skipString()
}

func (v MultiPolygon) HumanReadableString() string {
	polys := make([]string, len(v.Polygons))
	for i, l := range v.Polygons {
		polys[i] = l.HumanReadableString()
	}
	s := fmt.Sprintf("SRID: %d MULTIPOLYGON(%s)", v.SRID, strings.Join(polys, ","))
	return strconv.Quote(s)
}
