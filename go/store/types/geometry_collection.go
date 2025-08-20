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

// GeomColl is a Noms Value wrapper around a string.
type GeomColl struct {
	Geometries []Value
	SRID       uint32
}

// Value interface
func (v GeomColl) Value(ctx context.Context) (Value, error) {
	return v, nil
}

func (v GeomColl) Equals(other Value) bool {
	// Compare types
	v2, ok := other.(GeomColl)
	if !ok {
		return false
	}
	// Compare SRID
	if v.SRID != v2.SRID {
		return false
	}
	// Compare lengths of geometries
	if len(v.Geometries) != len(v2.Geometries) {
		return false
	}
	// Compare each geometry
	for i := 0; i < len(v.Geometries); i++ {
		if !v.Geometries[i].Equals(v2.Geometries[i]) {
			return false
		}
	}
	return true
}

func (v GeomColl) Less(ctx context.Context, nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	// Compare types
	v2, ok := other.(GeomColl)
	if !ok {
		return GeometryCollectionKind < other.Kind(), nil
	}
	// Compare SRID
	if v.SRID != v2.SRID {
		return v.SRID < v2.SRID, nil
	}
	// Get shorter length
	var n int
	len1 := len(v.Geometries)
	len2 := len(v2.Geometries)
	if len1 < len2 {
		n = len1
	} else {
		n = len2
	}
	// Compare each polygon until there is one that is less
	for i := 0; i < n; i++ {
		if !v.Geometries[i].Equals(v2.Geometries[i]) {
			return v.Geometries[i].Less(ctx, nbf, v2.Geometries[i])
		}
	}
	// Determine based off length
	return len1 < len2, nil
}

func (v GeomColl) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(v, nbf)
}

func (v GeomColl) isPrimitive() bool {
	return true
}

func (v GeomColl) walkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (v GeomColl) typeOf() (*Type, error) {
	return PrimitiveTypeMap[GeometryCollectionKind], nil
}

func (v GeomColl) Kind() NomsKind {
	return GeometryCollectionKind
}

func (v GeomColl) valueReadWriter() ValueReadWriter {
	return nil
}

func (v GeomColl) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	err := GeometryCollectionKind.writeTo(w, nbf)
	if err != nil {
		return err
	}

	w.writeString(string(SerializeGeomColl(v)))
	return nil
}

func readGeomColl(nbf *NomsBinFormat, b *valueDecoder) (GeomColl, error) {
	buf := []byte(b.ReadString())
	srid, _, geomType, err := DeserializeEWKBHeader(buf)
	if err != nil {
		return GeomColl{}, err
	}
	if geomType != WKBGeomCollID {
		return GeomColl{}, errors.New("not a geometry collection")
	}
	buf = buf[EWKBHeaderSize:]
	return DeserializeTypesGeomColl(buf, false, srid), nil
}

func (v GeomColl) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	buf := []byte(b.ReadString())
	srid, _, geomType, err := DeserializeEWKBHeader(buf)
	if err != nil {
		return GeomColl{}, err
	}
	if geomType != WKBGeomCollID {
		return GeomColl{}, errors.New("not a geometry collection")
	}
	buf = buf[EWKBHeaderSize:]
	return DeserializeTypesGeomColl(buf, false, srid), nil
}

func (v GeomColl) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	b.skipString()
}

func (v GeomColl) HumanReadableString() string {
	geoms := make([]string, len(v.Geometries))
	for i, l := range v.Geometries {
		geoms[i] = l.HumanReadableString()
	}
	s := fmt.Sprintf("SRID: %d GEOMETRIES(%s)", v.SRID, strings.Join(geoms, ","))
	return strconv.Quote(s)
}
