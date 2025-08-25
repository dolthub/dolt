// Copyright 2021 Dolthub, Inc.
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

const (
	LengthSize = 4
)

// LineString is a Noms Value wrapper around a string.
type LineString struct {
	Points []Point
	SRID   uint32
}

// Value interface
func (v LineString) Value(ctx context.Context) (Value, error) {
	return v, nil
}

func (v LineString) Equals(other Value) bool {
	// Compare types
	v2, ok := other.(LineString)
	if !ok {
		return false
	}
	// Compare SRID
	if v.SRID != v2.SRID {
		return false
	}
	// Compare lengths of points
	if len(v.Points) != len(v2.Points) {
		return false
	}
	// Compare each point
	for i := 0; i < len(v.Points); i++ {
		if !v.Points[i].Equals(v2.Points[i]) {
			return false
		}
	}
	return true
}

func (v LineString) Less(ctx context.Context, nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	// Compare types
	v2, ok := other.(LineString)
	if !ok {
		return LineStringKind < other.Kind(), nil
	}
	// TODO: should I even take this into account?
	// Compare SRID
	if v.SRID != v2.SRID {
		return v.SRID < v2.SRID, nil
	}
	// Get shorter length
	var n int
	len1 := len(v.Points)
	len2 := len(v2.Points)
	if len1 < len2 {
		n = len1
	} else {
		n = len2
	}

	// Compare each point until there's one that is less than
	for i := 0; i < n; i++ {
		if !v.Points[i].Equals(v2.Points[i]) {
			return v.Points[i].Less(ctx, nbf, v2.Points[i])
		}
	}

	// Determine based off length
	return len1 < len2, nil
}

func (v LineString) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(v, nbf)
}

func (v LineString) isPrimitive() bool {
	return true
}

func (v LineString) walkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (v LineString) typeOf() (*Type, error) {
	return PrimitiveTypeMap[LineStringKind], nil
}

func (v LineString) Kind() NomsKind {
	return LineStringKind
}

func (v LineString) valueReadWriter() ValueReadWriter {
	return nil
}

func (v LineString) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	err := LineStringKind.writeTo(w, nbf)
	if err != nil {
		return err
	}

	buf := SerializeLineString(v)
	w.writeString(string(buf))
	return nil
}

func readLineString(nbf *NomsBinFormat, b *valueDecoder) (LineString, error) {
	buf := []byte(b.ReadString())
	srid, _, geomType, err := DeserializeEWKBHeader(buf)
	if err != nil {
		return LineString{}, err
	}
	if geomType != WKBLineID {
		return LineString{}, errors.New("not a linestring")
	}
	buf = buf[EWKBHeaderSize:]
	return DeserializeTypesLine(buf, false, srid), nil
}

func (v LineString) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	buf := []byte(b.ReadString())
	srid, _, geomType, err := DeserializeEWKBHeader(buf)
	if err != nil {
		return LineString{}, err
	}
	if geomType != WKBLineID {
		return LineString{}, errors.New("not a linestring")
	}
	buf = buf[EWKBHeaderSize:]
	return DeserializeTypesLine(buf, false, srid), nil
}

func (v LineString) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	b.skipString()
}

func (v LineString) HumanReadableString() string {
	points := make([]string, len(v.Points))
	for i, p := range v.Points {
		points[i] = p.HumanReadableString()
	}
	s := fmt.Sprintf("SRID: %d LINESTRING(%s)", v.SRID, strings.Join(points, ","))
	return strconv.Quote(s)
}
