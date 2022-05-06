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
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/dolthub/dolt/go/store/geometry"

	"github.com/dolthub/dolt/go/store/hash"
)

// Polygon is a Noms Value wrapper around a string.
type Polygon struct {
	SRID  uint32
	Lines []Linestring
}

// Value interface
func (v Polygon) Value(ctx context.Context) (Value, error) {
	return v, nil
}

func (v Polygon) Equals(other Value) bool {
	// Compare types
	v2, ok := other.(Polygon)
	if !ok {
		return false
	}
	// Compare SRID
	if v.SRID != v2.SRID {
		return false
	}
	// Compare lengths of lines
	if len(v.Lines) != len(v2.Lines) {
		return false
	}
	// Compare each line
	for i := 0; i < len(v.Lines); i++ {
		if !v.Lines[i].Equals(v2.Lines[i]) {
			return false
		}
	}
	return true
}

func (v Polygon) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	// Compare types
	v2, ok := other.(Polygon)
	if !ok {
		return PolygonKind < other.Kind(), nil
	}
	// Compare SRID
	if v.SRID != v2.SRID {
		return v.SRID < v2.SRID, nil
	}
	// Get shorter length
	var n int
	len1 := len(v.Lines)
	len2 := len(v2.Lines)
	if len1 < len2 {
		n = len1
	} else {
		n = len2
	}
	// Compare each point until there is one that is less
	for i := 0; i < n; i++ {
		if !v.Lines[i].Equals(v2.Lines[i]) {
			return v.Lines[i].Less(nbf, v2.Lines[i])
		}
	}
	// Determine based off length
	return len1 < len2, nil
}

func (v Polygon) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(v, nbf)
}

func (v Polygon) isPrimitive() bool {
	return true
}

func (v Polygon) walkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (v Polygon) typeOf() (*Type, error) {
	return PrimitiveTypeMap[PolygonKind], nil
}

func (v Polygon) Kind() NomsKind {
	return PolygonKind
}

func (v Polygon) valueReadWriter() ValueReadWriter {
	return nil
}

// WriteEWKBPolyData converts a Polygon into a byte array in EWKB format
func WriteEWKBPolyData(p Polygon, buf []byte) {
	// Write length of polygon
	binary.LittleEndian.PutUint32(buf[:LengthSize], uint32(len(p.Lines)))
	// Write each line
	start, stop := 0, LengthSize
	for _, l := range p.Lines {
		start, stop = stop, stop+LengthSize+geometry.PointSize*len(l.Points)
		WriteEWKBLineData(l, buf[start:stop])
	}
}

func (v Polygon) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	err := PolygonKind.writeTo(w, nbf)
	if err != nil {
		return err
	}

	// Calculate space for polygon buffer
	size := 0
	for _, l := range v.Lines {
		size += LengthSize + geometry.PointSize*len(l.Points)
	}

	// Allocate buffer for poly
	buf := make([]byte, geometry.EWKBHeaderSize+LengthSize+size)

	// Write header and data to buffer
	WriteEWKBHeader(v, buf)
	WriteEWKBPolyData(v, buf[geometry.EWKBHeaderSize:])

	w.writeString(string(buf))
	return nil
}

// ParseEWKBPoly converts the data portions of a WKB polygon to Polygon
// Very similar logic to the function in GMS
func ParseEWKBPoly(buf []byte, srid uint32) Polygon {
	// Read length of Polygon
	numLines := binary.LittleEndian.Uint32(buf[:LengthSize])

	// Parse lines
	s := LengthSize
	lines := make([]Linestring, numLines)
	for i := uint32(0); i < numLines; i++ {
		lines[i] = ParseEWKBLine(buf[s:], srid)
		s += LengthSize * geometry.PointSize * len(lines[i].Points)
	}

	return Polygon{SRID: srid, Lines: lines}
}

func readPolygon(nbf *NomsBinFormat, b *valueDecoder) (Polygon, error) {
	buf := []byte(b.ReadString())
	srid, _, geomType := geometry.ParseEWKBHeader(buf)
	if geomType != geometry.PolygonType {
		return Polygon{}, errors.New("not a polygon")
	}
	return ParseEWKBPoly(buf[geometry.EWKBHeaderSize:], srid), nil
}

func (v Polygon) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	buf := []byte(b.ReadString())
	srid, _, geomType := geometry.ParseEWKBHeader(buf)
	if geomType != geometry.PolygonType {
		return nil, errors.New("not a polygon")
	}
	return ParseEWKBPoly(buf[geometry.EWKBHeaderSize:], srid), nil
}

func (v Polygon) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	b.skipString()
}

func (v Polygon) HumanReadableString() string {
	lines := make([]string, len(v.Lines))
	for i, l := range v.Lines {
		lines[i] = l.HumanReadableString()
	}
	s := fmt.Sprintf("SRID: %d POLYGON(%s)", v.SRID, strings.Join(lines, ","))
	return strconv.Quote(s)
}
