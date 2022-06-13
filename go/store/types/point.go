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

	"github.com/dolthub/dolt/go/store/geometry"
	"github.com/dolthub/dolt/go/store/hash"
)

// Point is a Noms Value wrapper around the primitive string type (for now).
type Point struct {
	SRID uint32
	X    float64
	Y    float64
}

// Value interface
func (v Point) Value(ctx context.Context) (Value, error) {
	return v, nil
}

func (v Point) Equals(other Value) bool {
	if v2, ok := other.(Point); ok {
		return v.SRID == v2.SRID && v.X == v2.X && v.Y == v2.Y
	}
	return false
}

func (v Point) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	if v2, ok := other.(Point); ok {
		return v.SRID < v2.SRID || v.X < v2.X || v.Y < v2.Y, nil
	}
	return PointKind < other.Kind(), nil
}

func (v Point) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(v, nbf)
}

func (v Point) isPrimitive() bool {
	return true
}

func (v Point) walkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (v Point) typeOf() (*Type, error) {
	return PrimitiveTypeMap[PointKind], nil
}

func (v Point) Kind() NomsKind {
	return PointKind
}

func (v Point) valueReadWriter() ValueReadWriter {
	return nil
}

// WriteEWKBHeader writes the SRID, endianness, and type to the byte buffer
// This function assumes v is a valid spatial type
func WriteEWKBHeader(v interface{}, buf []byte) {
	switch v := v.(type) {
	case Point:
		// Write SRID and type
		geometry.WriteEWKBHeader(buf, v.SRID, geometry.PointType)
	case LineString:
		geometry.WriteEWKBHeader(buf, v.SRID, geometry.LineStringType)
	case Polygon:
		geometry.WriteEWKBHeader(buf, v.SRID, geometry.PolygonType)
	}
}

// WriteEWKBPointData converts a Point into a byte array in EWKB format
// Very similar to function in GMS
func WriteEWKBPointData(p Point, buf []byte) {
	geometry.WriteEWKBPointData(buf, p.X, p.Y)
}

func (v Point) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	// Mark as PointKind
	err := PointKind.writeTo(w, nbf)
	if err != nil {
		return err
	}

	// Allocate buffer for point 4 + 1 + 4 + 16
	buf := make([]byte, geometry.EWKBHeaderSize+geometry.PointSize)

	// Write header and data to buffer
	WriteEWKBHeader(v, buf)
	WriteEWKBPointData(v, buf[geometry.EWKBHeaderSize:])

	w.writeString(string(buf))
	return nil
}

// ParseEWKBPoint converts the data portion of a WKB point to Point
// Very similar logic to the function in GMS
func ParseEWKBPoint(buf []byte, srid uint32) Point {
	x, y := geometry.ParseEWKBPoint(buf)
	return Point{SRID: srid, X: x, Y: y}
}

func readPoint(nbf *NomsBinFormat, b *valueDecoder) (Point, error) {
	buf := []byte(b.ReadString())
	srid, _, geomType := geometry.ParseEWKBHeader(buf) // Assume it's always little endian
	if geomType != geometry.PointType {
		return Point{}, errors.New("not a point")
	}
	return ParseEWKBPoint(buf[geometry.EWKBHeaderSize:], srid), nil
}

func (v Point) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	buf := []byte(b.ReadString())
	srid, _, geomType := geometry.ParseEWKBHeader(buf) // Assume it's always little endian
	if geomType != geometry.PointType {
		return Point{}, errors.New("not a point")
	}
	return ParseEWKBPoint(buf[geometry.EWKBHeaderSize:], srid), nil
}

func (v Point) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	b.skipString()
}

func (v Point) HumanReadableString() string {
	s := fmt.Sprintf("SRID: %d POINT(%s %s)", v.SRID, strconv.FormatFloat(v.X, 'g', -1, 64), strconv.FormatFloat(v.Y, 'g', -1, 64))
	return strconv.Quote(s)
}
