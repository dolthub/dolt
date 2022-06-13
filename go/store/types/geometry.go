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

	"github.com/dolthub/dolt/go/store/geometry"

	"github.com/dolthub/dolt/go/store/hash"
)

// Geometry represents any of the types Point, LineString, or Polygon.
// TODO: Generics maybe?
type Geometry struct {
	Inner Value // Can be types.Point, types.LineString, or types.Polygon
}

// Value interface
func (v Geometry) Value(ctx context.Context) (Value, error) {
	return v, nil
}

func (v Geometry) Equals(other Value) bool {
	// If other is Geometry, recurse on other.Inner
	if otherGeom, ok := other.(Geometry); ok {
		return v.Equals(otherGeom.Inner)
	}

	// Compare based on v.Inner type
	return v.Inner.Equals(other)
}

func (v Geometry) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	return v.Inner.Less(nbf, other)
}

func (v Geometry) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(v, nbf)
}

func (v Geometry) isPrimitive() bool {
	return true
}

func (v Geometry) walkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (v Geometry) typeOf() (*Type, error) {
	return PrimitiveTypeMap[GeometryKind], nil
}

func (v Geometry) Kind() NomsKind {
	return GeometryKind
}

func (v Geometry) valueReadWriter() ValueReadWriter {
	return nil
}

func (v Geometry) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	// Mark as GeometryKind
	err := GeometryKind.writeTo(w, nbf)
	if err != nil {
		return err
	}

	switch inner := v.Inner.(type) {
	case Point:
		// Allocate buffer for point 4 + 1 + 4 + 16
		buf := make([]byte, geometry.EWKBHeaderSize+geometry.PointSize)
		// Write header and data to buffer
		WriteEWKBHeader(inner, buf)
		WriteEWKBPointData(inner, buf[geometry.EWKBHeaderSize:])
		w.writeString(string(buf))
	case LineString:
		// Allocate buffer for linestring
		buf := make([]byte, geometry.EWKBHeaderSize+LengthSize+geometry.PointSize*len(inner.Points))
		// Write header and data to buffer
		WriteEWKBHeader(inner, buf)
		WriteEWKBLineData(inner, buf[geometry.EWKBHeaderSize:])
		w.writeString(string(buf))
	case Polygon:
		// Calculate space for polygon buffer
		size := 0
		for _, l := range inner.Lines {
			size += LengthSize + geometry.PointSize*len(l.Points)
		}
		// Allocate buffer for poly
		buf := make([]byte, geometry.EWKBHeaderSize+LengthSize+size)
		// Write header and data to buffer
		WriteEWKBHeader(inner, buf)
		WriteEWKBPolyData(inner, buf[geometry.EWKBHeaderSize:])
		w.writeString(string(buf))
	default:
		return errors.New("wrong Inner type")
	}
	return nil
}

func readGeometry(nbf *NomsBinFormat, b *valueDecoder) (Geometry, error) {
	buf := []byte(b.ReadString())
	srid, _, geomType := geometry.ParseEWKBHeader(buf) // Assume it's always little endian
	var inner Value
	switch geomType {
	case geometry.PointType:
		inner = ParseEWKBPoint(buf[geometry.EWKBHeaderSize:], srid)
	case geometry.LineStringType:
		inner = ParseEWKBLine(buf[geometry.EWKBHeaderSize:], srid)
	case geometry.PolygonType:
		inner = ParseEWKBPoly(buf[geometry.EWKBHeaderSize:], srid)
	default:
		return Geometry{}, errors.New("not a geometry")
	}
	return Geometry{Inner: inner}, nil
}

func (v Geometry) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	buf := []byte(b.ReadString())
	srid, _, geomType := geometry.ParseEWKBHeader(buf) // Assume it's always little endian
	var inner Value
	switch geomType {
	case geometry.PointType:
		inner = ParseEWKBPoint(buf[geometry.EWKBHeaderSize:], srid)
	case geometry.LineStringType:
		inner = ParseEWKBLine(buf[geometry.EWKBHeaderSize:], srid)
	case geometry.PolygonType:
		inner = ParseEWKBPoly(buf[geometry.EWKBHeaderSize:], srid)
	default:
		return Geometry{}, errors.New("not a geometry")
	}
	return Geometry{Inner: inner}, nil
}

func (v Geometry) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	b.skipString()
}

func (v Geometry) HumanReadableString() string {
	return v.Inner.HumanReadableString()
}
