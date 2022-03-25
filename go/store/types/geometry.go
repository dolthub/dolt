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

// Geometry represents any of the types Point, Linestring, or Polygon.
// TODO: Generics maybe?
type Geometry struct {
	Inner interface{} // Can be types.Point, types.Linestring, or types.Polygon
}

// Value interface
func (v Geometry) Value(ctx context.Context) (Value, error) {
	return v, nil
}

func (v Geometry) Equals(other Value) bool {
	switch this := v.Inner.(type) {
	case Point:
		return this.Equals(other)
	case Linestring:
		return this.Equals(other)
	case Polygon:
		return this.Equals(other)
	default:
		return false
	}
}

func (v Geometry) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	switch this := v.Inner.(type) {
	case Point:
		return this.Less(nbf, other)
	case Linestring:
		return this.Less(nbf, other)
	case Polygon:
		return this.Less(nbf, other)
	default:
		return GeometryKind < other.Kind(), nil
	}
}

func (v Geometry) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(v, nbf)
}

func (v Geometry) isPrimitive() bool {
	return true
}

func (v Geometry) WalkValues(ctx context.Context, cb ValueCallback) error {
	return cb(v)
}

func (v Geometry) WalkRefs(nbf *NomsBinFormat, cb RefCallback) error {
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

	// TODO: write helper functions
	// Call the appropriate writeTo
	switch this := v.Inner.(type) {
	case Point:
		// Allocate buffer for point 4 + 1 + 4 + 16
		buf := make([]byte, geometry.EWKBHeaderSize+geometry.PointSize)
		// Write header and data to buffer
		WriteEWKBHeader(this, buf)
		WriteEWKBPointData(this, buf[geometry.EWKBHeaderSize:])
		w.writeString(string(buf))
	case Linestring:
		// Allocate buffer for linestring
		buf := make([]byte, geometry.EWKBHeaderSize+LengthSize+geometry.PointSize*len(this.Points))
		// Write header and data to buffer
		WriteEWKBHeader(this, buf)
		WriteEWKBLineData(this, buf[geometry.EWKBHeaderSize:])
		w.writeString(string(buf))
	case Polygon:
		// Calculate space for polygon buffer
		size := 0
		for _, l := range this.Lines {
			size += LengthSize + geometry.PointSize*len(l.Points)
		}
		// Allocate buffer for poly
		buf := make([]byte, geometry.EWKBHeaderSize+LengthSize+size)
		// Write header and data to buffer
		WriteEWKBHeader(this, buf)
		WriteEWKBPolyData(this, buf[geometry.EWKBHeaderSize:])
		w.writeString(string(buf))
	default:
		return errors.New("wrong Inner type")
	}
	return nil
}

func readGeometry(nbf *NomsBinFormat, b *valueDecoder) (Geometry, error) {
	buf := []byte(b.ReadString())
	srid, _, geomType := geometry.ParseEWKBHeader(buf) // Assume it's always little endian
	switch geomType {
	case geometry.PointType:
		point := ParseEWKBPoint(buf[geometry.EWKBHeaderSize:], srid)
		return Geometry{Inner: point}, nil
	case geometry.LinestringType:
		line := ParseEWKBLine(buf[geometry.EWKBHeaderSize:], srid)
		return Geometry{Inner: line}, nil
	case geometry.PolygonType:
		poly := ParseEWKBPoly(buf[geometry.EWKBHeaderSize:], srid)
		return Geometry{Inner: poly}, nil
	default:
		return Geometry{}, nil
	}
}

func (v Geometry) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	buf := []byte(b.ReadString())
	srid, _, geomType := geometry.ParseEWKBHeader(buf) // Assume it's always little endian
	switch geomType {
	case geometry.PointType:
		return ParseEWKBPoint(buf[geometry.EWKBHeaderSize:], srid), nil
	case geometry.LinestringType:
		return ParseEWKBLine(buf[geometry.EWKBHeaderSize:], srid), nil
	case geometry.PolygonType:
		return ParseEWKBPoly(buf[geometry.EWKBHeaderSize:], srid), nil
	default:
		return Geometry{}, errors.New("not a geometry")
	}
}

func (v Geometry) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	b.skipString()
}

func (v Geometry) HumanReadableString() string {
	switch this := v.Inner.(type) {
	case Point:
		return this.HumanReadableString()
	case Linestring:
		return this.HumanReadableString()
	case Polygon:
		return this.HumanReadableString()
	default:
		return "what the heck happened here?"
	}
}
