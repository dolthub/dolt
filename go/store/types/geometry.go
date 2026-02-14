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

	"github.com/dolthub/dolt/go/store/hash"
)

// Geometry represents any of the types Point, LineString, or Polygon.
// TODO: maybe this should just be an interface?
type Geometry struct {
	Inner Value
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

func (v Geometry) Less(ctx context.Context, nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	return v.Inner.Less(ctx, nbf, other)
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
		buf := SerializePoint(inner)
		w.writeString(string(buf))
	case LineString:
		buf := SerializeLineString(inner)
		w.writeString(string(buf))
	case Polygon:
		buf := SerializePolygon(inner)
		w.writeString(string(buf))
	case MultiPoint:
		buf := SerializeMultiPoint(inner)
		w.writeString(string(buf))
	default:
		return errors.New("wrong Inner type")
	}
	return nil
}

func readGeometry(nbf *NomsBinFormat, b *valueDecoder) (Geometry, error) {
	buf := []byte(b.ReadString())
	srid, _, geomType, err := DeserializeEWKBHeader(buf) // Assume it's always little endian
	if err != nil {
		return Geometry{}, err
	}
	buf = buf[EWKBHeaderSize:]
	var inner Value
	switch geomType {
	case WKBPointID:
		inner = DeserializeTypesPoint(buf, false, srid)
	case WKBLineID:
		inner = DeserializeTypesLine(buf, false, srid)
	case WKBPolyID:
		inner = DeserializeTypesPoly(buf, false, srid)
	case WKBMultiPointID:
		inner = DeserializeTypesMPoint(buf, false, srid)
	default:
		return Geometry{}, errors.New("not a geometry")
	}
	return Geometry{Inner: inner}, nil
}

func (v Geometry) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	buf := []byte(b.ReadString())
	srid, _, geomType, err := DeserializeEWKBHeader(buf) // Assume it's always little endian
	if err != nil {
		return Geometry{}, err
	}
	buf = buf[EWKBHeaderSize:]
	var inner Value
	switch geomType {
	case WKBPointID:
		inner = DeserializeTypesPoint(buf, false, srid)
	case WKBLineID:
		inner = DeserializeTypesLine(buf, false, srid)
	case WKBPolyID:
		inner = DeserializeTypesPoly(buf, false, srid)
	case WKBMultiPointID:
		inner = DeserializeTypesMPoint(buf, false, srid)
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
