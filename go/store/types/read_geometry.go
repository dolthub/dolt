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
	"github.com/dolthub/go-mysql-server/sql"
)

func ConvertTypesGeometryToSQLGeometry(g Geometry) interface{} {
	switch inner := g.Inner.(type) {
	case Point:
		return ConvertTypesPointToSQLPoint(inner)
	case LineString:
		return ConvertTypesLineStringToSQLLineString(inner)
	case Polygon:
		return ConvertTypesPolygonToSQLPolygon(inner)
	default:
		panic("used an invalid type types.Geometry.Inner")
	}
}

func ConvertTypesPointToSQLPoint(p Point) sql.Point {
	return sql.Point{SRID: p.SRID, X: p.X, Y: p.Y}
}

func ConvertTypesLineStringToSQLLineString(l LineString) sql.LineString {
	points := make([]sql.Point, len(l.Points))
	for i, p := range l.Points {
		points[i] = ConvertTypesPointToSQLPoint(p)
	}
	return sql.LineString{SRID: l.SRID, Points: points}
}

func ConvertTypesPolygonToSQLPolygon(p Polygon) sql.Polygon {
	lines := make([]sql.LineString, len(p.Lines))
	for i, l := range p.Lines {
		lines[i] = ConvertTypesLineStringToSQLLineString(l)
	}
	return sql.Polygon{SRID: p.SRID, Lines: lines}
}

func ConvertTypesMultiPointToSQLMultiPoint(p MultiPoint) sql.MultiPoint {
	points := make([]sql.Point, len(p.Points))
	for i, point := range p.Points {
		points[i] = ConvertTypesPointToSQLPoint(point)
	}
	return sql.MultiPoint{SRID: p.SRID, Points: points}
}

func ConvertSQLGeometryToTypesGeometry(p interface{}) Value {
	switch inner := p.(type) {
	case sql.Point:
		return ConvertSQLPointToTypesPoint(inner)
	case sql.LineString:
		return ConvertSQLLineStringToTypesLineString(inner)
	case sql.Polygon:
		return ConvertSQLPolygonToTypesPolygon(inner)
	case sql.MultiPoint:
		return ConvertSQLMultiPointToTypesMultiPoint(inner)
	default:
		panic("used an invalid type sql.Geometry.Inner")
	}
}

func ConvertSQLPointToTypesPoint(p sql.Point) Point {
	return Point{SRID: p.SRID, X: p.X, Y: p.Y}
}

func ConvertSQLLineStringToTypesLineString(l sql.LineString) LineString {
	points := make([]Point, len(l.Points))
	for i, p := range l.Points {
		points[i] = ConvertSQLPointToTypesPoint(p)
	}
	return LineString{SRID: l.SRID, Points: points}
}

func ConvertSQLPolygonToTypesPolygon(p sql.Polygon) Polygon {
	lines := make([]LineString, len(p.Lines))
	for i, l := range p.Lines {
		lines[i] = ConvertSQLLineStringToTypesLineString(l)
	}
	return Polygon{SRID: p.SRID, Lines: lines}
}

func ConvertSQLMultiPointToTypesMultiPoint(p sql.MultiPoint) MultiPoint {
	points := make([]Point, len(p.Points))
	for i, point := range p.Points {
		points[i] = ConvertSQLPointToTypesPoint(point)
	}
	return MultiPoint{SRID: p.SRID, Points: points}
}

// TODO: all methods here just defer to their SQL equivalents, and assume we always receive good data

func DeserializeEWKBHeader(buf []byte) (uint32, bool, uint32, error) {
	return sql.DeserializeEWKBHeader(buf)
}

func DeserializeWKBHeader(buf []byte) (bool, uint32, error) {
	return sql.DeserializeWKBHeader(buf)
}

func DeserializePoint(buf []byte, isBig bool, srid uint32) sql.Point {
	p, _ := sql.DeserializePoint(buf, isBig, srid)
	return p
}

func DeserializeLine(buf []byte, isBig bool, srid uint32) sql.LineString {
	l, _ := sql.DeserializeLine(buf, isBig, srid)
	return l
}

func DeserializePoly(buf []byte, isBig bool, srid uint32) sql.Polygon {
	p, _ := sql.DeserializePoly(buf, isBig, srid)
	return p
}

func DeserializeMPoint(buf []byte, isBig bool, srid uint32) sql.MultiPoint {
	p, _ := sql.DeserializeMPoint(buf, isBig, srid)
	return p
}

// TODO: noms needs results to be in types

func DeserializeTypesPoint(buf []byte, isBig bool, srid uint32) Point {
	return ConvertSQLPointToTypesPoint(DeserializePoint(buf, isBig, srid))
}

func DeserializeTypesLine(buf []byte, isBig bool, srid uint32) LineString {
	return ConvertSQLLineStringToTypesLineString(DeserializeLine(buf, isBig, srid))
}

func DeserializeTypesPoly(buf []byte, isBig bool, srid uint32) Polygon {
	return ConvertSQLPolygonToTypesPolygon(DeserializePoly(buf, isBig, srid))
}

func DeserializeTypesMPoint(buf []byte, isBig bool, srid uint32) MultiPoint {
	return ConvertSQLMultiPointToTypesMultiPoint(DeserializeMPoint(buf, isBig, srid))
}
