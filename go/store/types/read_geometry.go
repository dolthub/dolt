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
	case MultiPoint:
		return ConvertTypesMultiPointToSQLMultiPoint(inner)
	case MultiLineString:
		return ConvertTypesMultiLineStringToSQLMultiLineString(inner)
	case MultiPolygon:
		return ConvertTypesMultiPolygonToSQLMultiPolygon(inner)
	case GeomColl:
		return ConvertTypesGeomCollToSQLGeomColl(inner)
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

func ConvertTypesMultiLineStringToSQLMultiLineString(l MultiLineString) sql.MultiLineString {
	lines := make([]sql.LineString, len(l.Lines))
	for i, l := range l.Lines {
		lines[i] = ConvertTypesLineStringToSQLLineString(l)
	}
	return sql.MultiLineString{SRID: l.SRID, Lines: lines}
}

func ConvertTypesMultiPolygonToSQLMultiPolygon(p MultiPolygon) sql.MultiPolygon {
	polys := make([]sql.Polygon, len(p.Polygons))
	for i, p := range p.Polygons {
		polys[i] = ConvertTypesPolygonToSQLPolygon(p)
	}
	return sql.MultiPolygon{SRID: p.SRID, Polygons: polys}
}

func ConvertTypesGeomCollToSQLGeomColl(g GeomColl) sql.GeomColl {
	geoms := make([]sql.GeometryValue, len(g.Geometries))
	for i, geom := range g.Geometries {
		switch geo := geom.(type) {
		case Point:
			geoms[i] = ConvertTypesPointToSQLPoint(geo)
		case LineString:
			geoms[i] = ConvertTypesLineStringToSQLLineString(geo)
		case Polygon:
			geoms[i] = ConvertTypesPolygonToSQLPolygon(geo)
		case MultiPoint:
			geoms[i] = ConvertTypesMultiPointToSQLMultiPoint(geo)
		case MultiLineString:
			geoms[i] = ConvertTypesMultiLineStringToSQLMultiLineString(geo)
		case MultiPolygon:
			geoms[i] = ConvertTypesMultiPolygonToSQLMultiPolygon(geo)
		case GeomColl:
			geoms[i] = ConvertTypesGeomCollToSQLGeomColl(geo)
		}
	}
	return sql.GeomColl{SRID: g.SRID, Geoms: geoms}
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
	case sql.MultiLineString:
		return ConvertSQLMultiLineStringToTypesMultiLineString(inner)
	case sql.MultiPolygon:
		return ConvertSQLMultiPolygonToTypesMultiPolygon(inner)
	case sql.GeomColl:
		return ConvertSQLGeomCollToTypesGeomColl(inner)
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

func ConvertSQLMultiLineStringToTypesMultiLineString(p sql.MultiLineString) MultiLineString {
	lines := make([]LineString, len(p.Lines))
	for i, l := range p.Lines {
		lines[i] = ConvertSQLLineStringToTypesLineString(l)
	}
	return MultiLineString{SRID: p.SRID, Lines: lines}
}

func ConvertSQLMultiPolygonToTypesMultiPolygon(p sql.MultiPolygon) MultiPolygon {
	polys := make([]Polygon, len(p.Polygons))
	for i, p := range p.Polygons {
		polys[i] = ConvertSQLPolygonToTypesPolygon(p)
	}
	return MultiPolygon{SRID: p.SRID, Polygons: polys}
}

func ConvertSQLGeomCollToTypesGeomColl(g sql.GeomColl) GeomColl {
	geoms := make([]Value, len(g.Geoms))
	for i, geom := range g.Geoms {
		switch geo := geom.(type) {
		case sql.Point:
			geoms[i] = ConvertSQLPointToTypesPoint(geo)
		case sql.LineString:
			geoms[i] = ConvertSQLLineStringToTypesLineString(geo)
		case sql.Polygon:
			geoms[i] = ConvertSQLPolygonToTypesPolygon(geo)
		case sql.MultiPoint:
			geoms[i] = ConvertSQLMultiPointToTypesMultiPoint(geo)
		case sql.MultiLineString:
			geoms[i] = ConvertSQLMultiLineStringToTypesMultiLineString(geo)
		case sql.MultiPolygon:
			geoms[i] = ConvertSQLMultiPolygonToTypesMultiPolygon(geo)
		case sql.GeomColl:
			geoms[i] = ConvertSQLGeomCollToTypesGeomColl(geo)
		}
	}
	return GeomColl{SRID: g.SRID, Geometries: geoms}
}

// TODO: all methods here just defer to their SQL equivalents, and assume we always receive good data

func DeserializeEWKBHeader(buf []byte) (uint32, bool, uint32, error) {
	return sql.DeserializeEWKBHeader(buf)
}

func DeserializeWKBHeader(buf []byte) (bool, uint32, error) {
	return sql.DeserializeWKBHeader(buf)
}

func DeserializePoint(buf []byte, isBig bool, srid uint32) sql.Point {
	p, _, err := sql.DeserializePoint(buf, isBig, srid)
	if err != nil {
		panic(err)
	}
	return p
}

func DeserializeLine(buf []byte, isBig bool, srid uint32) sql.LineString {
	l, _, err := sql.DeserializeLine(buf, isBig, srid)
	if err != nil {
		panic(err)
	}
	return l
}

func DeserializePoly(buf []byte, isBig bool, srid uint32) sql.Polygon {
	p, _, err := sql.DeserializePoly(buf, isBig, srid)
	if err != nil {
		panic(err)
	}
	return p
}

func DeserializeMPoint(buf []byte, isBig bool, srid uint32) sql.MultiPoint {
	p, _, err := sql.DeserializeMPoint(buf, isBig, srid)
	if err != nil {
		panic(err)
	}
	return p
}

func DeserializeMLine(buf []byte, isBig bool, srid uint32) sql.MultiLineString {
	p, _, err := sql.DeserializeMLine(buf, isBig, srid)
	if err != nil {
		panic(err)
	}
	return p
}

func DeserializeMPoly(buf []byte, isBig bool, srid uint32) sql.MultiPolygon {
	p, _, err := sql.DeserializeMPoly(buf, isBig, srid)
	if err != nil {
		panic(err)
	}
	return p
}

func DeserializeGeomColl(buf []byte, isBig bool, srid uint32) sql.GeomColl {
	g, _, err := sql.DeserializeGeomColl(buf, isBig, srid)
	if err != nil {
		panic(err)
	}
	return g
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

func DeserializeTypesMLine(buf []byte, isBig bool, srid uint32) MultiLineString {
	return ConvertSQLMultiLineStringToTypesMultiLineString(DeserializeMLine(buf, isBig, srid))
}

func DeserializeTypesMPoly(buf []byte, isBig bool, srid uint32) MultiPolygon {
	return ConvertSQLMultiPolygonToTypesMultiPolygon(DeserializeMPoly(buf, isBig, srid))
}

func DeserializeTypesGeomColl(buf []byte, isBig bool, srid uint32) GeomColl {
	return ConvertSQLGeomCollToTypesGeomColl(DeserializeGeomColl(buf, isBig, srid))
}
