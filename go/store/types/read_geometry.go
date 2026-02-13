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
	"github.com/dolthub/go-mysql-server/sql/types"
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

func ConvertTypesPointToSQLPoint(p Point) types.Point {
	return types.Point{SRID: p.SRID, X: p.X, Y: p.Y}
}

func ConvertTypesLineStringToSQLLineString(l LineString) types.LineString {
	points := make([]types.Point, len(l.Points))
	for i, p := range l.Points {
		points[i] = ConvertTypesPointToSQLPoint(p)
	}
	return types.LineString{SRID: l.SRID, Points: points}
}

func ConvertTypesPolygonToSQLPolygon(p Polygon) types.Polygon {
	lines := make([]types.LineString, len(p.Lines))
	for i, l := range p.Lines {
		lines[i] = ConvertTypesLineStringToSQLLineString(l)
	}
	return types.Polygon{SRID: p.SRID, Lines: lines}
}

func ConvertTypesMultiPointToSQLMultiPoint(p MultiPoint) types.MultiPoint {
	points := make([]types.Point, len(p.Points))
	for i, point := range p.Points {
		points[i] = ConvertTypesPointToSQLPoint(point)
	}
	return types.MultiPoint{SRID: p.SRID, Points: points}
}

func ConvertTypesMultiLineStringToSQLMultiLineString(l MultiLineString) types.MultiLineString {
	lines := make([]types.LineString, len(l.Lines))
	for i, l := range l.Lines {
		lines[i] = ConvertTypesLineStringToSQLLineString(l)
	}
	return types.MultiLineString{SRID: l.SRID, Lines: lines}
}

func ConvertTypesMultiPolygonToSQLMultiPolygon(p MultiPolygon) types.MultiPolygon {
	polys := make([]types.Polygon, len(p.Polygons))
	for i, p := range p.Polygons {
		polys[i] = ConvertTypesPolygonToSQLPolygon(p)
	}
	return types.MultiPolygon{SRID: p.SRID, Polygons: polys}
}

func ConvertTypesGeomCollToSQLGeomColl(g GeomColl) types.GeomColl {
	geoms := make([]types.GeometryValue, len(g.Geometries))
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
	return types.GeomColl{SRID: g.SRID, Geoms: geoms}
}

func ConvertSQLGeometryToTypesGeometry(p interface{}) Value {
	switch inner := p.(type) {
	case types.Point:
		return ConvertSQLPointToTypesPoint(inner)
	case types.LineString:
		return ConvertSQLLineStringToTypesLineString(inner)
	case types.Polygon:
		return ConvertSQLPolygonToTypesPolygon(inner)
	case types.MultiPoint:
		return ConvertSQLMultiPointToTypesMultiPoint(inner)
	case types.MultiLineString:
		return ConvertSQLMultiLineStringToTypesMultiLineString(inner)
	case types.MultiPolygon:
		return ConvertSQLMultiPolygonToTypesMultiPolygon(inner)
	case types.GeomColl:
		return ConvertSQLGeomCollToTypesGeomColl(inner)
	default:
		panic("used an invalid type sql.Geometry.Inner")
	}
}

func ConvertSQLPointToTypesPoint(p types.Point) Point {
	return Point{SRID: p.SRID, X: p.X, Y: p.Y}
}

func ConvertSQLLineStringToTypesLineString(l types.LineString) LineString {
	points := make([]Point, len(l.Points))
	for i, p := range l.Points {
		points[i] = ConvertSQLPointToTypesPoint(p)
	}
	return LineString{SRID: l.SRID, Points: points}
}

func ConvertSQLPolygonToTypesPolygon(p types.Polygon) Polygon {
	lines := make([]LineString, len(p.Lines))
	for i, l := range p.Lines {
		lines[i] = ConvertSQLLineStringToTypesLineString(l)
	}
	return Polygon{SRID: p.SRID, Lines: lines}
}

func ConvertSQLMultiPointToTypesMultiPoint(p types.MultiPoint) MultiPoint {
	points := make([]Point, len(p.Points))
	for i, point := range p.Points {
		points[i] = ConvertSQLPointToTypesPoint(point)
	}
	return MultiPoint{SRID: p.SRID, Points: points}
}

func ConvertSQLMultiLineStringToTypesMultiLineString(p types.MultiLineString) MultiLineString {
	lines := make([]LineString, len(p.Lines))
	for i, l := range p.Lines {
		lines[i] = ConvertSQLLineStringToTypesLineString(l)
	}
	return MultiLineString{SRID: p.SRID, Lines: lines}
}

func ConvertSQLMultiPolygonToTypesMultiPolygon(p types.MultiPolygon) MultiPolygon {
	polys := make([]Polygon, len(p.Polygons))
	for i, p := range p.Polygons {
		polys[i] = ConvertSQLPolygonToTypesPolygon(p)
	}
	return MultiPolygon{SRID: p.SRID, Polygons: polys}
}

func ConvertSQLGeomCollToTypesGeomColl(g types.GeomColl) GeomColl {
	geoms := make([]Value, len(g.Geoms))
	for i, geom := range g.Geoms {
		switch geo := geom.(type) {
		case types.Point:
			geoms[i] = ConvertSQLPointToTypesPoint(geo)
		case types.LineString:
			geoms[i] = ConvertSQLLineStringToTypesLineString(geo)
		case types.Polygon:
			geoms[i] = ConvertSQLPolygonToTypesPolygon(geo)
		case types.MultiPoint:
			geoms[i] = ConvertSQLMultiPointToTypesMultiPoint(geo)
		case types.MultiLineString:
			geoms[i] = ConvertSQLMultiLineStringToTypesMultiLineString(geo)
		case types.MultiPolygon:
			geoms[i] = ConvertSQLMultiPolygonToTypesMultiPolygon(geo)
		case types.GeomColl:
			geoms[i] = ConvertSQLGeomCollToTypesGeomColl(geo)
		}
	}
	return GeomColl{SRID: g.SRID, Geometries: geoms}
}

// TODO: all methods here just defer to their SQL equivalents, and assume we always receive good data

func DeserializeEWKBHeader(buf []byte) (uint32, bool, uint32, error) {
	return types.DeserializeEWKBHeader(buf)
}

func DeserializePoint(buf []byte, isBig bool, srid uint32) types.Point {
	p, _, err := types.DeserializePoint(buf, isBig, srid)
	if err != nil {
		panic(err)
	}
	return p
}

func DeserializeLine(buf []byte, isBig bool, srid uint32) types.LineString {
	l, _, err := types.DeserializeLine(buf, isBig, srid)
	if err != nil {
		panic(err)
	}
	return l
}

func DeserializePoly(buf []byte, isBig bool, srid uint32) types.Polygon {
	p, _, err := types.DeserializePoly(buf, isBig, srid)
	if err != nil {
		panic(err)
	}
	return p
}

func DeserializeMPoint(buf []byte, isBig bool, srid uint32) types.MultiPoint {
	p, _, err := types.DeserializeMPoint(buf, isBig, srid)
	if err != nil {
		panic(err)
	}
	return p
}

func DeserializeMLine(buf []byte, isBig bool, srid uint32) types.MultiLineString {
	p, _, err := types.DeserializeMLine(buf, isBig, srid)
	if err != nil {
		panic(err)
	}
	return p
}

func DeserializeMPoly(buf []byte, isBig bool, srid uint32) types.MultiPolygon {
	p, _, err := types.DeserializeMPoly(buf, isBig, srid)
	if err != nil {
		panic(err)
	}
	return p
}

func DeserializeGeomColl(buf []byte, isBig bool, srid uint32) types.GeomColl {
	g, _, err := types.DeserializeGeomColl(buf, isBig, srid)
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
