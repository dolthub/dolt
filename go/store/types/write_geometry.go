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

const (
	CartesianSRID  = types.CartesianSRID
	GeoSpatialSRID = types.GeoSpatialSRID
)

const (
	SRIDSize       = types.SRIDSize
	EndianSize     = types.EndianSize
	TypeSize       = types.TypeSize
	EWKBHeaderSize = types.EWKBHeaderSize

	PointSize = types.PointSize
	CountSize = types.CountSize
)

const (
	WKBUnknown      = types.WKBUnknown
	WKBPointID      = types.WKBPointID
	WKBLineID       = types.WKBLineID
	WKBPolyID       = types.WKBPolyID
	WKBMultiPointID = types.WKBMultiPointID
	WKBMultiLineID  = types.WKBMultiLineID
	WKBMultiPolyID  = types.WKBMultiPolyID
	WKBGeomCollID   = types.WKBGeomCollID
)

// TODO: all methods here just defer to their SQL equivalents, and assume we always receive good data

func WriteEWKBHeader(buf []byte, srid, typ uint32) {
	types.WriteEWKBHeader(buf, srid, typ)
}

func SerializePoint(p Point) []byte {
	return ConvertTypesPointToSQLPoint(p).Serialize()
}

func SerializeLineString(l LineString) []byte {
	return ConvertTypesLineStringToSQLLineString(l).Serialize()
}

func SerializePolygon(p Polygon) []byte {
	return ConvertTypesPolygonToSQLPolygon(p).Serialize()
}

func SerializeMultiPoint(p MultiPoint) []byte {
	return ConvertTypesMultiPointToSQLMultiPoint(p).Serialize()
}

func SerializeMultiLineString(p MultiLineString) []byte {
	return ConvertTypesMultiLineStringToSQLMultiLineString(p).Serialize()
}

func SerializeMultiPolygon(p MultiPolygon) []byte {
	return ConvertTypesMultiPolygonToSQLMultiPolygon(p).Serialize()
}

func SerializeGeomColl(g GeomColl) []byte {
	return ConvertTypesGeomCollToSQLGeomColl(g).Serialize()
}
