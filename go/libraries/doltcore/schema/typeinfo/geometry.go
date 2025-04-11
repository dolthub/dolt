// Copyright 2020 Dolthub, Inc.
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

package typeinfo

import (
	"context"
	"fmt"
	"strconv"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/store/types"
)

// This is a dolt implementation of the MySQL type Geometry, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type geometryType struct {
	sqlGeometryType gmstypes.GeometryType // References the corresponding GeometryType in GMS
}

var _ TypeInfo = (*geometryType)(nil)

var GeometryType = &geometryType{gmstypes.GeometryType{}}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *geometryType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	// Check for null
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}

	// Expect a Geometry type, return a sql.Geometry
	switch val := v.(type) {
	case types.Geometry:
		return types.ConvertTypesGeometryToSQLGeometry(val), nil
	case types.Point:
		return types.ConvertTypesPointToSQLPoint(val), nil
	case types.LineString:
		return types.ConvertTypesLineStringToSQLLineString(val), nil
	case types.Polygon:
		return types.ConvertTypesPolygonToSQLPolygon(val), nil
	case types.MultiPoint:
		return types.ConvertTypesMultiPointToSQLMultiPoint(val), nil
	case types.MultiLineString:
		return types.ConvertTypesMultiLineStringToSQLMultiLineString(val), nil
	case types.MultiPolygon:
		return types.ConvertTypesMultiPolygonToSQLMultiPolygon(val), nil
	case types.GeomColl:
		return types.ConvertTypesGeomCollToSQLGeomColl(val), nil
	default:
		return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
	}
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *geometryType) ReadFrom(nbf *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	var val types.Value
	var err error

	k := reader.ReadKind()
	switch k {
	case types.PointKind:
		if val, err = reader.ReadPoint(); err != nil {
			return nil, err
		}
	case types.LineStringKind:
		if val, err = reader.ReadLineString(); err != nil {
			return nil, err
		}
	case types.PolygonKind:
		if val, err = reader.ReadPolygon(); err != nil {
			return nil, err
		}
	case types.MultiPointKind:
		if val, err = reader.ReadMultiPoint(); err != nil {
			return nil, err
		}
	case types.MultiLineStringKind:
		if val, err = reader.ReadMultiLineString(); err != nil {
			return nil, err
		}
	case types.MultiPolygonKind:
		if val, err = reader.ReadMultiPolygon(); err != nil {
			return nil, err
		}
	case types.GeometryCollectionKind:
		if val, err = reader.ReadGeomColl(); err != nil {
			return nil, err
		}
	case types.GeometryKind:
		// Note: GeometryKind is no longer written
		// included here for backward compatibility
		if val, err = reader.ReadGeometry(); err != nil {
			return nil, err
		}
	case types.NullKind:
		return nil, nil
	default:
		return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
	}

	return ti.ConvertNomsValueToValue(val)
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *geometryType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	// Check for null
	if v == nil {
		return types.NullValue, nil
	}

	// Convert accordingly
	geom, _, err := ti.sqlGeometryType.Convert(v)
	if err != nil {
		return nil, err
	}
	return types.ConvertSQLGeometryToTypesGeometry(geom), nil
}

// Equals implements TypeInfo interface.
func (ti *geometryType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if o, ok := other.(*geometryType); ok {
		// if either ti or other has defined SRID, then check SRID value; otherwise,
		return (!ti.sqlGeometryType.DefinedSRID && !o.sqlGeometryType.DefinedSRID) || ti.sqlGeometryType.SRID == o.sqlGeometryType.SRID
	}
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *geometryType) FormatValue(v types.Value) (*string, error) {
	// Received null value
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}

	// Expect one of the Geometry types
	switch val := v.(type) {
	case types.Point:
		return PointType.FormatValue(val)
	case types.LineString:
		return LineStringType.FormatValue(val)
	case types.Polygon:
		return PolygonType.FormatValue(val)
	case types.MultiPoint:
		return MultiPointType.FormatValue(val)
	case types.MultiLineString:
		return MultiLineStringType.FormatValue(val)
	case types.MultiPolygon:
		return MultiPolygonType.FormatValue(val)
	case types.GeomColl:
		return GeomCollType.FormatValue(val)
	case types.Geometry:
		switch inner := val.Inner.(type) {
		case types.Point:
			return PointType.FormatValue(inner)
		case types.LineString:
			return LineStringType.FormatValue(inner)
		case types.Polygon:
			return PolygonType.FormatValue(inner)
		case types.MultiPoint:
			return MultiPointType.FormatValue(inner)
		case types.MultiLineString:
			return MultiLineStringType.FormatValue(inner)
		case types.MultiPolygon:
			return MultiPolygonType.FormatValue(val)
		case types.GeomColl:
			return GeomCollType.FormatValue(val)
		default:
			return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v.Kind())
		}
	default:
		return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v.Kind())
	}
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *geometryType) GetTypeIdentifier() Identifier {
	return GeometryTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *geometryType) GetTypeParams() map[string]string {
	return map[string]string{"SRID": strconv.FormatUint(uint64(ti.sqlGeometryType.SRID), 10),
		"DefinedSRID": strconv.FormatBool(ti.sqlGeometryType.DefinedSRID)}
}

// IsValid implements TypeInfo interface.
func (ti *geometryType) IsValid(v types.Value) bool {
	if _, ok := v.(types.Null); ok || v == nil {
		return true
	}

	switch v.(type) {
	case types.Geometry,
		types.Point,
		types.LineString,
		types.Polygon,
		types.MultiPoint,
		types.MultiLineString,
		types.MultiPolygon:
		return true
	default:
		return false
	}
}

// NomsKind implements TypeInfo interface.
func (ti *geometryType) NomsKind() types.NomsKind {
	return types.GeometryKind
}

// Promote implements TypeInfo interface.
func (ti *geometryType) Promote() TypeInfo {
	return ti
}

// String implements TypeInfo interface.
func (ti *geometryType) String() string {
	return "Geometry"
}

// ToSqlType implements TypeInfo interface.
func (ti *geometryType) ToSqlType() sql.Type {
	return ti.sqlGeometryType
}

// geometryTypeConverter is an internal function for GetTypeConverter that handles the specific type as the source TypeInfo.
func geometryTypeConverter(ctx context.Context, src *geometryType, destTi TypeInfo) (tc TypeConverter, needsConversion bool, err error) {
	switch dest := destTi.(type) {
	case *bitType:
		return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
			return types.Uint(0), nil
		}, true, nil
	case *blobStringType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *boolType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *datetimeType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *decimalType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *enumType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *floatType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *geomcollType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *geometryType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *inlineBlobType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *intType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *jsonType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *linestringType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *multilinestringType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *multipointType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *multipolygonType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *pointType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *polygonType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *setType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *timeType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *uintType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *uuidType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *varBinaryType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *varStringType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *yearType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	default:
		return nil, false, UnhandledTypeConversion.New(src.String(), destTi.String())
	}
}

func CreateGeometryTypeFromParams(params map[string]string) (TypeInfo, error) {
	var (
		err     error
		sridVal uint64
		def     bool
	)
	if s, ok := params["SRID"]; ok {
		sridVal, err = strconv.ParseUint(s, 10, 32)
		if err != nil {
			return nil, err
		}
	}
	if d, ok := params["DefinedSRID"]; ok {
		def, err = strconv.ParseBool(d)
		if err != nil {
			return nil, err
		}
	}

	return CreateGeometryTypeFromSqlGeometryType(gmstypes.GeometryType{SRID: uint32(sridVal), DefinedSRID: def}), nil
}

func CreateGeometryTypeFromSqlGeometryType(sqlGeometryType gmstypes.GeometryType) TypeInfo {
	return &geometryType{sqlGeometryType: sqlGeometryType}
}
