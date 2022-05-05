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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/store/types"
)

// This is a dolt implementation of the MySQL type Geometry, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type geometryType struct {
	sqlGeometryType sql.GeometryType // References the corresponding GeometryType in GMS
	innerType       TypeInfo         // References the actual typeinfo (pointType, linestringType, polygonType)
}

var _ TypeInfo = (*geometryType)(nil)

var GeometryType = &geometryType{sql.GeometryType{}, nil}

// ConvertTypesGeometryToSQLGeometry basically makes a deep copy of sql.Geometry
func ConvertTypesGeometryToSQLGeometry(g types.Geometry) sql.Geometry {
	switch inner := g.Inner.(type) {
	case types.Point:
		return sql.Geometry{Inner: ConvertTypesPointToSQLPoint(inner)}
	case types.Linestring:
		return sql.Geometry{Inner: ConvertTypesLinestringToSQLLinestring(inner)}
	case types.Polygon:
		return sql.Geometry{Inner: ConvertTypesPolygonToSQLPolygon(inner)}
	default:
		panic("used an invalid type types.Geometry.Inner")
	}
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *geometryType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	// Check for null
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}

	// Expect a Geometry type, return a sql.Geometry
	switch val := v.(type) {
	case types.Geometry:
		return ConvertTypesGeometryToSQLGeometry(val), nil
	case types.Point:
		return sql.Geometry{Inner: ConvertTypesPointToSQLPoint(val)}, nil
	case types.Linestring:
		return sql.Geometry{Inner: ConvertTypesLinestringToSQLLinestring(val)}, nil
	case types.Polygon:
		return sql.Geometry{Inner: ConvertTypesPolygonToSQLPolygon(val)}, nil
	default:
		return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
	}
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *geometryType) ReadFrom(nbf *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.ReadKind()
	switch k {
	case types.GeometryKind:
		p, err := reader.ReadGeometry()
		if err != nil {
			return nil, err
		}
		return ti.ConvertNomsValueToValue(p)
	case types.NullKind:
		return nil, nil
	default:
		return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
	}
}

func ConvertSQLGeometryToTypesGeometry(p sql.Geometry) types.Geometry {
	switch inner := p.Inner.(type) {
	case sql.Point:
		return types.Geometry{Inner: ConvertSQLPointToTypesPoint(inner)}
	case sql.Linestring:
		return types.Geometry{Inner: ConvertSQLLinestringToTypesLinestring(inner)}
	case sql.Polygon:
		return types.Geometry{Inner: ConvertSQLPolygonToTypesPolygon(inner)}
	default:
		panic("used an invalid type sql.Geometry.Inner")
	}

}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *geometryType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	// Check for null
	if v == nil {
		return types.NullValue, nil
	}

	// Convert accordingly
	geom, err := ti.sqlGeometryType.Convert(v)
	if err != nil {
		return nil, err
	}
	return ConvertSQLGeometryToTypesGeometry(geom.(sql.Geometry)), nil
}

// Equals implements TypeInfo interface.
func (ti *geometryType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	_, ok := other.(*geometryType)
	return ok
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
	case types.Linestring:
		return LinestringType.FormatValue(val)
	case types.Polygon:
		return PolygonType.FormatValue(val)
	case types.Geometry:
		switch inner := val.Inner.(type) {
		case types.Point:
			return PointType.FormatValue(inner)
		case types.Linestring:
			return LinestringType.FormatValue(inner)
		case types.Polygon:
			return PolygonType.FormatValue(inner)
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
	return map[string]string{}
}

// IsValid implements TypeInfo interface.
func (ti *geometryType) IsValid(v types.Value) bool {
	if _, ok := v.(types.Null); ok || v == nil {
		return true
	}

	switch v.(type) {
	case types.Geometry,
		types.Point,
		types.Linestring,
		types.Polygon:
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
	return &geometryType{ti.sqlGeometryType.Promote().(sql.GeometryType), ti.innerType.Promote()}
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
	case *geometryType:
		return identityTypeConverter, false, nil
	case *inlineBlobType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *intType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *jsonType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *linestringType:
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
