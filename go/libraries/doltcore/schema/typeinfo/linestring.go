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

	"github.com/dolthub/dolt/go/store/geometry"
	"github.com/dolthub/dolt/go/store/types"
)

// This is a dolt implementation of the MySQL type Point, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type linestringType struct {
	sqlLineStringType sql.LineStringType
}

var _ TypeInfo = (*linestringType)(nil)

var LineStringType = &linestringType{sql.LineStringType{}}

// ConvertTypesLineStringToSQLLineString basically makes a deep copy of sql.LineString
func ConvertTypesLineStringToSQLLineString(l types.LineString) sql.LineString {
	points := make([]sql.Point, len(l.Points))
	for i, p := range l.Points {
		points[i] = ConvertTypesPointToSQLPoint(p)
	}
	return sql.LineString{SRID: l.SRID, Points: points}
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *linestringType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	// Check for null
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	// Expect a types.LineString, return a sql.LineString
	if val, ok := v.(types.LineString); ok {
		return ConvertTypesLineStringToSQLLineString(val), nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *linestringType) ReadFrom(nbf *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.ReadKind()
	switch k {
	case types.LineStringKind:
		l, err := reader.ReadLineString()
		if err != nil {
			return nil, err
		}
		return ti.ConvertNomsValueToValue(l)
	case types.NullKind:
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
}

func ConvertSQLLineStringToTypesLineString(l sql.LineString) types.LineString {
	points := make([]types.Point, len(l.Points))
	for i, p := range l.Points {
		points[i] = ConvertSQLPointToTypesPoint(p)
	}
	return types.LineString{SRID: l.SRID, Points: points}
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *linestringType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	// Check for null
	if v == nil {
		return types.NullValue, nil
	}

	// Convert to sql.LineStringType
	line, err := ti.sqlLineStringType.Convert(v)
	if err != nil {
		return nil, err
	}

	return ConvertSQLLineStringToTypesLineString(line.(sql.LineString)), nil
}

// Equals implements TypeInfo interface.
func (ti *linestringType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if o, ok := other.(*linestringType); ok {
		// if either ti or other has defined SRID, then check SRID value; otherwise,
		return (!ti.sqlLineStringType.DefinedSRID && !o.sqlLineStringType.DefinedSRID) || ti.sqlLineStringType.SRID == o.sqlLineStringType.SRID
	}
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *linestringType) FormatValue(v types.Value) (*string, error) {
	if val, ok := v.(types.LineString); ok {
		buf := make([]byte, geometry.EWKBHeaderSize+types.LengthSize+geometry.PointSize*len(val.Points))
		types.WriteEWKBHeader(val, buf[:geometry.EWKBHeaderSize])
		types.WriteEWKBLineData(val, buf[geometry.EWKBHeaderSize:])
		resStr := string(buf)
		return &resStr, nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v.Kind())
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *linestringType) GetTypeIdentifier() Identifier {
	return LineStringTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *linestringType) GetTypeParams() map[string]string {
	return map[string]string{"SRID": strconv.FormatUint(uint64(ti.sqlLineStringType.SRID), 10),
		"DefinedSRID": strconv.FormatBool(ti.sqlLineStringType.DefinedSRID)}
}

// IsValid implements TypeInfo interface.
func (ti *linestringType) IsValid(v types.Value) bool {
	if _, ok := v.(types.LineString); ok {
		return true
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return true
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *linestringType) NomsKind() types.NomsKind {
	return types.LineStringKind
}

// Promote implements TypeInfo interface.
func (ti *linestringType) Promote() TypeInfo {
	return &linestringType{ti.sqlLineStringType.Promote().(sql.LineStringType)}
}

// String implements TypeInfo interface.
func (ti *linestringType) String() string {
	return "LineString"
}

// ToSqlType implements TypeInfo interface.
func (ti *linestringType) ToSqlType() sql.Type {
	return ti.sqlLineStringType
}

// linestringTypeConverter is an internal function for GetTypeConverter that handles the specific type as the source TypeInfo.
func linestringTypeConverter(ctx context.Context, src *linestringType, destTi TypeInfo) (tc TypeConverter, needsConversion bool, err error) {
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
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
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

func CreateLineStringTypeFromParams(params map[string]string) (TypeInfo, error) {
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
	return &linestringType{sqlLineStringType: sql.LineStringType{SRID: uint32(sridVal), DefinedSRID: def}}, nil
}
