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

	"github.com/dolthub/dolt/go/store/geometry"
	"github.com/dolthub/dolt/go/store/types"
)

// This is a dolt implementation of the MySQL type Point, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type linestringType struct {
	sqlLinestringType sql.LinestringType
}

var _ TypeInfo = (*linestringType)(nil)

var LinestringType = &linestringType{sql.LinestringType{}}

// ConvertTypesLinestringToSQLLinestring basically makes a deep copy of sql.Linestring
func ConvertTypesLinestringToSQLLinestring(l types.Linestring) sql.Linestring {
	points := make([]sql.Point, len(l.Points))
	for i, p := range l.Points {
		points[i] = ConvertTypesPointToSQLPoint(p)
	}
	return sql.Linestring{SRID: l.SRID, Points: points}
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *linestringType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	// Expect a types.Linestring, return a sql.Linestring
	if val, ok := v.(types.Linestring); ok {
		return ConvertTypesLinestringToSQLLinestring(val), nil
	}
	// Check for null
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *linestringType) ReadFrom(nbf *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.ReadKind()
	switch k {
	case types.LinestringKind:
		l, err := reader.ReadLinestring()
		if err != nil {
			return nil, err
		}
		return ti.ConvertNomsValueToValue(l)
	case types.NullKind:
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
}

func ConvertSQLLinestringToTypesLinestring(l sql.Linestring) types.Linestring {
	points := make([]types.Point, len(l.Points))
	for i, p := range l.Points {
		points[i] = ConvertSQLPointToTypesPoint(p)
	}
	return types.Linestring{SRID: l.SRID, Points: points}
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *linestringType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	// Check for null
	if v == nil {
		return types.NullValue, nil
	}

	// Convert to sql.LinestringType
	line, err := ti.sqlLinestringType.Convert(v)
	if err != nil {
		return nil, err
	}

	return ConvertSQLLinestringToTypesLinestring(line.(sql.Linestring)), nil
}

// Equals implements TypeInfo interface.
func (ti *linestringType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	_, ok := other.(*linestringType)
	return ok
}

// FormatValue implements TypeInfo interface.
func (ti *linestringType) FormatValue(v types.Value) (*string, error) {
	if val, ok := v.(types.Linestring); ok {
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
	return LinestringTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *linestringType) GetTypeParams() map[string]string {
	return map[string]string{}
}

// IsValid implements TypeInfo interface.
func (ti *linestringType) IsValid(v types.Value) bool {
	if _, ok := v.(types.Linestring); ok {
		return true
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return true
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *linestringType) NomsKind() types.NomsKind {
	return types.LinestringKind
}

// Promote implements TypeInfo interface.
func (ti *linestringType) Promote() TypeInfo {
	return &linestringType{ti.sqlLinestringType.Promote().(sql.LinestringType)}
}

// String implements TypeInfo interface.
func (ti *linestringType) String() string {
	return "Linestring"
}

// ToSqlType implements TypeInfo interface.
func (ti *linestringType) ToSqlType() sql.Type {
	return ti.sqlLinestringType
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
	case *inlineBlobType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *intType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *jsonType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *linestringType:
		return identityTypeConverter, false, nil
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
