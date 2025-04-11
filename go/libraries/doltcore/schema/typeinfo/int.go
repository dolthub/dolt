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
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/store/types"
)

const (
	intTypeParams_Width    = "width"
	intTypeParams_Width_8  = "8"
	intTypeParams_Width_16 = "16"
	intTypeParams_Width_24 = "24"
	intTypeParams_Width_32 = "32"
	intTypeParams_Width_64 = "64"
)

type intType struct {
	sqlIntType sql.NumberType
}

var _ TypeInfo = (*intType)(nil)
var (
	Int8Type  = &intType{gmstypes.Int8}
	Int16Type = &intType{gmstypes.Int16}
	Int24Type = &intType{gmstypes.Int24}
	Int32Type = &intType{gmstypes.Int32}
	Int64Type = &intType{gmstypes.Int64}
)

func CreateIntTypeFromParams(params map[string]string) (TypeInfo, error) {
	if width, ok := params[intTypeParams_Width]; ok {
		switch width {
		case intTypeParams_Width_8:
			return Int8Type, nil
		case intTypeParams_Width_16:
			return Int16Type, nil
		case intTypeParams_Width_24:
			return Int24Type, nil
		case intTypeParams_Width_32:
			return Int32Type, nil
		case intTypeParams_Width_64:
			return Int64Type, nil
		default:
			return nil, fmt.Errorf(`create int type info has "%v" param with value "%v"`, intTypeParams_Width, width)
		}
	}
	return nil, fmt.Errorf(`create int type info is missing "%v" param`, intTypeParams_Width)
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *intType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Int); ok {
		switch ti.sqlIntType {
		case gmstypes.Int8:
			return int8(val), nil
		case gmstypes.Int16:
			return int16(val), nil
		case gmstypes.Int24:
			return int32(val), nil
		case gmstypes.Int32:
			return int32(val), nil
		case gmstypes.Int64:
			return int64(val), nil
		}
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *intType) ReadFrom(_ *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.ReadKind()
	switch k {
	case types.IntKind:
		val := reader.ReadInt()
		switch ti.sqlIntType {
		case gmstypes.Int8:
			return int8(val), nil
		case gmstypes.Int16:
			return int16(val), nil
		case gmstypes.Int24:
			return int32(val), nil
		case gmstypes.Int32:
			return int32(val), nil
		case gmstypes.Int64:
			return int64(val), nil
		}
	case types.NullKind:
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *intType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	if v == nil {
		return types.NullValue, nil
	}
	intVal, _, err := ti.sqlIntType.Convert(v)
	if err != nil {
		return nil, err
	}
	switch val := intVal.(type) {
	case int8:
		return types.Int(val), nil
	case int16:
		return types.Int(val), nil
	case int32:
		return types.Int(val), nil
	case int64:
		return types.Int(val), nil
	default:
		return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v)
	}
}

// Equals implements TypeInfo interface.
func (ti *intType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*intType); ok {
		return ti.sqlIntType.Type() == ti2.sqlIntType.Type() &&
			ti.sqlIntType.DisplayWidth() == ti2.sqlIntType.DisplayWidth()
	}
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *intType) FormatValue(v types.Value) (*string, error) {
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	intVal, err := ti.ConvertNomsValueToValue(v)
	if err != nil {
		return nil, err
	}
	switch val := intVal.(type) {
	case int8:
		res := strconv.FormatInt(int64(val), 10)
		return &res, nil
	case int16:
		res := strconv.FormatInt(int64(val), 10)
		return &res, nil
	case int32:
		res := strconv.FormatInt(int64(val), 10)
		return &res, nil
	case int64:
		res := strconv.FormatInt(val, 10)
		return &res, nil
	default:
		return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v)
	}
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *intType) GetTypeIdentifier() Identifier {
	return IntTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *intType) GetTypeParams() map[string]string {
	sqlParam := ""
	switch ti.sqlIntType.Type() {
	case sqltypes.Int8:
		sqlParam = intTypeParams_Width_8
	case sqltypes.Int16:
		sqlParam = intTypeParams_Width_16
	case sqltypes.Int24:
		sqlParam = intTypeParams_Width_24
	case sqltypes.Int32:
		sqlParam = intTypeParams_Width_32
	case sqltypes.Int64:
		sqlParam = intTypeParams_Width_64
	default:
		panic(fmt.Errorf(`unknown int type info sql type "%v"`, ti.sqlIntType.Type().String()))
	}
	return map[string]string{intTypeParams_Width: sqlParam}
}

// IsValid implements TypeInfo interface.
func (ti *intType) IsValid(v types.Value) bool {
	if val, ok := v.(types.Int); ok {
		_, _, err := ti.sqlIntType.Convert(int64(val))
		if err != nil {
			return false
		}
		return true
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return true
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *intType) NomsKind() types.NomsKind {
	return types.IntKind
}

// Promote implements TypeInfo interface.
func (ti *intType) Promote() TypeInfo {
	return &intType{ti.sqlIntType.Promote().(sql.NumberType)}
}

// String implements TypeInfo interface.
func (ti *intType) String() string {
	switch ti.sqlIntType.Type() {
	case sqltypes.Int8:
		return "Int8"
	case sqltypes.Int16:
		return "Int16"
	case sqltypes.Int24:
		return "Int24"
	case sqltypes.Int32:
		return "Int32"
	case sqltypes.Int64:
		return "Int64"
	default:
		panic(fmt.Errorf(`unknown int type info sql type "%v"`, ti.sqlIntType.Type().String()))
	}
}

// ToSqlType implements TypeInfo interface.
func (ti *intType) ToSqlType() sql.Type {
	return ti.sqlIntType
}

// intTypeConverter is an internal function for GetTypeConverter that handles the specific type as the source TypeInfo.
func intTypeConverter(ctx context.Context, src *intType, destTi TypeInfo) (tc TypeConverter, needsConversion bool, err error) {
	switch dest := destTi.(type) {
	case *bitType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *blobStringType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *boolType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *datetimeType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *decimalType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *enumType:
		return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
			if v == nil || v == types.NullValue {
				return types.NullValue, nil
			}
			val, ok := v.(types.Int)
			if !ok {
				return nil, fmt.Errorf("unexpected type converting int to enum: %T", v)
			}
			if val == 0 {
				return types.Uint(0), nil
			}
			return dest.ConvertValueToNomsValue(ctx, vrw, int64(val))
		}, true, nil
	case *floatType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *geomcollType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *geometryType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *inlineBlobType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *intType:
		return wrapIsValid(dest.IsValid, src, dest)
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
