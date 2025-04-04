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
	"math"
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/store/types"
)

type FloatWidth int8

const (
	floatTypeParam_Width    = "width"
	floatTypeParam_Width_32 = "32"
	floatTypeParam_Width_64 = "64"
)

type floatType struct {
	sqlFloatType sql.NumberType
}

var _ TypeInfo = (*floatType)(nil)
var (
	Float32Type = &floatType{gmstypes.Float32}
	Float64Type = &floatType{gmstypes.Float64}
)

func CreateFloatTypeFromParams(params map[string]string) (TypeInfo, error) {
	if width, ok := params[floatTypeParam_Width]; ok {
		switch width {
		case floatTypeParam_Width_32:
			return Float32Type, nil
		case floatTypeParam_Width_64:
			return Float64Type, nil
		default:
			return nil, fmt.Errorf(`create float type info has "%v" param with value "%v"`, floatTypeParam_Width, width)
		}
	}
	return nil, fmt.Errorf(`create float type info is missing "%v" param`, floatTypeParam_Width)
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *floatType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Float); ok {
		switch ti.sqlFloatType {
		case gmstypes.Float32:
			return float32(val), nil
		case gmstypes.Float64:
			return float64(val), nil
		}
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *floatType) ReadFrom(nbf *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.ReadKind()
	switch k {
	case types.FloatKind:
		f := reader.ReadFloat(nbf)
		switch ti.sqlFloatType {
		case gmstypes.Float32:
			return float32(f), nil
		case gmstypes.Float64:
			return f, nil
		}
	case types.NullKind:
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *floatType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	if v == nil {
		return types.NullValue, nil
	}
	fltVal, _, err := ti.sqlFloatType.Convert(ctx, v)
	if err != nil {
		return nil, err
	}
	switch val := fltVal.(type) {
	case float32:
		return types.Float(val), nil
	case float64:
		return types.Float(val), nil
	default:
		return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v)
	}
}

// Equals implements TypeInfo interface.
func (ti *floatType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*floatType); ok {
		return ti.sqlFloatType.Type() == ti2.sqlFloatType.Type()
	}
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *floatType) FormatValue(v types.Value) (*string, error) {
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	fltVal, err := ti.ConvertNomsValueToValue(v)
	if err != nil {
		return nil, err
	}
	switch val := fltVal.(type) {
	case float32:
		res := strconv.FormatFloat(float64(val), 'f', -1, 64)
		return &res, nil
	case float64:
		res := strconv.FormatFloat(val, 'f', -1, 64)
		return &res, nil
	default:
		return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v)
	}
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *floatType) GetTypeIdentifier() Identifier {
	return FloatTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *floatType) GetTypeParams() map[string]string {
	sqlParam := ""
	switch ti.sqlFloatType.Type() {
	case sqltypes.Float32:
		sqlParam = floatTypeParam_Width_32
	case sqltypes.Float64:
		sqlParam = floatTypeParam_Width_64
	default:
		panic(fmt.Errorf(`unknown float type info sql type "%v"`, ti.sqlFloatType.Type().String()))
	}
	return map[string]string{floatTypeParam_Width: sqlParam}
}

// IsValid implements TypeInfo interface.
func (ti *floatType) IsValid(v types.Value) bool {
	// TODO: Add context parameter
	ctx := sql.NewEmptyContext()
	if val, ok := v.(types.Float); ok {
		_, _, err := ti.sqlFloatType.Convert(ctx, float64(val))
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
func (ti *floatType) NomsKind() types.NomsKind {
	return types.FloatKind
}

// Promote implements TypeInfo interface.
func (ti *floatType) Promote() TypeInfo {
	return &floatType{ti.sqlFloatType.Promote().(sql.NumberType)}
}

// String implements TypeInfo interface.
func (ti *floatType) String() string {
	switch ti.sqlFloatType.Type() {
	case sqltypes.Float32:
		return "Float32"
	case sqltypes.Float64:
		return "Float64"
	default:
		panic(fmt.Errorf(`unknown float type info sql type "%v"`, ti.sqlFloatType.Type().String()))
	}
}

// ToSqlType implements TypeInfo interface.
func (ti *floatType) ToSqlType() sql.Type {
	return ti.sqlFloatType
}

// floatTypeConverter is an internal function for GetTypeConverter that handles the specific type as the source TypeInfo.
func floatTypeConverter(ctx context.Context, src *floatType, destTi TypeInfo) (tc TypeConverter, needsConversion bool, err error) {
	switch dest := destTi.(type) {
	case *bitType:
		return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
			if v == nil || v == types.NullValue {
				return types.NullValue, nil
			}
			val, ok := v.(types.Float)
			if !ok {
				return nil, fmt.Errorf("unexpected type converting float to enum: %T", v)
			}
			fltVal := floatTypeRoundToZero(float64(val))
			intVal, _, err := gmstypes.Int64.Convert(ctx, fltVal)
			if err != nil {
				return nil, err
			}
			return dest.ConvertValueToNomsValue(ctx, vrw, uint64(intVal.(int64)))
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
		return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
			if v == nil || v == types.NullValue {
				return types.NullValue, nil
			}
			val, ok := v.(types.Float)
			if !ok {
				return nil, fmt.Errorf("unexpected type converting float to enum: %T", v)
			}
			if val == 0 {
				return types.Uint(0), nil
			}
			return dest.ConvertValueToNomsValue(ctx, vrw, float64(val))
		}, true, nil
	case *floatType:
		return wrapIsValid(dest.IsValid, src, dest)
	case *geomcollType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *geometryType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *inlineBlobType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *intType:
		return floatTypeConverterRoundToZero(ctx, src, destTi)
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
		return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
			if v == nil || v == types.NullValue {
				return types.NullValue, nil
			}
			val, ok := v.(types.Float)
			if !ok {
				return nil, fmt.Errorf("unexpected type converting float to set: %T", v)
			}
			if float64(val) != math.Trunc(float64(val)) { // not a whole number
				return nil, fmt.Errorf("invalid set value: %v", float64(val))
			}
			return dest.ConvertValueToNomsValue(ctx, vrw, float64(val))
		}, true, nil
	case *timeType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *uintType:
		return floatTypeConverterRoundToZero(ctx, src, destTi)
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

func floatTypeRoundToZero(val float64) float64 {
	truncated := math.Trunc(val)
	if math.Abs(val-truncated) > 0.5 {
		return truncated + math.Copysign(1, val)
	}
	return truncated
}

func floatTypeConverterRoundToZero(ctx context.Context, src *floatType, destTi TypeInfo) (tc TypeConverter, needsConversion bool, err error) {
	return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
		if v == nil || v == types.NullValue {
			return types.NullValue, nil
		}
		val, ok := v.(types.Float)
		if !ok {
			return nil, fmt.Errorf("unexpected type converting float to %s: %T", strings.ToLower(destTi.String()), v)
		}
		return destTi.ConvertValueToNomsValue(ctx, vrw, floatTypeRoundToZero(float64(val)))
	}, true, nil
}
