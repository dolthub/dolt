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
	"github.com/shopspring/decimal"

	"github.com/dolthub/dolt/go/store/types"
)

const (
	decimalTypeParam_Precision = "prec"
	decimalTypeParam_Scale     = "scale"
)

// This is a dolt implementation of the MySQL type Decimal, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type decimalType struct {
	sqlDecimalType sql.DecimalType
}

var _ TypeInfo = (*decimalType)(nil)

func CreateDecimalTypeFromParams(params map[string]string) (TypeInfo, error) {
	var precision uint8
	if precisionStr, ok := params[decimalTypeParam_Precision]; ok {
		precisionUint, err := strconv.ParseUint(precisionStr, 10, 8)
		if err != nil {
			return nil, err
		}
		precision = uint8(precisionUint)
	} else {
		return nil, fmt.Errorf(`create decimal type info is missing param "%v"`, decimalTypeParam_Precision)
	}
	var scale uint8
	if scaleStr, ok := params[decimalTypeParam_Scale]; ok {
		scaleUint, err := strconv.ParseUint(scaleStr, 10, 8)
		if err != nil {
			return nil, err
		}
		scale = uint8(scaleUint)
	} else {
		return nil, fmt.Errorf(`create decimal type info is missing param "%v"`, decimalTypeParam_Scale)
	}
	sqlDecimalType, err := sql.CreateDecimalType(precision, scale)
	if err != nil {
		return nil, err
	}
	return &decimalType{sqlDecimalType}, nil
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *decimalType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Decimal); ok {
		return decimal.Decimal(val).StringFixed(int32(ti.sqlDecimalType.Scale())), nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *decimalType) ReadFrom(_ *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.ReadKind()
	switch k {
	case types.DecimalKind:
		dec, err := reader.ReadDecimal()

		if err != nil {
			return nil, err
		}

		return dec.StringFixed(int32(ti.sqlDecimalType.Scale())), nil
	case types.NullKind:
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *decimalType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	if v == nil {
		return types.NullValue, nil
	}
	decVal, err := ti.sqlDecimalType.ConvertToDecimal(v)
	if err != nil {
		return nil, err
	}
	if !decVal.Valid {
		return nil, fmt.Errorf(`"%v" has unexpectedly encountered a null value from embedded type`, ti.String())
	}
	return types.Decimal(decVal.Decimal), nil
}

// Equals implements TypeInfo interface.
func (ti *decimalType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*decimalType); ok {
		return ti.sqlDecimalType.Precision() == ti2.sqlDecimalType.Precision() &&
			ti.sqlDecimalType.Scale() == ti2.sqlDecimalType.Scale()
	}
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *decimalType) FormatValue(v types.Value) (*string, error) {
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	strVal, err := ti.ConvertNomsValueToValue(v)
	if err != nil {
		return nil, err
	}
	val, ok := strVal.(string)
	if !ok {
		return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v)
	}
	return &val, nil
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *decimalType) GetTypeIdentifier() Identifier {
	return DecimalTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *decimalType) GetTypeParams() map[string]string {
	return map[string]string{
		decimalTypeParam_Precision: strconv.FormatUint(uint64(ti.sqlDecimalType.Precision()), 10),
		decimalTypeParam_Scale:     strconv.FormatUint(uint64(ti.sqlDecimalType.Scale()), 10),
	}
}

// IsValid implements TypeInfo interface.
func (ti *decimalType) IsValid(v types.Value) bool {
	if val, ok := v.(types.Decimal); ok {
		_, err := ti.sqlDecimalType.Convert(decimal.Decimal(val))
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
func (ti *decimalType) NomsKind() types.NomsKind {
	return types.DecimalKind
}

// Promote implements TypeInfo interface.
func (ti *decimalType) Promote() TypeInfo {
	return &decimalType{ti.sqlDecimalType.Promote().(sql.DecimalType)}
}

// String implements TypeInfo interface.
func (ti *decimalType) String() string {
	return fmt.Sprintf("Decimal(%v, %v)", ti.sqlDecimalType.Precision(), ti.sqlDecimalType.Scale())
}

// ToSqlType implements TypeInfo interface.
func (ti *decimalType) ToSqlType() sql.Type {
	return ti.sqlDecimalType
}

// decimalTypeConverter is an internal function for GetTypeConverter that handles the specific type as the source TypeInfo.
func decimalTypeConverter(ctx context.Context, src *decimalType, destTi TypeInfo) (tc TypeConverter, needsConversion bool, err error) {
	switch dest := destTi.(type) {
	case *bitType:
		return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
			if v == nil || v == types.NullValue {
				return types.NullValue, nil
			}
			val, ok := v.(types.Decimal)
			if !ok {
				return nil, fmt.Errorf("unexpected type converting decimal to bit: %T", v)
			}
			return dest.ConvertValueToNomsValue(ctx, vrw, decimal.Decimal(val))
		}, true, nil
	case *blobStringType:
		return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
			s, err := src.ConvertNomsValueToValue(v)
			if err != nil {
				return nil, err
			}
			return dest.ConvertValueToNomsValue(ctx, vrw, s)
		}, true, nil
	case *boolType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *datetimeType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *decimalType:
		return wrapIsValid(dest.IsValid, src, dest)
	case *enumType:
		if src.sqlDecimalType.Scale() > 0 {
			return nil, false, IncompatibleTypeConversion.New(src.String(), destTi.String())
		}
		return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
			if v == nil || v == types.NullValue {
				return types.NullValue, nil
			}
			val, ok := v.(types.Decimal)
			if !ok {
				return nil, fmt.Errorf("unexpected type converting decimal to enum: %T", v)
			}
			uintVal, err := sql.Uint64.Convert(decimal.Decimal(val))
			if err != nil {
				return nil, err
			}
			if uintVal.(uint64) == 0 {
				return types.Uint(0), nil
			}
			return dest.ConvertValueToNomsValue(ctx, vrw, uintVal)
		}, true, nil
	case *floatType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *inlineBlobType:
		return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
			s, err := src.ConvertNomsValueToValue(v)
			if err != nil {
				return nil, err
			}
			return dest.ConvertValueToNomsValue(ctx, vrw, s)
		}, true, nil
	case *intType:
		return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
			if v == nil || v == types.NullValue {
				return types.NullValue, nil
			}
			val, ok := v.(types.Decimal)
			if !ok {
				return nil, fmt.Errorf("unexpected type converting decimal to int: %T", v)
			}
			return dest.ConvertValueToNomsValue(ctx, vrw, decimal.Decimal(val).Round(0))
		}, true, nil
	case *jsonType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *linestringType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *pointType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *polygonType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *setType:
		return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
			s, err := src.ConvertNomsValueToValue(v)
			if err != nil {
				return nil, err
			}
			return dest.ConvertValueToNomsValue(ctx, vrw, s)
		}, true, nil
	case *timeType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *uintType:
		return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
			if v == nil || v == types.NullValue {
				return types.NullValue, nil
			}
			val, ok := v.(types.Decimal)
			if !ok {
				return nil, fmt.Errorf("unexpected type converting decimal to uint: %T", v)
			}
			return dest.ConvertValueToNomsValue(ctx, vrw, decimal.Decimal(val).Round(0))
		}, true, nil
	case *uuidType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *varBinaryType:
		return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
			s, err := src.ConvertNomsValueToValue(v)
			if err != nil {
				return nil, err
			}
			return dest.ConvertValueToNomsValue(ctx, vrw, s)
		}, true, nil
	case *varStringType:
		return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
			s, err := src.ConvertNomsValueToValue(v)
			if err != nil {
				return nil, err
			}
			return dest.ConvertValueToNomsValue(ctx, vrw, s)
		}, true, nil
	case *yearType:
		return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
			if v == nil || v == types.NullValue {
				return types.NullValue, nil
			}
			val, ok := v.(types.Decimal)
			if !ok {
				return nil, fmt.Errorf("unexpected type converting decimal to year: %T", v)
			}
			intVal, err := sql.Int64.Convert(decimal.Decimal(val))
			if err != nil {
				return nil, err
			}
			return dest.ConvertValueToNomsValue(ctx, vrw, intVal)
		}, true, nil
	default:
		return nil, false, UnhandledTypeConversion.New(src.String(), destTi.String())
	}
}
