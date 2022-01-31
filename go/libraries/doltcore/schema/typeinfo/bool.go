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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/store/types"
)

type boolType struct {
	sqlBitType sql.BitType
}

var _ TypeInfo = (*boolType)(nil)

var BoolType TypeInfo = &boolType{sql.MustCreateBitType(1)}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *boolType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Bool); ok {
		if val {
			return uint64(1), nil
		}
		return uint64(0), nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *boolType) ReadFrom(_ *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.ReadKind()
	switch k {
	case types.BoolKind:
		b := reader.ReadBool()
		if b {
			return uint64(1), nil
		}

		return uint64(0), nil

	case types.NullKind:
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *boolType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	switch val := v.(type) {
	case nil:
		return types.NullValue, nil
	case bool:
		return types.Bool(val), nil
	case int:
		return types.Bool(val != 0), nil
	case int8:
		return types.Bool(val != 0), nil
	case int16:
		return types.Bool(val != 0), nil
	case int32:
		return types.Bool(val != 0), nil
	case int64:
		return types.Bool(val != 0), nil
	case uint:
		return types.Bool(val != 0), nil
	case uint8:
		return types.Bool(val != 0), nil
	case uint16:
		return types.Bool(val != 0), nil
	case uint32:
		return types.Bool(val != 0), nil
	case uint64:
		return types.Bool(val != 0), nil
	case float32:
		return types.Bool(int64(math.Round(float64(val))) != 0), nil
	case float64:
		return types.Bool(int64(math.Round(val)) != 0), nil
	case string:
		b, err := strconv.ParseBool(val)
		if err == nil {
			return types.Bool(b), nil
		}
		valInt, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil, fmt.Errorf(`"%v" cannot convert value "%v" as it is invalid`, ti.String(), val)
		}
		return types.Bool(valInt != 0), nil
	case []byte:
		return ti.ConvertValueToNomsValue(context.Background(), nil, string(val))
	default:
		return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
	}
}

// Equals implements TypeInfo interface.
func (ti *boolType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	_, ok := other.(*boolType)
	return ok
}

// FormatValue implements TypeInfo interface.
func (ti *boolType) FormatValue(v types.Value) (*string, error) {
	if val, ok := v.(types.Bool); ok {
		res := ""
		if val {
			res = "1"
		} else {
			res = "0"
		}
		return &res, nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a string`, ti.String(), v.Kind())
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *boolType) GetTypeIdentifier() Identifier {
	return BoolTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *boolType) GetTypeParams() map[string]string {
	return nil
}

// IsValid implements TypeInfo interface.
func (ti *boolType) IsValid(v types.Value) bool {
	_, err := ti.ConvertNomsValueToValue(v)
	return err == nil
}

// NomsKind implements TypeInfo interface.
func (ti *boolType) NomsKind() types.NomsKind {
	return types.BoolKind
}

// Promote implements TypeInfo interface.
func (ti *boolType) Promote() TypeInfo {
	return ti
}

// String implements TypeInfo interface.
func (ti *boolType) String() string {
	return "Bool"
}

// ToSqlType implements TypeInfo interface.
func (ti *boolType) ToSqlType() sql.Type {
	return ti.sqlBitType
}

// boolTypeConverter is an internal function for GetTypeConverter that handles the specific type as the source TypeInfo.
func boolTypeConverter(ctx context.Context, src *boolType, destTi TypeInfo) (tc TypeConverter, needsConversion bool, err error) {
	switch dest := destTi.(type) {
	case *bitType:
		return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
			if v == nil || v == types.NullValue {
				return types.NullValue, nil
			}
			val := v.(types.Bool)
			if val {
				return types.Uint(1), nil
			} else {
				return types.Uint(0), nil
			}
		}, true, nil
	case *blobStringType:
		return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
			if v == nil || v == types.NullValue {
				return types.NullValue, nil
			}
			val := v.(types.Bool)
			var newVal int
			if val {
				newVal = 1
			} else {
				newVal = 0
			}
			return dest.ConvertValueToNomsValue(ctx, vrw, newVal)
		}, true, nil
	case *boolType:
		return identityTypeConverter, false, nil
	case *datetimeType:
		return nil, false, IncompatibleTypeConversion.New(src.String(), destTi.String())
	case *decimalType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *enumType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *floatType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *inlineBlobType:
		return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
			if v == nil || v == types.NullValue {
				return types.NullValue, nil
			}
			val := v.(types.Bool)
			var newVal int
			if val {
				newVal = 1
			} else {
				newVal = 0
			}
			return dest.ConvertValueToNomsValue(ctx, vrw, newVal)
		}, true, nil
	case *jsonType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *linestringType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *pointType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *polygonType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *intType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *setType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *timeType:
		return nil, false, IncompatibleTypeConversion.New(src.String(), destTi.String())
	case *uintType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *uuidType:
		return nil, false, IncompatibleTypeConversion.New(src.String(), destTi.String())
	case *varBinaryType:
		return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
			if v == nil || v == types.NullValue {
				return types.NullValue, nil
			}
			val := v.(types.Bool)
			var newVal int
			if val {
				newVal = 1
			} else {
				newVal = 0
			}
			return dest.ConvertValueToNomsValue(ctx, vrw, newVal)
		}, true, nil
	case *varStringType:
		return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
			if v == nil || v == types.NullValue {
				return types.NullValue, nil
			}
			val := v.(types.Bool)
			var newVal int
			if val {
				newVal = 1
			} else {
				newVal = 0
			}
			return dest.ConvertValueToNomsValue(ctx, vrw, newVal)
		}, true, nil
	case *yearType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	default:
		return nil, false, UnhandledTypeConversion.New(src.String(), destTi.String())
	}
}
