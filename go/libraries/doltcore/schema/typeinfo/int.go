// Copyright 2020 Liquidata, Inc.
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
	"fmt"
	"math"
	"strconv"

	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/store/types"
)

type IntWidth int8

const (
	IntWidth8  IntWidth = 8
	IntWidth16 IntWidth = 16
	IntWidth24 IntWidth = 24
	IntWidth32 IntWidth = 32
	IntWidth64 IntWidth = 64
)

const (
	MaxInt24            = 1<<23 - 1
	MinInt24            = -1 << 23
	intTypeParams_Width = "width"
)

type intImpl struct {
	Width IntWidth
}

var _ TypeInfo = (*intImpl)(nil)
var (
	Int8Type  TypeInfo = &intImpl{IntWidth8}
	Int16Type TypeInfo = &intImpl{IntWidth16}
	Int24Type TypeInfo = &intImpl{IntWidth24}
	Int32Type TypeInfo = &intImpl{IntWidth32}
	Int64Type TypeInfo = &intImpl{IntWidth64}
)

func CreateIntTypeFromParams(params map[string]string) (TypeInfo, error) {
	if width, ok := params[intTypeParams_Width]; ok {
		switch width {
		case "8":
			return Int8Type, nil
		case "16":
			return Int16Type, nil
		case "24":
			return Int24Type, nil
		case "32":
			return Int32Type, nil
		case "64":
			return Int64Type, nil
		default:
			return nil, fmt.Errorf(`create int type info has "%v" param with value "%v"`, intTypeParams_Width, width)
		}
	}
	return nil, fmt.Errorf(`create int type info is missing "%v" param`, intTypeParams_Width)
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *intImpl) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Int); ok {
		switch ti.Width {
		case IntWidth8:
			return int8(val), nil
		case IntWidth16:
			return int16(val), nil
		case IntWidth24:
			return int32(val), nil
		case IntWidth32:
			return int32(val), nil
		case IntWidth64:
			return int64(val), nil
		default:
			panic(fmt.Errorf(`int width "%v" is not valid`, ti.Width))
		}
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *intImpl) ConvertValueToNomsValue(v interface{}) (types.Value, error) {
	if artifact, ok := ti.isValid(v); ok {
		switch val := v.(type) {
		case nil:
			return types.NullValue, nil
		case bool:
			if val {
				return types.Int(1), nil
			}
			return types.Int(0), nil
		case int:
			return types.Int(val), nil
		case int8:
			return types.Int(val), nil
		case int16:
			return types.Int(val), nil
		case int32:
			return types.Int(val), nil
		case int64:
			return types.Int(val), nil
		case uint:
			return types.Int(val), nil
		case uint8:
			return types.Int(val), nil
		case uint16:
			return types.Int(val), nil
		case uint32:
			return types.Int(val), nil
		case uint64:
			return types.Int(val), nil
		case float32:
			return types.Int(val), nil
		case float64:
			return types.Int(val), nil
		case string:
			return types.Int(artifact), nil
		case types.Null:
			return types.NullValue, nil
		case types.Bool:
			if val {
				return types.Int(1), nil
			}
			return types.Int(0), nil
		case types.Int:
			return val, nil
		case types.Uint:
			return types.Int(val), nil
		case types.Float:
			return types.Int(val), nil
		case types.String:
			return types.Int(artifact), nil
		default:
			return nil, fmt.Errorf(`"%v" has falsely evaluated value "%v" of type "%T" as valid`, ti.String(), val, val)
		}
	}
	return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
}

// Equals implements TypeInfo interface.
func (ti *intImpl) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*intImpl); ok {
		return ti.Width == ti2.Width
	}
	return false
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *intImpl) GetTypeIdentifier() Identifier {
	return IntTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *intImpl) GetTypeParams() map[string]string {
	return map[string]string{intTypeParams_Width: strconv.Itoa(int(ti.Width))}
}

// IsValid implements TypeInfo interface.
func (ti *intImpl) IsValid(v interface{}) bool {
	_, ok := ti.isValid(v)
	return ok
}

// NomsKind implements TypeInfo interface.
func (ti *intImpl) NomsKind() types.NomsKind {
	return types.IntKind
}

// String implements TypeInfo interface.
func (ti *intImpl) String() string {
	switch ti.Width {
	case IntWidth8:
		return "Int8"
	case IntWidth16:
		return "Int16"
	case IntWidth24:
		return "Int24"
	case IntWidth32:
		return "Int32"
	case IntWidth64:
		return "Int64"
	default:
		panic(fmt.Errorf(`int width "%v" is not valid`, ti.Width))
	}
}

// ToSqlType implements TypeInfo interface.
func (ti *intImpl) ToSqlType() sql.Type {
	switch ti.Width {
	case IntWidth8:
		return sql.Int8
	case IntWidth16:
		return sql.Int16
	case IntWidth24:
		return sql.Int24
	case IntWidth32:
		return sql.Int32
	case IntWidth64:
		return sql.Int64
	default:
		panic(fmt.Errorf(`int width "%v" is not valid`, ti.Width))
	}
}

// isValid is an internal implementation for the TypeInfo interface function IsValid.
// Some validity checks process the value into its final form, which may be returned
// as an artifact so that a value doesn't need to be processed twice in some scenarios.
func (ti *intImpl) isValid(v interface{}) (artifact int64, ok bool) {
	var minValue int64
	var maxValue int64
	switch ti.Width {
	case IntWidth8:
		minValue = math.MinInt8
		maxValue = math.MaxInt8
	case IntWidth16:
		minValue = math.MinInt16
		maxValue = math.MaxInt16
	case IntWidth24:
		minValue = MinInt24
		maxValue = MaxInt24
	case IntWidth32:
		minValue = math.MinInt32
		maxValue = math.MaxInt32
	case IntWidth64:
		minValue = math.MinInt64
		maxValue = math.MaxInt64
	default:
		panic(fmt.Errorf(`int width "%v" is not valid`, ti.Width))
	}

	switch val := v.(type) {
	case nil:
		return 0, true
	case bool:
		return 0, true
	case int:
		return 0, int64(val) >= minValue && int64(val) <= maxValue
	case int8:
		return 0, int64(val) >= minValue && int64(val) <= maxValue
	case int16:
		return 0, int64(val) >= minValue && int64(val) <= maxValue
	case int32:
		return 0, int64(val) >= minValue && int64(val) <= maxValue
	case int64:
		return 0, val >= minValue && val <= maxValue
	case uint:
		return 0, int64(val) <= maxValue
	case uint8:
		return 0, int64(val) <= maxValue
	case uint16:
		return 0, int64(val) <= maxValue
	case uint32:
		return 0, int64(val) <= maxValue
	case uint64:
		return 0, val <= uint64(maxValue)
	case float32:
		return 0, int64(val) >= minValue && int64(val) <= maxValue
	case float64:
		return 0, int64(val) >= minValue && int64(val) <= maxValue
	case string:
		intVal, err := strconv.ParseInt(val, 10, 64)
		return intVal, err == nil
	case types.Null:
		return 0, true
	case types.Bool:
		return 0, true
	case types.Int:
		return 0, int64(val) >= minValue && int64(val) <= maxValue
	case types.Uint:
		return 0, uint64(val) <= uint64(maxValue)
	case types.Float:
		return 0, int64(val) >= minValue && int64(val) <= maxValue
	case types.String:
		intVal, err := strconv.ParseInt(string(val), 10, 64)
		return intVal, err == nil
	default:
		return 0, false
	}
}
