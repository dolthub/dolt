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

type FloatWidth int8

const (
	FloatWidth32         FloatWidth = 32
	FloatWidth64         FloatWidth = 64
	floatTypeParam_Width            = "width"
)

type floatImpl struct {
	Width FloatWidth
}

var _ TypeInfo = (*floatImpl)(nil)
var (
	Float32Type TypeInfo = &floatImpl{FloatWidth32}
	Float64Type TypeInfo = &floatImpl{FloatWidth64}
)

func CreateFloatTypeFromParams(params map[string]string) (TypeInfo, error) {
	if width, ok := params[floatTypeParam_Width]; ok {
		switch width {
		case "32":
			return Float32Type, nil
		case "64":
			return Float64Type, nil
		default:
			return nil, fmt.Errorf(`create float type info has "%v" param with value "%v"`, floatTypeParam_Width, width)
		}
	}
	return nil, fmt.Errorf(`create float type info is missing "%v" param`, floatTypeParam_Width)
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *floatImpl) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Float); ok {
		switch ti.Width {
		case FloatWidth32:
			return float32(val), nil
		case FloatWidth64:
			return float64(val), nil
		default:
			panic(fmt.Errorf(`float width "%v" is not valid`, ti.Width))
		}
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *floatImpl) ConvertValueToNomsValue(v interface{}) (types.Value, error) {
	if artifact, ok := ti.isValid(v); ok {
		switch val := v.(type) {
		case nil:
			return types.NullValue, nil
		case bool:
			if val {
				return types.Float(1), nil
			}
			return types.Float(0), nil
		case int:
			return types.Float(val), nil
		case int8:
			return types.Float(val), nil
		case int16:
			return types.Float(val), nil
		case int32:
			return types.Float(val), nil
		case int64:
			return types.Float(val), nil
		case uint:
			return types.Float(val), nil
		case uint8:
			return types.Float(val), nil
		case uint16:
			return types.Float(val), nil
		case uint32:
			return types.Float(val), nil
		case uint64:
			return types.Float(val), nil
		case float32:
			return types.Float(val), nil
		case float64:
			return types.Float(val), nil
		case string:
			return types.Float(artifact), nil
		case types.Null:
			return types.NullValue, nil
		case types.Bool:
			if val {
				return types.Float(1), nil
			}
			return types.Float(0), nil
		case types.Int:
			return types.Float(val), nil
		case types.Uint:
			return types.Float(val), nil
		case types.Float:
			return val, nil
		case types.String:
			return types.Float(artifact), nil
		default:
			return nil, fmt.Errorf(`"%v" has falsely evaluated value "%v" of type "%T" as valid`, ti.String(), val, val)
		}
	}
	return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
}

// Equals implements TypeInfo interface.
func (ti *floatImpl) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*floatImpl); ok {
		return ti.Width == ti2.Width
	}
	return false
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *floatImpl) GetTypeIdentifier() Identifier {
	return FloatTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *floatImpl) GetTypeParams() map[string]string {
	return map[string]string{floatTypeParam_Width: strconv.Itoa(int(ti.Width))}
}

// IsValid implements TypeInfo interface.
func (ti *floatImpl) IsValid(v interface{}) bool {
	_, ok := ti.isValid(v)
	return ok
}

// NomsKind implements TypeInfo interface.
func (ti *floatImpl) NomsKind() types.NomsKind {
	return types.FloatKind
}

// String implements TypeInfo interface.
func (ti *floatImpl) String() string {
	switch ti.Width {
	case FloatWidth32:
		return "Float32"
	case FloatWidth64:
		return "Float64"
	default:
		panic(fmt.Errorf(`float width "%v" is not valid`, ti.Width))
	}
}

// ToSqlType implements TypeInfo interface.
func (ti *floatImpl) ToSqlType() sql.Type {
	switch ti.Width {
	case FloatWidth32:
		return sql.Float32
	case FloatWidth64:
		return sql.Float64
	default:
		panic(fmt.Errorf(`float width "%v" is not valid`, ti.Width))
	}
}

// isValid is an internal implementation for the TypeInfo interface function IsValid.
// Some validity checks process the value into its final form, which may be returned
// as an artifact so that a value doesn't need to be processed twice in some scenarios.
func (ti *floatImpl) isValid(v interface{}) (artifact float64, ok bool) {
	switch val := v.(type) {
	case nil:
		return 0, true
	case bool:
		return 0, true
	case int:
		return 0, true
	case int8:
		return 0, true
	case int16:
		return 0, true
	case int32:
		return 0, true
	case int64:
		return 0, true
	case uint:
		return 0, true
	case uint8:
		return 0, true
	case uint16:
		return 0, true
	case uint32:
		return 0, true
	case uint64:
		return 0, true
	case float32:
		return 0, true
	case float64:
		if ti.Width == FloatWidth32 {
			return 0, val >= -math.MaxFloat32 && val <= math.MaxFloat32
		}
		return 0, true
	case string:
		fltVal, err := strconv.ParseFloat(val, 64)
		return fltVal, err == nil
	case types.Null:
		return 0, true
	case types.Bool:
		return 0, true
	case types.Int:
		return 0, true
	case types.Uint:
		return 0, true
	case types.Float:
		if ti.Width == FloatWidth32 {
			return 0, val >= -math.MaxFloat32 && val <= math.MaxFloat32
		}
		return 0, true
	case types.String:
		fltVal, err := strconv.ParseFloat(string(val), 64)
		return fltVal, err == nil
	default:
		return 0, false
	}
}
