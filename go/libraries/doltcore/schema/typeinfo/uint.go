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

type UintWidth int8

const (
	UintWidth8          UintWidth = 8
	UintWidth16         UintWidth = 16
	UintWidth24         UintWidth = 24
	UintWidth32         UintWidth = 32
	UintWidth64         UintWidth = 64
	uintTypeParam_Width           = "width"
)

const (
	MaxUint24 = 1 << 24
)

type uintImpl struct {
	Width UintWidth
}

var _ TypeInfo = (*uintImpl)(nil)
var (
	Uint8Type  TypeInfo = &uintImpl{UintWidth8}
	Uint16Type TypeInfo = &uintImpl{UintWidth16}
	Uint24Type TypeInfo = &uintImpl{UintWidth24}
	Uint32Type TypeInfo = &uintImpl{UintWidth32}
	Uint64Type TypeInfo = &uintImpl{UintWidth64}
)

func CreateUintTypeFromParams(params map[string]string) (TypeInfo, error) {
	if width, ok := params[uintTypeParam_Width]; ok {
		switch width {
		case "8":
			return Uint8Type, nil
		case "16":
			return Uint16Type, nil
		case "24":
			return Uint24Type, nil
		case "32":
			return Uint32Type, nil
		case "64":
			return Uint64Type, nil
		default:
			return nil, fmt.Errorf(`create uint type info has "%v" param with value "%v"`, uintTypeParam_Width, width)
		}
	}
	return nil, fmt.Errorf(`create uint type info is missing "%v" param`, uintTypeParam_Width)
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *uintImpl) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Uint); ok {
		switch ti.Width {
		case UintWidth8:
			return uint8(val), nil
		case UintWidth16:
			return uint16(val), nil
		case UintWidth24:
			return uint32(val), nil
		case UintWidth32:
			return uint32(val), nil
		case UintWidth64:
			return uint64(val), nil
		default:
			panic(fmt.Errorf(`uint width "%v" is not valid`, ti.Width))
		}
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *uintImpl) ConvertValueToNomsValue(v interface{}) (types.Value, error) {
	if artifact, ok := ti.isValid(v); ok {
		switch val := v.(type) {
		case nil:
			return types.NullValue, nil
		case bool:
			if val {
				return types.Uint(1), nil
			}
			return types.Uint(0), nil
		case int:
			return types.Uint(val), nil
		case int8:
			return types.Uint(val), nil
		case int16:
			return types.Uint(val), nil
		case int32:
			return types.Uint(val), nil
		case int64:
			return types.Uint(val), nil
		case uint:
			return types.Uint(val), nil
		case uint8:
			return types.Uint(val), nil
		case uint16:
			return types.Uint(val), nil
		case uint32:
			return types.Uint(val), nil
		case uint64:
			return types.Uint(val), nil
		case float32:
			return types.Uint(val), nil
		case float64:
			return types.Uint(val), nil
		case string:
			return types.Uint(artifact), nil
		case types.Null:
			return types.NullValue, nil
		case types.Bool:
			if val {
				return types.Uint(1), nil
			}
			return types.Uint(0), nil
		case types.Int:
			return types.Uint(val), nil
		case types.Uint:
			return val, nil
		case types.Float:
			return types.Uint(val), nil
		case types.String:
			return types.Uint(artifact), nil
		default:
			return nil, fmt.Errorf(`"%v" has falsely evaluated value "%v" of type "%T" as valid`, ti.String(), val, val)
		}
	}
	return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
}

// Equals implements TypeInfo interface.
func (ti *uintImpl) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*uintImpl); ok {
		return ti.Width == ti2.Width
	}
	return false
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *uintImpl) GetTypeIdentifier() Identifier {
	return UintTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *uintImpl) GetTypeParams() map[string]string {
	return map[string]string{uintTypeParam_Width: strconv.Itoa(int(ti.Width))}
}

// IsValid implements TypeInfo interface.
func (ti *uintImpl) IsValid(v interface{}) bool {
	_, ok := ti.isValid(v)
	return ok
}

// NomsKind implements TypeInfo interface.
func (ti *uintImpl) NomsKind() types.NomsKind {
	return types.UintKind
}

// String implements TypeInfo interface.
func (ti *uintImpl) String() string {
	switch ti.Width {
	case UintWidth8:
		return "Uint8"
	case UintWidth16:
		return "Uint16"
	case UintWidth24:
		return "Uint24"
	case UintWidth32:
		return "Uint32"
	case UintWidth64:
		return "Uint64"
	default:
		panic(fmt.Errorf(`uint width "%v" is not valid`, ti.Width))
	}
}

// ToSqlType implements TypeInfo interface.
func (ti *uintImpl) ToSqlType() sql.Type {
	switch ti.Width {
	case UintWidth8:
		return sql.Uint8
	case UintWidth16:
		return sql.Uint16
	case UintWidth24:
		return sql.Uint24
	case UintWidth32:
		return sql.Uint32
	case UintWidth64:
		return sql.Uint64
	default:
		panic(fmt.Errorf(`uint width "%v" is not valid`, ti.Width))
	}
}

// isValid is an internal implementation for the TypeInfo interface function IsValid.
// Some validity checks process the value into its final form, which may be returned
// as an artifact so that a value doesn't need to be processed twice in some scenarios.
func (ti *uintImpl) isValid(v interface{}) (artifact uint64, ok bool) {
	var maxValue uint64
	switch ti.Width {
	case UintWidth8:
		maxValue = math.MaxUint8
	case UintWidth16:
		maxValue = math.MaxUint16
	case UintWidth24:
		maxValue = MaxUint24
	case UintWidth32:
		maxValue = math.MaxUint32
	case UintWidth64:
		maxValue = math.MaxUint64
	default:
		panic(fmt.Errorf(`uint width "%v" is not valid`, ti.Width))
	}

	switch val := v.(type) {
	case nil:
		return 0, true
	case bool:
		return 0, true
	case int:
		return 0, val >= 0 && uint64(val) <= maxValue
	case int8:
		return 0, val >= 0
	case int16:
		return 0, val >= 0 && uint64(val) <= maxValue
	case int32:
		return 0, val >= 0 && uint64(val) <= maxValue
	case int64:
		return 0, val >= 0 && uint64(val) <= maxValue
	case uint:
		return 0, uint64(val) <= maxValue
	case uint8:
		return 0, uint64(val) <= maxValue
	case uint16:
		return 0, uint64(val) <= maxValue
	case uint32:
		return 0, uint64(val) <= maxValue
	case uint64:
		return 0, val <= maxValue
	case float32:
		return 0, val >= 0 && uint64(val) <= maxValue
	case float64:
		return 0, val >= 0 && uint64(val) <= maxValue
	case string:
		uintVal, err := strconv.ParseUint(val, 10, 64)
		return uintVal, err == nil
	case types.Null:
		return 0, true
	case types.Bool:
		return 0, true
	case types.Int:
		return 0, int64(val) >= 0 && uint64(val) <= maxValue
	case types.Uint:
		return 0, uint64(val) <= maxValue
	case types.Float:
		return 0, val >= 0 && uint64(val) <= maxValue
	case types.String:
		uintVal, err := strconv.ParseUint(string(val), 10, 64)
		return uintVal, err == nil
	default:
		return 0, false
	}
}
