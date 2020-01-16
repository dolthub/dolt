// Copyright 2019 Liquidata, Inc.
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

	"github.com/liquidata-inc/dolt/go/store/types"
	"github.com/src-d/go-mysql-server/sql"
)

type boolImpl struct{}

var _ TypeInfo = (*boolImpl)(nil)

func CreateBoolType(map[string]string) (TypeInfo, error) {
	return &boolImpl{}, nil
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *boolImpl) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Bool); ok {
		if val {
			return uint64(1), nil
		}
		return uint64(0), nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *boolImpl) ConvertValueToNomsValue(v interface{}) (types.Value, error) {
	if artifact, ok := ti.isValid(v); ok {
		switch val := v.(type) {
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
			return types.Bool(artifact != 0), nil
		case types.Bool:
			return val, nil
		case types.Int:
			return types.Bool(val != 0), nil
		case types.Uint:
			return types.Bool(val != 0), nil
		case types.Float:
			return types.Bool(int64(math.Round(float64(val))) != 0), nil
		case types.String:
			return types.Bool(artifact != 0), nil
		default:
			return nil, fmt.Errorf(`"%v" has falsely evaluated value "%v" of type "%T" as valid`, ti.String(), val, val)
		}
	}
	return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
}

// Equals implements TypeInfo interface.
func (ti *boolImpl) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	_, ok := other.(*boolImpl)
	return ok
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *boolImpl) GetTypeIdentifier() Identifier {
	return BoolType
}

// GetTypeParams implements TypeInfo interface.
func (ti *boolImpl) GetTypeParams() map[string]string {
	return nil
}

// IsValid implements TypeInfo interface.
func (ti *boolImpl) IsValid(v interface{}) bool {
	_, ok := ti.isValid(v)
	return ok
}

// NomsKind implements TypeInfo interface.
func (ti *boolImpl) NomsKind() types.NomsKind {
	return types.BoolKind
}

// String implements TypeInfo interface.
func (ti *boolImpl) String() string {
	return "Bool"
}

// ToSqlType implements TypeInfo interface.
func (ti *boolImpl) ToSqlType() sql.Type {
	return sql.MustCreateBitType(1)
}

// isValid is an internal implementation for the TypeInfo interface function IsValid.
// Some validity checks process the value into its final form, which may be returned
// as an artifact so that a value doesn't need to be processed twice in some scenarios.
func (ti *boolImpl) isValid(v interface{}) (artifact int64, ok bool) {
	switch val := v.(type) {
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
		return 0, true
	case string:
		valInt, err := strconv.ParseInt(val, 10, 64)
		return valInt, err == nil
	case types.Bool:
		return 0, true
	case types.Int:
		return 0, true
	case types.Uint:
		return 0, true
	case types.Float:
		return 0, true
	case types.String:
		valInt, err := strconv.ParseInt(string(val), 10, 64)
		return valInt, err == nil
	default:
		return 0, false
	}
}
