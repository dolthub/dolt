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

	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/store/types"
)

type int64Impl struct{}

var _ TypeInfo = (*int64Impl)(nil)

// AppliesToValue implements TypeInfo interface.
func (ti *int64Impl) AppliesToKind(kind types.NomsKind) error {
	if kind != types.IntKind {
		return fmt.Errorf(`kind "%v" is not applicable to typeinfo "%v"`, kind.String(), ti.String())
	}
	return nil
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *int64Impl) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Int); ok {
		return int64(val), nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *int64Impl) ConvertValueToNomsValue(v interface{}) (types.Value, error) {
	return intConvertValueToNomsValue(ti, v)
}

// Equals implements TypeInfo interface.
func (ti *int64Impl) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*int64Impl); ok {
		return *ti == *ti2
	}
	return false
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *int64Impl) GetTypeIdentifier() Identifier {
	return Int64Type
}

// GetTypeParams implements TypeInfo interface.
func (ti *int64Impl) GetTypeParams() map[string]string {
	return nil
}

// IsValid implements TypeInfo interface.
func (ti *int64Impl) IsValid(v interface{}) bool {
	switch val := v.(type) {
	case bool:
		return true
	case int:
		return true
	case int8:
		return true
	case int16:
		return true
	case int32:
		return true
	case int64:
		return true
	case uint:
		return val <= math.MaxInt64
	case uint8:
		return true
	case uint16:
		return true
	case uint32:
		return true
	case uint64:
		return val <= math.MaxInt64
	default:
		return false
	}
}

// PreferredNomsKind implements TypeInfo interface.
func (ti *int64Impl) PreferredNomsKind() types.NomsKind {
	return types.IntKind
}

// String implements TypeInfo interface.
func (ti *int64Impl) String() string {
	return "Int64"
}

// ToSqlType implements TypeInfo interface.
func (ti *int64Impl) ToSqlType() (sql.Type, error) {
	return sql.Int64, nil
}
