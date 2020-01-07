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

	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/store/types"
)

type uint24Impl struct{}

const (
	MaxUint24 = 1 << 24
)

var _ TypeInfo = (*uint24Impl)(nil)

// AppliesToValue implements TypeInfo interface.
func (ti *uint24Impl) AppliesToKind(kind types.NomsKind) error {
	if kind != types.UintKind {
		return fmt.Errorf(`kind "%v" is not applicable to typeinfo "%v"`, kind.String(), ti.String())
	}
	return nil
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *uint24Impl) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Uint); ok {
		return uint32(val), nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *uint24Impl) ConvertValueToNomsValue(v interface{}) (types.Value, error) {
	return uintConvertValueToNomsValue(ti, v)
}

// Equals implements TypeInfo interface.
func (ti *uint24Impl) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*uint24Impl); ok {
		return *ti == *ti2
	}
	return false
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *uint24Impl) GetTypeIdentifier() Identifier {
	return Uint24Type
}

// GetTypeParams implements TypeInfo interface.
func (ti *uint24Impl) GetTypeParams() map[string]string {
	return nil
}

// IsValid implements TypeInfo interface.
func (ti *uint24Impl) IsValid(v interface{}) bool {
	switch val := v.(type) {
	case bool:
		return true
	case int:
		return val >= 0 && val <= MaxUint24
	case int8:
		return val >= 0
	case int16:
		return val >= 0
	case int32:
		return val >= 0 && val <= MaxUint24
	case int64:
		return val >= 0 && val <= MaxUint24
	case uint:
		return val <= MaxUint24
	case uint8:
		return true
	case uint16:
		return true
	case uint32:
		return val <= MaxUint24
	case uint64:
		return val <= MaxUint24
	default:
		return false
	}
}

// PreferredNomsKind implements TypeInfo interface.
func (ti *uint24Impl) PreferredNomsKind() types.NomsKind {
	return types.UintKind
}

// String implements TypeInfo interface.
func (ti *uint24Impl) String() string {
	return "Uint24"
}

// ToSqlType implements TypeInfo interface.
func (ti *uint24Impl) ToSqlType() (sql.Type, error) {
	return sql.Uint24, nil
}
