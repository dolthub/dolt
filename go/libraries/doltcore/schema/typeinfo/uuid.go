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

	"github.com/google/uuid"
	"github.com/src-d/go-mysql-server/sql"
	"vitess.io/vitess/go/sqltypes"

	"github.com/liquidata-inc/dolt/go/store/types"
)

type uuidImpl struct{}

var _ TypeInfo = (*uuidImpl)(nil)

var UuidType TypeInfo = &uuidImpl{}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *uuidImpl) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.UUID); ok {
		return val.String(), nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *uuidImpl) ConvertValueToNomsValue(v interface{}) (types.Value, error) {
	if artifact, ok := ti.isValid(v); ok {
		switch val := v.(type) {
		case nil:
			return types.NullValue, nil
		case string:
			return types.UUID(artifact), nil
		case types.Null:
			return types.NullValue, nil
		case uuid.UUID:
			return types.UUID(val), nil
		case types.String:
			return types.UUID(artifact), nil
		case types.UUID:
			return val, nil
		default:
			return nil, fmt.Errorf(`"%v" has falsely evaluated value "%v" of type "%T" as valid`, ti.String(), val, val)
		}
	}
	return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
}

// Equals implements TypeInfo interface.
func (ti *uuidImpl) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	_, ok := other.(*uuidImpl)
	return ok
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *uuidImpl) GetTypeIdentifier() Identifier {
	return UuidTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *uuidImpl) GetTypeParams() map[string]string {
	return nil
}

// IsValid implements TypeInfo interface.
func (ti *uuidImpl) IsValid(v interface{}) bool {
	_, ok := ti.isValid(v)
	return ok
}

// NomsKind implements TypeInfo interface.
func (ti *uuidImpl) NomsKind() types.NomsKind {
	return types.UUIDKind
}

// String implements TypeInfo interface.
func (ti *uuidImpl) String() string {
	return "Uuid"
}

// ToSqlType implements TypeInfo interface.
func (ti *uuidImpl) ToSqlType() sql.Type {
	return sql.MustCreateString(sqltypes.Char, 36, sql.Collation_ascii_bin)
}

// isValid is an internal implementation for the TypeInfo interface function IsValid.
// Some validity checks process the value into its final form, which may be returned
// as an artifact so that a value doesn't need to be processed twice in some scenarios.
func (ti *uuidImpl) isValid(v interface{}) (artifact uuid.UUID, ok bool) {
	switch val := v.(type) {
	case nil:
		return uuid.UUID{}, true
	case string:
		valUuid, err := uuid.Parse(val)
		return valUuid, err == nil
	case types.Null:
		return uuid.UUID{}, true
	case uuid.UUID:
		return val, true
	case types.String:
		valUuid, err := uuid.Parse(string(val))
		return valUuid, err == nil
	case types.UUID:
		return uuid.UUID(val), true
	default:
		return uuid.UUID{}, false
	}
}
