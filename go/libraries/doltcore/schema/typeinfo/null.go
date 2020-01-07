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
	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/store/types"
)

type nullImpl struct{}

var _ TypeInfo = (*nullImpl)(nil)

// AppliesToValue implements TypeInfo interface.
func (ti *nullImpl) AppliesToKind(types.NomsKind) error {
	return nil
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *nullImpl) ConvertNomsValueToValue(types.Value) (interface{}, error) {
	return nil, nil
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *nullImpl) ConvertValueToNomsValue(v interface{}) (types.Value, error) {
	return types.NullValue, nil
}

// Equals implements TypeInfo interface.
func (ti *nullImpl) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*nullImpl); ok {
		return *ti == *ti2
	}
	return false
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *nullImpl) GetTypeIdentifier() Identifier {
	return NullType
}

// GetTypeParams implements TypeInfo interface.
func (ti *nullImpl) GetTypeParams() map[string]string {
	return nil
}

// IsValid implements TypeInfo interface.
func (ti *nullImpl) IsValid(interface{}) bool {
	return true
}

// PreferredNomsKind implements TypeInfo interface.
func (ti *nullImpl) PreferredNomsKind() types.NomsKind {
	return types.NullKind
}

// String implements TypeInfo interface.
func (ti *nullImpl) String() string {
	return "Null"
}

// ToSqlType implements TypeInfo interface.
func (ti *nullImpl) ToSqlType() (sql.Type, error) {
	return sql.Null, nil
}
