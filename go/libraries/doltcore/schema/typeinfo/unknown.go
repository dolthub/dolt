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
	"errors"
	"fmt"

	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/store/types"
)

type unknownImpl struct{}

var _ TypeInfo = (*unknownImpl)(nil)

// AppliesToValue implements TypeInfo interface.
func (ti *unknownImpl) AppliesToKind(kind types.NomsKind) error {
	return fmt.Errorf(`kind "%v" is not applicable to typeinfo "Unknown"`, kind.String())
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *unknownImpl) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	return nil, fmt.Errorf(`"Unknown" cannot convert any Noms value to a go value`)
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *unknownImpl) ConvertValueToNomsValue(v interface{}) (types.Value, error) {
	return nil, fmt.Errorf(`"Unknown" cannot convert any go value to a Noms value`)
}

// Equals implements TypeInfo interface.
func (ti *unknownImpl) Equals(TypeInfo) bool {
	return false
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *unknownImpl) GetTypeIdentifier() Identifier {
	return UnknownType
}

// GetTypeParams implements TypeInfo interface.
func (ti *unknownImpl) GetTypeParams() map[string]string {
	return nil
}

// IsValid implements TypeInfo interface.
func (ti *unknownImpl) IsValid(v interface{}) bool {
	return false
}

// PreferredNomsKind implements TypeInfo interface.
func (ti *unknownImpl) PreferredNomsKind() types.NomsKind {
	return types.UnknownKind
}

// String implements TypeInfo interface.
func (ti *unknownImpl) String() string {
	return "Unknown"
}

// ToSqlType implements TypeInfo interface.
func (ti *unknownImpl) ToSqlType() (sql.Type, error) {
	return nil, errors.New("unknown type info does not have a relevant SQL type")
}
