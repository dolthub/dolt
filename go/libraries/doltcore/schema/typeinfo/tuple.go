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

	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/store/types"
)

type tupleImpl struct{}

var _ TypeInfo = (*tupleImpl)(nil)

var TupleType TypeInfo = &tupleImpl{}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *tupleImpl) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	return v, nil
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *tupleImpl) ConvertValueToNomsValue(v interface{}) (types.Value, error) {
	if tVal, ok := v.(types.Value); ok {
		return tVal, nil
	}
	if v == nil {
		return types.NullValue, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
}

// Equals implements TypeInfo interface.
func (ti *tupleImpl) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	_, ok := other.(*tupleImpl)
	return ok
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *tupleImpl) GetTypeIdentifier() Identifier {
	return TupleIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *tupleImpl) GetTypeParams() map[string]string {
	return nil
}

// IsValid implements TypeInfo interface.
func (ti *tupleImpl) IsValid(v interface{}) bool {
	if v == nil {
		return true
	}
	_, ok := v.(types.Value)
	return ok
}

// NomsKind implements TypeInfo interface.
func (ti *tupleImpl) NomsKind() types.NomsKind {
	return types.TupleKind
}

// String implements TypeInfo interface.
func (ti *tupleImpl) String() string {
	return "Tuple"
}

// ToSqlType implements TypeInfo interface.
func (ti *tupleImpl) ToSqlType() sql.Type {
	return sql.LongText
}
