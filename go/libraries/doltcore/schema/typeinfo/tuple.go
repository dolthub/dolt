// Copyright 2020 Dolthub, Inc.
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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/store/types"
)

type tupleType struct{}

var _ TypeInfo = (*tupleType)(nil)

// This is for internal use only. Used in merge conflicts.
var TupleType = &tupleType{}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *tupleType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if _, ok := v.(types.Null); ok {
		return nil, nil
	}
	return v, nil
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *tupleType) ReadFrom(_ *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.ReadKind()
	switch k {
	case types.NullKind:
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *tupleType) ConvertValueToNomsValue(v interface{}) (types.Value, error) {
	if tVal, ok := v.(types.Value); ok {
		return tVal, nil
	}
	if v == nil {
		return types.NullValue, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
}

// Equals implements TypeInfo interface.
func (ti *tupleType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	_, ok := other.(*tupleType)
	return ok
}

// FormatValue implements TypeInfo interface.
func (ti *tupleType) FormatValue(v types.Value) (*string, error) {
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a string`, ti.String(), v.Kind())
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *tupleType) GetTypeIdentifier() Identifier {
	return TupleTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *tupleType) GetTypeParams() map[string]string {
	panic("cannot persist tuple type")
}

// IsValid implements TypeInfo interface.
func (ti *tupleType) IsValid(v types.Value) bool {
	if v == nil {
		return true
	}
	_, ok := v.(types.Value)
	return ok
}

// NomsKind implements TypeInfo interface.
func (ti *tupleType) NomsKind() types.NomsKind {
	return types.TupleKind
}

// ParseValue implements TypeInfo interface.
func (ti *tupleType) ParseValue(str *string) (types.Value, error) {
	return nil, fmt.Errorf(`"%v" cannot parse strings`, ti.String())
}

// Promote implements TypeInfo interface.
func (ti *tupleType) Promote() TypeInfo {
	return ti
}

// String implements TypeInfo interface.
func (ti *tupleType) String() string {
	return "Tuple"
}

// ToSqlType implements TypeInfo interface.
func (ti *tupleType) ToSqlType() sql.Type {
	panic("we should never be calling the SQL type on a Tuple column")
}
