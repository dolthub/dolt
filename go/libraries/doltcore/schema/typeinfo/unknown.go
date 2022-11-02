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
	"context"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/store/types"
)

type unknownType struct{}

var _ TypeInfo = (*unknownType)(nil)

var UnknownType TypeInfo = &unknownType{}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *unknownType) ConvertNomsValueToValue(types.Value) (interface{}, error) {
	return nil, fmt.Errorf(`"Unknown" cannot convert any Noms value to a go value`)
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *unknownType) ReadFrom(_ *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	return nil, fmt.Errorf(`"Unknown" cannot read any Noms value to a go value`)
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *unknownType) ConvertValueToNomsValue(context.Context, types.ValueReadWriter, interface{}) (types.Value, error) {
	return nil, fmt.Errorf(`"Unknown" cannot convert any go value to a Noms value`)
}

// Equals implements TypeInfo interface.
func (ti *unknownType) Equals(TypeInfo) bool {
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *unknownType) FormatValue(types.Value) (*string, error) {
	return nil, fmt.Errorf(`"Unknown" cannot convert any Noms value to a string`)
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *unknownType) GetTypeIdentifier() Identifier {
	return UnknownTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *unknownType) GetTypeParams() map[string]string {
	panic("cannot persist unknown type")
}

// IsValid implements TypeInfo interface.
func (ti *unknownType) IsValid(types.Value) bool {
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *unknownType) NomsKind() types.NomsKind {
	return types.UnknownKind
}

// Promote implements TypeInfo interface.
func (ti *unknownType) Promote() TypeInfo {
	return ti
}

// String implements TypeInfo interface.
func (ti *unknownType) String() string {
	return "Unknown"
}

// ToSqlType implements TypeInfo interface.
func (ti *unknownType) ToSqlType() sql.Type {
	panic(fmt.Errorf("unknown type info does not have a relevant SQL type"))
}
