// Copyright 2024 Dolthub, Inc.
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
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/store/types"
)

const (
	extendedTypeParams_string_encoded = "string_encoded"
)

// extendedType is a type that refers to an ExtendedType in GMS. These are only supported in the new format, and have many
// more limitations than traditional types (for now).
type extendedType struct {
	sqlExtendedType gmstypes.ExtendedType
}

var _ TypeInfo = (*extendedType)(nil)

// CreateExtendedTypeFromParams creates a TypeInfo from the given parameter map.
func CreateExtendedTypeFromParams(params map[string]string) (TypeInfo, error) {
	if encodedString, ok := params[extendedTypeParams_string_encoded]; ok {
		t, err := gmstypes.DeserializeTypeFromString(encodedString)
		if err != nil {
			return nil, err
		}
		return &extendedType{t}, nil
	}
	return nil, fmt.Errorf(`create extended type info is missing "%v" param`, extendedTypeParams_string_encoded)
}

// CreateExtendedTypeFromSqlType creates a TypeInfo from the given extended type.
func CreateExtendedTypeFromSqlType(typ gmstypes.ExtendedType) TypeInfo {
	return &extendedType{typ}
}

// ConvertNomsValueToValue implements the TypeInfo interface.
func (ti *extendedType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	return nil, fmt.Errorf(`"%v" is not valid in the old format`, ti.String())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *extendedType) ReadFrom(_ *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	return nil, fmt.Errorf(`"%v" is not valid in the old format`, ti.String())
}

// ConvertValueToNomsValue implements the TypeInfo interface.
func (ti *extendedType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	return nil, fmt.Errorf(`"%v" is not valid in the old format`, ti.String())
}

// Equals implements the TypeInfo interface.
func (ti *extendedType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*extendedType); ok {
		return ti.sqlExtendedType.Equals(ti2.sqlExtendedType)
	}
	return false
}

// FormatValue implements the TypeInfo interface.
func (ti *extendedType) FormatValue(v types.Value) (*string, error) {
	return nil, fmt.Errorf(`"%v" is not valid in the old format`, ti.String())
}

// GetTypeIdentifier implements the TypeInfo interface.
func (ti *extendedType) GetTypeIdentifier() Identifier {
	return ExtendedTypeIdentifier
}

// GetTypeParams implements the TypeInfo interface.
func (ti *extendedType) GetTypeParams() map[string]string {
	serializedString, err := gmstypes.SerializeTypeToString(ti.sqlExtendedType)
	if err != nil {
		panic(err)
	}
	return map[string]string{extendedTypeParams_string_encoded: serializedString}
}

// IsValid implements the TypeInfo interface.
func (ti *extendedType) IsValid(v types.Value) bool {
	return true
}

// NomsKind implements the TypeInfo interface.
func (ti *extendedType) NomsKind() types.NomsKind {
	return types.ExtendedKind
}

// Promote implements the TypeInfo interface.
func (ti *extendedType) Promote() TypeInfo {
	return &extendedType{ti.sqlExtendedType.Promote().(gmstypes.ExtendedType)}
}

// String implements the TypeInfo interface.
func (ti *extendedType) String() string {
	return ti.sqlExtendedType.String()
}

// ToSqlType implements the TypeInfo interface.
func (ti *extendedType) ToSqlType() sql.Type {
	return ti.sqlExtendedType
}
