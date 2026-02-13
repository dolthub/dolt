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
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/google/uuid"

	"github.com/dolthub/dolt/go/store/types"
)

type uuidType struct {
	sqlCharType sql.StringType
}

var _ TypeInfo = (*uuidType)(nil)

var UuidType = &uuidType{gmstypes.MustCreateString(sqltypes.Char, 36, sql.Collation_ascii_bin)}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *uuidType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.UUID); ok {
		return val.String(), nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *uuidType) ReadFrom(_ *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.ReadKind()
	switch k {
	case types.UUIDKind:
		val := reader.ReadUUID()
		return val.String(), nil
	case types.NullKind:
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *uuidType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	switch val := v.(type) {
	case nil:
		return types.NullValue, nil
	case string:
		valUuid, err := uuid.Parse(val)
		if err != nil {
			return nil, err
		}
		return types.UUID(valUuid), err
	case uuid.UUID:
		return types.UUID(val), nil
	default:
		return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
	}
}

// Equals implements TypeInfo interface.
func (ti *uuidType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	_, ok := other.(*uuidType)
	return ok
}

// FormatValue implements TypeInfo interface.
func (ti *uuidType) FormatValue(v types.Value) (*string, error) {
	if val, ok := v.(types.UUID); ok {
		res := val.String()
		return &res, nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a string`, ti.String(), v.Kind())
}

// IsValid implements TypeInfo interface.
func (ti *uuidType) IsValid(v types.Value) bool {
	if _, ok := v.(types.UUID); ok {
		return true
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return true
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *uuidType) NomsKind() types.NomsKind {
	return types.UUIDKind
}

// Promote implements TypeInfo interface.
func (ti *uuidType) Promote() TypeInfo {
	return ti
}

// String implements TypeInfo interface.
func (ti *uuidType) String() string {
	return "Uuid"
}

// ToSqlType implements TypeInfo interface.
func (ti *uuidType) ToSqlType() sql.Type {
	return ti.sqlCharType
}
