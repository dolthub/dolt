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

// This is a dolt implementation of the MySQL type Point, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type linestringType struct {
	sqlLinestringType sql.LinestringType
}

var _ TypeInfo = (*linestringType)(nil)

var LinestringType = &linestringType{sql.Linestring}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *linestringType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Linestring); ok {
		return val, nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *linestringType) ReadFrom(nbf *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.ReadKind()
	switch k {
	case types.LinestringKind:
		s := reader.ReadString()
		return s, nil
	case types.NullKind:
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *linestringType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	if v == nil {
		return types.NullValue, nil
	}

	strVal, err := ti.sqlLinestringType.Convert(v)
	if err != nil {
		return nil, err
	}

	if val, ok := strVal.(string); ok {
		return types.Linestring(val), nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
}

// Equals implements TypeInfo interface.
func (ti *linestringType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*linestringType); ok {
		return ti.sqlLinestringType.Type() == ti2.sqlLinestringType.Type()
	}
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *linestringType) FormatValue(v types.Value) (*string, error) {
	if val, ok := v.(types.Linestring); ok {
		res, err := ti.ConvertNomsValueToValue(val)
		if err != nil {
			return nil, err
		}
		if resStr, ok := res.(string); ok {
			return &resStr, nil
		}
		return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v)
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v.Kind())
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *linestringType) GetTypeIdentifier() Identifier {
	return LinestringTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *linestringType) GetTypeParams() map[string]string {
	return map[string]string{}
}

// IsValid implements TypeInfo interface.
func (ti *linestringType) IsValid(v types.Value) bool {
	if val, ok := v.(types.Linestring); ok {
		_, err := ti.sqlLinestringType.Convert(string(val))
		if err != nil {
			return false
		}
		return true
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return true
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *linestringType) NomsKind() types.NomsKind {
	return types.LinestringKind
}

// ParseValue implements TypeInfo interface.
func (ti *linestringType) ParseValue(ctx context.Context, vrw types.ValueReadWriter, str *string) (types.Value, error) {
	if str == nil || *str == "" {
		return types.NullValue, nil
	}
	return ti.ConvertValueToNomsValue(context.Background(), nil, *str)
}

// Promote implements TypeInfo interface.
func (ti *linestringType) Promote() TypeInfo {
	return &linestringType{ti.sqlLinestringType.Promote().(sql.LinestringType)}
}

// String implements TypeInfo interface.
func (ti *linestringType) String() string {
	return "Linestring()"
}

// ToSqlType implements TypeInfo interface.
func (ti *linestringType) ToSqlType() sql.Type {
	return ti.sqlLinestringType
}
