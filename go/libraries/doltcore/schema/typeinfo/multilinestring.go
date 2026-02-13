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

	"github.com/dolthub/dolt/go/store/types"
)

// This is a dolt implementation of the MySQL type Point, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type multilinestringType struct {
	sqlMultiLineStringType gmstypes.MultiLineStringType
}

var _ TypeInfo = (*multilinestringType)(nil)

var MultiLineStringType = &multilinestringType{gmstypes.MultiLineStringType{}}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *multilinestringType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	// Check for null
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	// Expect a types.MultiLineString, return a sql.MultiLineString
	if val, ok := v.(types.MultiLineString); ok {
		return types.ConvertTypesMultiLineStringToSQLMultiLineString(val), nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *multilinestringType) ReadFrom(nbf *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.ReadKind()
	switch k {
	case types.MultiLineStringKind:
		p, err := reader.ReadMultiLineString()
		if err != nil {
			return nil, err
		}
		return ti.ConvertNomsValueToValue(p)
	case types.NullKind:
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *multilinestringType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	// Check for null
	if v == nil {
		return types.NullValue, nil
	}

	// Convert to sql.MultiLineString
	mline, _, err := ti.sqlMultiLineStringType.Convert(ctx, v)
	if err != nil {
		return nil, err
	}

	return types.ConvertSQLMultiLineStringToTypesMultiLineString(mline.(gmstypes.MultiLineString)), nil
}

// Equals implements TypeInfo interface.
func (ti *multilinestringType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if o, ok := other.(*multilinestringType); ok {
		// if either ti or other has defined SRID, then check SRID value; otherwise,
		return (!ti.sqlMultiLineStringType.DefinedSRID && !o.sqlMultiLineStringType.DefinedSRID) || ti.sqlMultiLineStringType.SRID == o.sqlMultiLineStringType.SRID
	}
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *multilinestringType) FormatValue(v types.Value) (*string, error) {
	if val, ok := v.(types.MultiLineString); ok {
		resStr := string(types.SerializeMultiLineString(val))
		return &resStr, nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v.Kind())
}

// IsValid implements TypeInfo interface.
func (ti *multilinestringType) IsValid(v types.Value) bool {
	if _, ok := v.(types.MultiLineString); ok {
		return true
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return true
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *multilinestringType) NomsKind() types.NomsKind {
	return types.MultiLineStringKind
}

// Promote implements TypeInfo interface.
func (ti *multilinestringType) Promote() TypeInfo {
	return &multilinestringType{ti.sqlMultiLineStringType.Promote().(gmstypes.MultiLineStringType)}
}

// String implements TypeInfo interface.
func (ti *multilinestringType) String() string {
	return "MultiLineString"
}

// ToSqlType implements TypeInfo interface.
func (ti *multilinestringType) ToSqlType() sql.Type {
	return ti.sqlMultiLineStringType
}
