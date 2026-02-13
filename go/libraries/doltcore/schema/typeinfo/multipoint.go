// Copyright 2022 Dolthub, Inc.
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
type multipointType struct {
	sqlMultiPointType gmstypes.MultiPointType
}

var _ TypeInfo = (*multipointType)(nil)

var MultiPointType = &multipointType{gmstypes.MultiPointType{}}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *multipointType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	// Check for null
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	// Expect a types.MultiPoint, return a sql.MultiPoint
	if val, ok := v.(types.MultiPoint); ok {
		return types.ConvertTypesMultiPointToSQLMultiPoint(val), nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *multipointType) ReadFrom(nbf *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.ReadKind()
	switch k {
	case types.MultiPointKind:
		p, err := reader.ReadMultiPoint()
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
func (ti *multipointType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	// Check for null
	if v == nil {
		return types.NullValue, nil
	}

	// Convert to sql.MultiPointType
	multipoint, _, err := ti.sqlMultiPointType.Convert(ctx, v)
	if err != nil {
		return nil, err
	}

	return types.ConvertSQLMultiPointToTypesMultiPoint(multipoint.(gmstypes.MultiPoint)), nil
}

// Equals implements TypeInfo interface.
func (ti *multipointType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if o, ok := other.(*multipointType); ok {
		// if either ti or other has defined SRID, then check SRID value; otherwise,
		return (!ti.sqlMultiPointType.DefinedSRID && !o.sqlMultiPointType.DefinedSRID) || ti.sqlMultiPointType.SRID == o.sqlMultiPointType.SRID
	}
	return false
}

// IsValid implements TypeInfo interface.
func (ti *multipointType) IsValid(v types.Value) bool {
	if _, ok := v.(types.MultiPoint); ok {
		return true
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return true
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *multipointType) NomsKind() types.NomsKind {
	return types.MultiPointKind
}

// Promote implements TypeInfo interface.
func (ti *multipointType) Promote() TypeInfo {
	return &multipointType{ti.sqlMultiPointType.Promote().(gmstypes.MultiPointType)}
}

// String implements TypeInfo interface.
func (ti *multipointType) String() string {
	return "multipoint"
}

// ToSqlType implements TypeInfo interface.
func (ti *multipointType) ToSqlType() sql.Type {
	return ti.sqlMultiPointType
}
