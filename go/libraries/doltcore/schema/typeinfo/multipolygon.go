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
type multipolygonType struct {
	sqlMultiPolygonType gmstypes.MultiPolygonType
}

var _ TypeInfo = (*multipolygonType)(nil)

var MultiPolygonType = &multipolygonType{gmstypes.MultiPolygonType{}}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *multipolygonType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	// Check for null
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	// Expect a types.MultiPolygon, return a sql.MultiPolygon
	if val, ok := v.(types.MultiPolygon); ok {
		return types.ConvertTypesMultiPolygonToSQLMultiPolygon(val), nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *multipolygonType) ReadFrom(nbf *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.ReadKind()
	switch k {
	case types.MultiPolygonKind:
		p, err := reader.ReadMultiPolygon()
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
func (ti *multipolygonType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	// Check for null
	if v == nil {
		return types.NullValue, nil
	}

	// Convert to sql.MultiPolygon
	mpoly, _, err := ti.sqlMultiPolygonType.Convert(ctx, v)
	if err != nil {
		return nil, err
	}

	return types.ConvertSQLMultiPolygonToTypesMultiPolygon(mpoly.(gmstypes.MultiPolygon)), nil
}

// Equals implements TypeInfo interface.
func (ti *multipolygonType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if o, ok := other.(*multipolygonType); ok {
		// if either ti or other has defined SRID, then check SRID value; otherwise,
		return (!ti.sqlMultiPolygonType.DefinedSRID && !o.sqlMultiPolygonType.DefinedSRID) || ti.sqlMultiPolygonType.SRID == o.sqlMultiPolygonType.SRID
	}
	return false
}

// IsValid implements TypeInfo interface.
func (ti *multipolygonType) IsValid(v types.Value) bool {
	if _, ok := v.(types.MultiPolygon); ok {
		return true
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return true
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *multipolygonType) NomsKind() types.NomsKind {
	return types.MultiPolygonKind
}

// Promote implements TypeInfo interface.
func (ti *multipolygonType) Promote() TypeInfo {
	return &multipolygonType{ti.sqlMultiPolygonType.Promote().(gmstypes.MultiPolygonType)}
}

// String implements TypeInfo interface.
func (ti *multipolygonType) String() string {
	return "MultiPolygon"
}

// ToSqlType implements TypeInfo interface.
func (ti *multipolygonType) ToSqlType() sql.Type {
	return ti.sqlMultiPolygonType
}
