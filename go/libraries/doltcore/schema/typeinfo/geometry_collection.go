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
type geomcollType struct {
	sqlGeomCollType gmstypes.GeomCollType
}

var _ TypeInfo = (*geomcollType)(nil)

var GeomCollType = &geomcollType{gmstypes.GeomCollType{}}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *geomcollType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	// Check for null
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	// Expect a types.GeomColl, return a sql.GeomColl
	if val, ok := v.(types.GeomColl); ok {
		return types.ConvertTypesGeomCollToSQLGeomColl(val), nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *geomcollType) ReadFrom(nbf *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.ReadKind()
	switch k {
	case types.GeometryCollectionKind:
		p, err := reader.ReadGeomColl()
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
func (ti *geomcollType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	// Check for null
	if v == nil {
		return types.NullValue, nil
	}

	// Convert to sql.GeomColl
	geomColl, _, err := ti.sqlGeomCollType.Convert(ctx, v)
	if err != nil {
		return nil, err
	}

	return types.ConvertSQLGeomCollToTypesGeomColl(geomColl.(gmstypes.GeomColl)), nil
}

// Equals implements TypeInfo interface.
func (ti *geomcollType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if o, ok := other.(*geomcollType); ok {
		// if either ti or other has defined SRID, then check SRID value; otherwise,
		return (!ti.sqlGeomCollType.DefinedSRID && !o.sqlGeomCollType.DefinedSRID) || ti.sqlGeomCollType.SRID == o.sqlGeomCollType.SRID
	}
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *geomcollType) FormatValue(v types.Value) (*string, error) {
	if val, ok := v.(types.GeomColl); ok {
		resStr := string(types.SerializeGeomColl(val))
		return &resStr, nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v.Kind())
}

// IsValid implements TypeInfo interface.
func (ti *geomcollType) IsValid(v types.Value) bool {
	if _, ok := v.(types.GeomColl); ok {
		return true
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return true
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *geomcollType) NomsKind() types.NomsKind {
	return types.GeometryCollectionKind
}

// Promote implements TypeInfo interface.
func (ti *geomcollType) Promote() TypeInfo {
	return &geomcollType{ti.sqlGeomCollType.Promote().(gmstypes.GeomCollType)}
}

// String implements TypeInfo interface.
func (ti *geomcollType) String() string {
	return "GeometryCollection"
}

// ToSqlType implements TypeInfo interface.
func (ti *geomcollType) ToSqlType() sql.Type {
	return ti.sqlGeomCollType
}
