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
type pointType struct {
	sqlPointType gmstypes.PointType
}

var _ TypeInfo = (*pointType)(nil)

var PointType = &pointType{gmstypes.PointType{}}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *pointType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	// Check for null
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	// Expect a types.Point, return a sql.Point
	if val, ok := v.(types.Point); ok {
		return types.ConvertTypesPointToSQLPoint(val), nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *pointType) ReadFrom(nbf *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.ReadKind()
	switch k {
	case types.PointKind:
		p, err := reader.ReadPoint()
		if err != nil {
			return nil, err
		}
		return ti.ConvertNomsValueToValue(p)
	case types.NullKind:
		return nil, nil
	default:
		return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
	}
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *pointType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	// Check for null
	if v == nil {
		return types.NullValue, nil
	}

	// Convert to sql.PointType
	point, _, err := ti.sqlPointType.Convert(ctx, v)
	if err != nil {
		return nil, err
	}

	return types.ConvertSQLPointToTypesPoint(point.(gmstypes.Point)), nil
}

// Equals implements TypeInfo interface.
func (ti *pointType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if o, ok := other.(*pointType); ok {
		// if either ti or other has defined SRID, then check SRID value; otherwise,
		return (!ti.sqlPointType.DefinedSRID && !o.sqlPointType.DefinedSRID) || ti.sqlPointType.SRID == o.sqlPointType.SRID
	}
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *pointType) FormatValue(v types.Value) (*string, error) {
	if val, ok := v.(types.Point); ok {
		resStr := string(types.SerializePoint(val))
		return &resStr, nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v.Kind())
}

// IsValid implements TypeInfo interface.
func (ti *pointType) IsValid(v types.Value) bool {
	if _, ok := v.(types.Point); ok {
		return true
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return true
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *pointType) NomsKind() types.NomsKind {
	return types.PointKind
}

// Promote implements TypeInfo interface.
func (ti *pointType) Promote() TypeInfo {
	return &pointType{ti.sqlPointType.Promote().(gmstypes.PointType)}
}

// String implements TypeInfo interface.
func (ti *pointType) String() string {
	return "Point"
}

// ToSqlType implements TypeInfo interface.
func (ti *pointType) ToSqlType() sql.Type {
	return ti.sqlPointType
}

func CreatePointTypeFromSqlPointType(sqlPointType gmstypes.PointType) TypeInfo {
	return &pointType{sqlPointType: sqlPointType}
}
