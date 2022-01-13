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
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/go-mysql-server/sql"
)

// This is a dolt implementation of the MySQL type Point, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type pointType struct {
	sqlPointType sql.PointType
}

var _ TypeInfo = (*pointType)(nil)

var PointType = &pointType{sql.PointType{}}

// ConvertTypesPointToSQLPoint basically makes a deep copy of sql.Point
func ConvertTypesPointToSQLPoint(p types.Point) sql.Point {
	return sql.Point{SRID: p.SRID, X: p.X, Y: p.Y}
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *pointType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	// Expect a types.Point, return a sql.Point
	if val, ok := v.(types.Point); ok {
		return ConvertTypesPointToSQLPoint(val), nil
	}
	// Check for null
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *pointType) ReadFrom(nbf *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.ReadKind()
	switch k {
	case types.PointKind:
		return reader.ReadPoint()
	case types.NullKind:
		return nil, nil
	default:
		return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
	}
}

// TODO: define constants for WKB?

func ConvertSQLPointToTypesPoint(p sql.Point) types.Point {
	return types.Point{SRID: p.SRID, X: p.X, Y: p.Y}
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *pointType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	// Check for null
	if v == nil {
		return types.NullValue, nil
	}

	// Convert to sql.PointType
	point, err := ti.sqlPointType.Convert(v)
	if err != nil {
		return nil, err
	}

	return ConvertSQLPointToTypesPoint(point.(sql.Point)), nil
}

// Equals implements TypeInfo interface.
func (ti *pointType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*pointType); ok {
		return ti.sqlPointType.Type() == ti2.sqlPointType.Type()
	}
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *pointType) FormatValue(v types.Value) (*string, error) {
	if val, ok := v.(types.Point); ok {
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
func (ti *pointType) GetTypeIdentifier() Identifier {
	return PointTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *pointType) GetTypeParams() map[string]string {
	return map[string]string{}
}

// IsValid implements TypeInfo interface.
func (ti *pointType) IsValid(v types.Value) bool {
	if val, ok := v.(types.Point); ok {
		_, err := ti.sqlPointType.Convert(val)
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
func (ti *pointType) NomsKind() types.NomsKind {
	return types.PointKind
}

// ParseValue implements TypeInfo interface.
func (ti *pointType) ParseValue(ctx context.Context, vrw types.ValueReadWriter, str *string) (types.Value, error) {
	if str == nil || *str == "" {
		return types.NullValue, nil
	}
	return ti.ConvertValueToNomsValue(context.Background(), nil, *str)
}

// Promote implements TypeInfo interface.
func (ti *pointType) Promote() TypeInfo {
	return &pointType{ti.sqlPointType.Promote().(sql.PointType)}
}

// String implements TypeInfo interface.
func (ti *pointType) String() string {
	return "Point()"
}

// ToSqlType implements TypeInfo interface.
func (ti *pointType) ToSqlType() sql.Type {
	return ti.sqlPointType
}
