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
type polygonType struct {
	sqlPolygonType sql.PolygonType
}

var _ TypeInfo = (*polygonType)(nil)

var PolygonType = &polygonType{sql.PolygonType{}}

// ConvertTypesPolygonToSQLPolygon basically makes a deep copy of sql.Linestring
func ConvertTypesPolygonToSQLPolygon(p types.Polygon) sql.Polygon {
	lines := make([]sql.Linestring, len(p.Lines))
	for i, l := range p.Lines {
		lines[i] = ConvertTypesLinestringToSQLLinestring(l)
	}
	return sql.Polygon{SRID: p.SRID, Lines: lines}
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *polygonType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	// Expect a types.Polygon, return a sql.Polygon
	if val, ok := v.(types.Polygon); ok {
		return ConvertTypesPolygonToSQLPolygon(val), nil
	}
	// Check for null
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *polygonType) ReadFrom(nbf *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.ReadKind()
	switch k {
	case types.PolygonKind:
		return reader.ReadPolygon()
	case types.NullKind:
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
}

func ConvertSQLPolygonToTypesPolygon(p sql.Polygon) types.Polygon {
	lines := make([]types.Linestring, len(p.Lines))
	for i, l := range p.Lines {
		lines[i] = ConvertSQLLinestringToTypesLinestring(l)
	}
	return types.Polygon{SRID: p.SRID, Lines: lines}
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *polygonType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	// Check for null
	if v == nil {
		return types.NullValue, nil
	}

	// Convert to sql.PolygonType
	poly, err := ti.sqlPolygonType.Convert(v)
	if err != nil {
		return nil, err
	}

	return ConvertSQLPolygonToTypesPolygon(poly.(sql.Polygon)), nil
}

// Equals implements TypeInfo interface.
func (ti *polygonType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*polygonType); ok {
		return ti.sqlPolygonType.Type() == ti2.sqlPolygonType.Type()
	}
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *polygonType) FormatValue(v types.Value) (*string, error) {
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
func (ti *polygonType) GetTypeIdentifier() Identifier {
	return PolygonTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *polygonType) GetTypeParams() map[string]string {
	return map[string]string{}
}

// IsValid implements TypeInfo interface.
func (ti *polygonType) IsValid(v types.Value) bool {
	if val, ok := v.(types.Linestring); ok {
		_, err := ti.sqlPolygonType.Convert(val)
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
func (ti *polygonType) NomsKind() types.NomsKind {
	return types.PolygonKind
}

// ParseValue implements TypeInfo interface.
func (ti *polygonType) ParseValue(ctx context.Context, vrw types.ValueReadWriter, str *string) (types.Value, error) {
	if str == nil || *str == "" {
		return types.NullValue, nil
	}
	return ti.ConvertValueToNomsValue(context.Background(), nil, *str)
}

// Promote implements TypeInfo interface.
func (ti *polygonType) Promote() TypeInfo {
	return &polygonType{ti.sqlPolygonType.Promote().(sql.PolygonType)}
}

// String implements TypeInfo interface.
func (ti *polygonType) String() string {
	return "Polygon()"
}

// ToSqlType implements TypeInfo interface.
func (ti *polygonType) ToSqlType() sql.Type {
	return ti.sqlPolygonType
}
