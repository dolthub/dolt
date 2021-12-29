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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/store/types"
)

// This is a dolt implementation of the MySQL type Point, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type linestringType struct {
	sqlLinestringType sql.LinestringType
}

var _ TypeInfo = (*linestringType)(nil)

var LinestringType = &linestringType{sql.LinestringType{}}

func ConvertStringToSQLLinestring(s string) (interface{}, error) {
	// Get everything between parentheses
	s = s[len("LINESTRING("):len(s)-1]
	// Split into points
	vals := strings.Split(s, ",POINT")
	// Parse points
	var points = make([]sql.Point, len(vals))
	for i, s := range vals {
		// Re-add delimiter
		if i != 0 {
			s = "POINT" + s
		}
		// Convert to Point type
		point, err := ConvertStringToSQLPoint(s)
		// TODO: This is never true
		if err != nil {
			return nil, err
		}
		// TODO: necessary? should throw error
		if point == nil {
			return nil, nil
		}
		points[i] = point.(sql.Point)
	}
	// Create sql.Linestring object
	return sql.Linestring{Points: points}, nil
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *linestringType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	// Expect a types.Linestring, return a sql.Linestring
	if val, ok := v.(types.Linestring); ok {
		return ConvertStringToSQLLinestring(string(val))
	}
	// Check for null
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
		return reader.ReadString(), nil
	case types.NullKind:
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
}

func ConvertSQLLinestringToString(l sql.Linestring) (types.Linestring, error) {
	// Convert each sql.Point into types.Point
	var pointStrs = make([]string, len(l.Points))
	for i, p := range l.Points {
		pointStr, err := ConvertSQLPointToString(p)
		if err != nil {
			return "", err
		}
		pointStrs[i] = string(pointStr)
	}
	lineStr := fmt.Sprintf("LINESTRING(%s)", strings.Join(pointStrs, ","))
	return types.Linestring(lineStr), nil
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *linestringType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	// Check for null
	if v == nil {
		return types.NullValue, nil
	}

	// Convert to sql.LinestringType
	line, err := ti.sqlLinestringType.Convert(v)
	if err != nil {
		return nil, err
	}

	return ConvertSQLLinestringToString(line.(sql.Linestring))
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
