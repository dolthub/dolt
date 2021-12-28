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
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/store/types"
)

// This is a dolt implementation of the MySQL type Point, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type pointType struct {
	sqlPointType sql.PointType
}

var _ TypeInfo = (*pointType)(nil)

var PointType = &pointType{sql.PointType{}}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *pointType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	// Expect a types.Point, return a sql.Point
	if val, ok := v.(types.Point); ok {
		// Get everything between parentheses
		val = val[len("point("):len(val)-1]
		// Split into x and y strings; maybe should length check
		vals := strings.Split(string(val), ",")
		// Parse x as float64
		x, err := strconv.ParseFloat(vals[0], 64)
		if err != nil {
			return nil, err
		}
		// Parse y as float64
		y, err := strconv.ParseFloat(vals[1], 64)
		if err != nil {
			return nil, err
		}
		// Create sql.Point object
		return sql.Point{X: x, Y: y}, nil
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
		s := reader.ReadString()
		return s, nil
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
	point, err := ti.sqlPointType.Convert(v)
	if err != nil {
		return nil, err
	}
	p := point.(sql.Point)

	// Convert point to string / types.Point
	pointStr := fmt.Sprintf("POINT(%s, %s)",strconv.FormatFloat(p.X, 'g', -1, 64), strconv.FormatFloat(p.Y, 'g', -1, 64))

	// Create types.Point
	return types.Point(pointStr), nil
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
		_, err := ti.sqlPointType.Convert(string(val))
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
