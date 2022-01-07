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
	"encoding/binary"
	"fmt"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/go-mysql-server/sql"
	"math"
)

// This is a dolt implementation of the MySQL type Point, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type pointType struct {
	sqlPointType sql.PointType
}

var _ TypeInfo = (*pointType)(nil)

var PointType = &pointType{sql.PointType{}}

func ParseEWKBHeader(buf []byte) (uint32, bool, uint32) {
	srid := binary.LittleEndian.Uint32(buf[0:4]) // First 4 bytes is SRID always in little endian
	isBig := buf[4] == 0 // Next byte is endianness
	geomType := binary.LittleEndian.Uint32(buf[5:9]) // Next 4 bytes is type
	return srid, isBig, geomType
}

// ConvertEWKBToPoint converts the data portion of a WKB point to sql.Point
// Very similar logic to the function in GMS
func ConvertEWKBToPoint(buf []byte, isBig bool, srid uint32) sql.Point {
	// Read floats x and y
	var x, y float64
	if isBig {
		x = math.Float64frombits(binary.BigEndian.Uint64(buf[:8]))
		y = math.Float64frombits(binary.BigEndian.Uint64(buf[8:]))
	} else {
		x = math.Float64frombits(binary.LittleEndian.Uint64(buf[:8]))
		y = math.Float64frombits(binary.LittleEndian.Uint64(buf[8:]))
	}
	return sql.Point{SRID: srid, X: x, Y: y}
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *pointType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	// Expect a types.Point, return a sql.Point
	if val, ok := v.(types.Point); ok {
		// Assume buf is correct length (25)
		srid, isBig, _ := ParseEWKBHeader(val) // TODO: geomType should be impossible to fail
		return ConvertEWKBToPoint(val[9:], isBig, srid), nil
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
		return reader.ReadString(), nil
	case types.NullKind:
		return nil, nil
	default:
		return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
	}
}

// TODO: define constants for WKB?

// WriteEWKBHeader writes the SRID, endianness, and type to the byte buffer
// This function assumes v is a valid spatial type
func WriteEWKBHeader(v interface{}, buf []byte) {
	// Write endianness byte (always little endian)
	buf[4] = 1

	// Parse data
	switch v := v.(type) {
	case sql.Point:
		// Write SRID and type
		binary.LittleEndian.PutUint32(buf[0:4], v.SRID)
		binary.LittleEndian.PutUint32(buf[5:9], 1)
	case sql.Linestring:
		binary.LittleEndian.PutUint32(buf[0:4], v.SRID)
		binary.LittleEndian.PutUint32(buf[5:9], 2)
	case sql.Polygon:
		binary.LittleEndian.PutUint32(buf[0:4], v.SRID)
		binary.LittleEndian.PutUint32(buf[5:9], 3)
	}
}

// WriteEWKBPointData converts a Point into a byte array in EWKB format
// Very similar to function in GMS
func WriteEWKBPointData(p sql.Point, buf []byte) {
	binary.LittleEndian.PutUint64(buf[0:8], math.Float64bits(p.X))
	binary.LittleEndian.PutUint64(buf[8:16], math.Float64bits(p.Y))
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

	// Allocate buffer for point 4 + 1 + 4 + 16
	buf := make([]byte, 25)

	// Write header and data to buffer
	WriteEWKBHeader(point, buf)
	WriteEWKBPointData(point.(sql.Point), buf[9:])

	return types.Point(buf), nil
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
