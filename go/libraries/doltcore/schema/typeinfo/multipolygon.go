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
	mpoly, _, err := ti.sqlMultiPolygonType.Convert(v)
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

// FormatValue implements TypeInfo interface.
func (ti *multipolygonType) FormatValue(v types.Value) (*string, error) {
	if val, ok := v.(types.MultiPolygon); ok {
		resStr := string(types.SerializeMultiPolygon(val))
		return &resStr, nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v.Kind())
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *multipolygonType) GetTypeIdentifier() Identifier {
	return MultiPolygonTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *multipolygonType) GetTypeParams() map[string]string {
	return map[string]string{"SRID": strconv.FormatUint(uint64(ti.sqlMultiPolygonType.SRID), 10),
		"DefinedSRID": strconv.FormatBool(ti.sqlMultiPolygonType.DefinedSRID)}
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

// multipolygonTypeConverter is an internal function for GetTypeConverter that handles the specific type as the source TypeInfo.
func multipolygonTypeConverter(ctx context.Context, src *multipolygonType, destTi TypeInfo) (tc TypeConverter, needsConversion bool, err error) {
	switch dest := destTi.(type) {
	case *bitType:
		return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
			return types.Uint(0), nil
		}, true, nil
	case *blobStringType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *boolType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *datetimeType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *decimalType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *enumType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *floatType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *geomcollType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *geometryType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *inlineBlobType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *intType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *jsonType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *linestringType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *multilinestringType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *multipointType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *multipolygonType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *pointType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *polygonType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *setType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *timeType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *uintType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *uuidType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *varBinaryType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *varStringType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *yearType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	default:
		return nil, false, UnhandledTypeConversion.New(src.String(), destTi.String())
	}
}

func CreateMultiPolygonTypeFromParams(params map[string]string) (TypeInfo, error) {
	var (
		err     error
		sridVal uint64
		def     bool
	)
	if s, ok := params["SRID"]; ok {
		sridVal, err = strconv.ParseUint(s, 10, 32)
		if err != nil {
			return nil, err
		}
	}
	if d, ok := params["DefinedSRID"]; ok {
		def, err = strconv.ParseBool(d)
		if err != nil {
			return nil, err
		}
	}

	return &multipolygonType{sqlMultiPolygonType: gmstypes.MultiPolygonType{SRID: uint32(sridVal), DefinedSRID: def}}, nil
}
