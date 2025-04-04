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
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/store/types"
)

const (
	uintTypeParam_Width    = "width"
	uintTypeParam_Width_8  = "8"
	uintTypeParam_Width_16 = "16"
	uintTypeParam_Width_24 = "24"
	uintTypeParam_Width_32 = "32"
	uintTypeParam_Width_64 = "64"
)

type uintType struct {
	sqlUintType sql.NumberType
}

var _ TypeInfo = (*uintType)(nil)
var (
	Uint8Type  = &uintType{gmstypes.Uint8}
	Uint16Type = &uintType{gmstypes.Uint16}
	Uint24Type = &uintType{gmstypes.Uint24}
	Uint32Type = &uintType{gmstypes.Uint32}
	Uint64Type = &uintType{gmstypes.Uint64}
)

func CreateUintTypeFromParams(params map[string]string) (TypeInfo, error) {
	if width, ok := params[uintTypeParam_Width]; ok {
		switch width {
		case uintTypeParam_Width_8:
			return Uint8Type, nil
		case uintTypeParam_Width_16:
			return Uint16Type, nil
		case uintTypeParam_Width_24:
			return Uint24Type, nil
		case uintTypeParam_Width_32:
			return Uint32Type, nil
		case uintTypeParam_Width_64:
			return Uint64Type, nil
		default:
			return nil, fmt.Errorf(`create uint type info has "%v" param with value "%v"`, uintTypeParam_Width, width)
		}
	}
	return nil, fmt.Errorf(`create uint type info is missing "%v" param`, uintTypeParam_Width)
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *uintType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Uint); ok {
		switch ti.sqlUintType {
		case gmstypes.Uint8:
			return uint8(val), nil
		case gmstypes.Uint16:
			return uint16(val), nil
		case gmstypes.Uint24:
			return uint32(val), nil
		case gmstypes.Uint32:
			return uint32(val), nil
		case gmstypes.Uint64:
			return uint64(val), nil
		}
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *uintType) ReadFrom(_ *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.ReadKind()
	switch k {
	case types.UintKind:
		val := reader.ReadUint()
		switch ti.sqlUintType {
		case gmstypes.Uint8:
			return uint8(val), nil
		case gmstypes.Uint16:
			return uint16(val), nil
		case gmstypes.Uint24:
			return uint32(val), nil
		case gmstypes.Uint32:
			return uint32(val), nil
		case gmstypes.Uint64:
			return val, nil
		}
	case types.NullKind:
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *uintType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	if v == nil {
		return types.NullValue, nil
	}
	uintVal, _, err := ti.sqlUintType.Convert(ctx, v)
	if err != nil {
		return nil, err
	}
	switch val := uintVal.(type) {
	case uint8:
		return types.Uint(val), nil
	case uint16:
		return types.Uint(val), nil
	case uint32:
		return types.Uint(val), nil
	case uint64:
		return types.Uint(val), nil
	default:
		return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v)
	}
}

// Equals implements TypeInfo interface.
func (ti *uintType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*uintType); ok {
		return ti.sqlUintType.Type() == ti2.sqlUintType.Type() &&
			ti.sqlUintType.DisplayWidth() == ti2.sqlUintType.DisplayWidth()
	}
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *uintType) FormatValue(v types.Value) (*string, error) {
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	uintVal, err := ti.ConvertNomsValueToValue(v)
	if err != nil {
		return nil, err
	}
	switch val := uintVal.(type) {
	case uint8:
		res := strconv.FormatUint(uint64(val), 10)
		return &res, nil
	case uint16:
		res := strconv.FormatUint(uint64(val), 10)
		return &res, nil
	case uint32:
		res := strconv.FormatUint(uint64(val), 10)
		return &res, nil
	case uint64:
		res := strconv.FormatUint(val, 10)
		return &res, nil
	default:
		return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v)
	}
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *uintType) GetTypeIdentifier() Identifier {
	return UintTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *uintType) GetTypeParams() map[string]string {
	sqlParam := ""
	switch ti.sqlUintType.Type() {
	case sqltypes.Uint8:
		sqlParam = uintTypeParam_Width_8
	case sqltypes.Uint16:
		sqlParam = uintTypeParam_Width_16
	case sqltypes.Uint24:
		sqlParam = uintTypeParam_Width_24
	case sqltypes.Uint32:
		sqlParam = uintTypeParam_Width_32
	case sqltypes.Uint64:
		sqlParam = uintTypeParam_Width_64
	default:
		panic(fmt.Errorf(`unknown uint type info sql type "%v"`, ti.sqlUintType.Type().String()))
	}
	return map[string]string{uintTypeParam_Width: sqlParam}
}

// IsValid implements TypeInfo interface.
func (ti *uintType) IsValid(v types.Value) bool {
	// TODO: Add context parameter to IsValid, or delete the typeinfo package
	ctx := context.Background()
	if val, ok := v.(types.Uint); ok {
		_, _, err := ti.sqlUintType.Convert(ctx, uint64(val))
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
func (ti *uintType) NomsKind() types.NomsKind {
	return types.UintKind
}

// Promote implements TypeInfo interface.
func (ti *uintType) Promote() TypeInfo {
	return &uintType{ti.sqlUintType.Promote().(sql.NumberType)}
}

// String implements TypeInfo interface.
func (ti *uintType) String() string {
	switch ti.sqlUintType.Type() {
	case sqltypes.Uint8:
		return "Uint8"
	case sqltypes.Uint16:
		return "Uint16"
	case sqltypes.Uint24:
		return "Uint24"
	case sqltypes.Uint32:
		return "Uint32"
	case sqltypes.Uint64:
		return "Uint64"
	default:
		panic(fmt.Errorf(`unknown uint type info sql type "%v"`, ti.sqlUintType.Type().String()))
	}
}

// ToSqlType implements TypeInfo interface.
func (ti *uintType) ToSqlType() sql.Type {
	return ti.sqlUintType
}

// uintTypeConverter is an internal function for GetTypeConverter that handles the specific type as the source TypeInfo.
func uintTypeConverter(ctx context.Context, src *uintType, destTi TypeInfo) (tc TypeConverter, needsConversion bool, err error) {
	switch dest := destTi.(type) {
	case *bitType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *blobStringType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *boolType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *datetimeType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *decimalType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *enumType:
		return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
			if v == nil || v == types.NullValue {
				return types.NullValue, nil
			}
			val, ok := v.(types.Uint)
			if !ok {
				return nil, fmt.Errorf("unexpected type converting uint to enum: %T", v)
			}
			if val == 0 {
				return types.Uint(0), nil
			}
			return dest.ConvertValueToNomsValue(ctx, vrw, uint64(val))
		}, true, nil
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
		return wrapIsValid(dest.IsValid, src, dest)
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
