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
	"time"
	"unsafe"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/shopspring/decimal"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/json"
	"github.com/dolthub/dolt/go/store/types"
)

var IncompatibleTypeConversion = errors.NewKind("`%s` cannot convert any values to `%s`")
var UnhandledTypeConversion = errors.NewKind("`%s` does not know how to handle type conversions to `%s`")
var InvalidTypeConversion = errors.NewKind("`%s` cannot convert the value `%v` to `%s`")

// TypeConverter is a function that is used to convert a Noms value from one TypeInfo to another.
type TypeConverter func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error)

// GetTypeConverter returns a TypeConverter that will convert a Noms value from the source type to the destination type.
// If the source type does not have a valid converter for the destination type, then this returns an error. When the
// given types are similar enough, no conversion is needed, thus this will return false. In such cases, although a valid
// TypeConverter will still be returned, it is equivalent to calling IsValid on the destination TypeInfo. Rather than
// returning nil values, any such returned nils will instead be types.NullValue.
func GetTypeConverter(ctx context.Context, srcTi TypeInfo, destTi TypeInfo) (tc TypeConverter, needsConversion bool, err error) {
	switch dest := destTi.(type) {
	case *tupleType, *unknownType:
		return nil, false, fmt.Errorf("'%s' can not be converted to by any type", dest.String())
	}

	if srcTi.Equals(destTi) {
		return identityTypeConverter, false, nil
	}

	switch src := srcTi.(type) {
	case *bitType:
		return bitTypeConverter(ctx, src, destTi)
	case *blobStringType:
		return blobStringTypeConverter(ctx, src, destTi)
	case *boolType:
		return boolTypeConverter(ctx, src, destTi)
	case *datetimeType:
		return datetimeTypeConverter(ctx, src, destTi)
	case *decimalType:
		return decimalTypeConverter(ctx, src, destTi)
	case *enumType:
		return enumTypeConverter(ctx, src, destTi)
	case *floatType:
		return floatTypeConverter(ctx, src, destTi)
	case *inlineBlobType:
		return inlineBlobTypeConverter(ctx, src, destTi)
	case *intType:
		return intTypeConverter(ctx, src, destTi)
	case *jsonType:
		return jsonTypeConverter(ctx, src, destTi)
	case *linestringType:
		return linestringTypeConverter(ctx, src, destTi)
	case *pointType:
		return pointTypeConverter(ctx, src, destTi)
	case *polygonType:
		return polygonTypeConverter(ctx, src, destTi)
	case *setType:
		return setTypeConverter(ctx, src, destTi)
	case *timeType:
		return timeTypeConverter(ctx, src, destTi)
	case *uintType:
		return uintTypeConverter(ctx, src, destTi)
	case *uuidType:
		return uuidTypeConverter(ctx, src, destTi)
	case *varBinaryType:
		return varBinaryTypeConverter(ctx, src, destTi)
	case *varStringType:
		return varStringTypeConverter(ctx, src, destTi)
	case *yearType:
		return yearTypeConverter(ctx, src, destTi)
	case *tupleType, *unknownType:
		return nil, false, fmt.Errorf("'%s' can not be converted from", src.String())
	default:
		return nil, false, UnhandledTypeConversion.New(src.String(), destTi.String())
	}
}

// identityTypeConverter immediately returns the given value.
func identityTypeConverter(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
	return v, nil
}

// wrapConvertValueToNomsValue is a helper function that takes a ConvertValueToNomsValue function and returns a TypeConverter.
func wrapConvertValueToNomsValue(
	cvtnv func(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error),
) (tc TypeConverter, needsConversion bool, err error) {
	return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
		if v == nil || v == types.NullValue {
			return types.NullValue, nil
		}

		// Handle converting each Noms value to its representative go type.
		var vInt interface{}
		switch val := v.(type) {
		case types.Blob:
			str, err := fromBlob(val)
			if err != nil {
				return nil, err
			}
			vInt = str
		case types.Bool:
			vInt = bool(val)
		case types.Decimal:
			vInt = decimal.Decimal(val).String()
		case types.Float:
			vInt = float64(val)
		case types.InlineBlob:
			vInt = *(*string)(unsafe.Pointer(&val))
		case types.Int:
			vInt = int64(val)
		case types.JSON:
			var err error
			vInt, err = json.NomsJSON(val).ToString(sql.NewEmptyContext())
			if err != nil {
				return nil, err
			}
		case types.Linestring:
			vInt = ConvertTypesLinestringToSQLLinestring(val)
		case types.Point:
			vInt = ConvertTypesPointToSQLPoint(val)
		case types.Polygon:
			vInt = ConvertTypesPolygonToSQLPolygon(val)
		case types.String:
			vInt = string(val)
		case types.Timestamp:
			vInt = time.Time(val).UTC()
		case types.UUID:
			vInt = val.String()
		case types.Uint:
			vInt = uint64(val)
		default:
			return nil, fmt.Errorf("unknown type in conversion: `%s`", val.Kind().String())
		}

		return cvtnv(ctx, vrw, vInt)
	}, true, nil
}

// wrapIsValid is a helper function that takes an IsValid function and returns a TypeConverter.
func wrapIsValid(isValid func(v types.Value) bool, srcTi TypeInfo, destTi TypeInfo) (tc TypeConverter, needsConversion bool, err error) {
	return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
		if v == nil || v == types.NullValue {
			return types.NullValue, nil
		}
		if !isValid(v) {
			return nil, InvalidTypeConversion.New(srcTi.String(), v, destTi.String())
		}
		return v, nil
	}, false, nil
}
