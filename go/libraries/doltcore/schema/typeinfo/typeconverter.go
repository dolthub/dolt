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

	"github.com/shopspring/decimal"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/json"
	"github.com/dolthub/dolt/go/store/types"
)

var UnhandledTypeConversion = errors.NewKind("`%s` does not know how to handle type conversions to `%s`")
var InvalidTypeConversion = errors.NewKind("`%s` cannot convert the value `%v` to `%s`")

// TypeConverter is a function that is used to convert a Noms value from one TypeInfo to another.
type TypeConverter func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error)

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
			vInt = string(str)
		case types.Bool:
			vInt = bool(val)
		case types.Extended:
			return nil, fmt.Errorf("cannot convert to a extended type")
		case types.Decimal:
			vInt = decimal.Decimal(val).String()
		case types.Float:
			vInt = float64(val)
		case types.InlineBlob:
			vInt = *(*string)(unsafe.Pointer(&val))
		case types.SerialMessage:
			vInt = *(*string)(unsafe.Pointer(&val))
		case types.Int:
			vInt = int64(val)
		case types.JSON:
			var err error
			vInt, err = json.NomsJSON(val).JSONString()
			if err != nil {
				return nil, err
			}
		case types.LineString:
			vInt = types.ConvertTypesLineStringToSQLLineString(val)
		case types.Point:
			vInt = types.ConvertTypesPointToSQLPoint(val)
		case types.Polygon:
			vInt = types.ConvertTypesPolygonToSQLPolygon(val)
		case types.MultiPoint:
			vInt = types.ConvertTypesMultiPointToSQLMultiPoint(val)
		case types.MultiLineString:
			vInt = types.ConvertTypesMultiLineStringToSQLMultiLineString(val)
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
