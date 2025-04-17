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

package expreval

import (
	"strconv"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/store/types"
)

func literalAsInt64(literal sql.LiteralExpression) (int64, error) {
	v := literal.LiteralValue()
	switch typedVal := v.(type) {
	case bool:
		if typedVal {
			return 1, nil
		} else {
			return 0, nil
		}
	case int:
		return int64(typedVal), nil
	case int8:
		return int64(typedVal), nil
	case int16:
		return int64(typedVal), nil
	case int32:
		return int64(typedVal), nil
	case int64:
		return typedVal, nil
	case uint:
		return int64(typedVal), nil
	case uint8:
		return int64(typedVal), nil
	case uint16:
		return int64(typedVal), nil
	case uint32:
		return int64(typedVal), nil
	case uint64:
		if typedVal&0x8000000000000000 != 0 {
			return 0, errInvalidConversion.New(literal.String(), "uint64", "int64")
		}

		return int64(typedVal), nil
	case float64:
		i64 := int64(typedVal)
		if i64 == int64(typedVal+0.9999) {
			return i64, nil
		} else {
			return 0, errInvalidConversion.New(literal.String(), "float64", "int64")
		}
	case float32:
		i64 := int64(typedVal)
		if i64 == int64(typedVal+0.9999) {
			return i64, nil
		} else {
			return 0, errInvalidConversion.New(literal.String(), "float32", "int64")
		}
	case string:
		return strconv.ParseInt(typedVal, 10, 64)
	}

	return 0, errInvalidConversion.New(literal.String(), literal.Type().String(), "int64")
}

func literalAsUint64(literal sql.LiteralExpression) (uint64, error) {
	v := literal.LiteralValue()
	switch typedVal := v.(type) {
	case bool:
		if typedVal {
			return 1, nil
		} else {
			return 0, nil
		}
	case int:
		if typedVal < 0 {
			return 0, errInvalidConversion.New(literal.String(), "int", "uint64")
		}

		return uint64(typedVal), nil
	case int8:
		if typedVal < 0 {
			return 0, errInvalidConversion.New(literal.String(), "int8", "uint64")
		}

		return uint64(typedVal), nil
	case int16:
		if typedVal < 0 {
			return 0, errInvalidConversion.New(literal.String(), "int16", "uint64")
		}

		return uint64(typedVal), nil
	case int32:
		if typedVal < 0 {
			return 0, errInvalidConversion.New(literal.String(), "int32", "uint64")
		}

		return uint64(typedVal), nil
	case int64:
		if typedVal < 0 {
			return 0, errInvalidConversion.New(literal.String(), "int64", "uint64")
		}

		return uint64(typedVal), nil
	case uint:
		return uint64(typedVal), nil
	case uint8:
		return uint64(typedVal), nil
	case uint16:
		return uint64(typedVal), nil
	case uint32:
		return uint64(typedVal), nil
	case uint64:
		return typedVal, nil
	case float64:
		if typedVal < 0 {
			return 0, errInvalidConversion.New(literal.String(), "float64", "uint64")
		}

		u64 := uint64(typedVal)
		if u64 == uint64(typedVal+0.9999) {
			return u64, nil
		} else {
			return 0, errInvalidConversion.New(literal.String(), "float64", "uint64")
		}
	case float32:
		u64 := uint64(typedVal)
		if u64 == uint64(typedVal+0.9999) {
			return u64, nil
		} else {
			return 0, errInvalidConversion.New(literal.String(), "float32", "uint64")
		}
	case string:
		return strconv.ParseUint(typedVal, 10, 64)
	}

	return 0, errInvalidConversion.New(literal.String(), literal.Type().String(), "int64")
}

func literalAsFloat64(literal sql.LiteralExpression) (float64, error) {
	v := literal.LiteralValue()
	switch typedVal := v.(type) {
	case int:
		return float64(typedVal), nil
	case int8:
		return float64(typedVal), nil
	case int16:
		return float64(typedVal), nil
	case int32:
		return float64(typedVal), nil
	case int64:
		return float64(typedVal), nil
	case uint:
		return float64(typedVal), nil
	case uint8:
		return float64(typedVal), nil
	case uint16:
		return float64(typedVal), nil
	case uint32:
		return float64(typedVal), nil
	case uint64:
		return float64(typedVal), nil
	case float64:
		return typedVal, nil
	case float32:
		return float64(typedVal), nil
	case string:
		return strconv.ParseFloat(typedVal, 64)
	}

	return 0, errInvalidConversion.New(literal.String(), literal.Type().String(), "float64")
}

func literalAsBool(literal sql.LiteralExpression) (bool, error) {
	v := literal.LiteralValue()
	switch typedVal := v.(type) {
	case bool:
		return typedVal, nil
	case string:
		b, err := strconv.ParseBool(typedVal)

		if err == nil {
			return b, nil
		}

		return false, errInvalidConversion.New(literal.String(), literal.Type().String(), "bool")
	case int:
		return typedVal != 0, nil
	case int8:
		return typedVal != 0, nil
	case int16:
		return typedVal != 0, nil
	case int32:
		return typedVal != 0, nil
	case int64:
		return typedVal != 0, nil
	case uint:
		return typedVal != 0, nil
	case uint8:
		return typedVal != 0, nil
	case uint16:
		return typedVal != 0, nil
	case uint32:
		return typedVal != 0, nil
	case uint64:
		return typedVal != 0, nil
	}

	return false, errInvalidConversion.New(literal.String(), literal.Type().String(), "bool")
}

func literalAsString(literal sql.LiteralExpression) (string, error) {
	v := literal.LiteralValue()
	switch typedVal := v.(type) {
	case string:
		return typedVal, nil
	case int, int8, int16, int32, int64:
		i64, _ := literalAsInt64(literal)
		return strconv.FormatInt(i64, 10), nil
	case uint, uint8, uint16, uint32, uint64:
		u64, _ := literalAsUint64(literal)
		return strconv.FormatUint(u64, 10), nil
	case float32, float64:
		f64, _ := literalAsFloat64(literal)
		return strconv.FormatFloat(f64, 'f', -1, 64), nil
	case bool:
		return strconv.FormatBool(typedVal), nil
	}

	return "", errInvalidConversion.New(literal.String(), literal.Type().String(), "bool")
}

func parseDate(s string) (time.Time, error) {
	for _, layout := range gmstypes.TimestampDatetimeLayouts {
		res, err := time.Parse(layout, s)

		if err == nil {
			return res, nil
		}
	}

	return time.Time{}, gmstypes.ErrConvertingToTime.New(s)
}

func literalAsTimestamp(literal sql.LiteralExpression) (time.Time, error) {
	v := literal.LiteralValue()
	switch typedVal := v.(type) {
	case time.Time:
		return typedVal, nil
	case string:
		ts, err := parseDate(typedVal)

		if err != nil {
			return time.Time{}, err
		}

		return ts, nil
	}

	return time.Time{}, errInvalidConversion.New(literal.String(), literal.Type().String(), "datetime")
}

// LiteralToNomsValue converts a go-mysql-servel Literal into a noms value.
func LiteralToNomsValue(kind types.NomsKind, literal sql.LiteralExpression) (types.Value, error) {
	if literal.LiteralValue() == nil {
		return types.NullValue, nil
	}

	switch kind {
	case types.IntKind:
		i64, err := literalAsInt64(literal)

		if err != nil {
			return nil, err
		}

		return types.Int(i64), nil

	case types.UintKind:
		u64, err := literalAsUint64(literal)

		if err != nil {
			return nil, err
		}

		return types.Uint(u64), nil

	case types.FloatKind:
		f64, err := literalAsFloat64(literal)

		if err != nil {
			return nil, err
		}

		return types.Float(f64), nil

	case types.BoolKind:
		b, err := literalAsBool(literal)

		if err != nil {
			return nil, err
		}

		return types.Bool(b), err

	case types.StringKind:
		s, err := literalAsString(literal)

		if err != nil {
			return nil, err
		}

		return types.String(s), nil

	case types.TimestampKind:
		ts, err := literalAsTimestamp(literal)

		if err != nil {
			return nil, err
		}

		return types.Timestamp(ts), nil
	}

	return nil, errInvalidConversion.New(literal.String(), literal.Type().String(), kind.String())
}
