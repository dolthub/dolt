// Copyright 2020 Liquidata, Inc.
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
	"fmt"
	"strconv"
	"strings"

	"github.com/shopspring/decimal"
	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	decimalTypeParam_Precision = "prec"
	decimalTypeParam_Scale     = "scale"
)

// This is a dolt implementation of the MySQL type Decimal, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type decimalType struct {
	sqlDecimalType sql.DecimalType
}

var _ TypeInfo = (*decimalType)(nil)

func CreateDecimalTypeFromParams(params map[string]string) (TypeInfo, error) {
	var precision uint8
	if precisionStr, ok := params[decimalTypeParam_Precision]; ok {
		precisionUint, err := strconv.ParseUint(precisionStr, 10, 8)
		if err != nil {
			return nil, err
		}
		precision = uint8(precisionUint)
	} else {
		return nil, fmt.Errorf(`create decimal type info is missing param "%v"`, decimalTypeParam_Precision)
	}
	var scale uint8
	if scaleStr, ok := params[decimalTypeParam_Scale]; ok {
		scaleUint, err := strconv.ParseUint(scaleStr, 10, 8)
		if err != nil {
			return nil, err
		}
		scale = uint8(scaleUint)
	} else {
		return nil, fmt.Errorf(`create decimal type info is missing param "%v"`, decimalTypeParam_Scale)
	}
	sqlDecimalType, err := sql.CreateDecimalType(precision, scale)
	if err != nil {
		return nil, err
	}
	return &decimalType{sqlDecimalType}, nil
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *decimalType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.String); ok {
		res, err := ti.unmarshal(string(val))
		if err != nil {
			return nil, fmt.Errorf(`"%v" cannot convert "%v" to value`, ti.String(), val)
		}
		return res, nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *decimalType) ConvertValueToNomsValue(v interface{}) (types.Value, error) {
	if v == nil {
		return types.NullValue, nil
	}
	val, err := ti.marshal(v)
	if err != nil {
		return nil, err
	}
	return types.String(val), nil
}

// Equals implements TypeInfo interface.
func (ti *decimalType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*decimalType); ok {
		return ti.sqlDecimalType.Precision() == ti2.sqlDecimalType.Precision() &&
			ti.sqlDecimalType.Scale() == ti2.sqlDecimalType.Scale()
	}
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *decimalType) FormatValue(v types.Value) (*string, error) {
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	strVal, err := ti.ConvertNomsValueToValue(v)
	if err != nil {
		return nil, err
	}
	val, ok := strVal.(string)
	if !ok {
		return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v)
	}
	return &val, nil
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *decimalType) GetTypeIdentifier() Identifier {
	return DecimalTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *decimalType) GetTypeParams() map[string]string {
	return map[string]string{
		decimalTypeParam_Precision: strconv.FormatUint(uint64(ti.sqlDecimalType.Precision()), 10),
		decimalTypeParam_Scale:     strconv.FormatUint(uint64(ti.sqlDecimalType.Scale()), 10),
	}
}

// IsValid implements TypeInfo interface.
func (ti *decimalType) IsValid(v types.Value) bool {
	_, err := ti.ConvertNomsValueToValue(v)
	return err == nil
}

// NomsKind implements TypeInfo interface.
func (ti *decimalType) NomsKind() types.NomsKind {
	return types.StringKind
}

// ParseValue implements TypeInfo interface.
func (ti *decimalType) ParseValue(str *string) (types.Value, error) {
	if str == nil || *str == "" {
		return types.NullValue, nil
	}
	val, err := ti.marshal(*str)
	if err != nil {
		return nil, err
	}
	return types.String(val), nil
}

// String implements TypeInfo interface.
func (ti *decimalType) String() string {
	return fmt.Sprintf("Decimal(%v, %v)", ti.sqlDecimalType.Precision(), ti.sqlDecimalType.Scale())
}

// ToSqlType implements TypeInfo interface.
func (ti *decimalType) ToSqlType() sql.Type {
	return ti.sqlDecimalType
}

func (ti *decimalType) marshal(v interface{}) (string, error) {
	precision := ti.sqlDecimalType.Precision()
	scale := ti.sqlDecimalType.Scale()
	// The goal here is to return a string that can be sorted regardless of value.
	// For example, for DEC(4,2) let's say w == "-5.33", x == "-1.00", y == "2.20", z == "10.05".
	// Attempting to sort these ascending as-is would give the order x,w,z,y (in go, - < 0).
	// To fix this, we can shift up by the upperbound.
	// This would then give us w == "4.67", x == "9.00", y == "102.20", z == "110.05"
	// To make the strings of equal length, we can prepend zeros onto the originally-negative numbers.
	// Our final values would w == "004.67", x == "009.00", y == "102.20", z == "110.05", which are sorted.
	nullDecimal, err := ti.sqlDecimalType.ConvertToDecimal(v)
	if err != nil {
		return "", err
	}
	if !nullDecimal.Valid {
		return "", sql.ErrMarshalNullDecimal.New()
	}
	dec := nullDecimal.Decimal
	decStr := ti.sqlDecimalType.ExclusiveUpperBound().Add(dec).StringFixed(int32(scale))
	if dec.Sign() < 0 && precision != scale {
		if strings.Index(decStr, ".") != -1 {
			decStr = strings.Repeat("0", int(precision)-len(decStr)+2) + decStr
		} else {
			decStr = strings.Repeat("0", int(precision)-len(decStr)+1) + decStr
		}
	}
	return decStr, nil
}

func (ti *decimalType) unmarshal(v string) (string, error) {
	if len(v) == 0 {
		return "", sql.ErrMarshalNullDecimal.New()
	}
	var subtractor decimal.Decimal
	var err error
	if decIndex := strings.Index(v, "."); decIndex > 0 {
		subtractor, err = decimal.NewFromString("1" + strings.Repeat("0", len(v[:decIndex])-1))
		if err != nil {
			return "", err
		}
	} else {
		subtractor, err = decimal.NewFromString("1" + strings.Repeat("0", len(v)-1))
		if err != nil {
			return "", err
		}
	}
	dec, err := decimal.NewFromString(v)
	if err != nil {
		return "", err
	}
	res, err := ti.sqlDecimalType.ConvertToDecimal(dec.Sub(subtractor))
	if err != nil {
		return "", err
	}
	return res.Decimal.StringFixed(int32(ti.sqlDecimalType.Scale())), nil
}
