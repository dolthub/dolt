// Copyright 2019 Liquidata, Inc.
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

	"github.com/liquidata-inc/dolt/go/store/types"
	"github.com/src-d/go-mysql-server/sql"
)

const (
	decimalTypeParam_Precision = "prec"
	decimalTypeParam_Scale = "scale"
)

// This is a dolt implementation of the MySQL type Decimal, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type decimalImpl struct{
	sqlDecimalType sql.DecimalType
}

var _ TypeInfo = (*decimalImpl)(nil)

func CreateDecimalType(params map[string]string) (TypeInfo, error) {
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
	return &decimalImpl{sqlDecimalType}, nil
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *decimalImpl) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.String); ok {
		res, err := ti.sqlDecimalType.Convert(string(val))
		if err != nil {
			return nil, fmt.Errorf(`"%v" cannot convert "%v" to value`, ti.String(), val)
		}
		return res, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *decimalImpl) ConvertValueToNomsValue(v interface{}) (types.Value, error) {
	if artifact, ok := ti.isValid(v); ok {
		return types.String(artifact), nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
}

// Equals implements TypeInfo interface.
func (ti *decimalImpl) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*decimalImpl); ok {
		return ti.sqlDecimalType.Precision() == ti2.sqlDecimalType.Precision() &&
			ti.sqlDecimalType.Scale() == ti2.sqlDecimalType.Scale()
	}
	return false
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *decimalImpl) GetTypeIdentifier() Identifier {
	return DecimalType
}

// GetTypeParams implements TypeInfo interface.
func (ti *decimalImpl) GetTypeParams() map[string]string {
	return map[string]string{
		decimalTypeParam_Precision: strconv.FormatUint(uint64(ti.sqlDecimalType.Precision()), 10),
		decimalTypeParam_Scale: strconv.FormatUint(uint64(ti.sqlDecimalType.Scale()), 10),
	}
}

// IsValid implements TypeInfo interface.
func (ti *decimalImpl) IsValid(v interface{}) bool {
	_, ok := ti.isValid(v)
	return ok
}

// NomsKind implements TypeInfo interface.
func (ti *decimalImpl) NomsKind() types.NomsKind {
	return types.StringKind
}

// String implements TypeInfo interface.
func (ti *decimalImpl) String() string {
	return fmt.Sprintf("Decimal(%v, %v)", ti.sqlDecimalType.Precision(), ti.sqlDecimalType.Scale())
}

// ToSqlType implements TypeInfo interface.
func (ti *decimalImpl) ToSqlType() sql.Type {
	return ti.sqlDecimalType
}

// isValid is an internal implementation for the TypeInfo interface function IsValid.
// Some validity checks process the value into its final form, which may be returned
// as an artifact so that a value doesn't need to be processed twice in some scenarios.
func (ti *decimalImpl) isValid(v interface{}) (artifact string, ok bool) {
	// convert some Noms values to their standard golang equivalents
	switch val := v.(type) {
	case types.Bool:
		v = bool(val)
	case types.Int:
		v = int64(val)
	case types.Uint:
		v = uint64(val)
	case types.Float:
		v = float64(val)
	case types.String:
		v = string(val)
	}
	res, err := ti.sqlDecimalType.Convert(v)
	resStr, ok := res.(string)
	return resStr, err == nil && ok
}
