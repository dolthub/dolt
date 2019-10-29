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

package doltcore

import (
	"fmt"
	"strconv"

	"github.com/liquidata-inc/dolt/go/store/types"
)

type ConversionError struct {
	fromKind types.NomsKind
	toKind   types.NomsKind
	err      error
}

func (ce ConversionError) Error() string {
	toKindStr := types.KindToString[ce.toKind]
	fromKindStr := types.KindToString[ce.fromKind]
	return fmt.Sprint("error converting", fromKindStr, "to", toKindStr+":", ce.err.Error())
}

func IsConversionError(err error) bool {
	_, ok := err.(ConversionError)

	return ok
}

func GetFromAndToKinds(err error) (from, to types.NomsKind) {
	ce, ok := err.(ConversionError)

	if !ok {
		panic("Check that this is a conversion error before using this.")
	}

	return ce.fromKind, ce.toKind
}

func GetUnderlyingError(err error) error {
	ce, ok := err.(ConversionError)

	if !ok {
		panic("Check that this is a conversion error before using this.")
	}

	return ce.err
}

// ConvFunc is a function that converts one noms value to another of a different type.
type ConvFunc func(types.Value) (types.Value, error)

var convFuncMap = map[types.NomsKind]map[types.NomsKind]ConvFunc{
	types.StringKind: {
		types.StringKind: identityConvFunc,
		types.UUIDKind:   convStringToUUID,
		types.UintKind:   convStringToUint,
		types.IntKind:    convStringToInt,
		types.FloatKind:  convStringToFloat,
		types.BoolKind:   convStringToBool,
		types.NullKind:   convToNullFunc},
	types.UUIDKind: {
		types.StringKind: convUUIDToString,
		types.UUIDKind:   identityConvFunc,
		types.UintKind:   nil,
		types.IntKind:    nil,
		types.FloatKind:  nil,
		types.BoolKind:   nil,
		types.NullKind:   convToNullFunc},
	types.UintKind: {
		types.StringKind: convUintToString,
		types.UUIDKind:   nil,
		types.UintKind:   identityConvFunc,
		types.IntKind:    convUintToInt,
		types.FloatKind:  convUintToFloat,
		types.BoolKind:   convUintToBool,
		types.NullKind:   convToNullFunc},
	types.IntKind: {
		types.StringKind: convIntToString,
		types.UUIDKind:   nil,
		types.UintKind:   convIntToUint,
		types.IntKind:    identityConvFunc,
		types.FloatKind:  convIntToFloat,
		types.BoolKind:   convIntToBool,
		types.NullKind:   convToNullFunc},
	types.FloatKind: {
		types.StringKind: convFloatToString,
		types.UUIDKind:   nil,
		types.UintKind:   convFloatToUint,
		types.IntKind:    convFloatToInt,
		types.FloatKind:  identityConvFunc,
		types.BoolKind:   convFloatToBool,
		types.NullKind:   convToNullFunc},
	types.BoolKind: {
		types.StringKind: convBoolToString,
		types.UUIDKind:   nil,
		types.UintKind:   convBoolToUint,
		types.IntKind:    convBoolToInt,
		types.FloatKind:  convBoolToFloat,
		types.BoolKind:   identityConvFunc,
		types.NullKind:   convToNullFunc},
	types.NullKind: {
		types.StringKind: convToNullFunc,
		types.UUIDKind:   convToNullFunc,
		types.UintKind:   convToNullFunc,
		types.IntKind:    convToNullFunc,
		types.FloatKind:  convToNullFunc,
		types.BoolKind:   convToNullFunc,
		types.NullKind:   convToNullFunc},
}

// GetConvFunc takes in a source kind and a destination kind and returns a ConvFunc which can convert values of the
// source kind to values of the destination kind.
func GetConvFunc(srcKind, destKind types.NomsKind) ConvFunc {
	var convFunc ConvFunc
	if destKindMap, ok := convFuncMap[srcKind]; ok {
		convFunc = destKindMap[destKind]
	}

	return convFunc
}

var identityConvFunc = func(value types.Value) (types.Value, error) {
	if value == nil {
		return nil, nil
	}
	if value.Equals(types.String("\"\"\"")) || value.Equals(types.String("\"\"")) {
		return types.Value(types.String("")), nil
	}
	return value, nil
}

var convToNullFunc = func(types.Value) (types.Value, error) {
	return types.NullValue, nil
}

func convStringToFloat(val types.Value) (types.Value, error) {
	if val == nil {
		return nil, nil
	}

	if val.Equals(types.String("\"\"")) {
		return types.NullValue, nil
	}

	return stringToFloat(string(val.(types.String)))
}

func convStringToBool(val types.Value) (types.Value, error) {
	if val == nil {
		return nil, nil
	}

	if val.Equals(types.String("\"\"")) {
		return types.NullValue, nil
	}

	return stringToBool(string(val.(types.String)))
}

func convStringToInt(val types.Value) (types.Value, error) {
	if val == nil {
		return nil, nil
	}

	if val.Equals(types.String("\"\"")) {
		return types.NullValue, nil
	}

	return stringToInt(string(val.(types.String)))
}

func convStringToUint(val types.Value) (types.Value, error) {
	if val == nil {
		return nil, nil
	}

	if val.Equals(types.String("\"\"")) {
		return types.NullValue, nil
	}

	return stringToUint(string(val.(types.String)))
}

func convStringToUUID(val types.Value) (types.Value, error) {
	if val == nil {
		return nil, nil
	}

	if val.Equals(types.String("\"\"")) {
		return types.NullValue, nil
	}

	return stringToUUID(string(val.(types.String)))
}

func convUUIDToString(val types.Value) (types.Value, error) {
	if val == nil {
		return nil, nil
	}

	return types.String(val.(types.UUID).String()), nil
}

func convUintToString(val types.Value) (types.Value, error) {
	if val == nil {
		return nil, nil
	}

	n := uint64(val.(types.Uint))
	str := strconv.FormatUint(n, 10)

	return types.String(str), nil
}

func convUintToInt(val types.Value) (types.Value, error) {
	if val == nil {
		return nil, nil
	}

	n := uint64(val.(types.Uint))
	return types.Int(int64(n)), nil
}

func convUintToFloat(val types.Value) (types.Value, error) {
	if val == nil {
		return nil, nil
	}

	n := uint64(val.(types.Uint))
	return types.Float(float64(n)), nil
}

func convUintToBool(val types.Value) (types.Value, error) {
	if val == nil {
		return nil, nil
	}

	n := uint64(val.(types.Uint))
	return types.Bool(n != 0), nil
}

func convIntToString(val types.Value) (types.Value, error) {
	if val == nil {
		return nil, nil
	}

	n := int64(val.(types.Int))
	str := strconv.FormatInt(n, 10)

	return types.String(str), nil
}

func convIntToUint(val types.Value) (types.Value, error) {
	if val == nil {
		return nil, nil
	}

	n := int64(val.(types.Int))
	return types.Uint(uint64(n)), nil
}

func convIntToFloat(val types.Value) (types.Value, error) {
	if val == nil {
		return nil, nil
	}

	n := int64(val.(types.Int))
	return types.Float(float64(n)), nil
}

func convIntToBool(val types.Value) (types.Value, error) {
	if val == nil {
		return nil, nil
	}

	n := int64(val.(types.Int))
	return types.Bool(n != 0), nil
}

func convFloatToString(val types.Value) (types.Value, error) {
	if val == nil {
		return nil, nil
	}

	fl := float64(val.(types.Float))
	str := strconv.FormatFloat(fl, 'f', -1, 64)
	return types.String(str), nil
}

func convFloatToUint(val types.Value) (types.Value, error) {
	if val == nil {
		return nil, nil
	}

	fl := float64(val.(types.Float))
	return types.Uint(uint64(fl)), nil
}

func convFloatToInt(val types.Value) (types.Value, error) {
	if val == nil {
		return nil, nil
	}

	fl := float64(val.(types.Float))
	return types.Int(int(fl)), nil
}

func convFloatToBool(val types.Value) (types.Value, error) {
	if val == nil {
		return nil, nil
	}

	fl := float64(val.(types.Float))
	return types.Bool(fl != 0), nil
}

var trueValStr = types.String("true")
var falseValStr = types.String("false")

func convBoolToString(val types.Value) (types.Value, error) {
	if val == nil {
		return nil, nil
	}

	b := val.(types.Bool)

	if b {
		return trueValStr, nil
	}

	return falseValStr, nil
}

var zeroUintVal = types.Uint(0)
var oneUintVal = types.Uint(1)

func convBoolToUint(val types.Value) (types.Value, error) {
	if val == nil {
		return nil, nil
	}

	b := val.(types.Bool)

	if b {
		return oneUintVal, nil
	}

	return zeroUintVal, nil
}

var zeroIntVal = types.Int(0)
var oneIntVal = types.Int(1)

func convBoolToInt(val types.Value) (types.Value, error) {
	if val == nil {
		return nil, nil
	}

	b := val.(types.Bool)

	if b {
		return oneIntVal, nil
	}

	return zeroIntVal, nil
}

var zeroFloatVal = types.Float(0)
var oneFloatVal = types.Float(1)

func convBoolToFloat(val types.Value) (types.Value, error) {
	if val == nil {
		return nil, nil
	}

	b := val.(types.Bool)

	if b {
		return oneFloatVal, nil
	}

	return zeroFloatVal, nil
}
