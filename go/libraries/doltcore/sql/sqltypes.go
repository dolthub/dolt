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

package sql

import (
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

var DoltToSQLType = map[types.NomsKind]string{
	types.StringKind: VARCHAR,
	types.BoolKind:   BOOL,
	types.FloatKind:  FLOAT_TYPE,
	types.IntKind:    INT,
	types.UintKind:   INT + " " + UNSIGNED,
	types.UUIDKind:   UUID,
}

// TypeConversionFn is a function that converts one noms value to another of a different type in a guaranteed fashion,
// i.e. any conversion that could possibly fail (e.g. string -> int) are not allowed. Only SQL-safe conversions are
// allowed, even if they are guaranteed to be safe, so that e.g. there is no way to convert a numeric type to a string.
// These kinds of conversions must be explicit in SQL.
type TypeConversionFn func(types.Value) types.Value

var convFuncMap = map[types.NomsKind]map[types.NomsKind]TypeConversionFn{
	types.StringKind: {
		types.StringKind: identityConvFunc,
		types.NullKind:   convToNullFunc,
	},
	types.UUIDKind: {
		types.UUIDKind: identityConvFunc,
		types.NullKind: convToNullFunc,
	},
	types.UintKind: {
		types.UintKind:  identityConvFunc,
		types.IntKind:   convUintToInt,
		types.FloatKind: convUintToFloat,
		types.NullKind:  convToNullFunc,
	},
	types.IntKind: {
		types.UintKind:  convIntToUint,
		types.IntKind:   identityConvFunc,
		types.FloatKind: convIntToFloat,
		types.NullKind:  convToNullFunc,
	},
	types.FloatKind: {
		types.FloatKind: identityConvFunc,
		types.NullKind:  convToNullFunc,
	},
	types.BoolKind: {
		types.BoolKind: identityConvFunc,
		types.NullKind: convToNullFunc,
	},
	types.NullKind: {
		types.StringKind: convToNullFunc,
		types.UUIDKind:   convToNullFunc,
		types.UintKind:   convToNullFunc,
		types.IntKind:    convToNullFunc,
		types.FloatKind:  convToNullFunc,
		types.BoolKind:   convToNullFunc,
		types.NullKind:   convToNullFunc,
	},
}

// GetTypeConversionFn takes in a source kind and a destination kind and returns a TypeConversionFn which can convert
// values of the source kind to values of the destination kind in a type-safe manner, or nil if no such conversion is
// possible.
func GetTypeConversionFn(srcKind, destKind types.NomsKind) TypeConversionFn {
	var convFunc TypeConversionFn
	if destKindMap, ok := convFuncMap[srcKind]; ok {
		convFunc = destKindMap[destKind]
	}

	return convFunc
}

func identityConvFunc(value types.Value) types.Value {
	return value
}

var convToNullFunc = func(types.Value) types.Value {
	return nil
}

func convUintToInt(val types.Value) types.Value {
	if val == nil {
		return nil
	}

	n := uint64(val.(types.Uint))
	return types.Int(int64(n))
}

func convUintToFloat(val types.Value) types.Value {
	if val == nil {
		return nil
	}

	n := uint64(val.(types.Uint))
	return types.Float(float64(n))
}

func convIntToUint(val types.Value) types.Value {
	if val == nil {
		return nil
	}

	n := int64(val.(types.Int))
	return types.Uint(uint64(n))
}

func convIntToFloat(val types.Value) types.Value {
	if val == nil {
		return nil
	}

	n := int64(val.(types.Int))
	return types.Float(float64(n))
}
