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
	"errors"
	"math"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

// StringToValue takes a string and a NomsKind and tries to convert the string to a noms Value.
func StringToValue(s string, kind types.NomsKind) (types.Value, error) {
	if !types.IsPrimitiveKind(kind) || kind == types.BlobKind {
		return nil, errors.New("Only primitive type support")
	}

	switch kind {
	case types.StringKind:
		return types.String(s), nil
	case types.FloatKind:
		return stringToFloat(s)
	case types.BoolKind:
		return stringToBool(s)
	case types.IntKind:
		return stringToInt(s)
	case types.UintKind:
		return stringToUint(s)
	case types.UUIDKind:
		return stringToUUID(s)
	case types.NullKind:
		return types.NullValue, nil
	}

	panic("Unsupported type " + kind.String())
}

func stringToFloat(s string) (types.Value, error) {
	if len(s) == 0 {
		return types.NullValue, nil
	}

	f, err := strconv.ParseFloat(s, 64)

	if err != nil {
		return types.Float(math.NaN()), ConversionError{types.StringKind, types.FloatKind, err}
	}

	return types.Float(f), nil
}

func stringToBool(s string) (types.Value, error) {
	if len(s) == 0 {
		return types.NullValue, nil
	}

	b, err := strconv.ParseBool(strings.ToLower(s))

	if err != nil {
		return types.Bool(false), ConversionError{types.StringKind, types.BoolKind, err}
	}

	return types.Bool(b), nil
}

func stringToInt(s string) (types.Value, error) {
	if len(s) == 0 {
		return types.NullValue, nil
	}

	n, err := strconv.ParseInt(s, 10, 64)

	if err != nil {
		return types.Int(0), ConversionError{types.StringKind, types.IntKind, err}
	}

	return types.Int(n), nil
}

func stringToUint(s string) (types.Value, error) {
	if len(s) == 0 {
		return types.NullValue, nil
	}

	n, err := strconv.ParseUint(s, 10, 64)

	if err != nil {
		return types.Uint(0), ConversionError{types.StringKind, types.UintKind, err}
	}

	return types.Uint(n), nil
}

func stringToUUID(s string) (types.Value, error) {
	if len(s) == 0 {
		return types.NullValue, nil
	}

	u, err := uuid.Parse(s)

	if err != nil {
		return types.UUID(u), ConversionError{types.StringKind, types.UUIDKind, err}
	}

	return types.UUID(u), nil
}
