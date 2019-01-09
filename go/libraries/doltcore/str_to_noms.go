package doltcore

import (
	"errors"
	"math"
	"strconv"
	"strings"

	"github.com/attic-labs/noms/go/types"
	"github.com/google/uuid"
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
		return types.Float(math.NaN()), err
	}

	return types.Float(f), err
}

func stringToBool(s string) (types.Value, error) {
	if len(s) == 0 {
		return types.NullValue, nil
	}

	b, err := strconv.ParseBool(strings.ToLower(s))
	return types.Bool(b), err
}

func stringToInt(s string) (types.Value, error) {
	if len(s) == 0 {
		return types.NullValue, nil
	}

	n, err := strconv.ParseInt(s, 10, 64)
	return types.Int(n), err
}

func stringToUint(s string) (types.Value, error) {
	if len(s) == 0 {
		return types.NullValue, nil
	}

	n, err := strconv.ParseUint(s, 10, 64)
	return types.Uint(n), err
}

func stringToUUID(s string) (types.Value, error) {
	if len(s) == 0 {
		return types.NullValue, nil
	}

	u, err := uuid.Parse(s)
	return types.UUID(u), err
}
