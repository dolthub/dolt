package doltcore

import (
	"github.com/google/uuid"
	"github.com/liquidata-inc/ld/dolt/go/store/go/types"
	"testing"
)

const (
	zeroUUIDStr = "00000000-0000-0000-0000-000000000000"
)

var zeroUUID = uuid.Must(uuid.Parse(zeroUUIDStr))

func TestConv(t *testing.T) {
	tests := []struct {
		input       types.Value
		expectedOut types.Value
		expectFunc  ConvFunc
		expectErr   bool
	}{
		{types.String("test"), types.String("test"), identityConvFunc, false},
		{types.String(zeroUUIDStr), types.UUID(zeroUUID), convStringToUUID, false},
		{types.String("10"), types.Uint(10), convStringToUint, false},
		{types.String("-101"), types.Int(-101), convStringToInt, false},
		{types.String("3.25"), types.Float(3.25), convStringToFloat, false},
		{types.String("true"), types.Bool(true), convStringToBool, false},
		{types.String("anything"), types.NullValue, convToNullFunc, false},

		{types.UUID(zeroUUID), types.String(zeroUUIDStr), convUUIDToString, false},
		{types.UUID(zeroUUID), types.UUID(zeroUUID), identityConvFunc, false},
		{types.UUID(zeroUUID), types.Uint(0), nil, false},
		{types.UUID(zeroUUID), types.Int(0), nil, false},
		{types.UUID(zeroUUID), types.Float(0), nil, false},
		{types.UUID(zeroUUID), types.Bool(false), nil, false},
		{types.UUID(zeroUUID), types.NullValue, convToNullFunc, false},

		{types.Uint(10), types.String("10"), convUintToString, false},
		{types.Uint(100), types.UUID(zeroUUID), nil, false},
		{types.Uint(1000), types.Uint(1000), identityConvFunc, false},
		{types.Uint(10000), types.Int(10000), convUintToInt, false},
		{types.Uint(100000), types.Float(100000), convUintToFloat, false},
		{types.Uint(1000000), types.Bool(true), convUintToBool, false},
		{types.Uint(10000000), types.NullValue, convToNullFunc, false},

		{types.Int(-10), types.String("-10"), convIntToString, false},
		{types.Int(-100), types.UUID(zeroUUID), nil, false},
		{types.Int(1000), types.Uint(1000), convIntToUint, false},
		{types.Int(-10000), types.Int(-10000), identityConvFunc, false},
		{types.Int(-100000), types.Float(-100000), convIntToFloat, false},
		{types.Int(-1000000), types.Bool(true), convIntToBool, false},
		{types.Int(-10000000), types.NullValue, convToNullFunc, false},

		{types.Float(1.5), types.String("1.5"), convFloatToString, false},
		{types.Float(10.5), types.UUID(zeroUUID), nil, false},
		{types.Float(100.5), types.Uint(100), convFloatToUint, false},
		{types.Float(1000.5), types.Int(1000), convFloatToInt, false},
		{types.Float(10000.5), types.Float(10000.5), identityConvFunc, false},
		{types.Float(100000.5), types.Bool(true), convFloatToBool, false},
		{types.Float(1000000.5), types.NullValue, convToNullFunc, false},

		{types.Bool(true), types.String("true"), convBoolToString, false},
		{types.Bool(false), types.UUID(zeroUUID), nil, false},
		{types.Bool(true), types.Uint(1), convBoolToUint, false},
		{types.Bool(false), types.Int(0), convBoolToInt, false},
		{types.Bool(true), types.Float(1), convBoolToFloat, false},
		{types.Bool(false), types.Bool(false), identityConvFunc, false},
		{types.Bool(true), types.NullValue, convToNullFunc, false},
	}

	for _, test := range tests {
		convFunc := GetConvFunc(test.input.Kind(), test.expectedOut.Kind())

		if convFunc == nil && test.expectFunc != nil {
			t.Error("Did not receive correct conversion function for conversion from", test.input.Kind(), "to", test.expectedOut.Kind())
		} else if convFunc != nil {
			result, err := convFunc(test.input)

			if (err != nil) != test.expectErr {
				t.Error("input:", test.input, "expected err:", test.expectErr, "actual err:", err != nil)
			}

			if !test.expectedOut.Equals(result) {
				t.Error("input:", test.input, "expected result:", test.expectedOut, "actual result:", result)
			}
		}
	}
}

var convertibleTypes = []types.NomsKind{types.StringKind, types.UUIDKind, types.UintKind, types.IntKind, types.FloatKind, types.BoolKind}

func TestNullConversion(t *testing.T) {
	for _, srcKind := range convertibleTypes {
		for _, destKind := range convertibleTypes {
			convFunc := GetConvFunc(srcKind, destKind)

			if convFunc != nil {
				res, err := convFunc(nil)

				if res != nil || err != nil {
					t.Error("null conversion failed")
				}
			}
		}
	}
}
