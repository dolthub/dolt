package graphql_test

import (
	"math"
	"reflect"
	"testing"

	"github.com/attic-labs/graphql"
)

type intSerializationTest struct {
	Value    interface{}
	Expected interface{}
}
type float64SerializationTest struct {
	Value    interface{}
	Expected interface{}
}

type stringSerializationTest struct {
	Value    interface{}
	Expected string
}

type boolSerializationTest struct {
	Value    interface{}
	Expected bool
}

func TestTypeSystem_Scalar_SerializesOutputInt(t *testing.T) {
	tests := []intSerializationTest{
		{1, 1},
		{0, 0},
		{-1, -1},
		{float32(0.1), 0},
		{float32(1.1), 1},
		{float32(-1.1), -1},
		{float32(1e5), 100000},
		{float32(math.MaxFloat32), nil},
		{float64(0.1), 0},
		{float64(1.1), 1},
		{float64(-1.1), -1},
		{float64(1e5), 100000},
		{float64(math.MaxFloat32), nil},
		{float64(math.MaxFloat64), nil},
		// Maybe a safe Go/Javascript `int`, but bigger than 2^32, so not
		// representable as a GraphQL Int
		{9876504321, nil},
		{-9876504321, nil},
		// Too big to represent as an Int in Go, JavaScript or GraphQL
		{float64(1e100), nil},
		{float64(-1e100), nil},
		{"-1.1", -1},
		{"one", nil},
		{false, 0},
		{true, 1},
		{int8(1), 1},
		{int16(1), 1},
		{int32(1), 1},
		{int64(1), 1},
		{uint(1), 1},
		// Maybe a safe Go `uint`, but bigger than 2^32, so not
		// representable as a GraphQL Int
		{uint(math.MaxInt32 + 1), nil},
		{uint8(1), 1},
		{uint16(1), 1},
		{uint32(1), 1},
		{uint32(math.MaxUint32), nil},
		{uint64(1), 1},
		{uint64(math.MaxInt32), math.MaxInt32},
		{int64(math.MaxInt32) + int64(1), nil},
		{int64(math.MinInt32) - int64(1), nil},
		{uint64(math.MaxInt64) + uint64(1), nil},
		{byte(127), 127},
		{'世', int('世')},
		// testing types that don't match a value in the array.
		{[]int{}, nil},
	}

	for i, test := range tests {
		val := graphql.Int.Serialize(test.Value)
		if val != test.Expected {
			reflectedTestValue := reflect.ValueOf(test.Value)
			reflectedExpectedValue := reflect.ValueOf(test.Expected)
			reflectedValue := reflect.ValueOf(val)
			t.Fatalf("Failed test #%d - Int.Serialize(%v(%v)), expected: %v(%v), got %v(%v)",
				i, reflectedTestValue.Type(), test.Value,
				reflectedExpectedValue.Type(), test.Expected,
				reflectedValue.Type(), val,
			)
		}
	}
}

func TestTypeSystem_Scalar_SerializesOutputFloat(t *testing.T) {
	tests := []float64SerializationTest{
		{int(1), 1.0},
		{int(0), 0.0},
		{int(-1), -1.0},
		{float32(0.1), float32(0.1)},
		{float32(1.1), float32(1.1)},
		{float32(-1.1), float32(-1.1)},
		{float64(0.1), float64(0.1)},
		{float64(1.1), float64(1.1)},
		{float64(-1.1), float64(-1.1)},
		{"-1.1", -1.1},
		{"one", nil},
		{false, 0.0},
		{true, 1.0},
	}

	for i, test := range tests {
		val := graphql.Float.Serialize(test.Value)
		if val != test.Expected {
			reflectedTestValue := reflect.ValueOf(test.Value)
			reflectedExpectedValue := reflect.ValueOf(test.Expected)
			reflectedValue := reflect.ValueOf(val)
			t.Fatalf("Failed test #%d - Float.Serialize(%v(%v)), expected: %v(%v), got %v(%v)",
				i, reflectedTestValue.Type(), test.Value,
				reflectedExpectedValue.Type(), test.Expected,
				reflectedValue.Type(), val,
			)
		}
	}
}

func TestTypeSystem_Scalar_SerializesOutputStrings(t *testing.T) {
	tests := []stringSerializationTest{
		{"string", "string"},
		{int(1), "1"},
		{float32(-1.1), "-1.1"},
		{float64(-1.1), "-1.1"},
		{true, "true"},
		{false, "false"},
	}

	for _, test := range tests {
		val := graphql.String.Serialize(test.Value)
		if val != test.Expected {
			reflectedValue := reflect.ValueOf(test.Value)
			t.Fatalf("Failed String.Serialize(%v(%v)), expected: %v, got %v", reflectedValue.Type(), test.Value, test.Expected, val)
		}
	}
}

func TestTypeSystem_Scalar_SerializesOutputBoolean(t *testing.T) {
	tests := []boolSerializationTest{
		{"true", true},
		{"false", false},
		{"string", true},
		{"", false},
		{int(1), true},
		{int(0), false},
		{true, true},
		{false, false},
	}

	for _, test := range tests {
		val := graphql.Boolean.Serialize(test.Value)
		if val != test.Expected {
			reflectedValue := reflect.ValueOf(test.Value)
			t.Fatalf("Failed String.Boolean(%v(%v)), expected: %v, got %v", reflectedValue.Type(), test.Value, test.Expected, val)
		}
	}
}
