package doltcore

import (
	"testing"

	"github.com/attic-labs/noms/go/types"
)

type strToNomsTypeTests struct {
	s      string
	k      types.NomsKind
	expVal types.Value
	expErr bool
}

var tests = []strToNomsTypeTests{
	{"test string", types.StringKind, types.String("test string"), false},
	{"1.3294398", types.FloatKind, types.Float(1.3294398), false},
	{"-3294398", types.FloatKind, types.Float(-3294398), false},
	{"TRuE", types.BoolKind, types.Bool(true), false},
	{"False", types.BoolKind, types.Bool(false), false},
	{"-123456", types.IntKind, types.Int(-123456), false},
	{"123456", types.IntKind, types.Int(123456), false},
	{"100000000000", types.UintKind, types.Uint(100000000000), false},
	{"0", types.UintKind, types.Uint(0), false},
	{
		"01234567-89ab-cdef-FEDC-BA9876543210",
		types.UUIDKind,
		types.UUID([16]byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10}),
		false},
	{"0", types.UintKind, types.Uint(0), false},
	{"", types.NullKind, types.NullValue, false},

	{"test failure", types.FloatKind, nil, true},
	{"test failure", types.BoolKind, nil, true},
	{"test failure", types.IntKind, nil, true},
	{"0xdeadbeef", types.IntKind, nil, true},
	{"test failure", types.UintKind, nil, true},
	{"-1", types.UintKind, nil, true},
	{"0123456789abcdeffedcba9876543210abc", types.UUIDKind, nil, true},
	{"0", types.UUIDKind, nil, true},
}

func TestStrConversion(t *testing.T) {
	for _, test := range tests {
		val, err := StringToValue(test.s, test.k)

		if (err != nil) != test.expErr {
			t.Errorf("Conversion of \"%s\" returned unexpected error: %v", test.s, err)
		}

		if err == nil && val != test.expVal {
			t.Errorf("Conversion of \"%s\" returned unexpected error: %v", test.s, err)
		}
	}
}
