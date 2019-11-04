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
	"testing"

	"github.com/liquidata-inc/dolt/go/store/types"
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
	{"0.123", types.IntKind, types.Int(0), false},
	{".123", types.IntKind, types.Int(0), false},
	{"-0.123", types.IntKind, types.Int(0), false},
	{"-.123", types.IntKind, types.Int(0), false},
	{"100000000000", types.UintKind, types.Uint(100000000000), false},
	{"0", types.UintKind, types.Uint(0), false},
	{
		"01234567-89ab-cdef-FEDC-BA9876543210",
		types.UUIDKind,
		types.UUID([16]byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10}),
		false},
	{"0", types.UintKind, types.Uint(0), false},
	{"", types.NullKind, types.NullValue, false},
	{
		"61626364656667",
		types.InlineBlobKind,
		types.InlineBlob([]byte{0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67}),
		false},
	{
		"61D184E19083F09D95AB",
		types.InlineBlobKind,
		types.InlineBlob([]byte{0x61, 0xd1, 0x84, 0xe1, 0x90, 0x83, 0xf0, 0x9d, 0x95, 0xab}),
		false},

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

		if err == nil && !val.Equals(test.expVal) {
			t.Errorf("Conversion of \"%s\" returned unexpected error: %v", test.s, err)
		}
	}
}
