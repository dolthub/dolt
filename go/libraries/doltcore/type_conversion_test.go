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
	"time"

	"github.com/google/uuid"

	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	zeroUUIDStr = "00000000-0000-0000-0000-000000000000"
)

var zeroUUID = uuid.Must(uuid.Parse(zeroUUIDStr))

func TestConv(t *testing.T) {
	tests := []struct {
		input       types.Value
		expectedOut types.Value
		expectFunc  bool
		expectErr   bool
	}{
		{types.String("test"), types.String("test"), true, false},
		{types.String(zeroUUIDStr), types.UUID(zeroUUID), true, false},
		{types.String("10"), types.Uint(10), true, false},
		{types.String("-101"), types.Int(-101), true, false},
		{types.String("3.25"), types.Float(3.25), true, false},
		{types.String("true"), types.Bool(true), true, false},
		{types.String("61D184E19083F09D95AB"), types.InlineBlob([]byte{0x61, 0xd1, 0x84, 0xe1, 0x90, 0x83, 0xf0, 0x9d, 0x95, 0xab}),
			true, false},
		{types.String("1970/12/31"),
			types.Timestamp(time.Date(1970, 12, 31, 0, 0, 0, 0, time.UTC)),
			true, false},
		{types.String("anything"), types.NullValue, true, false},

		{types.UUID(zeroUUID), types.String(zeroUUIDStr), true, false},
		{types.UUID(zeroUUID), types.UUID(zeroUUID), true, false},
		{types.UUID(zeroUUID), types.Uint(0), false, false},
		{types.UUID(zeroUUID), types.Int(0), false, false},
		{types.UUID(zeroUUID), types.Float(0), false, false},
		{types.UUID(zeroUUID), types.Bool(false), false, false},
		{types.UUID(zeroUUID), types.InlineBlob{}, false, false},
		{types.UUID(zeroUUID), types.Timestamp{}, false, false},
		{types.UUID(zeroUUID), types.NullValue, true, false},

		{types.Uint(10), types.String("10"), true, false},
		{types.Uint(100), types.UUID(zeroUUID), false, false},
		{types.Uint(1000), types.Uint(1000), true, false},
		{types.Uint(10000), types.Int(10000), true, false},
		{types.Uint(100000), types.Float(100000), true, false},
		{types.Uint(1000000), types.Bool(true), true, false},
		{types.Uint(10000000), types.InlineBlob{}, false, false},
		{types.Uint(100000000), types.Timestamp(time.Unix(100000000, 0).UTC()), true, false},
		{types.Uint(1000000000), types.NullValue, true, false},

		{types.Int(-10), types.String("-10"), true, false},
		{types.Int(-100), types.UUID(zeroUUID), false, false},
		{types.Int(1000), types.Uint(1000), true, false},
		{types.Int(-10000), types.Int(-10000), true, false},
		{types.Int(-100000), types.Float(-100000), true, false},
		{types.Int(-1000000), types.Bool(true), true, false},
		{types.Int(-10000000), types.InlineBlob{}, false, false},
		{types.Int(-100000000), types.Timestamp(time.Unix(-100000000, 0).UTC()), true, false},
		{types.Int(-1000000000), types.NullValue, true, false},

		{types.Float(1.5), types.String("1.5"), true, false},
		{types.Float(10.5), types.UUID(zeroUUID), false, false},
		{types.Float(100.5), types.Uint(100), true, false},
		{types.Float(1000.5), types.Int(1000), true, false},
		{types.Float(10000.5), types.Float(10000.5), true, false},
		{types.Float(100000.5), types.Bool(true), true, false},
		{types.Float(1000000.5), types.InlineBlob{}, false, false},
		{types.Float(10000000.5), types.Timestamp(time.Unix(10000000, 500000000).UTC()), true, false},
		{types.Float(100000000.5), types.NullValue, true, false},

		{types.Bool(true), types.String("true"), true, false},
		{types.Bool(false), types.UUID(zeroUUID), false, false},
		{types.Bool(true), types.Uint(1), true, false},
		{types.Bool(false), types.Int(0), true, false},
		{types.Bool(true), types.Float(1), true, false},
		{types.Bool(false), types.Bool(false), true, false},
		{types.Bool(false), types.InlineBlob{}, false, true},
		{types.Bool(false), types.Timestamp{}, false, true},
		{types.Bool(true), types.NullValue, true, false},

		{types.InlineBlob([]byte{0x61, 0xd1, 0x84, 0xe1, 0x90, 0x83, 0xf0, 0x9d, 0x95, 0xab}),
			types.String("61D184E19083F09D95AB"), true, false},
		{types.InlineBlob([]byte{}), types.UUID(zeroUUID), false, false},
		{types.InlineBlob([]byte{}), types.Uint(1583200922), false, false},
		{types.InlineBlob([]byte{}), types.Int(1901502183), false, false},
		{types.InlineBlob([]byte{}), types.Float(2219803444.4), false, false},
		{types.InlineBlob([]byte{}), types.Bool(false), false, true},
		{types.InlineBlob([]byte{1, 10, 100}), types.InlineBlob([]byte{1, 10, 100}), true, false},
		{types.InlineBlob([]byte{}), types.NullValue, true, false},

		{types.Timestamp(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)),
			types.String("2000-01-01 00:00:00 +0000"), true, false},
		{types.Timestamp(time.Date(2010, 2, 2, 1, 1, 1, 0, time.UTC)),
			types.UUID(zeroUUID), false, false},
		{types.Timestamp(time.Date(2020, 3, 3, 2, 2, 2, 0, time.UTC)),
			types.Uint(1583200922), true, false},
		{types.Timestamp(time.Date(2030, 4, 4, 3, 3, 3, 0, time.UTC)),
			types.Int(1901502183), true, false},
		{types.Timestamp(time.Date(2040, 5, 5, 4, 4, 4, 400000000, time.UTC)),
			types.Float(2219803444.4), true, false},
		{types.Timestamp(time.Date(2050, 6, 6, 5, 5, 5, 0, time.UTC)),
			types.Bool(false), false, true},
		{types.Timestamp(time.Date(2060, 7, 7, 6, 6, 6, 678912345, time.UTC)),
			types.Timestamp(time.Unix(2856405966, 678912345).UTC()), true, false},
		{types.Timestamp(time.Date(2070, 8, 8, 7, 7, 7, 0, time.UTC)),
			types.NullValue, true, false},
	}

	for _, test := range tests {
		convFunc, err := GetConvFunc(test.input.Kind(), test.expectedOut.Kind())

		if convFunc == nil && err != nil && test.expectFunc == true {
			t.Error("Did not receive conversion function for conversion from", test.input.Kind(), "to", test.expectedOut.Kind())
		} else if convFunc != nil {
			if test.expectFunc == false {
				t.Error("Incorrectly received conversion function for conversion from", test.input.Kind(), "to", test.expectedOut.Kind())
				continue
			}

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

var convertibleTypes = []types.NomsKind{types.StringKind, types.UUIDKind, types.UintKind, types.IntKind, types.FloatKind, types.BoolKind, types.InlineBlobKind}

func TestNullConversion(t *testing.T) {
	for _, srcKind := range convertibleTypes {
		for _, destKind := range convertibleTypes {
			convFunc, err := GetConvFunc(srcKind, destKind)

			if convFunc != nil && err == nil {
				res, err := convFunc(nil)

				if res != nil || err != nil {
					t.Error("null conversion failed")
				}
			}
		}
	}
}
