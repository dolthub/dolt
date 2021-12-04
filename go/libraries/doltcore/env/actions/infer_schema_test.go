// Copyright 2019 Dolthub, Inc.
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

package actions

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/csv"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/types"
)

var maxIntPlusTwo uint64 = 1<<63 + 1

func TestLeastPermissiveType(t *testing.T) {
	tests := []struct {
		name           string
		valStr         string
		floatThreshold float64
		expType        typeinfo.TypeInfo
	}{
		{"empty string", "", 0.0, typeinfo.UnknownType},
		{"valid uuid", "00000000-0000-0000-0000-000000000000", 0.0, typeinfo.UuidType},
		{"invalid uuid", "00000000-0000-0000-0000-00000000000z", 0.0, typeinfo.StringDefaultType},
		{"lower bool", "true", 0.0, typeinfo.BoolType},
		{"upper bool", "FALSE", 0.0, typeinfo.BoolType},
		{"yes", "yes", 0.0, typeinfo.StringDefaultType},
		{"one", "1", 0.0, typeinfo.Uint32Type},
		{"negative one", "-1", 0.0, typeinfo.Int32Type},
		{"negative one point 0", "-1.0", 0.0, typeinfo.Float32Type},
		{"negative one point 0 with FT of 0.1", "-1.0", 0.1, typeinfo.Int32Type},
		{"negative one point one with FT of 0.1", "-1.1", 0.1, typeinfo.Float32Type},
		{"negative one point 999 with FT of 1.0", "-1.999", 1.0, typeinfo.Int32Type},
		{"zero point zero zero zero zero", "0.0000", 0.0, typeinfo.Float32Type},
		{"max int", strconv.FormatUint(math.MaxInt64, 10), 0.0, typeinfo.Uint64Type},
		{"bigger than max int", strconv.FormatUint(math.MaxUint64, 10) + "0", 0.0, typeinfo.StringDefaultType},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actualType := leastPermissiveType(test.valStr, test.floatThreshold)
			assert.Equal(t, test.expType, actualType, "val: %s, expected: %v, actual: %v", test.valStr, test.expType, actualType)
		})
	}
}

func TestLeastPermissiveNumericType(t *testing.T) {
	tests := []struct {
		name           string
		valStr         string
		floatThreshold float64
		expType        typeinfo.TypeInfo
	}{
		{"zero", "0", 0.0, typeinfo.Uint32Type},
		{"zero float", "0.0", 0.0, typeinfo.Float32Type},
		{"zero float with floatThreshold of 0.1", "0.0", 0.1, typeinfo.Int32Type},
		{"negative float", "-1.3451234", 0.0, typeinfo.Float32Type},
		{"double decimal point", "0.00.0", 0.0, typeinfo.UnknownType},
		{"leading zero floats", "05.78", 0.0, typeinfo.Float32Type},
		{"zero float with high precision", "0.0000", 0.0, typeinfo.Float32Type},
		{"all zeroes", "0000", 0.0, typeinfo.StringDefaultType},
		{"leading zeroes", "01", 0.0, typeinfo.StringDefaultType},
		{"negative int", "-1234", 0.0, typeinfo.Int32Type},
		{"fits in uint64 but not int64", strconv.FormatUint(math.MaxUint64, 10), 0.0, typeinfo.Uint64Type},
		{"negative less than math.MinInt64", "-" + strconv.FormatUint(math.MaxUint64, 10), 0.0, typeinfo.UnknownType},
		{"math.MinInt64", strconv.FormatInt(math.MinInt64, 10), 0.0, typeinfo.Int64Type},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actualType := leastPermissiveNumericType(test.valStr, test.floatThreshold)
			assert.Equal(t, test.expType, actualType, "val: %s, expected: %v, actual: %v", test.valStr, test.expType, actualType)
		})
	}
}

func TestLeastPermissiveChronoType(t *testing.T) {
	tests := []struct {
		name    string
		valStr  string
		expType typeinfo.TypeInfo
	}{
		{"empty string", "", typeinfo.UnknownType},
		{"random string", "asdf", typeinfo.UnknownType},
		{"time", "9:27:10.485214", typeinfo.TimeType},
		{"date", "2020-02-02", typeinfo.DateType},
		{"also date", "2020-02-02 00:00:00.0", typeinfo.DateType},
		{"datetime", "2030-01-02 04:06:03.472382", typeinfo.DatetimeType},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actualType := leastPermissiveChronoType(test.valStr)
			assert.Equal(t, test.expType, actualType, "val: %s, expected: %v, actual: %v", test.valStr, test.expType, actualType)
		})
	}
}

type commonTypeTest struct {
	name     string
	inferSet typeInfoSet
	expType  typeinfo.TypeInfo
}

func TestFindCommonType(t *testing.T) {
	testFindCommonType(t)
	testFindCommonTypeFromSingleType(t)
	testFindCommonChronologicalType(t)
}

func testFindCommonType(t *testing.T) {
	tests := []commonTypeTest{
		{
			name: "all signed ints",
			inferSet: typeInfoSet{
				typeinfo.Int32Type: {},
				typeinfo.Int64Type: {},
			},
			expType: typeinfo.Int64Type,
		},
		{
			name: "all unsigned ints",
			inferSet: typeInfoSet{
				typeinfo.Uint32Type: {},
				typeinfo.Uint64Type: {},
			},
			expType: typeinfo.Uint64Type,
		},
		{
			name: "all floats",
			inferSet: typeInfoSet{
				typeinfo.Float32Type: {},
				typeinfo.Float64Type: {},
			},
			expType: typeinfo.Float64Type,
		},
		{
			name: "32 bit ints and uints",
			inferSet: typeInfoSet{
				typeinfo.Int32Type:  {},
				typeinfo.Uint32Type: {},
			},
			expType: typeinfo.Int32Type,
		},
		{
			name: "64 bit ints and uints",
			inferSet: typeInfoSet{
				typeinfo.Int64Type:  {},
				typeinfo.Uint64Type: {},
			},
			expType: typeinfo.Int64Type,
		},
		{
			name: "32 bit ints, uints, and floats",
			inferSet: typeInfoSet{
				typeinfo.Int32Type:   {},
				typeinfo.Uint32Type:  {},
				typeinfo.Float32Type: {},
			},
			expType: typeinfo.Float32Type,
		},
		{
			name: "64 bit ints, uints, and floats",
			inferSet: typeInfoSet{
				typeinfo.Int64Type:   {},
				typeinfo.Uint64Type:  {},
				typeinfo.Float64Type: {},
			},
			expType: typeinfo.Float64Type,
		},
		{
			name: "ints and bools",
			inferSet: typeInfoSet{
				typeinfo.Int32Type: {},
				typeinfo.BoolType:  {},
			},
			expType: typeinfo.StringDefaultType,
		},
		{
			name: "floats and bools",
			inferSet: typeInfoSet{
				typeinfo.Float32Type: {},
				typeinfo.BoolType:    {},
			},
			expType: typeinfo.StringDefaultType,
		},
		{
			name: "floats and uuids",
			inferSet: typeInfoSet{
				typeinfo.Float32Type: {},
				typeinfo.UuidType:    {},
			},
			expType: typeinfo.StringDefaultType,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actualType := findCommonType(test.inferSet)
			assert.Equal(t, test.expType, actualType)
		})
	}
}

func testFindCommonTypeFromSingleType(t *testing.T) {
	allTypes := []typeinfo.TypeInfo{
		typeinfo.Uint8Type,
		typeinfo.Uint16Type,
		typeinfo.Uint24Type,
		typeinfo.Uint32Type,
		typeinfo.Uint64Type,
		typeinfo.Int8Type,
		typeinfo.Int16Type,
		typeinfo.Int24Type,
		typeinfo.Int32Type,
		typeinfo.Int64Type,
		typeinfo.Float32Type,
		typeinfo.Float64Type,
		typeinfo.BoolType,
		typeinfo.UuidType,
		typeinfo.YearType,
		typeinfo.DateType,
		typeinfo.TimeType,
		typeinfo.TimestampType,
		typeinfo.DatetimeType,
		typeinfo.StringDefaultType,
	}

	for _, ti := range allTypes {
		tests := []commonTypeTest{
			{
				name: fmt.Sprintf("only %s", ti.String()),
				inferSet: typeInfoSet{
					ti: {},
				},
				expType: ti,
			},
			{
				name: fmt.Sprintf("Unknown and %s", ti.String()),
				inferSet: typeInfoSet{
					ti:                   {},
					typeinfo.UnknownType: {},
				},
				expType: ti,
			},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				actualType := findCommonType(test.inferSet)
				assert.Equal(t, test.expType, actualType)
			})
		}
	}
}

func testFindCommonChronologicalType(t *testing.T) {

	tests := []commonTypeTest{
		{
			name: "date and time",
			inferSet: typeInfoSet{
				typeinfo.DateType: {},
				typeinfo.TimeType: {},
			},
			expType: typeinfo.DatetimeType,
		},
		{
			name: "date and datetime",
			inferSet: typeInfoSet{
				typeinfo.DateType:     {},
				typeinfo.DatetimeType: {},
			},
			expType: typeinfo.DatetimeType,
		},
		{
			name: "time and datetime",
			inferSet: typeInfoSet{
				typeinfo.TimeType:     {},
				typeinfo.DatetimeType: {},
			},
			expType: typeinfo.DatetimeType,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actualType := findCommonType(test.inferSet)
			assert.Equal(t, test.expType, actualType)
		})
	}
}

var oneOfEachKindCSVStr = `uuid,int,uint,float,bool,string
00000000-0000-0000-0000-000000000000,-4,9223372036854775810,-4.1,true,this is
00000000-0000-0000-0000-000000000001,-3,9223372036854775810,-3.2,false,a test
00000000-0000-0000-0000-000000000002,-2,9223372036854775810,-2.3,TRUE,anything could
00000000-0000-0000-0000-000000000003,-1,9223372036854775810,-1.4,FALSE,be written
00000000-0000-0000-0000-000000000004,0,9223372036854775810,0.0,true,in these
00000000-0000-0000-0000-000000000005,1,9223372036854775810,1.5,false,string
00000000-0000-0000-0000-000000000006,2,9223372036854775810,2.6,TRUE,columns.
00000000-0000-0000-0000-000000000007,3,9223372036854775810,3.7,FALSE,Even emojis
00000000-0000-0000-0000-000000000008,4,9223372036854775810,4.8,true,🐈🐈🐈🐈`

var oneOfEachKindWithSomeNilsCSVStr = `uuid,int,uint,float,bool,string
00000000-0000-0000-0000-000000000000,-4,9223372036854775810,-4.1,true,this is
00000000-0000-0000-0000-000000000001,-3,9223372036854775810,-3.2,false,a test
00000000-0000-0000-0000-000000000002,,9223372036854775810,-2.3,TRUE,anything could
00000000-0000-0000-0000-000000000003,-1,9223372036854775810,-1.4,FALSE,be written
00000000-0000-0000-0000-000000000004,0,9223372036854775810,0.0,true,in these
00000000-0000-0000-0000-000000000005,1,9223372036854775810,1.5,false,string
00000000-0000-0000-0000-000000000006,,9223372036854775810,2.6,TRUE,columns.
00000000-0000-0000-0000-000000000007,3,9223372036854775810,3.7,FALSE,Even emojis
00000000-0000-0000-0000-000000000008,4,9223372036854775810,4.8,true,🐈🐈🐈🐈`

var mixUintsAndPositiveInts = `uuid,mix
00000000-0000-0000-0000-000000000000,9223372036854775810
00000000-0000-0000-0000-000000000001,0
00000000-0000-0000-0000-000000000002,1000000`

var floatsWithZeroForFractionalPortion = `uuid,float
00000000-0000-0000-0000-000000000000,0.0
00000000-0000-0000-0000-000000000001,-1.0
00000000-0000-0000-0000-000000000002,1.0`

var floatsWithLargeFractionalPortion = `uuid,float
00000000-0000-0000-0000-000000000000,0.0
00000000-0000-0000-0000-000000000001,-1.0
00000000-0000-0000-0000-000000000002,1.0`

var floatsWithTinyFractionalPortion = `uuid,float
00000000-0000-0000-0000-000000000000,0.0001
00000000-0000-0000-0000-000000000001,-1.0005
00000000-0000-0000-0000-000000000002,1.0001`

var identityMapper = make(rowconv.NameMapper)

type testInferenceArgs struct {
	ColMapper      rowconv.NameMapper
	floatThreshold float64
}

func (tia testInferenceArgs) ColNameMapper() rowconv.NameMapper {
	return tia.ColMapper
}

func (tia testInferenceArgs) FloatThreshold() float64 {
	return tia.floatThreshold
}

func TestInferSchema(t *testing.T) {
	tests := []struct {
		name         string
		csvContents  string
		infArgs      InferenceArgs
		expTypes     map[string]typeinfo.TypeInfo
		nullableCols *set.StrSet
	}{
		{
			"one of each kind",
			oneOfEachKindCSVStr,
			testInferenceArgs{
				ColMapper:      identityMapper,
				floatThreshold: 0,
			},
			map[string]typeinfo.TypeInfo{
				"int":    typeinfo.Int32Type,
				"uint":   typeinfo.Uint64Type,
				"uuid":   typeinfo.UuidType,
				"float":  typeinfo.Float32Type,
				"bool":   typeinfo.BoolType,
				"string": typeinfo.StringDefaultType,
			},
			nil,
		},
		{
			"mix uints and positive ints",
			mixUintsAndPositiveInts,
			testInferenceArgs{
				ColMapper:      identityMapper,
				floatThreshold: 0,
			},
			map[string]typeinfo.TypeInfo{
				"mix":  typeinfo.Uint64Type,
				"uuid": typeinfo.UuidType,
			},
			nil,
		},
		{
			"floats with zero fractional and float threshold of 0",
			floatsWithZeroForFractionalPortion,
			testInferenceArgs{
				ColMapper:      identityMapper,
				floatThreshold: 0,
			},
			map[string]typeinfo.TypeInfo{
				"float": typeinfo.Float32Type,
				"uuid":  typeinfo.UuidType,
			},
			nil,
		},
		{
			"floats with zero fractional and float threshold of 0.1",
			floatsWithZeroForFractionalPortion,
			testInferenceArgs{
				ColMapper:      identityMapper,
				floatThreshold: 0.1,
			},
			map[string]typeinfo.TypeInfo{
				"float": typeinfo.Int32Type,
				"uuid":  typeinfo.UuidType,
			},
			nil,
		},
		{
			"floats with large fractional and float threshold of 1.0",
			floatsWithLargeFractionalPortion,
			testInferenceArgs{
				ColMapper:      identityMapper,
				floatThreshold: 1.0,
			},
			map[string]typeinfo.TypeInfo{
				"float": typeinfo.Int32Type,
				"uuid":  typeinfo.UuidType,
			},
			nil,
		},
		{
			"float threshold smaller than some of the values",
			floatsWithTinyFractionalPortion,
			testInferenceArgs{
				ColMapper:      identityMapper,
				floatThreshold: 0.0002,
			},
			map[string]typeinfo.TypeInfo{
				"float": typeinfo.Float32Type,
				"uuid":  typeinfo.UuidType,
			},
			nil,
		},
	}

	const importFilePath = "/Users/home/datasets/test/import_file.csv"

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			dEnv := dtestutils.CreateTestEnv()

			wrCl, err := dEnv.FS.OpenForWrite(importFilePath, os.ModePerm)
			require.NoError(t, err)
			_, err = wrCl.Write([]byte(test.csvContents))
			require.NoError(t, err)
			err = wrCl.Close()
			require.NoError(t, err)

			rdCl, err := dEnv.FS.OpenForRead(importFilePath)
			require.NoError(t, err)

			csvRd, err := csv.NewCSVReader(types.Format_Default, rdCl, csv.NewCSVInfo())
			require.NoError(t, err)

			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)
			allCols, err := InferColumnTypesFromTableReader(context.Background(), root, csvRd, test.infArgs)
			require.NoError(t, err)

			assert.Equal(t, len(test.expTypes), allCols.Size())
			err = allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
				expectedType, ok := test.expTypes[col.Name]
				require.True(t, ok, "column not found: %s", col.Name)
				assert.Equal(t, expectedType, col.TypeInfo, "column: %s - expected: %s got: %s", col.Name, expectedType.String(), col.TypeInfo.String())
				return false, nil
			})
			require.NoError(t, err)

			if test.nullableCols == nil {
				test.nullableCols = set.NewStrSet(nil)
			}

			err = allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
				idx := schema.IndexOfConstraint(col.Constraints, schema.NotNullConstraintType)
				assert.True(t, idx == -1 == test.nullableCols.Contains(col.Name), "%s unexpected nullability", col.Name)
				return false, nil
			})
			require.NoError(t, err)
		})
	}
}
