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
		{"one", "1", 0.0, typeinfo.Int32Type},
		{"negative one", "-1", 0.0, typeinfo.Int32Type},
		{"negative one point 0", "-1.0", 0.0, typeinfo.Float32Type},
		{"negative one point 0 with FT of 0.1", "-1.0", 0.1, typeinfo.Int32Type},
		{"negative one point one with FT of 0.1", "-1.1", 0.1, typeinfo.Float32Type},
		{"negative one point 999 with FT of 1.0", "-1.999", 1.0, typeinfo.Int32Type},
		{"zero point zero zero zero zero", "0.0000", 0.0, typeinfo.Float32Type},
		{"max int", strconv.FormatUint(math.MaxInt64, 10), 0.0, typeinfo.Int64Type},
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
		{"zero", "0", 0.0, typeinfo.Int32Type},
		{"zero float", "0.0", 0.0, typeinfo.Float32Type},
		{"zero float with floatThreshold of 0.1", "0.0", 0.1, typeinfo.Int32Type},
		{"negative float", "-1.3451234", 0.0, typeinfo.Float32Type},
		{"double decimal point", "0.00.0", 0.0, typeinfo.UnknownType},
		{"leading zero floats", "05.78", 0.0, typeinfo.Float32Type},
		{"zero float with high precision", "0.0000", 0.0, typeinfo.Float32Type},
		{"all zeroes", "0000", 0.0, typeinfo.StringDefaultType},
		{"leading zeroes", "01", 0.0, typeinfo.StringDefaultType},
		{"negative int", "-1234", 0.0, typeinfo.Int32Type},
		{"fits in uint64 but not int64", strconv.FormatUint(math.MaxUint64, 10), 0.0, typeinfo.StringDefaultType},
		{"negative less than math.MinInt64", "-" + strconv.FormatUint(math.MaxUint64, 10), 0.0, typeinfo.StringDefaultType},
		{"math.MinInt64", strconv.FormatInt(math.MinInt64, 10), 0.0, typeinfo.Int64Type},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actualType := leastPermissiveNumericType(test.valStr, test.floatThreshold)
			assert.Equal(t, test.expType, actualType, "val: %s, expected: %v, actual: %v", test.valStr, test.expType, actualType)
		})
	}
}

func TestLeasPermissiveChronoType(t *testing.T) {
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
			name: "all floats",
			inferSet: typeInfoSet{
				typeinfo.Float32Type: {},
				typeinfo.Float64Type: {},
			},
			expType: typeinfo.Float64Type,
		},
		{
			name: "32 bit ints",
			inferSet: typeInfoSet{
				typeinfo.Int32Type: {},
			},
			expType: typeinfo.Int32Type,
		},
		{
			name: "64 bit ints",
			inferSet: typeInfoSet{
				typeinfo.Int64Type: {},
			},
			expType: typeinfo.Int64Type,
		},
		{
			name: "32 bit ints and floats",
			inferSet: typeInfoSet{
				typeinfo.Int32Type:   {},
				typeinfo.Float32Type: {},
			},
			expType: typeinfo.Float32Type,
		},
		{
			name: "64 bit ints and floats",
			inferSet: typeInfoSet{
				typeinfo.Int64Type:   {},
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
f3c600cd-74d3-4c2d-b5a5-88934e7c6018,-3,9223372036854775810,-3.2,false,a test
0b4fe00e-c690-4fbe-b014-b07f236cd7fd,-2,9223372036854775810,-2.3,TRUE,anything could
7d85f566-4b60-4cc0-a029-cfb679c3fd8b,-1,9223372036854775810,-1.4,FALSE,be written
df017641-5e4b-4ef5-9f7a-9e9f2ce044b3,0,9223372036854775810,0.0,true,in these
4b4fb1af-0701-48dc-8593-aa7e094e72cc,1,9223372036854775810,1.5,false,string
b97b7e95-c811-4327-8f75-8358595e9614,2,9223372036854775810,2.6,TRUE,columns.
68d7b8bb-9065-47a8-9302-d4d44e5a149d,3,9223372036854775810,3.7,FALSE,Even emojis
5b6b7354-f89e-44b2-9058-8aea7f17d5fb,4,9223372036854775810,4.8,true,🐈🐈🐈🐈`

var oneOfEachKindWithSomeNilsCSVStr = `uuid,int,uint,float,bool,string
6a52205c-2a46-4b04-9567-9e8707054774,-4,9223372036854775810,-4.1,true,this is
3975299d-71cf-4faa-afea-155d64bd1a9b,-3,9223372036854775810,-3.2,false,a test
8b682886-0ad7-4b96-a300-48fc1e10032e,,9223372036854775810,-2.3,TRUE,anything could
5da4b68c-58b1-41f9-8fed-4fbe15740b79,-1,9223372036854775810,-1.4,FALSE,be written
c659a238-3516-4380-a443-e528e55eb1bd,0,9223372036854775810,0.0,true,in these
86f3bb91-7476-48a1-8d14-6249cc696b02,1,9223372036854775810,1.5,false,string
42f2b05d-c279-4058-a13a-58f81a867401,,9223372036854775810,2.6,TRUE,columns.
935afaea-af2a-4cc9-b008-505a5edbeb3e,3,9223372036854775810,3.7,FALSE,Even emojis
aab7b641-124e-4193-8837-97cdfb13e8c4,4,9223372036854775810,4.8,true,🐈🐈🐈🐈`

var mixUintsAndPositiveInts = `uuid,mix
95e8ccf4-78a7-479e-a974-cf8605e6b01d,9223372036854775810
60d6b5f9-9c99-4165-a835-1159e54b82ca,0
cab7f39d-e1f2-46f8-b858-372067ec75a0,1000000`

var floatsWithZeroForFractionalPortion = `uuid,float
61c8e0ea-f2ff-49f8-98b8-df8142fd8a94,0.0
e5a3bdca-9315-4afe-b548-6e44924ff549,-1.0
e6b09e59-d524-4417-8252-f5c3eeeb5033,1.0`

var floatsWithLargeFractionalPortion = `uuid,float
ed929281-b4d0-4631-b931-8c2ebfb4ba84,0.0
7d7c9b56-301c-4aa0-9034-8e12315b35cf,-1.0
76bbed0b-7479-45bb-9813-ff1383bccee6,1.0`

var floatsWithTinyFractionalPortion = `uuid,float
da95f30d-4cde-4e53-a786-e00624ff2cbe,0.0001
6fb474ca-8bec-4e21-9af2-a1ba22f39f1d,-1.0005
aee125d4-e055-42e9-af3d-0bc676436ccd,1.0001`

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
				"uint":   typeinfo.StringDefaultType,
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
				"mix":  typeinfo.StringDefaultType,
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
			dEnv := dtestutils.CreateTestEnv()
			defer dEnv.DoltDB(ctx).Close()

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

			allCols, err := InferColumnTypesFromTableReader(context.Background(), csvRd, test.infArgs)
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
				assert.True(t, idx == -1, "%s unexpected not null constraint", col.Name)
				return false, nil
			})
			require.NoError(t, err)
		})
	}
}
