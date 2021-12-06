// Copyright 2021 Dolthub, Inc.
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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/csv"
	"github.com/dolthub/dolt/go/store/types"
)

func TestSqlLeastPermissiveType(t *testing.T) {
	tests := []struct {
		name           string
		valStr         string
		floatThreshold float64
		expType        sql.Type
	}{
		{"empty string", "", 0.0, sql.Null},
		{"valid uuid", "00000000-0000-0000-0000-000000000000", 0.0, sql.UUID},
		{"invalid uuid", "00000000-0000-0000-0000-00000000000z", 0.0, sql.Text},
		{"lower bool", "true", 0.0, sql.Boolean},
		{"upper bool", "FALSE", 0.0, sql.Boolean},
		{"yes", "yes", 0.0, sql.Text},
		{"one", "1", 0.0, sql.Uint32},
		{"negative one", "-1", 0.0, sql.Int32},
		{"negative one point 0", "-1.0", 0.0, sql.Float32},
		{"negative one point 0 with FT of 0.1", "-1.0", 0.1, sql.Int32},
		{"negative one point one with FT of 0.1", "-1.1", 0.1, sql.Float32},
		{"negative one point 999 with FT of 1.0", "-1.999", 1.0, sql.Int32},
		{"zero point zero zero zero zero", "0.0000", 0.0, sql.Float32},
		{"max int", strconv.FormatUint(math.MaxInt64, 10), 0.0, sql.Uint64},
		{"bigger than max int", strconv.FormatUint(math.MaxUint64, 10) + "0", 0.0, sql.Text},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actualType := sqlLeastPermissiveType(test.valStr, test.floatThreshold)
			assert.Equal(t, test.expType, actualType, "val: %s, expected: %v, actual: %v", test.valStr, test.expType, actualType)
		})
	}
}

func TestSqlLeastPermissiveNumericType(t *testing.T) {
	tests := []struct {
		name           string
		valStr         string
		floatThreshold float64
		expType        sql.Type
	}{
		{"zero", "0", 0.0, sql.Uint32},
		{"zero float", "0.0", 0.0, sql.Float32},
		{"zero float with floatThreshold of 0.1", "0.0", 0.1, sql.Int32},
		{"negative float", "-1.3451234", 0.0, sql.Float32},
		{"double decimal point", "0.00.0", 0.0, sql.Null},
		{"leading zero floats", "05.78", 0.0, sql.Float32},
		{"zero float with high precision", "0.0000", 0.0, sql.Float32},
		{"all zeroes", "0000", 0.0, sql.Text},
		{"leading zeroes", "01", 0.0, sql.Text},
		{"negative int", "-1234", 0.0, sql.Int32},
		{"fits in uint64 but not int64", strconv.FormatUint(math.MaxUint64, 10), 0.0, sql.Uint64},
		{"negative less than math.MinInt64", "-" + strconv.FormatUint(math.MaxUint64, 10), 0.0, sql.Null},
		{"math.MinInt64", strconv.FormatInt(math.MinInt64, 10), 0.0, sql.Int64},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actualType, _ := leastPermissiveSqlNumericType(test.valStr, test.floatThreshold)
			assert.Equal(t, test.expType, actualType, "val: %s, expected: %v, actual: %v", test.valStr, test.expType, actualType)
		})
	}
}

func TestSqlLeastPermissiveChronoType(t *testing.T) {
	tests := []struct {
		name    string
		valStr  string
		expType sql.Type
	}{
		{"empty string", "", sql.Null},
		{"random string", "asdf", sql.Null},
		{"time", "9:27:10.485214", sql.Time},
		{"date", "2020-02-02", sql.Date},
		{"also date", "2020-02-02 00:00:00.0", sql.Date},
		{"datetime", "2030-01-02 04:06:03.472382", sql.Datetime},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actualType, _ := leastPermissiveSqlChronoType(test.valStr)
			assert.Equal(t, test.expType, actualType, "val: %s, expected: %v, actual: %v", test.valStr, test.expType, actualType)
		})
	}
}

type sqlCommonTypeTest struct {
	name     string
	inferSet sqlTypeInfoSet
	expType  sql.Type
}

func TestSqlFindCommonType(t *testing.T) {
	testSqlFindCommonType(t)
	testSqlFindCommonTypeFromSingleType(t)
	testSqlFindCommonChronologicalType(t)
}

func testSqlFindCommonType(t *testing.T) {
	tests := []sqlCommonTypeTest{
		{
			name: "all signed ints",
			inferSet: sqlTypeInfoSet{
				sql.Int32: {},
				sql.Int64: {},
			},
			expType: sql.Int64,
		},
		{
			name: "all unsigned ints",
			inferSet: sqlTypeInfoSet{
				sql.Uint32: {},
				sql.Uint64: {},
			},
			expType: sql.Uint64,
		},
		{
			name: "all floats",
			inferSet: sqlTypeInfoSet{
				sql.Float32: {},
				sql.Float64: {},
			},
			expType: sql.Float64,
		},
		{
			name: "32 bit ints and uints",
			inferSet: sqlTypeInfoSet{
				sql.Int32:  {},
				sql.Uint32: {},
			},
			expType: sql.Int32,
		},
		{
			name: "64 bit ints and uints",
			inferSet: sqlTypeInfoSet{
				sql.Int64:  {},
				sql.Uint64: {},
			},
			expType: sql.Int64,
		},
		{
			name: "32 bit ints, uints, and floats",
			inferSet: sqlTypeInfoSet{
				sql.Int32:   {},
				sql.Uint32:  {},
				sql.Float32: {},
			},
			expType: sql.Float32,
		},
		{
			name: "64 bit ints, uints, and floats",
			inferSet: sqlTypeInfoSet{
				sql.Int64:   {},
				sql.Uint64:  {},
				sql.Float64: {},
			},
			expType: sql.Float64,
		},
		{
			name: "ints and bools",
			inferSet: sqlTypeInfoSet{
				sql.Int32:   {},
				sql.Boolean: {},
			},
			expType: sql.Text,
		},
		{
			name: "floats and bools",
			inferSet: sqlTypeInfoSet{
				sql.Float32: {},
				sql.Boolean: {},
			},
			expType: sql.Text,
		},
		{
			name: "floats and uuids",
			inferSet: sqlTypeInfoSet{
				sql.Float32: {},
				sql.UUID:    {},
			},
			expType: sql.Text,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actualType := findCommonSQlType(test.inferSet)
			assert.Equal(t, test.expType, actualType)
		})
	}
}

func testSqlFindCommonTypeFromSingleType(t *testing.T) {
	allTypes := []sql.Type{
		sql.Uint8,
		sql.Uint16,
		sql.Uint24,
		sql.Uint32,
		sql.Uint64,
		sql.Int8,
		sql.Int16,
		sql.Int24,
		sql.Int32,
		sql.Int64,
		sql.Float32,
		sql.Float64,
		sql.Boolean,
		sql.UUID,
		sql.Year,
		sql.Date,
		sql.Time,
		sql.Timestamp,
		sql.Date,
		sql.Text,
	}

	for _, ti := range allTypes {
		tests := []sqlCommonTypeTest{
			{
				name: fmt.Sprintf("only %s", ti.String()),
				inferSet: sqlTypeInfoSet{
					ti: {},
				},
				expType: ti,
			},
			{
				name: fmt.Sprintf("Unknown and %s", ti.String()),
				inferSet: sqlTypeInfoSet{
					ti:       {},
					sql.Null: {},
				},
				expType: ti,
			},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				actualType := findCommonSQlType(test.inferSet)
				assert.Equal(t, test.expType, actualType)
			})
		}
	}
}

func testSqlFindCommonChronologicalType(t *testing.T) {
	tests := []sqlCommonTypeTest{
		{
			name: "date and time",
			inferSet: sqlTypeInfoSet{
				sql.Date: {},
				sql.Time: {},
			},
			expType: sql.Datetime,
		},
		{
			name: "date and datetime",
			inferSet: sqlTypeInfoSet{
				sql.Date: {},
				sql.Date: {},
			},
			expType: sql.Date,
		},
		{
			name: "time and datetime",
			inferSet: sqlTypeInfoSet{
				sql.Time:     {},
				sql.Datetime: {},
			},
			expType: sql.Datetime,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actualType := findCommonSQlType(test.inferSet)
			assert.Equal(t, test.expType, actualType)
		})
	}
}

func TestSqlInferSchema(t *testing.T) {
	tests := []struct {
		name        string
		csvContents string
		infArgs     InferenceArgs
		expTypes    map[string]sql.Type
	}{
		{
			"one of each kind",
			oneOfEachKindCSVStr,
			testInferenceArgs{
				ColMapper:      identityMapper,
				floatThreshold: 0,
			},
			map[string]sql.Type{
				"int":    sql.Int32,
				"uint":   sql.Uint64,
				"uuid":   sql.UUID,
				"float":  sql.Float32,
				"bool":   sql.Boolean,
				"string": sql.Text,
			},
		},
		{
			"mix uints and positive ints",
			mixUintsAndPositiveInts,
			testInferenceArgs{
				ColMapper:      identityMapper,
				floatThreshold: 0,
			},
			map[string]sql.Type{
				"mix":  sql.Uint64,
				"uuid": sql.UUID,
			},
		},
		{
			"floats with zero fractional and float threshold of 0",
			floatsWithZeroForFractionalPortion,
			testInferenceArgs{
				ColMapper:      identityMapper,
				floatThreshold: 0,
			},
			map[string]sql.Type{
				"float": sql.Float32,
				"uuid":  sql.UUID,
			},
		},
		{
			"floats with zero fractional and float threshold of 0.1",
			floatsWithZeroForFractionalPortion,
			testInferenceArgs{
				ColMapper:      identityMapper,
				floatThreshold: 0.1,
			},
			map[string]sql.Type{
				"float": sql.Int32,
				"uuid":  sql.UUID,
			},
		},
		{
			"floats with large fractional and float threshold of 1.0",
			floatsWithLargeFractionalPortion,
			testInferenceArgs{
				ColMapper:      identityMapper,
				floatThreshold: 1.0,
			},
			map[string]sql.Type{
				"float": sql.Int32,
				"uuid":  sql.UUID,
			},
		},
		{
			"float threshold smaller than some of the values",
			floatsWithTinyFractionalPortion,
			testInferenceArgs{
				ColMapper:      identityMapper,
				floatThreshold: 0.0002,
			},
			map[string]sql.Type{
				"float": sql.Float32,
				"uuid":  sql.UUID,
			},
		},
	}

	const importFilePath = "/Users/home/datasets/test/import_file.csv"

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
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

			sch, err := InferSqlSchemaFromTableReader(context.Background(), csvRd, test.infArgs)
			require.NoError(t, err)

			assert.Equal(t, len(test.expTypes), len(sch))
			for _, col := range sch {
				expectedType, ok := test.expTypes[col.Name]
				require.True(t, ok, "column not found: %s", col.Name)
				assert.Equal(t, expectedType, col.Type, "column: %s - expected: %s got: %s", col.Name, expectedType.String(), col.Type.String())
			}
		})
	}
}
