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

package actions

import (
	"context"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/csv"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/set"
	"github.com/liquidata-inc/dolt/go/store/types"
)

var maxIntPlusTwo uint64 = 1<<63 + 1

func TestLeastPermissiveKind(t *testing.T) {
	tests := []struct {
		name           string
		valStr         string
		floatThreshold float64
		expKind        types.NomsKind
		expHasNegs     bool
	}{
		{"empty string", "", 0.0, types.NullKind, false},
		{"valid uuid", "00000000-0000-0000-0000-000000000000", 0.0, types.UUIDKind, false},
		{"invalid uuid", "00000000-0000-0000-0000-00000000000z", 0.0, types.StringKind, false},
		{"lower bool", "true", 0.0, types.BoolKind, false},
		{"upper bool", "FALSE", 0.0, types.BoolKind, false},
		{"yes", "yes", 0.0, types.StringKind, false},
		{"one", "1", 0.0, types.IntKind, false},
		{"negative one", "-1", 0.0, types.IntKind, true},
		{"negative one point 0", "-1.0", 0.0, types.FloatKind, true},
		{"negative one point 0 with FT of 0.1", "-1.0", 0.1, types.IntKind, true},
		{"negative one point one with FT of 0.1", "-1.1", 0.1, types.FloatKind, true},
		{"negative one point 999 with FT of 1.0", "-1.999", 1.0, types.IntKind, true},
		{"zero point zero zero zero zero", "0.0000", 0.0, types.FloatKind, false},
		{"max int", strconv.FormatUint(math.MaxInt64, 10), 0.0, types.IntKind, false},
		{"bigger than max int", strconv.FormatUint(maxIntPlusTwo, 10), 0.0, types.UintKind, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actualKind, hasNegativeNums := leastPermissiveKind(test.valStr, test.floatThreshold)
			assert.Equal(t, test.expKind, actualKind, "val: %s, expected: %v, actual: %v", test.valStr, test.expKind, actualKind)
			assert.Equal(t, test.expHasNegs, hasNegativeNums)
		})
	}
}

func TestLeastPermissiveNumericKind(t *testing.T) {
	tests := []struct {
		name           string
		valStr         string
		floatThreshold float64
		expKind        types.NomsKind
		expNegative    bool
	}{
		{"empty string", "", 0.0, types.NullKind, false},
		{"zero", "0", 0.0, types.IntKind, false},
		{"zero float", "0.0", 0.0, types.FloatKind, false},
		{"zero float with floatThreshold of 0.1", "0.0", 0.1, types.IntKind, false},
		{"negative float", "-1.3451234", 0.0, types.FloatKind, true},
		{"double decimal point", "0.00.0", 0.0, types.NullKind, false},
		{"zero float with high precision", "0.0000", 0.0, types.FloatKind, false},
		{"all zeroes", "0000", 0.0, types.NullKind, false},
		{"leading zeroes", "01", 0.0, types.NullKind, false},
		{"negative int", "-1234", 0.0, types.IntKind, true},
		{"fits in uint64 but not int64", strconv.FormatUint(maxIntPlusTwo, 10), 0.0, types.UintKind, false},
		{"negative less than math.MinInt64", "-" + strconv.FormatUint(maxIntPlusTwo, 10), 0.0, types.NullKind, false},
		{"math.MinInt64", strconv.FormatInt(math.MinInt64, 10), 0.0, types.IntKind, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			isNegative, actualKind := leastPermissiveNumericKind(test.valStr, test.floatThreshold)
			assert.Equal(t, test.expKind, actualKind, "val: %s, expected: %v, actual: %v", test.valStr, test.expKind, actualKind)
			assert.Equal(t, test.expNegative, isNegative)
		})
	}
}

func TestStringNumericProperties(t *testing.T) {
	tests := []struct {
		name       string
		valStr     string
		expIsNum   bool
		expIsFloat bool
		expIsNeg   bool
	}{
		{"empty string", "", false, false, false},
		{"zero", "0", true, false, false},
		{"zero point zero", "0.0", true, true, false},
		{"negative one", "-1", true, false, true},
		{"negative one point 0", "-1.0", true, true, true},
		{"version", "1.0.1.45556", false, false, false},
		{"words", "this is a test", false, false, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			isNum, isFloat, isNeg := stringNumericProperties(test.valStr)
			assert.Equal(t, test.expIsNum, isNum)
			assert.Equal(t, test.expIsFloat, isFloat)
			assert.Equal(t, test.expIsNeg, isNeg)
		})
	}
}

func TestTypeCountsToKind(t *testing.T) {
	tests := []struct {
		name         string
		typeToCount  map[types.NomsKind]int
		hasNegatives bool
		expKind      types.NomsKind
		expNullable  bool
	}{
		{
			name: "all ints",
			typeToCount: map[types.NomsKind]int{
				types.IntKind: 70,
			},
			hasNegatives: true,
			expKind:      types.IntKind,
			expNullable:  false,
		},
		{
			name: "ints or null",
			typeToCount: map[types.NomsKind]int{
				types.IntKind:  35,
				types.NullKind: 35,
			},
			hasNegatives: true,
			expKind:      types.IntKind,
			expNullable:  true,
		},
		{
			name: "all uints",
			typeToCount: map[types.NomsKind]int{
				types.UintKind: 70,
			},
			hasNegatives: false,
			expKind:      types.UintKind,
			expNullable:  false,
		},
		{
			name: "floats or null",
			typeToCount: map[types.NomsKind]int{
				types.FloatKind: 35,
				types.NullKind:  35,
			},
			hasNegatives: false,
			expKind:      types.FloatKind,
			expNullable:  true,
		},
		{
			name: "all floats",
			typeToCount: map[types.NomsKind]int{
				types.FloatKind: 70,
			},
			hasNegatives: false,
			expKind:      types.FloatKind,
			expNullable:  false,
		},
		{
			name: "uints or null",
			typeToCount: map[types.NomsKind]int{
				types.UintKind: 35,
				types.NullKind: 35,
			},
			hasNegatives: false,
			expKind:      types.UintKind,
			expNullable:  true,
		},
		{
			name: "all bools",
			typeToCount: map[types.NomsKind]int{
				types.BoolKind: 70,
			},
			hasNegatives: false,
			expKind:      types.BoolKind,
			expNullable:  false,
		},
		{
			name: "bools or null",
			typeToCount: map[types.NomsKind]int{
				types.BoolKind: 35,
				types.NullKind: 35,
			},
			hasNegatives: false,
			expKind:      types.BoolKind,
			expNullable:  true,
		},
		{
			name: "all uuids",
			typeToCount: map[types.NomsKind]int{
				types.UUIDKind: 70,
			},
			hasNegatives: false,
			expKind:      types.UUIDKind,
			expNullable:  false,
		},
		{
			name: "uuids or null",
			typeToCount: map[types.NomsKind]int{
				types.UUIDKind: 35,
				types.NullKind: 35,
			},
			hasNegatives: false,
			expKind:      types.UUIDKind,
			expNullable:  true,
		},
		{
			name: "all strings",
			typeToCount: map[types.NomsKind]int{
				types.StringKind: 70,
			},
			hasNegatives: false,
			expKind:      types.StringKind,
			expNullable:  false,
		},
		{
			name: "strings or null",
			typeToCount: map[types.NomsKind]int{
				types.StringKind: 35,
				types.NullKind:   35,
			},
			hasNegatives: false,
			expKind:      types.StringKind,
			expNullable:  true,
		},
		{
			name: "positive ints and uints",
			typeToCount: map[types.NomsKind]int{
				types.IntKind:  35,
				types.UintKind: 35,
			},
			hasNegatives: false,
			expKind:      types.UintKind,
			expNullable:  false,
		},
		{
			name: "positive and negative ints and uints",
			typeToCount: map[types.NomsKind]int{
				types.IntKind:  35,
				types.UintKind: 35,
			},
			hasNegatives: true,
			expKind:      types.StringKind,
			expNullable:  false,
		},
		{
			name: "positive and negative ints and floats",
			typeToCount: map[types.NomsKind]int{
				types.IntKind:   35,
				types.FloatKind: 35,
			},
			hasNegatives: true,
			expKind:      types.FloatKind,
			expNullable:  false,
		},
		{
			name: "positive and negative ints and bools",
			typeToCount: map[types.NomsKind]int{
				types.IntKind:  35,
				types.BoolKind: 35,
			},
			hasNegatives: true,
			expKind:      types.StringKind,
			expNullable:  false,
		},
		{
			name: "floats and bools",
			typeToCount: map[types.NomsKind]int{
				types.FloatKind: 35,
				types.BoolKind:  35,
			},
			hasNegatives: false,
			expKind:      types.StringKind,
			expNullable:  false,
		},
		{
			name: "floats and uuids",
			typeToCount: map[types.NomsKind]int{
				types.FloatKind: 35,
				types.UUIDKind:  35,
			},
			hasNegatives: false,
			expKind:      types.StringKind,
			expNullable:  false,
		},
		{
			name: "floats and uuids",
			typeToCount: map[types.NomsKind]int{
				types.FloatKind: 35,
				types.UUIDKind:  35,
			},
			hasNegatives: false,
			expKind:      types.StringKind,
			expNullable:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			kind, nullable := typeCountsToKind("test", test.typeToCount, test.hasNegatives)
			assert.Equal(t, test.expKind, kind)
			assert.Equal(t, test.expNullable, nullable)
		})
	}
}

func TestNextKind(t *testing.T) {
	cols := []schema.Column{
		schema.NewColumn("0", 0, types.StringKind, false),
		schema.NewColumn("1", 1, types.StringKind, false),
		schema.NewColumn("2", 2, types.StringKind, false),
		schema.NewColumn("3", 3, types.StringKind, false),
		schema.NewColumn("100", 100, types.StringKind, false),
		schema.NewColumn("101", 101, types.StringKind, false),
	}

	tests := []struct {
		name       string
		tag        uint64
		expNextTag uint64
	}{
		{"zero", 0, 4},
		{"one", 1, 4},
		{"two", 2, 4},
		{"three", 3, 4},
		{"four", 4, 4},
		{"five", 5, 5},
		{"ninety nine", 99, 99},
		{"one hundred", 100, 102},
		{"one hundred one", 101, 102},
		{"one hundred two", 102, 102},
		{"one hundred three", 103, 103},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			colColl, err := schema.NewColCollection(cols...)
			require.NoError(t, err)

			tag := nextTag(test.tag, colColl)
			assert.Equal(t, test.expNextTag, tag)
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

func TestInferSchema(t *testing.T) {
	_, uuidSch := untyped.NewUntypedSchema("uuid")

	updateTestColColl, err := schema.NewColCollection(
		schema.NewColumn("uuid", 0, types.StringKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("int", 1, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("only_in_prev", 2, types.StringKind, false, schema.NotNullConstraint{}),
	)
	require.NoError(t, err)

	tests := []struct {
		name         string
		csvContents  string
		pkCols       []string
		infArgs      *InferenceArgs
		expKinds     map[string]types.NomsKind
		nullableCols *set.StrSet
	}{
		{
			"one of each kind",
			oneOfEachKindCSVStr,
			[]string{"uuid"},
			&InferenceArgs{
				ExistingSch:    schema.EmptySchema,
				ColMapper:      IdentityMapper{},
				FloatThreshold: 0,
				KeepTypes:      false,
			},
			map[string]types.NomsKind{
				"int":    types.IntKind,
				"uint":   types.UintKind,
				"uuid":   types.UUIDKind,
				"float":  types.FloatKind,
				"bool":   types.BoolKind,
				"string": types.StringKind,
			},
			nil,
		},
		{
			"mix uints and positive ints",
			mixUintsAndPositiveInts,
			[]string{"uuid"},
			&InferenceArgs{
				ExistingSch:    schema.EmptySchema,
				ColMapper:      IdentityMapper{},
				FloatThreshold: 0,
				KeepTypes:      false,
			},
			map[string]types.NomsKind{
				"mix":  types.UintKind,
				"uuid": types.UUIDKind,
			},
			nil,
		},
		{
			"floats with zero fractional and float threshold of 0",
			floatsWithZeroForFractionalPortion,
			[]string{"uuid"},
			&InferenceArgs{
				ExistingSch:    schema.EmptySchema,
				ColMapper:      IdentityMapper{},
				FloatThreshold: 0,
				KeepTypes:      false,
			},
			map[string]types.NomsKind{
				"float": types.FloatKind,
				"uuid":  types.UUIDKind,
			},
			nil,
		},
		{
			"floats with zero fractional and float threshold of 0.1",
			floatsWithZeroForFractionalPortion,
			[]string{"uuid"},
			&InferenceArgs{
				ExistingSch:    schema.EmptySchema,
				ColMapper:      IdentityMapper{},
				FloatThreshold: 0.1,
				KeepTypes:      false,
			},
			map[string]types.NomsKind{
				"float": types.IntKind,
				"uuid":  types.UUIDKind,
			},
			nil,
		},
		{
			"floats with large fractional and float threshold of 1.0",
			floatsWithLargeFractionalPortion,
			[]string{"uuid"},
			&InferenceArgs{
				ExistingSch:    schema.EmptySchema,
				ColMapper:      IdentityMapper{},
				FloatThreshold: 1.0,
				KeepTypes:      false,
			},
			map[string]types.NomsKind{
				"float": types.IntKind,
				"uuid":  types.UUIDKind,
			},
			nil,
		},
		{
			"float threshold smaller than some of teh values",
			floatsWithTinyFractionalPortion,
			[]string{"uuid"},
			&InferenceArgs{
				ExistingSch:    schema.EmptySchema,
				ColMapper:      IdentityMapper{},
				FloatThreshold: 0.0002,
				KeepTypes:      false,
			},
			map[string]types.NomsKind{
				"float": types.FloatKind,
				"uuid":  types.UUIDKind,
			},
			nil,
		},
		{
			"Keep Types",
			floatsWithTinyFractionalPortion,
			[]string{"uuid"},
			&InferenceArgs{
				ExistingSch:    uuidSch,
				ColMapper:      IdentityMapper{},
				FloatThreshold: 0.0002,
				KeepTypes:      true,
			},
			map[string]types.NomsKind{
				"float": types.FloatKind,
				"uuid":  types.StringKind,
			},
			nil,
		},
		{
			"pk ordering",
			oneOfEachKindCSVStr,
			[]string{"float", "uuid", "string", "bool"},
			&InferenceArgs{
				ExistingSch:    schema.EmptySchema,
				ColMapper:      IdentityMapper{},
				FloatThreshold: 0,
				KeepTypes:      false,
			},
			map[string]types.NomsKind{
				"int":    types.IntKind,
				"uint":   types.UintKind,
				"uuid":   types.UUIDKind,
				"float":  types.FloatKind,
				"bool":   types.BoolKind,
				"string": types.StringKind,
			},
			nil,
		},
		{
			"update schema",
			oneOfEachKindWithSomeNilsCSVStr,
			[]string{"uuid"},
			&InferenceArgs{
				ExistingSch:    schema.SchemaFromCols(updateTestColColl),
				ColMapper:      IdentityMapper{},
				FloatThreshold: 0,
				KeepTypes:      true,
				Update:         true,
			},
			map[string]types.NomsKind{
				"uuid":         types.StringKind,
				"int":          types.StringKind,
				"only_in_prev": types.StringKind,
				"uint":         types.UintKind,
				"float":        types.FloatKind,
				"bool":         types.BoolKind,
				"string":       types.StringKind,
			},
			set.NewStrSet([]string{"int", "only_in_prev"}),
		},
	}

	const (
		cwd        = "/Users/home/datasets/test"
		importFile = "import_file.csv"
	)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fs := filesys.NewInMemFS(
				[]string{cwd},
				map[string][]byte{importFile: []byte(test.csvContents)},
				cwd)
			rdCl, err := fs.OpenForRead("import_file.csv")
			require.NoError(t, err)

			csvRd, err := csv.NewCSVReader(types.Format_Default, rdCl, csv.NewCSVInfo())
			require.NoError(t, err)

			sch, err := InferSchemaFromTableReader(context.Background(), csvRd, test.pkCols, test.infArgs)
			require.NoError(t, err)

			allCols := sch.GetAllCols()
			err = allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
				expectedKind, ok := test.expKinds[col.Name]
				assert.True(t, ok, "culumn not found: %s", col.Name)
				assert.Equal(t, expectedKind, col.Kind, "column: %s - expected: %s got: %s", col.Name, expectedKind.String(), col.Kind.String())
				return false, nil
			})
			assert.NoError(t, err)

			pkCols := sch.GetPKCols()
			require.Equal(t, pkCols.Size(), len(test.pkCols))

			for i := 0; i < len(test.pkCols); i++ {
				col := pkCols.GetByIndex(i)
				assert.Equal(t, test.pkCols[i], col.Name)
			}

			if test.nullableCols != nil {
				err = allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
					idx := schema.ConstraintOfTypeIndex(col.Constraints, schema.NotNullConstraintType)
					assert.True(t, idx == -1 == test.nullableCols.Contains(col.Name), "%s unexpected nullability", col.Name)
					return false, nil
				})
				assert.NoError(t, err)
			}
		})
	}
}
