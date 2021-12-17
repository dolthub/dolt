// Copyright 2020 Dolthub, Inc.
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

package index_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/utils/config"
)

type indexComp int

const (
	indexComp_Eq = iota
	indexComp_NEq
	indexComp_Gt
	indexComp_GtE
	indexComp_Lt
	indexComp_LtE
)

type doltIndexTestCase struct {
	indexName    string
	keys         []interface{}
	expectedRows []sql.Row
}

type doltIndexBetweenTestCase struct {
	indexName          string
	greaterThanOrEqual []interface{}
	lessThanOrEqual    []interface{}
	expectedRows       []sql.Row
}

var typesTests = []struct {
	indexName       string
	belowfirstValue []interface{}
	firstValue      []interface{}
	secondValue     []interface{}
	thirdValue      []interface{}
	fourthValue     []interface{}
	fifthValue      []interface{}
	abovefifthValue []interface{}
}{
	{
		"types:primaryKey",
		[]interface{}{-4},
		[]interface{}{-3},
		[]interface{}{-1},
		[]interface{}{0},
		[]interface{}{1},
		[]interface{}{3},
		[]interface{}{4},
	},
	{
		"types:idx_bit",
		[]interface{}{0},
		[]interface{}{1},
		[]interface{}{2},
		[]interface{}{3},
		[]interface{}{4},
		[]interface{}{5},
		[]interface{}{6},
	},
	{
		"types:idx_datetime",
		[]interface{}{"2020-05-14 11:59:59"},
		[]interface{}{"2020-05-14 12:00:00"},
		[]interface{}{"2020-05-14 12:00:01"},
		[]interface{}{"2020-05-14 12:00:02"},
		[]interface{}{"2020-05-14 12:00:03"},
		[]interface{}{"2020-05-14 12:00:04"},
		[]interface{}{"2020-05-14 12:00:05"},
	},
	{
		"types:idx_decimal",
		[]interface{}{"-4"},
		[]interface{}{"-3.3"},
		[]interface{}{"-1.1"},
		[]interface{}{0},
		[]interface{}{"1.1"},
		[]interface{}{"3.3"},
		[]interface{}{4},
	},
	{
		"types:idx_enum",
		[]interface{}{"_"},
		[]interface{}{"a"},
		[]interface{}{"b"},
		[]interface{}{"c"},
		[]interface{}{"d"},
		[]interface{}{"e"},
		[]interface{}{"."},
	},
	{
		"types:idx_double",
		[]interface{}{-4},
		[]interface{}{-3.3},
		[]interface{}{-1.1},
		[]interface{}{0},
		[]interface{}{1.1},
		[]interface{}{3.3},
		[]interface{}{4},
	},
	{
		"types:idx_set",
		[]interface{}{""},
		[]interface{}{"a"},
		[]interface{}{"a,b"},
		[]interface{}{"c"},
		[]interface{}{"a,c"},
		[]interface{}{"b,c"},
		[]interface{}{"a,b,c"},
	},
	{
		"types:idx_time",
		[]interface{}{"-00:04:04"},
		[]interface{}{"-00:03:03"},
		[]interface{}{"-00:01:01"},
		[]interface{}{"00:00:00"},
		[]interface{}{"00:01:01"},
		[]interface{}{"00:03:03"},
		[]interface{}{"00:04:04"},
	},
	{
		"types:idx_varchar",
		[]interface{}{"_"},
		[]interface{}{"a"},
		[]interface{}{"b"},
		[]interface{}{"c"},
		[]interface{}{"d"},
		[]interface{}{"e"},
		[]interface{}{"f"},
	},
	{
		"types:idx_year",
		[]interface{}{1975},
		[]interface{}{1980},
		[]interface{}{1990},
		[]interface{}{2000},
		[]interface{}{2010},
		[]interface{}{2020},
		[]interface{}{2025},
	},
}

var (
	typesTableRow1 = sql.Row{int32(-3), uint64(1), forceParseTime("2020-05-14 12:00:00"), "-3.30000", "a", -3.3, "a", "-00:03:03", "a", int16(1980)}
	typesTableRow2 = sql.Row{int32(-1), uint64(2), forceParseTime("2020-05-14 12:00:01"), "-1.10000", "b", -1.1, "a,b", "-00:01:01", "b", int16(1990)}
	typesTableRow3 = sql.Row{int32(0), uint64(3), forceParseTime("2020-05-14 12:00:02"), "0.00000", "c", 0.0, "c", "00:00:00", "c", int16(2000)}
	typesTableRow4 = sql.Row{int32(1), uint64(4), forceParseTime("2020-05-14 12:00:03"), "1.10000", "d", 1.1, "a,c", "00:01:01", "d", int16(2010)}
	typesTableRow5 = sql.Row{int32(3), uint64(5), forceParseTime("2020-05-14 12:00:04"), "3.30000", "e", 3.3, "b,c", "00:03:03", "e", int16(2020)}
)

func TestDoltIndexEqual(t *testing.T) {
	indexMap := doltIndexSetup(t)

	tests := []doltIndexTestCase{
		{
			"onepk:primaryKey",
			[]interface{}{1},
			[]sql.Row{{1, 1, 1}},
		},
		{
			"onepk:primaryKey",
			[]interface{}{3},
			[]sql.Row{{3, 3, 3}},
		},
		{
			"onepk:primaryKey",
			[]interface{}{0},
			nil,
		},
		{
			"onepk:primaryKey",
			[]interface{}{5},
			nil,
		},
		{
			"onepk:idx_v1",
			[]interface{}{1},
			[]sql.Row{{1, 1, 1}, {2, 1, 2}},
		},
		{
			"onepk:idx_v1",
			[]interface{}{3},
			[]sql.Row{{3, 3, 3}},
		},
		{
			"twopk:primaryKey",
			[]interface{}{1, 1},
			[]sql.Row{{1, 1, 3, 3}},
		},
		{
			"twopk:primaryKey",
			[]interface{}{2, 0},
			nil,
		},
		{
			"twopk:primaryKey",
			[]interface{}{2, 1},
			[]sql.Row{{2, 1, 4, 4}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{3, 4},
			[]sql.Row{{2, 2, 4, 3}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{4, 3},
			[]sql.Row{{1, 2, 3, 4}},
		},
		//{
		//	"twopk:idx_v2v1_PARTIAL_1",
		//	[]interface{}{3},
		//	[]sql.Row{{1, 1, 3, 3}, {2, 2, 4, 3}},
		//},
		//{
		//	"twopk:idx_v2v1_PARTIAL_1",
		//	[]interface{}{4},
		//	[]sql.Row{{1, 2, 3, 4}, {2, 1, 4, 4}},
		//},
	}

	for _, typesTest := range typesTests {
		tests = append(tests, doltIndexTestCase{
			typesTest.indexName,
			typesTest.belowfirstValue,
			nil,
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.firstValue,
			[]sql.Row{
				typesTableRow1,
			},
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.secondValue,
			[]sql.Row{
				typesTableRow2,
			},
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.thirdValue,
			[]sql.Row{
				typesTableRow3,
			},
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.fourthValue,
			[]sql.Row{
				typesTableRow4,
			},
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.fifthValue,
			[]sql.Row{
				typesTableRow5,
			},
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.abovefifthValue,
			nil,
		})
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s|%v", test.indexName, test.keys), func(t *testing.T) {
			idx, ok := indexMap[test.indexName]
			require.True(t, ok)
			testDoltIndex(t, test.keys, test.expectedRows, idx, indexComp_Eq)
		})
	}
}

func TestDoltIndexGreaterThan(t *testing.T) {
	indexMap := doltIndexSetup(t)

	tests := []struct {
		indexName    string
		keys         []interface{}
		expectedRows []sql.Row
	}{
		{
			"onepk:primaryKey",
			[]interface{}{1},
			[]sql.Row{{2, 1, 2}, {3, 3, 3}, {4, 4, 3}},
		},
		{
			"onepk:primaryKey",
			[]interface{}{3},
			[]sql.Row{{4, 4, 3}},
		},
		{
			"onepk:primaryKey",
			[]interface{}{4},
			nil,
		},
		{
			"onepk:idx_v1",
			[]interface{}{1},
			[]sql.Row{{3, 3, 3}, {4, 4, 3}},
		},
		{
			"onepk:idx_v1",
			[]interface{}{3},
			[]sql.Row{{4, 4, 3}},
		},
		{
			"onepk:idx_v1",
			[]interface{}{4},
			nil,
		},
		{
			"twopk:primaryKey",
			[]interface{}{1, 1},
			[]sql.Row{{2, 2, 4, 3}},
		},
		{
			"twopk:primaryKey",
			[]interface{}{2, 1},
			nil,
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{2, 3},
			[]sql.Row{{2, 1, 4, 4}, {2, 2, 4, 3}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{3, 3},
			[]sql.Row{{2, 1, 4, 4}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{3, 4},
			nil,
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{4, 3},
			nil,
		},
		//{
		//	"twopk:idx_v2v1_PARTIAL_1",
		//	[]interface{}{3},
		//	[]sql.Row{{1, 2, 3, 4}, {2, 1, 4, 4}},
		//},
		//{
		//	"twopk:idx_v2v1_PARTIAL_1",
		//	[]interface{}{4},
		//	nil,
		//},
	}

	for _, typesTest := range typesTests {
		tests = append(tests, doltIndexTestCase{
			typesTest.indexName,
			typesTest.belowfirstValue,
			[]sql.Row{
				typesTableRow1,
				typesTableRow2,
				typesTableRow3,
				typesTableRow4,
				typesTableRow5,
			},
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.firstValue,
			[]sql.Row{
				typesTableRow2,
				typesTableRow3,
				typesTableRow4,
				typesTableRow5,
			},
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.secondValue,
			[]sql.Row{
				typesTableRow3,
				typesTableRow4,
				typesTableRow5,
			},
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.thirdValue,
			[]sql.Row{
				typesTableRow4,
				typesTableRow5,
			},
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.fourthValue,
			[]sql.Row{
				typesTableRow5,
			},
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.fifthValue,
			nil,
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.abovefifthValue,
			nil,
		})
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s|%v", test.indexName, test.keys), func(t *testing.T) {
			index, ok := indexMap[test.indexName]
			require.True(t, ok)
			testDoltIndex(t, test.keys, test.expectedRows, index, indexComp_Gt)
		})
	}
}

func TestDoltIndexGreaterThanOrEqual(t *testing.T) {
	indexMap := doltIndexSetup(t)

	tests := []struct {
		indexName    string
		keys         []interface{}
		expectedRows []sql.Row
	}{
		{
			"onepk:primaryKey",
			[]interface{}{1},
			[]sql.Row{{1, 1, 1}, {2, 1, 2}, {3, 3, 3}, {4, 4, 3}},
		},
		{
			"onepk:primaryKey",
			[]interface{}{3},
			[]sql.Row{{3, 3, 3}, {4, 4, 3}},
		},
		{
			"onepk:primaryKey",
			[]interface{}{4},
			[]sql.Row{{4, 4, 3}},
		},
		{
			"onepk:idx_v1",
			[]interface{}{1},
			[]sql.Row{{1, 1, 1}, {2, 1, 2}, {3, 3, 3}, {4, 4, 3}},
		},
		{
			"onepk:idx_v1",
			[]interface{}{3},
			[]sql.Row{{3, 3, 3}, {4, 4, 3}},
		},
		{
			"onepk:idx_v1",
			[]interface{}{4},
			[]sql.Row{{4, 4, 3}},
		},
		{
			"twopk:primaryKey",
			[]interface{}{1, 1},
			[]sql.Row{{1, 1, 3, 3}, {1, 2, 3, 4}, {2, 1, 4, 4}, {2, 2, 4, 3}},
		},
		{
			"twopk:primaryKey",
			[]interface{}{2, 1},
			[]sql.Row{{2, 1, 4, 4}, {2, 2, 4, 3}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{3, 4},
			[]sql.Row{{2, 1, 4, 4}, {2, 2, 4, 3}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{4, 3},
			[]sql.Row{{1, 2, 3, 4}, {2, 1, 4, 4}},
		},
		//{
		//	"twopk:idx_v2v1_PARTIAL_1",
		//	[]interface{}{3},
		//	[]sql.Row{{1, 1, 3, 3}, {1, 2, 3, 4}, {2, 1, 4, 4}, {2, 2, 4, 3}},
		//},
		//{
		//	"twopk:idx_v2v1_PARTIAL_1",
		//	[]interface{}{4},
		//	[]sql.Row{{1, 2, 3, 4}, {2, 1, 4, 4}},
		//},
	}

	for _, typesTest := range typesTests {
		tests = append(tests, doltIndexTestCase{
			typesTest.indexName,
			typesTest.belowfirstValue,
			[]sql.Row{
				typesTableRow1,
				typesTableRow2,
				typesTableRow3,
				typesTableRow4,
				typesTableRow5,
			},
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.firstValue,
			[]sql.Row{
				typesTableRow1,
				typesTableRow2,
				typesTableRow3,
				typesTableRow4,
				typesTableRow5,
			},
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.secondValue,
			[]sql.Row{
				typesTableRow2,
				typesTableRow3,
				typesTableRow4,
				typesTableRow5,
			},
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.thirdValue,
			[]sql.Row{
				typesTableRow3,
				typesTableRow4,
				typesTableRow5,
			},
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.fourthValue,
			[]sql.Row{
				typesTableRow4,
				typesTableRow5,
			},
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.fifthValue,
			[]sql.Row{
				typesTableRow5,
			},
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.abovefifthValue,
			nil,
		})
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s|%v", test.indexName, test.keys), func(t *testing.T) {
			index, ok := indexMap[test.indexName]
			require.True(t, ok)
			testDoltIndex(t, test.keys, test.expectedRows, index, indexComp_GtE)
		})
	}
}

func TestDoltIndexLessThan(t *testing.T) {
	indexMap := doltIndexSetup(t)

	tests := []struct {
		indexName    string
		keys         []interface{}
		expectedRows []sql.Row
	}{
		{
			"onepk:primaryKey",
			[]interface{}{1},
			nil,
		},
		{
			"onepk:primaryKey",
			[]interface{}{3},
			[]sql.Row{{2, 1, 2}, {1, 1, 1}},
		},
		{
			"onepk:primaryKey",
			[]interface{}{4},
			[]sql.Row{{3, 3, 3}, {2, 1, 2}, {1, 1, 1}},
		},
		{
			"onepk:idx_v1",
			[]interface{}{1},
			nil,
		},
		{
			"onepk:idx_v1",
			[]interface{}{3},
			[]sql.Row{{2, 1, 2}, {1, 1, 1}},
		},
		{
			"onepk:idx_v1",
			[]interface{}{4},
			[]sql.Row{{3, 3, 3}, {2, 1, 2}, {1, 1, 1}},
		},
		{
			"twopk:primaryKey",
			[]interface{}{1, 1},
			nil,
		},
		{
			"twopk:primaryKey",
			[]interface{}{2, 1},
			nil,
		},
		{
			"twopk:primaryKey",
			[]interface{}{2, 2},
			[]sql.Row{{1, 1, 3, 3}},
		},
		{
			"twopk:primaryKey",
			[]interface{}{2, 3},
			[]sql.Row{{1, 2, 3, 4}, {1, 1, 3, 3}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{3, 4},
			nil,
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{4, 3},
			nil,
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{4, 4},
			[]sql.Row{{1, 1, 3, 3}},
		},
		//{
		//	"twopk:idx_v2v1_PARTIAL_1",
		//	[]interface{}{3},
		//	nil,
		//},
		//{
		//	"twopk:idx_v2v1_PARTIAL_1",
		//	[]interface{}{4},
		//	[]sql.Row{{2, 2, 4, 3}, {1, 1, 3, 3}},
		//},
	}

	for _, typesTest := range typesTests {
		tests = append(tests, doltIndexTestCase{
			typesTest.indexName,
			typesTest.belowfirstValue,
			nil,
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.firstValue,
			nil,
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.secondValue,
			[]sql.Row{
				typesTableRow1,
			},
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.thirdValue,
			[]sql.Row{
				typesTableRow2,
				typesTableRow1,
			},
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.fourthValue,
			[]sql.Row{
				typesTableRow3,
				typesTableRow2,
				typesTableRow1,
			},
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.fifthValue,
			[]sql.Row{
				typesTableRow4,
				typesTableRow3,
				typesTableRow2,
				typesTableRow1,
			},
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.abovefifthValue,
			[]sql.Row{
				typesTableRow5,
				typesTableRow4,
				typesTableRow3,
				typesTableRow2,
				typesTableRow1,
			},
		})
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s|%v", test.indexName, test.keys), func(t *testing.T) {
			index, ok := indexMap[test.indexName]
			require.True(t, ok)
			testDoltIndex(t, test.keys, test.expectedRows, index, indexComp_Lt)
		})
	}
}

func TestDoltIndexLessThanOrEqual(t *testing.T) {
	indexMap := doltIndexSetup(t)

	tests := []struct {
		indexName    string
		keys         []interface{}
		expectedRows []sql.Row
	}{
		{
			"onepk:primaryKey",
			[]interface{}{1},
			[]sql.Row{{1, 1, 1}},
		},
		{
			"onepk:primaryKey",
			[]interface{}{3},
			[]sql.Row{{3, 3, 3}, {2, 1, 2}, {1, 1, 1}},
		},
		{
			"onepk:primaryKey",
			[]interface{}{4},
			[]sql.Row{{4, 4, 3}, {3, 3, 3}, {2, 1, 2}, {1, 1, 1}},
		},
		{
			"onepk:idx_v1",
			[]interface{}{1},
			[]sql.Row{{2, 1, 2}, {1, 1, 1}},
		},
		{
			"onepk:idx_v1",
			[]interface{}{3},
			[]sql.Row{{3, 3, 3}, {2, 1, 2}, {1, 1, 1}},
		},
		{
			"onepk:idx_v1",
			[]interface{}{4},
			[]sql.Row{{4, 4, 3}, {3, 3, 3}, {2, 1, 2}, {1, 1, 1}},
		},
		{
			"twopk:primaryKey",
			[]interface{}{1, 1},
			[]sql.Row{{1, 1, 3, 3}},
		},
		{
			"twopk:primaryKey",
			[]interface{}{2, 1},
			[]sql.Row{{2, 1, 4, 4}, {1, 1, 3, 3}},
		},
		{
			"twopk:primaryKey",
			[]interface{}{2, 2},
			[]sql.Row{{1, 1, 3, 3}, {1, 2, 3, 4}, {2, 1, 4, 4}, {2, 2, 4, 3}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{3, 4},
			[]sql.Row{{2, 2, 4, 3}, {1, 1, 3, 3}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{4, 3},
			[]sql.Row{{1, 1, 3, 3}, {1, 2, 3, 4}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{4, 4},
			[]sql.Row{{1, 1, 3, 3}, {1, 2, 3, 4}, {2, 1, 4, 4}, {2, 2, 4, 3}},
		},
		//{
		//	"twopk:idx_v2v1_PARTIAL_1",
		//	[]interface{}{3},
		//	[]sql.Row{{1, 1, 3, 3}, {2, 2, 4, 3}},
		//},
		//{
		//	"twopk:idx_v2v1_PARTIAL_1",
		//	[]interface{}{4},
		//	[]sql.Row{{1, 1, 3, 3}, {1, 2, 3, 4}, {2, 1, 4, 4}, {2, 2, 4, 3}},
		//},
	}

	for _, typesTest := range typesTests {
		tests = append(tests, doltIndexTestCase{
			typesTest.indexName,
			typesTest.belowfirstValue,
			nil,
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.firstValue,
			[]sql.Row{
				typesTableRow1,
			},
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.secondValue,
			[]sql.Row{
				typesTableRow2,
				typesTableRow1,
			},
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.thirdValue,
			[]sql.Row{
				typesTableRow3,
				typesTableRow2,
				typesTableRow1,
			},
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.fourthValue,
			[]sql.Row{
				typesTableRow4,
				typesTableRow3,
				typesTableRow2,
				typesTableRow1,
			},
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.fifthValue,
			[]sql.Row{
				typesTableRow5,
				typesTableRow4,
				typesTableRow3,
				typesTableRow2,
				typesTableRow1,
			},
		}, doltIndexTestCase{
			typesTest.indexName,
			typesTest.abovefifthValue,
			[]sql.Row{
				typesTableRow5,
				typesTableRow4,
				typesTableRow3,
				typesTableRow2,
				typesTableRow1,
			},
		})
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s|%v", test.indexName, test.keys), func(t *testing.T) {
			index, ok := indexMap[test.indexName]
			require.True(t, ok)
			testDoltIndex(t, test.keys, test.expectedRows, index, indexComp_LtE)
		})
	}
}

func TestDoltIndexBetween(t *testing.T) {
	indexMap := doltIndexSetup(t)

	tests := []doltIndexBetweenTestCase{
		{
			"onepk:primaryKey",
			[]interface{}{1},
			[]interface{}{2},
			[]sql.Row{{1, 1, 1}, {2, 1, 2}},
		},
		{
			"onepk:primaryKey",
			[]interface{}{3},
			[]interface{}{3},
			[]sql.Row{{3, 3, 3}},
		},
		{
			"onepk:primaryKey",
			[]interface{}{4},
			[]interface{}{6},
			[]sql.Row{{4, 4, 3}},
		},
		{
			"onepk:primaryKey",
			[]interface{}{0},
			[]interface{}{10},
			[]sql.Row{{1, 1, 1}, {2, 1, 2}, {3, 3, 3}, {4, 4, 3}},
		},
		{
			"onepk:idx_v1",
			[]interface{}{1},
			[]interface{}{2},
			[]sql.Row{{1, 1, 1}, {2, 1, 2}},
		},
		{
			"onepk:idx_v1",
			[]interface{}{2},
			[]interface{}{4},
			[]sql.Row{{3, 3, 3}, {4, 4, 3}},
		},
		{
			"onepk:idx_v1",
			[]interface{}{1},
			[]interface{}{4},
			[]sql.Row{{1, 1, 1}, {2, 1, 2}, {3, 3, 3}, {4, 4, 3}},
		},
		{
			"twopk:primaryKey",
			[]interface{}{1, 1},
			[]interface{}{1, 1},
			[]sql.Row{{1, 1, 3, 3}},
		},
		{
			"twopk:primaryKey",
			[]interface{}{1, 1},
			[]interface{}{2, 1},
			[]sql.Row{{1, 1, 3, 3}, {2, 1, 4, 4}},
		},
		{
			"twopk:primaryKey",
			[]interface{}{1, 1},
			[]interface{}{2, 2},
			[]sql.Row{{1, 1, 3, 3}, {1, 2, 3, 4}, {2, 1, 4, 4}, {2, 2, 4, 3}},
		},
		{
			"twopk:primaryKey",
			[]interface{}{1, 1},
			[]interface{}{2, 5},
			[]sql.Row{{1, 1, 3, 3}, {1, 2, 3, 4}, {2, 1, 4, 4}, {2, 2, 4, 3}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{3, 3},
			[]interface{}{3, 4},
			[]sql.Row{{1, 1, 3, 3}, {2, 2, 4, 3}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{3, 4},
			[]interface{}{4, 4},
			[]sql.Row{{2, 1, 4, 4}, {2, 2, 4, 3}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{3, 3},
			[]interface{}{4, 4},
			[]sql.Row{{1, 1, 3, 3}, {1, 2, 3, 4}, {2, 1, 4, 4}, {2, 2, 4, 3}},
		},
		//{
		//	"twopk:idx_v2v1_PARTIAL_1",
		//	[]interface{}{3},
		//	[]interface{}{3},
		//	[]sql.Row{{1, 1, 3, 3}, {2, 2, 4, 3}},
		//},
		//{
		//	"twopk:idx_v2v1_PARTIAL_1",
		//	[]interface{}{4},
		//	[]interface{}{4},
		//	[]sql.Row{{1, 2, 3, 4}, {2, 1, 4, 4}},
		//},
		//{
		//	"twopk:idx_v2v1_PARTIAL_1",
		//	[]interface{}{3},
		//	[]interface{}{4},
		//	[]sql.Row{{1, 1, 3, 3}, {1, 2, 3, 4}, {2, 1, 4, 4}, {2, 2, 4, 3}},
		//},
	}

	for _, typesTest := range typesTests {
		tests = append(tests, doltIndexBetweenTestCase{
			typesTest.indexName,
			typesTest.belowfirstValue,
			typesTest.belowfirstValue,
			nil,
		}, doltIndexBetweenTestCase{
			typesTest.indexName,
			typesTest.belowfirstValue,
			typesTest.firstValue,
			[]sql.Row{
				typesTableRow1,
			},
		}, doltIndexBetweenTestCase{
			typesTest.indexName,
			typesTest.belowfirstValue,
			typesTest.secondValue,
			[]sql.Row{
				typesTableRow1,
				typesTableRow2,
			},
		}, doltIndexBetweenTestCase{
			typesTest.indexName,
			typesTest.belowfirstValue,
			typesTest.thirdValue,
			[]sql.Row{
				typesTableRow1,
				typesTableRow2,
				typesTableRow3,
			},
		}, doltIndexBetweenTestCase{
			typesTest.indexName,
			typesTest.secondValue,
			typesTest.secondValue,
			[]sql.Row{
				typesTableRow2,
			},
		}, doltIndexBetweenTestCase{
			typesTest.indexName,
			typesTest.thirdValue,
			typesTest.fifthValue,
			[]sql.Row{
				typesTableRow3,
				typesTableRow4,
				typesTableRow5,
			},
		}, doltIndexBetweenTestCase{
			typesTest.indexName,
			typesTest.fifthValue,
			typesTest.abovefifthValue,
			[]sql.Row{
				typesTableRow5,
			},
		}, doltIndexBetweenTestCase{
			typesTest.indexName,
			typesTest.abovefifthValue,
			typesTest.abovefifthValue,
			nil,
		})
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s|%v%v", test.indexName, test.greaterThanOrEqual, test.lessThanOrEqual), func(t *testing.T) {
			ctx := NewTestSQLCtx(context.Background())
			idx, ok := indexMap[test.indexName]
			require.True(t, ok)

			expectedRows := convertSqlRowToInt64(test.expectedRows)

			exprs := idx.Expressions()
			sqlIndex := sql.NewIndexBuilder(ctx, idx)
			for i := range test.greaterThanOrEqual {
				sqlIndex = sqlIndex.GreaterOrEqual(ctx, exprs[i], test.greaterThanOrEqual[i]).LessOrEqual(ctx, exprs[i], test.lessThanOrEqual[i])
			}
			indexLookup, err := sqlIndex.Build(ctx)
			require.NoError(t, err)

			indexIter, err := index.RowIterForIndexLookup(ctx, indexLookup, nil)
			require.NoError(t, err)

			var readRows []sql.Row
			var nextRow sql.Row
			for nextRow, err = indexIter.Next(ctx); err == nil; nextRow, err = indexIter.Next(ctx) {
				readRows = append(readRows, nextRow)
			}
			require.Equal(t, io.EOF, err)

			requireUnorderedRowsEqual(t, expectedRows, readRows)
		})
	}
}

type rowSlice struct {
	rows    []sql.Row
	sortErr error
}

func (r *rowSlice) setSortErr(err error) {
	if err == nil || r.sortErr != nil {
		return
	}

	r.sortErr = err
}

func (r *rowSlice) Len() int {
	return len(r.rows)
}

func (r *rowSlice) Less(i, j int) bool {
	r1 := r.rows[i]
	r2 := r.rows[j]

	longerLen := len(r1)
	if len(r2) > longerLen {
		longerLen = len(r2)
	}

	for pos := 0; pos < longerLen; pos++ {
		if pos == len(r1) {
			return true
		}

		if pos == len(r2) {
			return false
		}

		c1, c2 := r1[pos], r2[pos]

		var cmp int
		var err error
		switch typedVal := c1.(type) {
		case int:
			cmp, err = signedCompare(int64(typedVal), c2)
		case int64:
			cmp, err = signedCompare(typedVal, c2)
		case int32:
			cmp, err = signedCompare(int64(typedVal), c2)
		case int16:
			cmp, err = signedCompare(int64(typedVal), c2)
		case int8:
			cmp, err = signedCompare(int64(typedVal), c2)

		case uint:
			cmp, err = unsignedCompare(uint64(typedVal), c2)
		case uint64:
			cmp, err = unsignedCompare(typedVal, c2)
		case uint32:
			cmp, err = unsignedCompare(uint64(typedVal), c2)
		case uint16:
			cmp, err = unsignedCompare(uint64(typedVal), c2)
		case uint8:
			cmp, err = unsignedCompare(uint64(typedVal), c2)

		case float64:
			cmp, err = floatCompare(float64(typedVal), c2)
		case float32:
			cmp, err = floatCompare(float64(typedVal), c2)

		case string:
			cmp, err = stringCompare(typedVal, c2)

		default:
			panic("not implemented please add")
		}

		if err != nil {
			r.setSortErr(err)
			return false
		}

		if cmp != 0 {
			return cmp < 0
		}
	}

	// equal
	return false
}

func signedCompare(n1 int64, c interface{}) (int, error) {
	var n2 int64
	switch typedVal := c.(type) {
	case int:
		n2 = int64(typedVal)
	case int64:
		n2 = typedVal
	case int32:
		n2 = int64(typedVal)
	case int16:
		n2 = int64(typedVal)
	case int8:
		n2 = int64(typedVal)
	default:
		return 0, errors.New("comparing rows with different schemas")
	}

	return int(n1 - n2), nil
}

func unsignedCompare(n1 uint64, c interface{}) (int, error) {
	var n2 uint64
	switch typedVal := c.(type) {
	case uint:
		n2 = uint64(typedVal)
	case uint64:
		n2 = typedVal
	case uint32:
		n2 = uint64(typedVal)
	case uint16:
		n2 = uint64(typedVal)
	case uint8:
		n2 = uint64(typedVal)
	default:
		return 0, errors.New("comparing rows with different schemas")
	}

	if n1 == n2 {
		return 0, nil
	} else if n1 < n2 {
		return -1, nil
	} else {
		return 1, nil
	}
}

func floatCompare(n1 float64, c interface{}) (int, error) {
	var n2 float64
	switch typedVal := c.(type) {
	case float32:
		n2 = float64(typedVal)
	case float64:
		n2 = typedVal
	default:
		return 0, errors.New("comparing rows with different schemas")
	}

	if n1 == n2 {
		return 0, nil
	} else if n1 < n2 {
		return -1, nil
	} else {
		return 1, nil
	}
}

func stringCompare(s1 string, c interface{}) (int, error) {
	s2, ok := c.(string)
	if !ok {
		return 0, errors.New("comparing rows with different schemas")
	}

	return strings.Compare(s1, s2), nil
}

func (r *rowSlice) Swap(i, j int) {
	r.rows[i], r.rows[j] = r.rows[j], r.rows[i]
}

func requireUnorderedRowsEqual(t *testing.T, rows1, rows2 []sql.Row) {
	slice1 := &rowSlice{rows: rows1}
	sort.Stable(slice1)
	require.NoError(t, slice1.sortErr)

	slice2 := &rowSlice{rows: rows2}
	sort.Stable(slice2)
	require.NoError(t, slice2.sortErr)

	require.Equal(t, rows1, rows2)
}

func testDoltIndex(t *testing.T, keys []interface{}, expectedRows []sql.Row, idx sql.Index, cmp indexComp) {
	ctx := NewTestSQLCtx(context.Background())
	exprs := idx.Expressions()
	builder := sql.NewIndexBuilder(sql.NewEmptyContext(), idx)
	for i, key := range keys {
		switch cmp {
		case indexComp_Eq:
			builder = builder.Equals(ctx, exprs[i], key)
		case indexComp_NEq:
			builder = builder.NotEquals(ctx, exprs[i], key)
		case indexComp_Gt:
			builder = builder.GreaterThan(ctx, exprs[i], key)
		case indexComp_GtE:
			builder = builder.GreaterOrEqual(ctx, exprs[i], key)
		case indexComp_Lt:
			builder = builder.LessThan(ctx, exprs[i], key)
		case indexComp_LtE:
			builder = builder.LessOrEqual(ctx, exprs[i], key)
		default:
			panic("should not be hit")
		}
	}
	indexLookup, err := builder.Build(ctx)
	require.NoError(t, err)

	indexIter, err := index.RowIterForIndexLookup(ctx, indexLookup, nil)
	require.NoError(t, err)

	var readRows []sql.Row
	var nextRow sql.Row
	for nextRow, err = indexIter.Next(ctx); err == nil; nextRow, err = indexIter.Next(ctx) {
		readRows = append(readRows, nextRow)
	}
	require.Equal(t, io.EOF, err)

	requireUnorderedRowsEqual(t, convertSqlRowToInt64(expectedRows), readRows)
}

func doltIndexSetup(t *testing.T) map[string]index.DoltIndex {
	ctx := NewTestSQLCtx(context.Background())
	dEnv := dtestutils.CreateTestEnv()
	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		panic(err)
	}
	root, err = sqle.ExecuteSql(t, dEnv, root, `
CREATE TABLE onepk (
  pk1 BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT
);
CREATE TABLE twopk (
  pk1 BIGINT,
  pk2 BIGINT,
  v1 BIGINT,
  v2 BIGINT,
  PRIMARY KEY (pk1,pk2)
);
CREATE TABLE types (
  pk1 MEDIUMINT PRIMARY KEY,
  v1 BIT(4),
  v2 DATETIME,
  v3 DECIMAL(10, 5),
  v4 ENUM('_', 'a', 'b', 'c', 'd', 'e', '.'),
  v5 DOUBLE,
  v6 SET('a', 'b', 'c'),
  v7 TIME,
  v8 VARCHAR(2),
  v9 YEAR
);
CREATE INDEX idx_v1 ON onepk(v1);
CREATE INDEX idx_v2v1 ON twopk(v2, v1);
CREATE INDEX idx_bit ON types(v1);
CREATE INDEX idx_datetime ON types(v2);
CREATE INDEX idx_decimal ON types(v3);
CREATE INDEX idx_enum ON types(v4);
CREATE INDEX idx_double ON types(v5);
CREATE INDEX idx_set ON types(v6);
CREATE INDEX idx_time ON types(v7);
CREATE INDEX idx_varchar ON types(v8);
CREATE INDEX idx_year ON types(v9);
INSERT INTO onepk VALUES (1, 1, 1), (2, 1, 2), (3, 3, 3), (4, 4, 3);
INSERT INTO twopk VALUES (1, 1, 3, 3), (1, 2, 3, 4), (2, 1, 4, 4), (2, 2, 4, 3);
INSERT INTO types VALUES (-3, 1, '2020-05-14 12:00:00', -3.3, 'a', -3.3, 'a', '-00:03:03', 'a', 1980);
INSERT INTO types VALUES (3, 5, '2020-05-14 12:00:04', 3.3, 'e', 3.3, 'b,c', '00:03:03', 'e', 2020);
INSERT INTO types VALUES (0, 3, '2020-05-14 12:00:02', 0.0, 'c', 0.0, 'c', '00:00:00', 'c', 2000);
INSERT INTO types VALUES (-1, 2, '2020-05-14 12:00:01', -1.1, 'b', -1.1, 'a,b', '-00:01:01', 'b', 1990);
INSERT INTO types VALUES (1, 4, '2020-05-14 12:00:03', 1.1, 'd', 1.1, 'a,c', '00:01:01', 'd', 2010);
`)
	require.NoError(t, err)

	indexMap := make(map[string]index.DoltIndex)

	dbname := "dolt"
	for _, name := range []string{"onepk", "twopk", "types"} {
		tbl, ok, err := root.GetTable(ctx, name)
		require.NoError(t, err)
		require.True(t, ok)

		indexes, err := index.DoltIndexesFromTable(ctx, dbname, name, tbl)
		require.NoError(t, err)

		pkName := name + ":" + "primaryKey"
		indexMap[pkName] = indexes[0].(index.DoltIndex)

		for _, idx := range indexes[1:] {
			idxName := name + ":" + idx.ID()
			indexMap[idxName] = idx.(index.DoltIndex)
		}
	}

	return indexMap
}

func NewTestSQLCtx(ctx context.Context) *sql.Context {
	session := dsess.DefaultSession()
	s := session.NewDoltSession(config.NewMapConfig(make(map[string]string)))
	sqlCtx := sql.NewContext(
		ctx,
		sql.WithSession(s),
	).WithCurrentDB("dolt")

	return sqlCtx
}

func forceParseTime(timeString string) time.Time {
	tim, _ := time.Parse("2006-01-02 15:04:05", timeString)
	return tim
}

func convertSqlRowToInt64(sqlRows []sql.Row) []sql.Row {
	if sqlRows == nil {
		return nil
	}
	newSqlRows := make([]sql.Row, len(sqlRows))
	for i, sqlRow := range sqlRows {
		newSqlRow := make(sql.Row, len(sqlRow))
		for j := range sqlRow {
			switch v := sqlRow[j].(type) {
			case int:
				newSqlRow[j] = int64(v)
			case int8:
				newSqlRow[j] = int64(v)
			case int16:
				newSqlRow[j] = int64(v)
			case int32:
				newSqlRow[j] = int64(v)
			case int64:
				newSqlRow[j] = v
			case nil:
				newSqlRow[j] = nil
			default:
				return sqlRows
			}
		}
		newSqlRows[i] = newSqlRow
	}
	return newSqlRows
}
