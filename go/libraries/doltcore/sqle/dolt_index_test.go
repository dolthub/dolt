// Copyright 2020 Liquidata, Inc.
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

package sqle

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/store/types"
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
			index, ok := indexMap[test.indexName]
			require.True(t, ok)
			testDoltIndex(t, test.keys, test.expectedRows, index.Get)
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
			[]sql.Row{{1, 2, 3, 4}, {2, 1, 4, 4}, {2, 2, 4, 3}},
		},
		{
			"twopk:primaryKey",
			[]interface{}{2, 1},
			[]sql.Row{{2, 2, 4, 3}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{3, 4},
			[]sql.Row{{1, 2, 3, 4}, {2, 1, 4, 4}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{4, 3},
			[]sql.Row{{2, 1, 4, 4}},
		},
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
			testDoltIndex(t, test.keys, test.expectedRows, index.DescendGreater)
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
			[]sql.Row{{2, 2, 4, 3}, {1, 2, 3, 4}, {2, 1, 4, 4}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{4, 3},
			[]sql.Row{{1, 2, 3, 4}, {2, 1, 4, 4}},
		},
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
			testDoltIndex(t, test.keys, test.expectedRows, index.AscendGreaterOrEqual)
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
			[]sql.Row{{1, 2, 3, 4}, {1, 1, 3, 3}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{3, 4},
			[]sql.Row{{1, 1, 3, 3}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{4, 3},
			[]sql.Row{{2, 2, 4, 3}, {1, 1, 3, 3}},
		},
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
			testDoltIndex(t, test.keys, test.expectedRows, index.AscendLessThan)
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
			[]sql.Row{{2, 1, 4, 4}, {1, 2, 3, 4}, {1, 1, 3, 3}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{3, 4},
			[]sql.Row{{2, 2, 4, 3}, {1, 1, 3, 3}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{4, 3},
			[]sql.Row{{1, 2, 3, 4}, {2, 2, 4, 3}, {1, 1, 3, 3}},
		},
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
			testDoltIndex(t, test.keys, test.expectedRows, index.DescendLessOrEqual)
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
			[]sql.Row{{1, 1, 3, 3}, {1, 2, 3, 4}, {2, 1, 4, 4}},
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
			[]sql.Row{{2, 2, 4, 3}, {1, 2, 3, 4}, {2, 1, 4, 4}},
		},
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
			index, ok := indexMap[test.indexName]
			require.True(t, ok)

			expectedRows := convertSqlRowToInt64(test.expectedRows)

			indexLookup, err := index.AscendRange(test.greaterThanOrEqual, test.lessThanOrEqual)
			require.NoError(t, err)
			dil, ok := indexLookup.(*doltIndexLookup)
			require.True(t, ok)
			indexIter, err := dil.RowIter(NewTestSQLCtx(context.Background()))
			require.NoError(t, err)

			var readRows []sql.Row
			var nextRow sql.Row
			for nextRow, err = indexIter.Next(); err == nil; nextRow, err = indexIter.Next() {
				readRows = append(readRows, nextRow)
			}
			require.Equal(t, io.EOF, err)

			assert.Equal(t, expectedRows, readRows)

			indexLookup, err = index.DescendRange(test.lessThanOrEqual, test.greaterThanOrEqual)
			require.NoError(t, err)
			dil, ok = indexLookup.(*doltIndexLookup)
			require.True(t, ok)
			indexIter, err = dil.RowIter(NewTestSQLCtx(context.Background()))
			require.NoError(t, err)

			readRows = nil
			for nextRow, err = indexIter.Next(); err == nil; nextRow, err = indexIter.Next() {
				readRows = append(readRows, nextRow)
			}
			require.Equal(t, io.EOF, err)

			assert.Equal(t, expectedRows, readRows)
		})
	}
}

func testDoltIndex(t *testing.T, keys []interface{}, expectedRows []sql.Row, indexLookupFn func(keys ...interface{}) (sql.IndexLookup, error)) {
	indexLookup, err := indexLookupFn(keys...)
	require.NoError(t, err)
	dil, ok := indexLookup.(*doltIndexLookup)
	require.True(t, ok)
	indexIter, err := dil.RowIter(NewTestSQLCtx(context.Background()))
	require.NoError(t, err)

	var readRows []sql.Row
	var nextRow sql.Row
	for nextRow, err = indexIter.Next(); err == nil; nextRow, err = indexIter.Next() {
		readRows = append(readRows, nextRow)
	}
	require.Equal(t, io.EOF, err)

	assert.Equal(t, convertSqlRowToInt64(expectedRows), readRows)
}

func doltIndexSetup(t *testing.T) map[string]DoltIndex {
	ctx := NewTestSQLCtx(context.Background())
	dEnv := dtestutils.CreateTestEnv()
	db := NewDatabase("dolt", dEnv.DoltDB, dEnv.RepoState, dEnv.RepoStateWriter())
	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		panic(err)
	}
	root, err = ExecuteSql(dEnv, root, `
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

	tableMap := make(map[string]*doltdb.Table)
	tableDataMap := make(map[string]types.Map)
	tableSchemaMap := make(map[string]schema.Schema)

	var ok bool
	tableMap["onepk"], ok, err = root.GetTable(ctx, "onepk")
	require.NoError(t, err)
	require.True(t, ok)
	tableSchemaMap["onepk"], err = tableMap["onepk"].GetSchema(ctx)
	require.NoError(t, err)
	tableDataMap["onepk"], err = tableMap["onepk"].GetRowData(ctx)
	require.NoError(t, err)

	tableMap["twopk"], ok, err = root.GetTable(ctx, "twopk")
	require.NoError(t, err)
	require.True(t, ok)
	tableSchemaMap["twopk"], err = tableMap["twopk"].GetSchema(ctx)
	require.NoError(t, err)
	tableDataMap["twopk"], err = tableMap["twopk"].GetRowData(ctx)
	require.NoError(t, err)

	tableMap["types"], ok, err = root.GetTable(ctx, "types")
	require.NoError(t, err)
	require.True(t, ok)
	tableSchemaMap["types"], err = tableMap["types"].GetSchema(ctx)
	require.NoError(t, err)
	tableDataMap["types"], err = tableMap["types"].GetRowData(ctx)
	require.NoError(t, err)

	indexMap := map[string]DoltIndex{
		"onepk:primaryKey": &doltIndex{
			cols:         tableSchemaMap["onepk"].GetPKCols().GetColumns(),
			ctx:          ctx,
			db:           db,
			driver:       nil,
			id:           "onepk:primaryKey",
			indexRowData: tableDataMap["onepk"],
			indexSch:     tableSchemaMap["onepk"],
			table:        tableMap["onepk"],
			tableData:    tableDataMap["onepk"],
			tableName:    "onepk",
			tableSch:     tableSchemaMap["onepk"],
		},
		"twopk:primaryKey": &doltIndex{
			cols:         tableSchemaMap["twopk"].GetPKCols().GetColumns(),
			ctx:          ctx,
			db:           db,
			driver:       nil,
			id:           "twopk:primaryKey",
			indexRowData: tableDataMap["twopk"],
			indexSch:     tableSchemaMap["twopk"],
			table:        tableMap["twopk"],
			tableData:    tableDataMap["twopk"],
			tableName:    "twopk",
			tableSch:     tableSchemaMap["twopk"],
		},
		"types:primaryKey": &doltIndex{
			cols:         tableSchemaMap["types"].GetPKCols().GetColumns(),
			ctx:          ctx,
			db:           db,
			driver:       nil,
			id:           "types:primaryKey",
			indexRowData: tableDataMap["types"],
			indexSch:     tableSchemaMap["types"],
			table:        tableMap["types"],
			tableData:    tableDataMap["types"],
			tableName:    "types",
			tableSch:     tableSchemaMap["types"],
		},
	}

	for _, indexDetails := range []struct {
		indexName string
		tableName string
	}{
		{
			"idx_v1",
			"onepk",
		},
		{
			"idx_v2v1",
			"twopk",
		},
		{
			"idx_bit",
			"types",
		},
		{
			"idx_datetime",
			"types",
		},
		{
			"idx_decimal",
			"types",
		},
		{
			"idx_enum",
			"types",
		},
		{
			"idx_double",
			"types",
		},
		{
			"idx_set",
			"types",
		},
		{
			"idx_time",
			"types",
		},
		{
			"idx_varchar",
			"types",
		},
		{
			"idx_year",
			"types",
		},
	} {
		index := tableSchemaMap[indexDetails.tableName].Indexes().Get(indexDetails.indexName)
		indexData, err := tableMap[indexDetails.tableName].GetIndexRowData(ctx, index.Name())
		require.NoError(t, err)
		indexCols := make([]schema.Column, index.Count())
		for i, tag := range index.IndexedColumnTags() {
			indexCols[i], _ = index.GetColumn(tag)
		}

		indexId := indexDetails.tableName + ":" + index.Name()
		indexMap[indexId] = &doltIndex{
			cols:         indexCols,
			ctx:          ctx,
			db:           db,
			driver:       nil,
			id:           indexId,
			indexRowData: indexData,
			indexSch:     index.Schema(),
			table:        tableMap[indexDetails.tableName],
			tableData:    tableDataMap[indexDetails.tableName],
			tableName:    indexDetails.tableName,
			tableSch:     tableSchemaMap[indexDetails.tableName],
		}
	}

	return indexMap
}

func forceParseTime(timeString string) time.Time {
	tim, _ := time.Parse("2006-01-02 15:04:05", timeString)
	return tim
}
