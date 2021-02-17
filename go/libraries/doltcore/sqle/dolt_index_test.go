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

package sqle

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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
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
			indexIter, err := dil.RowIter(NewTestSQLCtx(context.Background()), dil.IndexRowData(), nil)
			require.NoError(t, err)

			var readRows []sql.Row
			var nextRow sql.Row
			for nextRow, err = indexIter.Next(); err == nil; nextRow, err = indexIter.Next() {
				readRows = append(readRows, nextRow)
			}
			require.Equal(t, io.EOF, err)

			requireUnorderedRowsEqual(t, expectedRows, readRows)

			indexLookup, err = index.DescendRange(test.lessThanOrEqual, test.greaterThanOrEqual)
			require.NoError(t, err)
			dil, ok = indexLookup.(*doltIndexLookup)
			require.True(t, ok)
			indexIter, err = dil.RowIter(NewTestSQLCtx(context.Background()), dil.IndexRowData(), nil)
			require.NoError(t, err)

			readRows = nil
			for nextRow, err = indexIter.Next(); err == nil; nextRow, err = indexIter.Next() {
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

func testDoltIndex(t *testing.T, keys []interface{}, expectedRows []sql.Row, indexLookupFn func(keys ...interface{}) (sql.IndexLookup, error)) {
	indexLookup, err := indexLookupFn(keys...)
	require.NoError(t, err)
	dil, ok := indexLookup.(*doltIndexLookup)
	require.True(t, ok)
	indexIter, err := dil.RowIter(NewTestSQLCtx(context.Background()), dil.IndexRowData(), nil)
	require.NoError(t, err)

	var readRows []sql.Row
	var nextRow sql.Row
	for nextRow, err = indexIter.Next(); err == nil; nextRow, err = indexIter.Next() {
		readRows = append(readRows, nextRow)
	}
	require.Equal(t, io.EOF, err)

	requireUnorderedRowsEqual(t, convertSqlRowToInt64(expectedRows), readRows)
}

func doltIndexSetup(t *testing.T) map[string]DoltIndex {
	ctx := NewTestSQLCtx(context.Background())
	dEnv := dtestutils.CreateTestEnv()
	db := NewDatabase("dolt", dEnv.DbData())
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
			db:           db,
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
			db:           db,
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
			db:           db,
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
		index := tableSchemaMap[indexDetails.tableName].Indexes().GetByName(indexDetails.indexName)
		indexData, err := tableMap[indexDetails.tableName].GetIndexRowData(ctx, index.Name())
		require.NoError(t, err)
		indexCols := make([]schema.Column, index.Count())
		for i, tag := range index.IndexedColumnTags() {
			indexCols[i], _ = index.GetColumn(tag)
		}

		indexId := indexDetails.tableName + ":" + index.Name()
		indexMap[indexId] = &doltIndex{
			cols:         indexCols,
			db:           db,
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
