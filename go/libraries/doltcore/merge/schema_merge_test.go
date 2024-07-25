// Copyright 2023 Dolthub, Inc.
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

package merge_test

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/writer"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

type schemaMergeTest struct {
	name                string
	ancestor            table
	left, right         *table
	merged              table
	conflict            bool
	skipNewFmt          bool
	skipOldFmt          bool
	skipFlipOnNewFormat bool
	skipFlipOnOldFormat bool
	dataTests           []dataTest
}

type dataTest struct {
	name                 string
	ancestor             []sql.Row
	left, right          []sql.Row
	merged               []sql.Row
	constraintViolations []constraintViolation
	dataConflict         bool
	skip                 bool
	skipFlip             bool
}

type table struct {
	ns   namedSchema
	rows []sql.Row
}

type namedSchema struct {
	name   string
	sch    schema.Schema
	create string
}

// TestSchemaMerge are schema merge integration tests from 2023
func TestSchemaMerge(t *testing.T) {
	t.Run("column add/drop tests", func(t *testing.T) {
		testSchemaMerge(t, columnAddDropTests)
	})
	t.Run("column default tests", func(t *testing.T) {
		testSchemaMerge(t, columnDefaultTests)
	})
	t.Run("collation tests", func(t *testing.T) {
		testSchemaMerge(t, collationTests)
	})
	t.Run("nullability tests", func(t *testing.T) {
		testSchemaMerge(t, nullabilityTests)
	})
	t.Run("column type change tests", func(t *testing.T) {
		testSchemaMerge(t, typeChangeTests)
	})
	t.Run("column reordering tests", func(t *testing.T) {
		testSchemaMerge(t, columnReorderingTests)
	})
	t.Run("primary key change tests", func(t *testing.T) {
		testSchemaMerge(t, keyChangeTests)
	})
	t.Run("secondary index tests", func(t *testing.T) {
		testSchemaMerge(t, secondaryIndexTests)
	})
	t.Run("simple conflict tests", func(t *testing.T) {
		testSchemaMerge(t, simpleConflictTests)
	})
	t.Run("json merge tests", func(t *testing.T) {
		testSchemaMerge(t, jsonMergeTests)
	})
}

var columnAddDropTests = []schemaMergeTest{
	{
		name:     "no schema changes",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY)"), row(1)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY)"), row(1)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY)"), row(1)),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY)"), row(1)),
	},
	// one side changes columns
	{
		name:     "left side column add",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       ")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       ")),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)")),
		dataTests: []dataTest{
			{
				name:     "left side adds column and assigns non-null value",
				ancestor: singleRow(1),
				left:     singleRow(1, 2),
				right:    singleRow(1),
				merged:   singleRow(1, 2),
			},
			{
				name:     "left side adds column and assigns null value",
				ancestor: singleRow(1),
				left:     singleRow(1, nil),
				right:    singleRow(1),
				merged:   singleRow(1, nil),
			},
		},
	},
	{
		name:     "left side column add with additional column after",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int)       ")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int)       ")),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)")),
		dataTests: []dataTest{
			{
				name:     "left side adds column and assigns non-null value, extra column is non-NULL",
				ancestor: singleRow(1, 3),
				left:     singleRow(1, 2, 3),
				right:    singleRow(1, 3),
				merged:   singleRow(1, 2, 3),
			},
			{
				name:     "left side adds column and assigns null value, extra column is non-NULL",
				ancestor: singleRow(1, 3),
				left:     singleRow(1, nil, 3),
				right:    singleRow(1, 3),
				merged:   singleRow(1, nil, 3),
			},
			{
				// Skipped because of (https://github.com/dolthub/dolt/issues/6745)
				name:     "left side adds column and assigns non-null value, extra column has data change on right",
				ancestor: singleRow(1, 3),
				left:     singleRow(1, 2, 3),
				right:    singleRow(1, 4),
				merged:   singleRow(1, 2, 4),
				skipFlip: true,
			},
			{
				// Skipped because of (https://github.com/dolthub/dolt/issues/6745)
				name:     "left side adds column and assigns non-null value, extra column has data change on right to NULL",
				ancestor: singleRow(1, 3),
				left:     singleRow(1, 2, 3),
				right:    singleRow(1, nil),
				merged:   singleRow(1, 2, nil),
				skipFlip: true,
			},
			{
				// Skipped because of (https://github.com/dolthub/dolt/issues/6745)
				name:     "left side adds column and assigns non-null value, extra column has data change on right to non-NULL",
				ancestor: singleRow(1, nil),
				left:     singleRow(1, 2, nil),
				right:    singleRow(1, 3),
				merged:   singleRow(1, 2, 3),
				skipFlip: true,
			},
			{
				name:     "left side adds column and assigns non-null value, extra column is NULL",
				ancestor: singleRow(1, nil),
				left:     singleRow(1, 2, nil),
				right:    singleRow(1, nil),
				merged:   singleRow(1, 2, nil),
			},
			{
				name:     "left side adds column and assigns null value, extra column is NULL",
				ancestor: singleRow(1, nil),
				left:     singleRow(1, nil, nil),
				right:    singleRow(1, nil),
				merged:   singleRow(1, nil, nil),
			},
			{
				// Skipped because of (https://github.com/dolthub/dolt/issues/6745)
				name:     "left side adds column and assigns null value, extra column has data change on right",
				ancestor: singleRow(1, 3),
				left:     singleRow(1, nil, 3),
				right:    singleRow(1, 4),
				merged:   singleRow(1, nil, 4),
				skipFlip: true,
			},
			{
				// Skipped because of (https://github.com/dolthub/dolt/issues/6745)
				name:     "left side adds column and assigns null value, extra column has data change on right to NULL",
				ancestor: singleRow(1, 3),
				left:     singleRow(1, nil, 3),
				right:    singleRow(1, nil),
				merged:   singleRow(1, nil, nil),
				skipFlip: true,
			},
			{
				// Skipped because of (https://github.com/dolthub/dolt/issues/6745)
				name:     "left side adds column and assigns null value, extra column has data change on right to non-NULL",
				ancestor: singleRow(1, nil),
				left:     singleRow(1, nil, nil),
				right:    singleRow(1, 3),
				merged:   singleRow(1, nil, 3),
				skipFlip: true,
			},
		},
	},
	{
		name:     "left side column drop",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int, a int)")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int)       ")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int, a int)")),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int)       ")),
		dataTests: []dataTest{
			{
				name:     "no data change",
				ancestor: singleRow(1, 2, 3),
				left:     singleRow(1, 2),
				right:    singleRow(1, 2, 3),
				merged:   singleRow(1, 2),
			},
			{
				name:         "one side sets to NULL, other drops non-NULL",
				ancestor:     singleRow(1, 2, 3),
				left:         singleRow(1, 2),
				right:        singleRow(1, 2, nil),
				dataConflict: true,
				skip:         true,
			},
			{
				name:         "one side sets to NULL, other drops non-NULL, plus data change",
				ancestor:     singleRow(1, 2, 3),
				left:         singleRow(1, 2),
				right:        singleRow(1, 3, nil),
				dataConflict: true,
			},
			{
				name:         "one side sets to non-NULL, other drops NULL",
				ancestor:     singleRow(1, 2, nil),
				left:         singleRow(1, 2),
				right:        singleRow(1, 2, 3),
				dataConflict: true,
			},
			{
				name:         "one side sets to non-NULL, other drops NULL, plus data change",
				ancestor:     singleRow(1, 2, nil),
				left:         singleRow(1, 3),
				right:        singleRow(1, 2, 3),
				dataConflict: true,
			},
			{
				name:         "one side sets to non-NULL, other drops non-NULL",
				ancestor:     singleRow(1, 2, 3),
				left:         singleRow(1, 2),
				right:        singleRow(1, 2, 4),
				dataConflict: true,
			},
			{
				name:     "one side drops column, other deletes row",
				ancestor: []sql.Row{row(1, 2, 3), row(4, 5, 6)},
				left:     []sql.Row{row(1, 2), row(4, 5)},
				right:    []sql.Row{row(1, 2, 3)},
				merged:   []sql.Row{row(1, 2)},
			},
		},
	},
	{
		name:     "left side column drop with additional column after",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int)       ")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)")),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int)       ")),
		dataTests: []dataTest{
			{
				name:     "no data change",
				ancestor: singleRow(1, 2, 3),
				left:     singleRow(1, 3),
				right:    singleRow(1, 2, 3),
				merged:   singleRow(1, 3),
			},
			{
				name:         "one side sets to NULL, other drops non-NULL",
				ancestor:     singleRow(1, 2, 3),
				left:         singleRow(1, 3),
				right:        singleRow(1, nil, 3),
				dataConflict: true,
			},
			{
				name:         "one side sets to NULL, other drops non-NULL, plus data change",
				ancestor:     singleRow(1, 2, 4),
				left:         singleRow(1, 3),
				right:        singleRow(1, nil, 4),
				dataConflict: true,
			},
			{
				name:         "one side sets to non-NULL, other drops NULL, plus data change",
				ancestor:     singleRow(1, nil, 3),
				left:         singleRow(1, 3),
				right:        singleRow(1, 2, 3),
				dataConflict: true,
			},
			{
				name:         "one side sets to non-NULL, other drops NULL, plus data change",
				ancestor:     singleRow(1, nil, 3),
				left:         singleRow(1, 4),
				right:        singleRow(1, 2, 3),
				dataConflict: true,
			},
			{
				name:         "one side sets to non-NULL, other drops non-NULL",
				ancestor:     singleRow(1, 2, 3),
				left:         singleRow(1, 3),
				right:        singleRow(1, 4, 3),
				dataConflict: true,
			},
		},
	},
	// both sides change columns
	{
		name:       "independent column adds",
		ancestor:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY)              "), row(1)),
		left:       tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)       "), row(1, 2)),
		right:      tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int)       "), row(1, 3)),
		merged:     *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)"), row(1, 2, 3)),
		skipNewFmt: true,
		skipOldFmt: true,
	},
	{
		name:     "independent column drops",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)"), row(1, 2, 3)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)       "), row(1, 2)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int)       "), row(1, 3)),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY)              "), row(1)),
	},
	{
		name:     "convergent column adds",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int)       ")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int, a int)")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int, a int)")),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int, a int)")),
		dataTests: []dataTest{
			{
				name:     "convergent adds assigning null",
				ancestor: singleRow(1, 2),
				left:     singleRow(1, 2, nil),
				right:    singleRow(1, 2, nil),
				merged:   singleRow(1, 2, nil),
			},
			{
				name:         "convergent adds with differing nullness",
				ancestor:     singleRow(1, 2),
				left:         singleRow(1, 2, nil),
				right:        singleRow(1, 2, 3),
				dataConflict: true,
			},
			{
				name:         "convergent adds with differing nullness, plus convergent data change",
				ancestor:     singleRow(1, 2),
				left:         singleRow(1, 3, nil),
				right:        singleRow(1, 3, 4),
				dataConflict: true,
			},
		},
	},
	{
		name:     "convergent column add in middle of schema",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)       ")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int, a int)")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int, a int)")),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int, a int)")),
		dataTests: []dataTest{
			{
				name:     "convergent adds assigning null",
				ancestor: singleRow(1, 2),
				left:     singleRow(1, nil, 2),
				right:    singleRow(1, nil, 2),
				merged:   singleRow(1, nil, 2),
			},
			{
				name:         "convergent adds with differing nullness",
				ancestor:     singleRow(1, 2),
				left:         singleRow(1, nil, 2),
				right:        singleRow(1, 3, 2),
				dataConflict: true,
			},
			{
				name:         "convergent adds with differing nullness, plus convergent data change",
				ancestor:     singleRow(1, 2),
				left:         singleRow(1, nil, 3),
				right:        singleRow(1, 4, 3),
				dataConflict: true,
			},
		},
	},
	{
		name:     "convergent column drops",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       ")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       ")),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       ")),
		dataTests: []dataTest{
			{
				name:     "no data change",
				ancestor: singleRow(1, 2),
				left:     singleRow(1),
				right:    singleRow(1),
				merged:   singleRow(1),
			},
			{
				name:     "convergent drops on new row",
				ancestor: nil,
				left:     singleRow(1),
				right:    singleRow(1),
				merged:   singleRow(1),
			},
		},
	},
	{
		name:     "convergent column adds, independent drops",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)"), row(1, 2, 3)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int, c int)"), row(1, 3, 4)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, c int)"), row(1, 2, 4)),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, c int)       "), row(1, 4)),
	},
	{
		name:       "convergent column drops, independent adds",
		ancestor:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)       "), row(1, 2)),
		left:       tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int)       "), row(1, 3)),
		right:      tbl(sch("CREATE TABLE t (id int PRIMARY KEY, c int)       "), row(1, 4)),
		merged:     *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int, c int)"), row(1, 3, 4)),
		skipNewFmt: true,
		skipOldFmt: true,
	},
	// one side changes columns, the other inserts rows
	{
		name:     "left side column add, right side insert row",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, 2)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1), row(11)),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, 2), row(11, nil)),
	},
	{
		name:     "left side column drop, right side insert row",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, 2)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, 2), row(11, 22)),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1), row(11)),
	},
	// both sides change columns and insert rows
	{
		name:       "independent column adds, both sides insert independent rows",
		ancestor:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY)              "), row(1)),
		left:       tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)       "), row(1, 2), row(12, 22)),
		right:      tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int)       "), row(1, 3), row(13, 33)),
		merged:     *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)"), row(1, 2, 3), row(12, 22, nil), row(13, nil, 33)),
		skipNewFmt: true,
		skipOldFmt: true,
	},
	{
		name:     "independent column drops, both sides insert independent rows",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)"), row(1, 2, 3)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)       "), row(1, 2), row(12, 22)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int)       "), row(1, 3), row(13, 33)),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY)              "), row(1), row(12), row(13)),
	},
	{
		name:     "convergent column adds, both sides insert independent rows",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, nil), row(12, 22)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, nil), row(13, 33)),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, nil), row(12, 22), row(13, 33)),
	},
	{
		name:     "convergent column drops, both sides insert independent rows",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, 2)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1), row(12)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1), row(13)),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1), row(12), row(13)),
	},
	{
		name:       "independent column adds, both sides insert same row",
		ancestor:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY)              "), row(1)),
		left:       tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)       "), row(1, 2), row(12, 22)),
		right:      tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int)       "), row(1, 3), row(12, 33)),
		merged:     *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)"), row(1, 2, 3), row(12, 22, 33)),
		skipNewFmt: true,
		skipOldFmt: true,
	},
	{
		name:     "independent column drops, both sides insert same row",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)"), row(1, 2, 3)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)       "), row(1, 2), row(12, 22)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int)       "), row(1, 3), row(12, 33)),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY)              "), row(1), row(12)),
	},
	{
		name:     "right side drops and adds column of same type",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, c int, a int)")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, c int, a int)")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, c int, b int)")),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, c int, b int)")),
		dataTests: []dataTest{
			{
				name:         "left side modifies dropped column",
				ancestor:     singleRow(1, 1, 2),
				left:         singleRow(1, 1, 3),
				right:        singleRow(1, 2, 2),
				dataConflict: true,
			},
		},
	},
	{
		name:     "right side drops and adds column of different type",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, c int, a int)")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, c int, a int)")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, c int, b text)")),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, c int, b text)")),
	},
}

type constraintViolation struct {
	violationType merge.CvType
	key, value    sql.Row
}

var collationTests = []schemaMergeTest{
	{
		name:     "left side changes collation",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a varchar(10) collate utf8mb4_0900_bin unique)")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a varchar(10) collate utf8mb4_0900_ai_ci unique)")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a varchar(10) collate utf8mb4_0900_bin unique)")),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a varchar(10) collate utf8mb4_0900_ai_ci unique)")),
		dataTests: []dataTest{
			{
				name:     "no data change",
				ancestor: singleRow(1, "hello"),
				left:     singleRow(1, "hello"),
				right:    singleRow(1, "hello"),
				merged:   singleRow(1, "hello"),
			},
			{
				name:     "right side insert",
				ancestor: []sql.Row{{1, "hello"}},
				left:     []sql.Row{{1, "hello"}},
				right:    []sql.Row{{1, "hello"}, {2, "world"}},
				merged:   []sql.Row{{1, "hello"}, {2, "world"}},
			},
			{
				name:     "right side delete",
				ancestor: []sql.Row{{1, "hello"}, {2, "world"}},
				left:     []sql.Row{{1, "hello"}, {2, "world"}},
				right:    []sql.Row{{1, "hello"}},
				merged:   []sql.Row{{1, "hello"}},
			},
			{
				name:     "right side insert causes unique violation",
				ancestor: []sql.Row{{1, "hello"}},
				left:     []sql.Row{{1, "hello"}},
				right:    []sql.Row{{1, "hello"}, {2, "HELLO"}},
				constraintViolations: []constraintViolation{
					{merge.CvType_UniqueIndex, sql.Row{int32(1)}, sql.Row{"hello"}},
					{merge.CvType_UniqueIndex, sql.Row{int32(2)}, sql.Row{"HELLO"}},
				},
			},
		},
	},
	{
		name:     "left side changes table collation",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a varchar(10) unique) collate utf8mb4_0900_bin")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a varchar(10) unique) collate utf8mb4_0900_ai_ci")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a varchar(10) unique) collate utf8mb4_0900_bin")),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a varchar(10) unique) collate utf8mb4_0900_ai_ci")),
		dataTests: []dataTest{
			{
				name:     "no data change",
				ancestor: singleRow(1, "hello"),
				left:     singleRow(1, "hello"),
				right:    singleRow(1, "hello"),
				merged:   singleRow(1, "hello"),
			},
			{
				name:     "right side insert",
				ancestor: []sql.Row{{1, "hello"}},
				left:     []sql.Row{{1, "hello"}},
				right:    []sql.Row{{1, "hello"}, {2, "world"}},
				merged:   []sql.Row{{1, "hello"}, {2, "world"}},
			},
			{
				name:     "right side delete",
				ancestor: []sql.Row{{1, "hello"}, {2, "world"}},
				left:     []sql.Row{{1, "hello"}, {2, "world"}},
				right:    []sql.Row{{1, "hello"}},
				merged:   []sql.Row{{1, "hello"}},
			},
			{
				name:     "right side insert causes unique violation",
				ancestor: []sql.Row{{1, "hello"}},
				left:     []sql.Row{{1, "hello"}},
				right:    []sql.Row{{1, "hello"}, {2, "HELLO"}},
				constraintViolations: []constraintViolation{
					{merge.CvType_UniqueIndex, sql.Row{int32(1)}, sql.Row{"hello"}},
					{merge.CvType_UniqueIndex, sql.Row{int32(2)}, sql.Row{"HELLO"}},
				},
			},
		},
	},
	{
		name:     "no collation changes",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int, c varchar(10) collate utf8mb4_0900_ai_ci unique, d decimal(5,3) unique)")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int, c varchar(10) collate utf8mb4_0900_ai_ci unique, d decimal(5,3) unique)")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int, c varchar(10) collate utf8mb4_0900_ai_ci unique, d decimal(5,3) unique)")),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int, c varchar(10) collate utf8mb4_0900_ai_ci unique, d decimal(5,3) unique)")),
		dataTests: []dataTest{
			{
				name:     "no data change",
				ancestor: singleRow(1, 1, 1, "foo", decimal.New(8, 0)),
				left:     singleRow(1, 1, 2, "foo", decimal.New(8, 0)),
				right:    singleRow(1, 2, 1, "foo", decimal.New(8, 0)),
				merged:   singleRow(1, 2, 2, "foo", decimal.New(8, 0)),
			},
			{
				name:     "replace varchar with equal replacement",
				ancestor: singleRow(1, 1, 1, "foo", decimal.New(100, 0)),
				left:     singleRow(1, 1, 2, "FOO", decimal.New(100, 0)),
				right:    singleRow(1, 2, 1, "foo", decimal.New(100, 0)),
				merged:   singleRow(1, 2, 2, "FOO", decimal.New(100, 0)),
			},
			{
				name:         "conflict removal and replace varchar with equal replacement",
				ancestor:     singleRow(1, 1, 1, "foo", decimal.New(100, 0)),
				left:         singleRow(1, 1, 2, "FOO", decimal.New(100, 0)),
				right:        nil,
				dataConflict: true,
			},
			{
				name:     "replace decimal with equal replacement",
				ancestor: singleRow(1, 1, 1, "foo", decimal.New(100, 0)),
				left:     singleRow(1, 1, 2, "foo", decimal.New(1, 2)),
				right:    singleRow(1, 2, 1, "foo", decimal.New(100, 0)),
				merged:   singleRow(1, 2, 2, "foo", decimal.New(1, 2)),
			},
			{
				name:     "conflict removal and replace decimal with equal replacement",
				ancestor: singleRow(1, 1, 1, "foo", decimal.New(100, 0)),
				left:     singleRow(1, 1, 1, "foo", decimal.New(1, 2)),
				right:    nil,
				merged:   nil,
			},
		},
	},
}

var columnDefaultTests = []schemaMergeTest{
	{
		name:     "left side add default",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)           ")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 42)")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)           ")),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 42)")),
	},
	{
		name:     "left side drop default",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 42)")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)           ")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 42)")),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)           ")),
	},
	{
		name:     "convergent add",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)           ")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 42)")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 42)")),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 42)")),
	},
	{
		name:     "convergent drop",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 42)")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)           ")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)           ")),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)           ")),
	},
	// one side changes columns, the other inserts rows
	{
		name:     "left side column add, right side insert row",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY)                  "), row(1)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 42)"), row(1, 42)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY)                  "), row(1), row(12)),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 42)"), row(1, 42), row(12, 42)),
	},
	// both sides change columns and insert rows
	{
		name:       "independent column adds, both sides insert independent rows",
		ancestor:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY)                                    "), row(1)),
		left:       tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 19)                  "), row(1, 2), row(12, 19)),
		right:      tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int DEFAULT 17)                  "), row(1, 3), row(13, 17)),
		merged:     *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 19, b int DEFAULT 17)"), row(1, 2, 3), row(12, 22, 17), row(13, 19, 33)),
		skipNewFmt: true,
		skipOldFmt: true,
	},
	{
		name:     "convergent column adds, both sides insert independent rows",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY)                  "), row(1)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 19)"), row(1, 19)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 19)"), row(1, 19)),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 19)"), row(1, 19)),
	},
}

var nullabilityTests = []schemaMergeTest{
	{
		name:                "add not null column to empty table",
		ancestor:            *tbl(sch("CREATE TABLE t (id int PRIMARY KEY)                ")),
		left:                tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int NOT NULL)")),
		right:               tbl(sch("CREATE TABLE t (id int PRIMARY KEY)                ")),
		merged:              *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int NOT NULL)")),
		skipOldFmt:          true,
		skipFlipOnOldFormat: true,
	},
	{
		name:                "add not null constraint to existing column",
		ancestor:            *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)         "), row(1, 1)),
		left:                tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int NOT NULL)"), row(1, 1)),
		right:               tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)         "), row(1, 1), row(2, 2)),
		merged:              *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int NOT NULL)"), row(1, 1), row(2, 2)),
		skipOldFmt:          true,
		skipFlipOnOldFormat: true,
	},
	{
		name:                "add not null column to non-empty table",
		ancestor:            *tbl(sch("CREATE TABLE t (id int PRIMARY KEY)                              "), row(1)),
		left:                tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int NOT NULL DEFAULT  '19')"), row(1, 19)),
		right:               tbl(sch("CREATE TABLE t (id int PRIMARY KEY)                              "), row(1), row(2)),
		merged:              *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int NOT NULL DEFAULT  '19')"), row(1, 19), row(2, 19)),
		skipOldFmt:          true,
		skipFlipOnOldFormat: true,
	},
	{
		name:                "table delete plus add not null column to empty table",
		ancestor:            *tbl(sch("CREATE TABLE t (id int PRIMARY KEY)                              ")),
		left:                tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int NOT NULL DEFAULT  '19')")),
		right:               nil,
		conflict:            true,
		skipOldFmt:          true,
		skipFlipOnOldFormat: true,
	},
	{
		name:                "table delete plus add not null column to non-empty table",
		ancestor:            *tbl(sch("CREATE TABLE t (id int PRIMARY KEY)                              "), row(1)),
		left:                tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int NOT NULL DEFAULT  '19')"), row(1, 19)),
		right:               nil,
		conflict:            true,
		skipOldFmt:          true,
		skipFlipOnOldFormat: true,
	},
}

var columnReorderingTests = []schemaMergeTest{}

var typeChangeTests = []schemaMergeTest{
	{
		name:     "modify column type on the left side",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)     "), row(1, 2, 3)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b int)"), row(1, "2", 3)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)     "), row(1, 2, 3)),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b int)"), row(1, "2", 3)),
	},
	{
		name:       "independently modify column type on the both sides",
		ancestor:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)          "), row(1, 2, 3)),
		left:       tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b int)     "), row(1, "2", 3)),
		right:      tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b char(20))     "), row(1, 2, "3")),
		merged:     *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b char(20))"), row(1, "2", "3")),
		skipNewFmt: true,
		skipOldFmt: true,
	},
	{
		name:     "convergently modify column type on the both sides",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)     "), row(1, 2, 3)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b int)"), row(1, "2", 3)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b int)"), row(1, "2", 3)),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b int)"), row(1, "2", 3)),
	},
	// column changes one side, data changes other side
	{
		name:     "modify column type on the left side between compatible string types",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a varchar(20), b int, c varchar(20))")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a text, b int, c text)")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a varchar(20), b int, c varchar(20))")),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a text, b int, c text)")),
		dataTests: []dataTest{
			{
				name:     "schema change, no data change",
				ancestor: singleRow(1, "test", 1, "test"),
				left:     singleRow(1, "test", 1, "test"),
				right:    singleRow(1, "test", 1, "test"),
				merged:   singleRow(1, "test", 1, "test"),
			},
			{
				name:     "insert and schema change on left, no change on right",
				ancestor: nil,
				left:     singleRow(1, "test", 1, "test"),
				right:    nil,
				merged:   singleRow(1, "test", 1, "test"),
			},
			{
				name:     "insert on right, schema change on left",
				ancestor: nil,
				left:     nil,
				right:    singleRow(1, "test", 1, "test"),
				merged:   singleRow(1, "test", 1, "test"),
			},
			{
				name:     "data and schema change on left, no change on right",
				ancestor: singleRow(1, "test", 1, "test"),
				left:     singleRow(1, "hello world", 1, "hello world"),
				right:    singleRow(1, "test", 1, "test"),
				merged:   singleRow(1, "hello world", 1, "hello world"),
			},
			{
				name:     "data change on right, schema change on left",
				ancestor: singleRow(1, "test", 1, "test"),
				left:     singleRow(1, "test", 1, "test"),
				right:    singleRow(1, "hello world", 1, "hello world"),
				merged:   singleRow(1, "hello world", 1, "hello world"),
			},
			{
				name:     "data set and schema change on left, no change on right",
				ancestor: singleRow(1, nil, 1, nil),
				left:     singleRow(1, "hello world", 1, "hello world"),
				right:    singleRow(1, nil, 1, nil),
				merged:   singleRow(1, "hello world", 1, "hello world"),
			},
			{
				name:     "data set on right, schema change on left",
				ancestor: singleRow(1, nil, 1, nil),
				left:     singleRow(1, nil, 1, nil),
				right:    singleRow(1, "hello world", 1, "hello world"),
				merged:   singleRow(1, "hello world", 1, "hello world"),
			},
			{
				name:     "convergent inserts",
				ancestor: nil,
				left:     singleRow(1, "test", 1, "test"),
				right:    singleRow(1, "test", 1, "test"),
				merged:   singleRow(1, "test", 1, "test"),
			},
			{
				name:         "conflicting inserts",
				ancestor:     nil,
				left:         singleRow(1, "test", 1, "test"),
				right:        singleRow(1, "hello world", 1, "hello world"),
				dataConflict: true,
			},
			{
				name:     "delete and schema change on left",
				ancestor: singleRow(1, "test", 1, "test"),
				left:     nil,
				right:    singleRow(1, "test", 1, "test"),
				merged:   nil,
			},
			{
				name:     "schema change on left, delete on right",
				ancestor: singleRow(1, "test", 1, "test"),
				left:     singleRow(1, "test", 1, "test"),
				right:    nil,
				merged:   nil,
			},
			{
				name:         "schema and value change on left, delete on right",
				ancestor:     singleRow(1, "test", 1, "test"),
				left:         singleRow(1, "hello", 1, "hello"),
				right:        nil,
				dataConflict: true,
			},
		},
	},
	{
		name:     "modify column type on the left side between compatible string types with unique secondary index",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a varchar(20) unique, b int, c varchar(20) unique)")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a text, b int, c text)")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a varchar(20) unique, b int, c varchar(20) unique)")),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a text, b int, c text)")),
		dataTests: []dataTest{
			{
				name:     "schema change, no data change",
				ancestor: singleRow(1, "test", 1, "test"),
				left:     singleRow(1, "test", 1, "test"),
				right:    singleRow(1, "test", 1, "test"),
				merged:   singleRow(1, "test", 1, "test"),
			},
			{
				name:     "insert and schema change on left, no change on right",
				ancestor: nil,
				left:     singleRow(1, "test", 1, "test"),
				right:    nil,
				merged:   singleRow(1, "test", 1, "test"),
			},
			{
				name:     "insert on right, schema change on left",
				ancestor: nil,
				left:     nil,
				right:    singleRow(1, "test", 1, "test"),
				merged:   singleRow(1, "test", 1, "test"),
			},
			{
				name:     "data and schema change on left, no change on right",
				ancestor: singleRow(1, "test", 1, "test"),
				left:     singleRow(1, "hello world", 1, "hello world"),
				right:    singleRow(1, "test", 1, "test"),
				merged:   singleRow(1, "hello world", 1, "hello world"),
			},
			{
				name:     "data change on right, schema change on left",
				ancestor: singleRow(1, "test", 1, "test"),
				left:     singleRow(1, "test", 1, "test"),
				right:    singleRow(1, "hello world", 1, "hello world"),
				merged:   singleRow(1, "hello world", 1, "hello world"),
			},
			{
				name:     "data set and schema change on left, no change on right",
				ancestor: singleRow(1, nil, 1, nil),
				left:     singleRow(1, "hello world", 1, "hello world"),
				right:    singleRow(1, nil, 1, nil),
				merged:   singleRow(1, "hello world", 1, "hello world"),
			},
			{
				name:     "data set on right, schema change on left",
				ancestor: singleRow(1, nil, 1, nil),
				left:     singleRow(1, nil, 1, nil),
				right:    singleRow(1, "hello world", 1, "hello world"),
				merged:   singleRow(1, "hello world", 1, "hello world"),
			},
			{
				name:     "convergent inserts",
				ancestor: nil,
				left:     singleRow(1, "test", 1, "test"),
				right:    singleRow(1, "test", 1, "test"),
				merged:   singleRow(1, "test", 1, "test"),
			},
			{
				name:         "conflicting inserts",
				ancestor:     nil,
				left:         singleRow(1, "test", 1, "test"),
				right:        singleRow(1, "hello world", 1, "hello world"),
				dataConflict: true,
			},
			{
				name:     "delete and schema change on left",
				ancestor: singleRow(1, "test", 1, "test"),
				left:     nil,
				right:    singleRow(1, "test", 1, "test"),
				merged:   nil,
			},
			{
				name:     "schema change on left, delete on right",
				ancestor: singleRow(1, "test", 1, "test"),
				left:     singleRow(1, "test", 1, "test"),
				right:    nil,
				merged:   nil,
			},
			{
				name:         "schema and value change on left, delete on right",
				ancestor:     singleRow(1, "test", 1, "test"),
				left:         singleRow(1, "hello", 1, "hello"),
				right:        nil,
				dataConflict: true,
			},
		},
	},
}

var keyChangeTests = []schemaMergeTest{
	{
		name:     "add a trailing primary key column on left side",
		ancestor: *tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", float32(3.0))),
		left:     tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a, b))"), row(1, "2", float32(3.0))),
		right:    tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", float32(3.0))),
		merged:   *tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a, b))"), row(1, "2", float32(3.0))),
	},
	{
		name:     "add a leading primary key column on left side",
		ancestor: *tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", float32(3.0))),
		left:     tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (b, a))"), row(1, "2", float32(3.0))),
		right:    tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", float32(3.0))),
		merged:   *tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (b, a))"), row(1, "2", float32(3.0))),
	},
	{
		name:                "remove a trailing primary key column on left side",
		ancestor:            *tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a, b))"), row(1, "2", float32(3.0))),
		left:                tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", float32(3.0))),
		right:               tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a, b))"), row(1, "2", float32(3.0))),
		merged:              *tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", float32(3.0))),
		skipFlipOnNewFormat: true,
	},
	{
		name:     "remove a trailing primary key column on both sides",
		ancestor: *tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a, b))"), row(1, "2", float32(3.0))),
		left:     tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", float32(3.0))),
		right:    tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", float32(3.0))),
		merged:   *tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", float32(3.0))),
	},
	{
		name:                "remove a leading primary key column on left side",
		ancestor:            *tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (b, a))"), row(1, "2", float32(3.0))),
		left:                tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", float32(3.0))),
		right:               tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (b, a))"), row(1, "2", float32(3.0))),
		merged:              *tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", float32(3.0))),
		skipFlipOnNewFormat: true,
	},
	{
		name:     "remove a leading primary key column on both sides",
		ancestor: *tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (b, a))"), row(1, "2", float32(3.0))),
		left:     tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", float32(3.0))),
		right:    tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", float32(3.0))),
		merged:   *tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", float32(3.0))),
	},
	{
		skipFlipOnNewFormat: true,
		skipFlipOnOldFormat: true,
		name:                "convert left side to a keyless table",
		ancestor:            *tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))"), row(1, "2", float32(3.0))),
		left:                tbl(sch("CREATE TABLE t (a int, b char(20), c float)                 "), row(1, "2", float32(3.0))),
		right:               tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))"), row(1, "2", float32(3.0))),
		merged:              *tbl(sch("CREATE TABLE t (a int, b char(20), c float)                 "), row(1, "2", float32(3.0))),
	},
	{
		name:       "convert both sides to keyless tables",
		ancestor:   *tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))"), row(1, "2", float32(3.0))),
		left:       tbl(sch("CREATE TABLE t (a int, b char(20), c float)                 "), row(1, "2", float32(3.0))),
		right:      tbl(sch("CREATE TABLE t (a int, b char(20), c float)                 "), row(1, "2", float32(3.0))),
		merged:     *tbl(sch("CREATE TABLE t (a int, b char(20), c float)                 "), row(1, "2", float32(3.0))),
		skipNewFmt: true,
		skipOldFmt: true,
	},
}

var secondaryIndexTests = []schemaMergeTest{
	{
		name:     "independent index adds",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float)                     "), row(1, "2", float32(3.0))),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX (a))          "), row(1, "2", float32(3.0))),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX (b))          "), row(1, "2", float32(3.0))),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX (a), INDEX(b))"), row(1, "2", float32(3.0))),
	},
	{
		name:     "independent composite index adds",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float)                            "), row(1, "2", float32(3.0))),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX (a, b))              "), row(1, "2", float32(3.0))),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX (b, a))              "), row(1, "2", float32(3.0))),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX (a, b), INDEX (b, a))"), row(1, "2", float32(3.0))),
	},
	{
		name:                "independent index drops",
		ancestor:            *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX (a), INDEX (b))"), row(1, "2", float32(3.0))),
		left:                tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX (a))           "), row(1, "2", float32(3.0))),
		right:               tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX (b))           "), row(1, "2", float32(3.0))),
		merged:              *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float)                      "), row(1, "2", float32(3.0))),
		skipOldFmt:          true,
		skipFlipOnOldFormat: true,
	},
	{
		name:     "dropping column on right shifts column index between compatible types (see pr/8154)",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b char(20), c char(20), INDEX(c))"), row(1, 1, "2", "3")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b char(20), c char(20), INDEX (c))"), row(1, 2, "2", "3")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, c char(20), INDEX (c))"), row(1, 1, "4")),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, c char(20), INDEX (c))"), row(1, 2, "4")),
	},
	{
		name:     "dropping column on right shifts column index between incompatible types (see pr/8154)",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b tinyint, c int, INDEX(c))"), row(1, 1, 2, 3)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b tinyint, c int, INDEX (c))"), row(1, 2, 2, 3)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, c int, INDEX (c))"), row(1, 1, 4)),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, c int, INDEX (c))"), row(1, 2, 4)),
	},
}

var simpleConflictTests = []schemaMergeTest{
	{
		name:     "conflicting column adds",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY)                "), row(1)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int NULL)    "), row(1, 2)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int NOT NULL)"), row(1, 2)),
		conflict: true,
	},
	{
		name:     "column add and table drop",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY)                "), row(1)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int NULL)    "), row(1, 2)),
		right:    nil,
		conflict: true,
	},
	{
		// TODO: This test case does NOT generate a conflict; the merge gets short circuited, because the table's
		//       right/left/anc hashes are all the same. This is an issue with the test framework, not with Dolt.
		//       The code we use in these tests to create a schema (sqlutil.ParseCreateTableStatement) silently
		//       drops index and check constraint definitions.
		skipNewFmt: true,
		skipOldFmt: true,
		name:       "conflicting index adds: same name and columns, different constraints",
		ancestor:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float)                      ")),
		left:       tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX idx (a))       ")),
		right:      tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, UNIQUE INDEX idx (a))")),
		conflict:   true,
	},
	{
		// TODO: This test case does NOT generate a conflict; the merge gets short circuited, because the table's
		//       right/left/anc hashes are all the same. This is an issue with the test framework, not with Dolt.
		//       The code we use in these tests to create a schema (sqlutil.ParseCreateTableStatement) silently
		//       drops index and check constraint definitions.
		skipNewFmt: true,
		skipOldFmt: true,
		// TODO: multiple indexes can exist for the same column set, so this shouldn't actually be a conflict;
		//       Dolt does report this as a schema conflict today, but we could merge the two indexes together.
		name:     "conflicting index adds: same column different names",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float)                 ")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX a_idx (a))")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX key_a (a))")),
		conflict: true,
	},
	{
		// TODO: This test case does NOT generate a conflict; the merge gets short circuited, because the table's
		//       right/left/anc hashes are all the same. This is an issue with the test framework, not with Dolt.
		//       The code we use in these tests to create a schema (sqlutil.ParseCreateTableStatement) silently
		//       drops index and check constraint definitions.
		skipNewFmt: true,
		skipOldFmt: true,
		name:       "conflicting index adds: same name different definitions",
		ancestor:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float)               ")),
		left:       tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX idx (a))")),
		right:      tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX idx (b))")),
		conflict:   true,
	},
	{
		name:     "add primary key columns at different key positions on left and right sides",
		ancestor: *tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", float32(3.0))),
		left:     tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a, b))"), row(1, "2", float32(3.0))),
		right:    tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (b, a))"), row(1, "2", float32(3.0))),
		conflict: true,
	},
	{
		name:     "remove different primary key columns on left and right sides",
		ancestor: *tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a, b))"), row(1, "2", float32(3.0))),
		left:     tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", float32(3.0))),
		right:    tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (b))   "), row(1, "2", float32(3.0))),
		conflict: true,
	},
}

var jsonMergeTests = []schemaMergeTest{
	{
		name:     "json merge",
		ancestor: *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int, j json)")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int, j json)")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int, j json)")),
		merged:   *tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int, j json)")),
		dataTests: []dataTest{
			{
				name:     "convergent insertion",
				ancestor: singleRow(1, 1, 1, `{}`),
				left:     singleRow(1, 2, 1, `{ "key1": "value1" }`),
				right:    singleRow(1, 1, 2, `{ "key1": "value1" }`),
				merged:   singleRow(1, 2, 2, `{ "key1": "value1" }`),
			},
			{
				name:     "convergent insertion with escaped quotes in keys",
				ancestor: singleRow(1, 1, 1, `{}`),
				left:     singleRow(1, 2, 1, `{ "\"key1\"": "\"value1\"" }`),
				right:    singleRow(1, 1, 2, `{ "\"key1\"": "\"value1\"" }`),
				merged:   singleRow(1, 2, 2, `{ "\"key1\"": "\"value1\"" }`),
			},
			{
				name:     `parallel insertion`,
				ancestor: singleRow(1, 1, 1, `{}`),
				left:     singleRow(1, 2, 1, `{ "key1": "value1" }`),
				right:    singleRow(1, 1, 2, `{ "key2": "value2" }`),
				merged:   singleRow(1, 2, 2, `{ "key1": "value1", "key2": "value2" }`),
			},
			{
				name:     `convergent modification`,
				ancestor: singleRow(1, 1, 1, `{ "key1": "value1" }`),
				left:     singleRow(1, 2, 1, `{ "key1": "value2" }`),
				right:    singleRow(1, 1, 2, `{ "key1": "value2" }`),
				merged:   singleRow(1, 2, 2, `{ "key1": "value2" }`),
			},
			{
				name:     `parallel modification`,
				ancestor: singleRow(1, 1, 1, `{ "key1": "value1", "key2": "value2" }`),
				left:     singleRow(1, 2, 1, `{ "key1": "value3", "key2": "value2" }`),
				right:    singleRow(1, 1, 2, `{ "key1": "value1", "key2": "value4" }`),
				merged:   singleRow(1, 2, 2, `{ "key1": "value3", "key2": "value4" }`),
			},
			{
				name:     `parallel deletion`,
				ancestor: singleRow(1, 1, 1, `{ "key1": "value1" }`),
				left:     singleRow(1, 2, 1, `{}`),
				right:    singleRow(1, 1, 2, `{}`),
				merged:   singleRow(1, 2, 2, `{}`),
			},
			{
				name:     `convergent deletion`,
				ancestor: singleRow(1, 1, 1, `{ "key1": "value1", "key2": "value2" }`),
				left:     singleRow(1, 2, 1, `{ "key2": "value2" }`),
				right:    singleRow(1, 1, 2, `{ "key1": "value1" }`),
				merged:   singleRow(1, 2, 2, `{}`),
			},
			{
				name:         `divergent insertion`,
				ancestor:     singleRow(1, 1, 1, `{}`),
				left:         singleRow(1, 2, 1, `{ "key1": "value1" }`),
				right:        singleRow(1, 1, 2, `{ "key1": "value2" }`),
				dataConflict: true,
			},
			{
				name:         `divergent modification`,
				ancestor:     singleRow(1, 1, 1, `{ "key1": "value1"}`),
				left:         singleRow(1, 2, 1, `{ "key1": "value2" }`),
				right:        singleRow(1, 1, 2, `{ "key1": "value3" }`),
				dataConflict: true,
			},
			{
				name:         `divergent modification and deletion`,
				ancestor:     singleRow(1, 1, 1, `{ "key1": "value1"}`),
				left:         singleRow(1, 2, 1, `{ "key1": "value2" }`),
				right:        singleRow(1, 1, 2, `{}`),
				dataConflict: true,
			},
			{
				name:     `nested insertion`,
				ancestor: singleRow(1, 1, 1, `{ "key1": {} }`),
				left:     singleRow(1, 2, 1, `{ "key1": { "key1a": "value1a" } }`),
				right:    singleRow(1, 1, 2, `{ "key1": { "key1b": "value1b" } }`),
				merged:   singleRow(1, 2, 2, `{ "key1": { "key1a": "value1a", "key1b": "value1b" } }`),
			},
			{
				name:     `nested insertion with escaped quotes in keys`,
				ancestor: singleRow(1, 1, 1, `{ "\"key1\"": {} }`),
				left:     singleRow(1, 2, 1, `{ "\"key1\"": { "\"key1a\"": "value1a" } }`),
				right:    singleRow(1, 1, 2, `{ "\"key1\"": { "\"key1b\"": "value1b" } }`),
				merged:   singleRow(1, 2, 2, `{ "\"key1\"": { "\"key1a\"": "value1a", "\"key1b\"": "value1b" } }`),
			},
			{
				name:     `nested modification`,
				ancestor: singleRow(1, 1, 1, `{ "key1": { "key1a": "value1a", "key1b": "value1b" } }`),
				left:     singleRow(1, 2, 1, `{ "key1": { "key1a": "value2a", "key1b": "value1b" } }`),
				right:    singleRow(1, 1, 2, `{ "key1": { "key1a": "value1a", "key1b": "value2b" } }`),
				merged:   singleRow(1, 2, 2, `{ "key1": { "key1a": "value2a", "key1b": "value2b" } }`),
			},
			{
				name:     `nested modification with escaped quotes in keys`,
				ancestor: singleRow(1, 1, 1, `{ "\"key1\"": { "\"key1a\"": "value1a", "\"key1b\"": "value1b" } }`),
				left:     singleRow(1, 2, 1, `{ "\"key1\"": { "\"key1a\"": "value2a", "\"key1b\"": "value1b" } }`),
				right:    singleRow(1, 1, 2, `{ "\"key1\"": { "\"key1a\"": "value1a", "\"key1b\"": "value2b" } }`),
				merged:   singleRow(1, 2, 2, `{ "\"key1\"": { "\"key1a\"": "value2a", "\"key1b\"": "value2b" } }`),
			},
			{
				name:     `nested deletion`,
				ancestor: singleRow(1, 1, 1, `{ "key1": { "key1a": "value1a", "key1b": "value1b" } }`),
				left:     singleRow(1, 2, 1, `{ "key1": { "key1a": "value1a" } }`),
				right:    singleRow(1, 1, 2, `{ "key1": { "key1b": "value1b" } }`),
				merged:   singleRow(1, 2, 2, `{ "key1": { } }`),
			},
			{
				name:     `nested deletion with escaped quotes in keys`,
				ancestor: singleRow(1, 1, 1, `{ "\"key1\"": { "\"key1a\"": "value1a", "\"key1b\"": "value1b" } }`),
				left:     singleRow(1, 2, 1, `{ "\"key1\"": { "\"key1a\"": "value1a" } }`),
				right:    singleRow(1, 1, 2, `{ "\"key1\"": { "\"key1b\"": "value1b" } }`),
				merged:   singleRow(1, 2, 2, `{ "\"key1\"": { } }`),
			},
			{
				name:     "complicated nested merge",
				ancestor: singleRow(1, 1, 1, `{ "removed": 1, "modified": 2, "nested": { "removed": 3, "modified": 4 } }`),
				left:     singleRow(1, 2, 1, `{ "added": 7, "modified": 2, "nested": { "removed": 3, "modified": 5 } }`),
				right:    singleRow(1, 1, 2, `{ "removed": 1, "modified": 6, "nested": { "added": 8, "modified": 4 } }`),
				merged:   singleRow(1, 2, 2, `{ "added": 7, "modified": 6, "nested": { "added": 8, "modified": 5 } }`),
			},
			{
				name:     "object with double quotes in keys",
				ancestor: singleRow(1, 1, 1, `{ "\"removed\"": 1, "\"modified\"": 2, "\"nested\"": { "\"removed\"": 3, "\"modified\"": 4 } }`),
				left:     singleRow(1, 2, 1, `{ "\"added\"": 7, "\"modified\"": 2, "\"nested\"": { "\"removed\"": 3, "\"modified\"": 5 } }`),
				right:    singleRow(1, 1, 2, `{ "\"removed\"": 1, "\"modified\"": 6, "\"nested\"": { "\"added\"": 8, "\"modified\"": 4 } }`),
				merged:   singleRow(1, 2, 2, `{ "\"added\"": 7, "\"modified\"": 6, "\"nested\"": { "\"added\"": 8, "\"modified\"": 5 } }`),
			},
			{
				name:     "changing types",
				ancestor: singleRow(1, 1, 1, `{ "key1": {}, "key2": 2 }`),
				left:     singleRow(1, 2, 1, `{ "key1": [], "key2": 2 }`),
				right:    singleRow(1, 1, 2, `{ "key1": {}, "key2": true }`),
				merged:   singleRow(1, 2, 2, `{ "key1": [], "key2": true }`),
			},
			{
				name:         "changing types conflict",
				ancestor:     singleRow(1, 1, 1, `{ "key1": {} }`),
				left:         singleRow(1, 2, 1, `{ "key1": [] }`),
				right:        singleRow(1, 1, 2, `{ "key1": 2 }`),
				dataConflict: true,
			},
			{
				name:         "object insert and modify conflict",
				ancestor:     singleRow(1, 1, 1, `{ "key1": {} }`),
				left:         singleRow(1, 2, 1, `{ "key1": { "key2": 2 } }`),
				right:        singleRow(1, 1, 2, `{ "key1": 2 }`),
				dataConflict: true,
			},
			{
				name:         "object insert and delete conflict",
				ancestor:     singleRow(1, 1, 1, `{ "key1": {} }`),
				left:         singleRow(1, 2, 1, `{ "key1": { "key2": 2 } }`),
				right:        singleRow(1, 1, 2, `{ }`),
				dataConflict: true,
			},
			{
				name:         "changing arrays conflict",
				ancestor:     singleRow(1, 1, 1, `{ "key1": [1] }`),
				left:         singleRow(1, 2, 1, `{ "key1": [1, 1] }`),
				right:        singleRow(1, 1, 2, `{ "key1": [] }`),
				dataConflict: true,
			},
			{
				// Which array element should go first?
				// We avoid making assumptions and flag this as a conflict.
				name:         "object inside array conflict",
				ancestor:     singleRow(1, 1, 1, `{ "key1": [ { } ] }`),
				left:         singleRow(1, 2, 1, `{ "key1": [ { "key2": "value2" } ] }`),
				right:        singleRow(1, 1, 2, `{ "key1": [ { "key3": "value3" } ] }`),
				dataConflict: true,
			},
			{
				// Did the left branch overwrite the first value in the array?
				// Or did it remove the last value and insert at the beginning?
				// Did the right branch overwrite the second value in the array?
				// Or did it remove the first value and insert at the end?
				// Diffs on arrays are ambiguous. We avoid making assumptions and flag this as a conflict.
				name:         "parallel array modification",
				ancestor:     singleRow(1, 1, 1, `{ "key1": [ 1, 1 ] }`),
				left:         singleRow(1, 2, 1, `{ "key1": [ 2, 1 ] }`),
				right:        singleRow(1, 1, 2, `{ "key1": [ 1, 2 ] }`),
				dataConflict: true,
			},
		},
	},
}

func testSchemaMerge(t *testing.T, tests []schemaMergeTest) {
	t.Run("merge left to right", func(t *testing.T) {
		testSchemaMergeHelper(t, tests, false)
	})
	t.Run("merge right to left", func(t *testing.T) {
		testSchemaMergeHelper(t, tests, true)
	})
}

func testSchemaMergeHelper(t *testing.T, tests []schemaMergeTest, flipSides bool) {
	for _, test := range tests {
		if flipSides {
			tmp := test.left
			test.left = test.right
			test.right = tmp
			for i, _ := range test.dataTests {
				tmp := test.dataTests[i].left
				test.dataTests[i].left = test.dataTests[i].right
				test.dataTests[i].right = tmp
			}
		}

		t.Run(test.name, func(t *testing.T) {
			runTest := func(t *testing.T, test schemaMergeTest, expectDataConflict bool, expConstraintViolations []constraintViolation) {
				a, l, r, m := setupSchemaMergeTest(t, test)

				ctx := context.Background()
				var mo merge.MergeOpts
				var eo editor.Options
				eo = eo.WithDeaf(editor.NewInMemDeaf(a.VRW()))
				// attempt merge before skipping to assert no panics
				result, err := merge.MergeRoots(sql.NewContext(ctx), l, r, a, rootish{r}, rootish{a}, eo, mo)
				maybeSkip(t, a.VRW().Format(), test, flipSides)

				if test.conflict {
					// TODO: Test the conflict error message more deeply
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					exp, err := doltdb.MapTableHashes(ctx, m)
					assert.NoError(t, err)
					act, err := doltdb.MapTableHashes(ctx, result.Root)
					assert.NoError(t, err)

					assert.Equal(t, len(exp), len(act))

					if expectDataConflict {
						foundDataConflict := false
						for name, _ := range exp {
							_, ok := act[name]
							assert.True(t, ok)
							actTbl, _, err := result.Root.GetTable(ctx, doltdb.TableName{Name: name})
							require.NoError(t, err)
							hasConflict, err := actTbl.HasConflicts(ctx)
							require.NoError(t, err)
							foundDataConflict = foundDataConflict || hasConflict
						}
						if !assert.True(t, foundDataConflict, "Expected data conflict, but didn't find one.") {
							for name, _ := range exp {
								table, _, err := result.Root.GetTable(ctx, doltdb.TableName{Name: name})
								require.NoError(t, err)
								t.Logf("table %s:", name)
								t.Log(table.DebugString(ctx, m.NodeStore()))
							}

						}
					} else {
						for name, addr := range exp {
							a, ok := act[name]
							assert.True(t, ok)

							actTbl, _, err := result.Root.GetTable(ctx, doltdb.TableName{Name: name})
							require.NoError(t, err)
							hasConflict, err := actTbl.HasConflicts(ctx)
							require.NoError(t, err)
							require.False(t, hasConflict, "Unexpected data conflict")

							numConstraintViolations, err := actTbl.NumConstraintViolations(ctx)
							require.NoError(t, err)
							require.EqualValues(t, numConstraintViolations, len(expConstraintViolations))

							sch, err := actTbl.GetSchema(ctx)
							require.NoError(t, err)
							kd, vd := sch.GetMapDescriptors()

							if len(expConstraintViolations) > 0 {
								artifacts, err := actTbl.GetArtifacts(ctx)
								require.NoError(t, err)
								artifactMap := durable.ProllyMapFromArtifactIndex(artifacts)
								artifactIter, err := artifactMap.IterAllCVs(ctx)
								require.NoError(t, err)

								// value tuples encoded in ConstraintViolationMeta may
								// violate the not null constraints assumed by fixed access
								kd = kd.WithoutFixedAccess()
								vd = vd.WithoutFixedAccess()
								for _, expectedViolation := range expConstraintViolations {
									violationType, key, value, err := merge.NextConstraintViolation(ctx, artifactIter, kd, vd, artifactMap.NodeStore())
									require.NoError(t, err)
									require.EqualValues(t, expectedViolation.violationType, violationType)
									require.EqualValues(t, expectedViolation.key, key)
									require.EqualValues(t, expectedViolation.value, value)
								}
							} else {
								if addr != a {
									expTbl, _, err := m.GetTable(ctx, doltdb.TableName{Name: name})
									require.NoError(t, err)
									expSchema, err := expTbl.GetSchema(ctx)
									require.NoError(t, err)
									expRowDataHash, err := expTbl.GetRowDataHash(ctx)
									require.NoError(t, err)
									actRowDataHash, err := actTbl.GetRowDataHash(ctx)
									require.NoError(t, err)
									if !expSchema.GetKeyDescriptor().Equals(kd) {
										t.Fatal("Primary key descriptors unequal")
									}
									if !expSchema.GetValueDescriptor().Equals(vd) {
										t.Fatal("Value descriptors unequal")
									}
									if expRowDataHash != actRowDataHash {
										t.Error("Rows unequal")
										t.Logf("expected rows: %s", expTbl.DebugString(ctx, m.NodeStore()))
										t.Logf("actual rows: %s", actTbl.DebugString(ctx, m.NodeStore()))
									}
									expIndexSet, err := expTbl.GetIndexSet(ctx)
									require.NoError(t, err)
									actIndexSet, err := actTbl.GetIndexSet(ctx)
									require.NoError(t, err)
									expSchema.Indexes().Iter(func(index schema.Index) (stop bool, err error) {
										expIndex, err := expIndexSet.GetIndex(ctx, expSchema, nil, index.Name())
										require.NoError(t, err)
										actIndex, err := actIndexSet.GetIndex(ctx, expSchema, nil, index.Name())
										require.NoError(t, err)
										expIndexHash, err := expIndex.HashOf()
										require.NoError(t, err)
										actIndexHash, err := actIndex.HashOf()
										require.NoError(t, err)
										if expIndexHash != actIndexHash {
											t.Errorf("Index %s unequal", index.Name())
											t.Logf("expected rows: %s", expIndex.DebugString(ctx, m.NodeStore(), expSchema))
											t.Logf("actual rows: %s", actIndex.DebugString(ctx, m.NodeStore(), expSchema))
										}
										return false, nil
									})

								}
							}
						}
					}
				}
			}
			t.Run("test schema merge", func(t *testing.T) {
				runTest(t, test, false, nil)
			})
			for _, data := range test.dataTests {
				// Copy the test so that the values from one data test don't affect subsequent data tests.
				dataTest := test
				newLeft := *test.left
				newRight := *test.right
				dataTest.left = &newLeft
				dataTest.right = &newRight
				dataTest.ancestor.rows = data.ancestor
				dataTest.left.rows = data.left
				dataTest.right.rows = data.right
				dataTest.merged.rows = data.merged
				dataTest.skipNewFmt = dataTest.skipNewFmt || data.skip
				dataTest.skipFlipOnNewFormat = dataTest.skipFlipOnNewFormat || data.skipFlip
				t.Run(data.name, func(t *testing.T) {
					if data.skip {
						t.Skip()
					}
					runTest(t, dataTest, data.dataConflict, data.constraintViolations)
				})
			}
		})
	}
}

func setupSchemaMergeTest(t *testing.T, test schemaMergeTest) (anc, left, right, merged doltdb.RootValue) {
	denv := dtestutils.CreateTestEnv()
	var eo editor.Options
	eo = eo.WithDeaf(editor.NewInMemDeaf(denv.DoltDB.ValueReadWriter()))
	anc = makeRootWithTable(t, denv.DoltDB, eo, test.ancestor)
	assert.NotNil(t, anc)
	if test.left != nil {
		left = makeRootWithTable(t, denv.DoltDB, eo, *test.left)
		assert.NotNil(t, left)
	} else {
		left = makeEmptyRoot(t, denv.DoltDB, eo)
	}
	if test.right != nil {
		right = makeRootWithTable(t, denv.DoltDB, eo, *test.right)
		assert.NotNil(t, right)
	} else {
		right = makeEmptyRoot(t, denv.DoltDB, eo)
	}
	if !test.conflict {
		merged = makeRootWithTable(t, denv.DoltDB, eo, test.merged)
		assert.NotNil(t, merged)
	}
	return
}

func maybeSkip(t *testing.T, nbf *types.NomsBinFormat, test schemaMergeTest, flipSides bool) {
	if types.IsFormat_DOLT(nbf) {
		if test.skipNewFmt || flipSides && test.skipFlipOnNewFormat {
			t.Skip()
		}
	} else {
		if test.skipOldFmt || flipSides && test.skipFlipOnOldFormat {
			t.Skip()
		}
	}
}

func tbl(ns namedSchema, rows ...sql.Row) *table {
	return &table{ns: ns, rows: rows}
}

func sch(definition string) namedSchema {
	denv := dtestutils.CreateTestEnv()
	vrw := denv.DoltDB.ValueReadWriter()
	ns := denv.DoltDB.NodeStore()
	ctx := context.Background()
	root, _ := doltdb.EmptyRootValue(ctx, vrw, ns)
	eng, dbName, _ := engine.NewSqlEngineForEnv(ctx, denv)
	sqlCtx, _ := eng.NewDefaultContext(ctx)
	sqlCtx.SetCurrentDatabase(dbName)
	// TODO: ParseCreateTableStatement silently drops any indexes or check constraints in the definition
	name, s, err := sqlutil.ParseCreateTableStatement(sqlCtx, root, eng.GetUnderlyingEngine(), definition)
	if err != nil {
		panic(err)
	}
	return namedSchema{name: name, sch: s, create: definition}
}

func row(values ...any) sql.Row {
	return sql.NewRow(values...)
}

func singleRow(values ...any) []sql.Row {
	return []sql.Row{row(values...)}
}
func makeEmptyRoot(t *testing.T, ddb *doltdb.DoltDB, eo editor.Options) doltdb.RootValue {
	ctx := context.Background()
	wsr, err := ref.WorkingSetRefForHead(ref.NewBranchRef("main"))
	require.NoError(t, err)
	ws, err := ddb.ResolveWorkingSet(ctx, wsr)
	require.NoError(t, err)

	gst, err := dsess.NewAutoIncrementTracker(ctx, "dolt", ws)
	require.NoError(t, err)
	sess := writer.NewWriteSession(ddb.Format(), ws, gst, eo)

	ws, err = sess.Flush(sql.NewContext(ctx))
	require.NoError(t, err)
	return ws.WorkingRoot()
}

func makeRootWithTable(t *testing.T, ddb *doltdb.DoltDB, eo editor.Options, tbl table) doltdb.RootValue {
	ctx := context.Background()
	wsr, err := ref.WorkingSetRefForHead(ref.NewBranchRef("main"))
	require.NoError(t, err)
	ws, err := ddb.ResolveWorkingSet(ctx, wsr)
	require.NoError(t, err)
	dt, err := doltdb.NewEmptyTable(ctx, ddb.ValueReadWriter(), ddb.NodeStore(), tbl.ns.sch)
	require.NoError(t, err)
	root, err := ws.WorkingRoot().PutTable(ctx, doltdb.TableName{Name: tbl.ns.name}, dt)
	require.NoError(t, err)
	ws = ws.WithWorkingRoot(root)

	gst, err := dsess.NewAutoIncrementTracker(ctx, "dolt", ws)
	require.NoError(t, err)
	noop := func(ctx *sql.Context, dbName string, root doltdb.RootValue) (err error) { return }
	sess := writer.NewWriteSession(ddb.Format(), ws, gst, eo)
	wr, err := sess.GetTableWriter(sql.NewContext(ctx), doltdb.TableName{Name: tbl.ns.name}, "test", noop)
	require.NoError(t, err)

	sctx := sql.NewEmptyContext()
	for _, r := range tbl.rows {
		err = wr.Insert(sctx, r)
		assert.NoError(t, err)
	}
	ws, err = sess.Flush(sql.NewContext(ctx))
	require.NoError(t, err)
	return ws.WorkingRoot()
}

type rootish struct {
	rv doltdb.RootValue
}

func (r rootish) ResolveRootValue(ctx context.Context) (doltdb.RootValue, error) {
	return r.rv, nil
}

func (r rootish) HashOf() (hash.Hash, error) {
	return hash.Hash{}, nil
}
