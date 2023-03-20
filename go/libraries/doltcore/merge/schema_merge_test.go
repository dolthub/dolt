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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
)

type schemaMergeTest struct {
	name        string
	ancestor    table
	left, right table
	result      table
	conflict    bool
}

type table struct {
	schema schema.Schema
	rows   []sql.Row
}

// TestMergeSchemas are schema merge integration tests from 2023
func TestSchemaMerge(t *testing.T) {
	t.Run("simple merge tests", func(t *testing.T) {
		testSchemaMerge(t, simpleMergeTests)
	})
	t.Run("simple conflict tests", func(t *testing.T) {
		testSchemaMerge(t, simpleConflictTests)
	})
	t.Run("column type change tests", func(t *testing.T) {
		testSchemaMerge(t, typeChangeTests)
	})
	t.Run("primary key change tests", func(t *testing.T) {
		testSchemaMerge(t, keyChangeTests)
	})
}

var simpleMergeTests = []schemaMergeTest{
	{
		name:     "no schema changes",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY)"), row(1)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY)"), row(1)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY)"), row(1)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY)"), row(1)),
	},
	{
		name:     "independent column adds",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY)              "), row(1)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)       "), row(1, 2)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int)       "), row(1, 3)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)"), row(1, 2, 3)),
	},
	{
		name:     "independent column drops",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)"), row(1, 2, 3)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)       "), row(1, 2)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int)       "), row(1, 3)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY)              "), row(1)),
	},
	{
		name:     "convergent column adds",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, nil)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, nil)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, nil)),
	},
	{
		name:     "convergent column drops",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, 2)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1)),
	},
	{
		name:     "independent index adds",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float)                     ")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX (a))          ")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX (b))          ")),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX (a), INDEX(b))")),
	},
	{
		name:     "independent composite index adds",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float)                            ")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX (a, b))              ")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX (b, a))              ")),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX (a, b), INDEX (b, a))")),
	},
	{
		name:     "independent index drops",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX (a), INDEX (b))")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX (a))           ")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX (b))           ")),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float)                      ")),
	},
}

var simpleConflictTests = []schemaMergeTest{
	{
		name:     "conflicting column adds",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY)                "), row(1)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int NULL)    "), row(1, 2)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int NOT NULL)"), row(1, 2)),
		conflict: true,
	},
	{
		name:     "conflicting index adds: same name and columns, different constraints",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float)                     ")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX idx (a))          ")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, UNIQUE INDEX idx (a))          ")),
		conflict: true,
	},
	{
		name:     "conflicting index adds: same column different names",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float)                     ")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX a_idx (a))          ")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX key_a (a))          ")),
		// todo: is it allowed to define multiple indexes over the same column?
		conflict: true,
	},
	{
		name:     "conflicting index adds: same name different definitions",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float)                     ")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX idx (a))          ")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX idx (b))          ")),
		conflict: true,
	},
}

var typeChangeTests = []schemaMergeTest{
	{
		name:     "new field values from right and left sides are converted",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)          "), row(1, 2, 3)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b int)     "), row(1, "2", 3)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b char(20))     "), row(1, 2, "3")),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b char(20))"), row(1, "2", "3")),
	},
}

var keyChangeTests = []schemaMergeTest{
	{
		name:     "add a trailing primary key column on left side",
		ancestor: tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", 3.0)),
		left:     tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a, b))"), row(1, "2", 3.0)),
		right:    tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", 3.0)),
		result:   tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a, b))"), row(1, "2", 3.0)),
	},
	{
		name:     "add a trailing primary key column on right side",
		ancestor: tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", 3.0)),
		left:     tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", 3.0)),
		right:    tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a, b))"), row(1, "2", 3.0)),
		result:   tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a, b))"), row(1, "2", 3.0)),
	},
	{
		name:     "add a leading primary key column on left side",
		ancestor: tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", 3.0)),
		left:     tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (b, a))"), row(1, "2", 3.0)),
		right:    tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", 3.0)),
		result:   tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (b, a))"), row(1, "2", 3.0)),
	},
	{
		name:     "add a leading primary key column on right side",
		ancestor: tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", 3.0)),
		left:     tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", 3.0)),
		right:    tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (b, a))"), row(1, "2", 3.0)),
		result:   tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (b, a))"), row(1, "2", 3.0)),
	},
	{
		name:     "add primary key columns at different key positions on left and right sides",
		ancestor: tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", 3.0)),
		left:     tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (b, a))"), row(1, "2", 3.0)),
		right:    tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (b, a))"), row(1, "2", 3.0)),
		conflict: true,
	},
}

func testSchemaMerge(t *testing.T, tests []schemaMergeTest) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Skip("implement me")
		})
	}
}

func tbl(sch schema.Schema, rows ...sql.Row) table {
	return table{schema: sch, rows: rows}
}

func sch(definition string) (s schema.Schema) {
	denv := dtestutils.CreateTestEnv()
	vrw := denv.DoltDB.ValueReadWriter()
	ns := denv.DoltDB.NodeStore()
	ctx := context.Background()
	root, _ := doltdb.EmptyRootValue(ctx, vrw, ns)
	_, s, _ = sqlutil.ParseCreateTableStatement(ctx, root, definition)
	return
}

func row(values ...any) sql.Row {
	return sql.NewRow(values...)
}
