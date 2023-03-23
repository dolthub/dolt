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
	t.Run("column add/drop tests", func(t *testing.T) {
		testSchemaMerge(t, columnAddDropTests)
	})
	t.Run("column default tests", func(t *testing.T) {
		testSchemaMerge(t, columnDefaultTests)
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
}

var columnAddDropTests = []schemaMergeTest{
	{
		name:     "no schema changes",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY)"), row(1)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY)"), row(1)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY)"), row(1)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY)"), row(1)),
	},
	// one side changes columns
	{
		name:     "left side column add",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, 2)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, 2)),
	},
	{
		name:     "left side column drop",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, 2)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, 2)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1)),
	},
	{
		name:     "right side column add",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, 2)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, 2)),
	},
	{
		name:     "right side column drop",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, 2)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, 2)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1)),
	},
	// both sides change columns
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
		name:     "convergent column adds, independent drops",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)"), row(1, 2, 3)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int, c int)"), row(1, 3, 4)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, c int)"), row(1, 2, 4)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, c int)       "), row(1, 4)),
	},
	{
		name:     "convergent column drops, independent adds",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)       "), row(1, 2)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int)       "), row(1, 3)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, c int)       "), row(1, 4)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int, c int)"), row(1, 3, 4)),
	},
	// one side changes columns, the other inserts rows
	{
		name:     "left side column add, right side insert row",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, 2)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1), row(11)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, 2), row(11, nil)),
	},
	{
		name:     "left side column drop, right side insert row",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, 2)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, 2), row(11, 22)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1), row(11)),
	},
	{
		name:     "right side column add, left side insert row",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1), row(11)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, 2)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, 2), row(11, nil)),
	},
	{
		name:     "right side column drop, left side insert row",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, 2)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, 2), row(11, 22)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1), row(11)),
	},
	// both sides change columns and insert rows
	{
		name:     "independent column adds, both sides insert independent rows",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY)              "), row(1)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)       "), row(1, 2), row(12, 22)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int)       "), row(1, 3), row(13, 33)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)"), row(1, 2, 3), row(12, 22, nil), row(13, nil, 33)),
	},
	{
		name:     "independent column drops, both sides insert independent rows",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)"), row(1, 2, 3)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)       "), row(1, 2), row(12, 22)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int)       "), row(1, 3), row(13, 33)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY)              "), row(1), row(12), row(13)),
	},
	{
		name:     "convergent column adds, both sides insert independent rows",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, nil), row(12, 22)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, nil), row(13, 33)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, nil), row(12, 22), row(13, 33)),
	},
	{
		name:     "convergent column drops, both sides insert independent rows",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)"), row(1, 2)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1), row(12)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1), row(13)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY)       "), row(1), row(12), row(13)),
	},
	{
		name:     "independent column adds, both sides insert same row",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY)              "), row(1)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)       "), row(1, 2), row(12, 22)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int)       "), row(1, 3), row(12, 33)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)"), row(1, 2, 3), row(12, 22, 33)),
	},
	{
		name:     "independent column drops, both sides insert same row",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)"), row(1, 2, 3)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)       "), row(1, 2), row(12, 22)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int)       "), row(1, 3), row(12, 33)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY)              "), row(1), row(12)),
	},
}

var columnDefaultTests = []schemaMergeTest{
	{
		name:     "left side add default",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)           ")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 42)")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)           ")),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 42)")),
	},
	{
		name:     "left side drop default",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 42)")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)           ")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 42)")),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)           ")),
	},
	{
		name:     "right side add default",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)           ")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)           ")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 42)")),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 42)")),
	},
	{
		name:     "right side drop default",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 42)")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 42)")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)           ")),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)           ")),
	},
	{
		name:     "convergent add",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)           ")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 42)")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 42)")),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 42)")),
	},
	{
		name:     "convergent drop",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 42)")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)           ")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)           ")),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int)           ")),
	},
	// one side changes columns, the other inserts rows
	{
		name:     "left side column add, right side insert row",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY)                  "), row(1)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 42)"), row(1, 42)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY)                  "), row(1), row(12)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 42)"), row(1, 42), row(12, 42)),
	},
	{
		name:     "right side column add, left side insert row",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY)                  "), row(1)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY)                  "), row(1), row(11)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 42)"), row(1, 42)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 42)"), row(1, 42), row(11, 42)),
	},
	// both sides change columns and insert rows
	{
		name:     "independent column adds, both sides insert independent rows",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY)                                    "), row(1)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 19)                  "), row(1, 2), row(12, 19)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, b int DEFAULT 17)                  "), row(1, 3), row(13, 17)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 19, b int DEFAULT 17)"), row(1, 2, 3), row(12, 22, 17), row(13, 19, 33)),
	},
	{
		name:     "convergent column adds, both sides insert independent rows",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY)                  "), row(1)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 19)"), row(1, 19)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 19)"), row(1, 19)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 19)"), row(1, 19)),
	},
}

var columnReorderingTests = []schemaMergeTest{}

var typeChangeTests = []schemaMergeTest{
	{
		name:     "modify column type on the left side",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)     "), row(1, 2, 3)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b int)"), row(1, "2", 3)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)     "), row(1, 2, 3)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b int)"), row(1, "2", 3)),
	},
	{
		name:     "modify column type on the right side",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)     "), row(1, 2, 3)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)     "), row(1, 2, 3)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b int)"), row(1, "2", 3)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b int)"), row(1, "2", 3)),
	},
	{
		name:     "independently modify column type on the both sides",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)          "), row(1, 2, 3)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b int)     "), row(1, "2", 3)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b char(20))     "), row(1, 2, "3")),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b char(20))"), row(1, "2", "3")),
	},
	{
		name:     "convergently modify column type on the both sides",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)     "), row(1, 2, 3)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b int)"), row(1, "2", 3)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b int)"), row(1, "2", 3)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b int)"), row(1, "2", 3)),
	},
	// column changes one side, data changes other side
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
		name:     "remove a trailing primary key column on left side",
		ancestor: tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a, b))"), row(1, "2", 3.0)),
		left:     tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", 3.0)),
		right:    tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a, b))"), row(1, "2", 3.0)),
		result:   tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", 3.0)),
	},
	{
		name:     "remove a trailing primary key column on right side",
		ancestor: tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a, b))"), row(1, "2", 3.0)),
		left:     tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a, b))"), row(1, "2", 3.0)),
		right:    tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", 3.0)),
		result:   tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", 3.0)),
	},
	{
		name:     "remove a trailing primary key column on both sides",
		ancestor: tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a, b))"), row(1, "2", 3.0)),
		left:     tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", 3.0)),
		right:    tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", 3.0)),
		result:   tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", 3.0)),
	},
	{
		name:     "remove a leading primary key column on left side",
		ancestor: tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (b, a))"), row(1, "2", 3.0)),
		left:     tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", 3.0)),
		right:    tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (b, a))"), row(1, "2", 3.0)),
		result:   tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", 3.0)),
	},
	{
		name:     "remove a leading primary key column on right side",
		ancestor: tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (b, a))"), row(1, "2", 3.0)),
		left:     tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (b, a))"), row(1, "2", 3.0)),
		right:    tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", 3.0)),
		result:   tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", 3.0)),
	},
	{
		name:     "remove a leading primary key column on both sides",
		ancestor: tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (b, a))"), row(1, "2", 3.0)),
		left:     tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", 3.0)),
		right:    tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", 3.0)),
		result:   tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", 3.0)),
	},
	{
		name:     "convert left side to a keyless table",
		ancestor: tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))"), row(1, "2", 3.0)),
		left:     tbl(sch("CREATE TABLE t (a int, b char(20), c float)                 "), row(1, "2", 3.0)),
		right:    tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))"), row(1, "2", 3.0)),
		result:   tbl(sch("CREATE TABLE t (a int, b char(20), c float)                 "), row(1, "2", 3.0)),
	},
	{
		name:     "convert left side to a keyless table",
		ancestor: tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))"), row(1, "2", 3.0)),
		left:     tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))"), row(1, "2", 3.0)),
		right:    tbl(sch("CREATE TABLE t (a int, b char(20), c float)                 "), row(1, "2", 3.0)),
		result:   tbl(sch("CREATE TABLE t (a int, b char(20), c float)                 "), row(1, "2", 3.0)),
	},
	{
		name:     "convert both sides to keyless tables",
		ancestor: tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))"), row(1, "2", 3.0)),
		left:     tbl(sch("CREATE TABLE t (a int, b char(20), c float)                 "), row(1, "2", 3.0)),
		right:    tbl(sch("CREATE TABLE t (a int, b char(20), c float)                 "), row(1, "2", 3.0)),
		result:   tbl(sch("CREATE TABLE t (a int, b char(20), c float)                 "), row(1, "2", 3.0)),
	},
}

var secondaryIndexTests = []schemaMergeTest{
	{
		name:     "independent index adds",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float)                     "), row(1, "2", 3.0)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX (a))          "), row(1, "2", 3.0)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX (b))          "), row(1, "2", 3.0)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX (a), INDEX(b))"), row(1, "2", 3.0)),
	},
	{
		name:     "independent composite index adds",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float)                            "), row(1, "2", 3.0)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX (a, b))              "), row(1, "2", 3.0)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX (b, a))              "), row(1, "2", 3.0)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX (a, b), INDEX (b, a))"), row(1, "2", 3.0)),
	},
	{
		name:     "independent index drops",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX (a), INDEX (b))"), row(1, "2", 3.0)),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX (a))           "), row(1, "2", 3.0)),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX (b))           "), row(1, "2", 3.0)),
		result:   tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float)                      "), row(1, "2", 3.0)),
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
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float)                      ")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX idx (a))       ")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, UNIQUE INDEX idx (a))")),
		conflict: true,
	},
	{
		name:     "conflicting index adds: same column different names",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float)                 ")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX a_idx (a))")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX key_a (a))")),
		// todo: is it allowed to define multiple indexes over the same column?
		conflict: true,
	},
	{
		name:     "conflicting index adds: same name different definitions",
		ancestor: tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float)               ")),
		left:     tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX idx (a))")),
		right:    tbl(sch("CREATE TABLE t (id int PRIMARY KEY, a char(20), b float, INDEX idx (b))")),
		conflict: true,
	},
	{
		name:     "add primary key columns at different key positions on left and right sides",
		ancestor: tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", 3.0)),
		left:     tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a, b))"), row(1, "2", 3.0)),
		right:    tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (b, a))"), row(1, "2", 3.0)),
		conflict: true,
	},
	{
		name:     "remove different primary key columns on left and right sides",
		ancestor: tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a, b))"), row(1, "2", 3.0)),
		left:     tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (a))   "), row(1, "2", 3.0)),
		right:    tbl(sch("CREATE TABLE t (a int, b char(20), c float, PRIMARY KEY (b))   "), row(1, "2", 3.0)),
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

func sch(definition string) schema.Schema {
	denv := dtestutils.CreateTestEnv()
	vrw := denv.DoltDB.ValueReadWriter()
	ns := denv.DoltDB.NodeStore()
	ctx := context.Background()
	root, _ := doltdb.EmptyRootValue(ctx, vrw, ns)
	_, s, err := sqlutil.ParseCreateTableStatement(ctx, root, definition)
	if err != nil {
		panic(err)
	}
	return s
}

func row(values ...any) sql.Row {
	return sql.NewRow(values...)
}
