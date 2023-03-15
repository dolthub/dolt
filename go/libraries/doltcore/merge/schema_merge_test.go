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
	"testing"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"

	"github.com/dolthub/go-mysql-server/sql"
)

// TestMergeSchemas are schema merge integration tests from 2023
func TestSchemaMerge(t *testing.T) {
	t.Run("column mapping tests", func(t *testing.T) {
		testSchemaMerge(t, colMappingTests)
	})
	t.Run("column type change tests", func(t *testing.T) {
		testSchemaMerge(t, colMappingTests)
	})
	t.Run("primary key change tests", func(t *testing.T) {
		testSchemaMerge(t, colMappingTests)
	})
}

var colMappingTests = []schemaMergeTest{
	{
		name:     "smoke test",
		ancestor: tbl(sch("CREATE TABLE t (pk int PRIMARY KEY)"), row(1), row(2)),
		left:     tbl(sch("CREATE TABLE t (pk int PRIMARY KEY)"), row(1), row(2)),
		right:    tbl(sch("CREATE TABLE t (pk int PRIMARY KEY)"), row(1), row(2)),
		result:   tbl(sch("CREATE TABLE t (pk int PRIMARY KEY)"), row(1), row(2)),
	},
}

var typeChangeTests = []schemaMergeTest{}

var keyChangeTests = []schemaMergeTest{}

func testSchemaMerge(t *testing.T, tests []schemaMergeTest) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Skip("implement me")
		})
	}
}

type schemaMergeTest struct {
	name        string
	ancestor    table
	left, right table
	result      table
}

type table struct {
	schema schema.Schema
	rows   []sql.Row
}

func tbl(sch schema.Schema, rows ...sql.Row) table {
	return table{schema: sch, rows: rows}
}

func sch(definition string) (s schema.Schema) {
	return // todo: implement me
}

func row(values ...any) sql.Row {
	return sql.NewRow(values...)
}
