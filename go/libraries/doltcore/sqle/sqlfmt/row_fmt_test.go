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

package sqlfmt

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/types"
)

const expectedDropSql = "DROP TABLE `table_name`;"
const expectedDropIfExistsSql = "DROP TABLE IF EXISTS `table_name`;"
const expectedAddColSql = "ALTER TABLE `table_name` ADD `c0` BIGINT NOT NULL;"
const expectedDropColSql = "ALTER TABLE `table_name` DROP `first_name`;"
const expectedRenameColSql = "ALTER TABLE `table_name` RENAME COLUMN `id` TO `pk`;"
const expectedRenameTableSql = "RENAME TABLE `table_name` TO `new_table_name`;"

type test struct {
	name           string
	row            row.Row
	sch            schema.Schema
	expectedOutput string
}

type updateTest struct {
	name           string
	row            row.Row
	sch            schema.Schema
	expectedOutput string
	collDiff       *set.StrSet
}

func TestTableDropStmt(t *testing.T) {
	stmt := DropTableStmt("table_name")

	assert.Equal(t, expectedDropSql, stmt)
}

func TestTableDropIfExistsStmt(t *testing.T) {
	stmt := DropTableIfExistsStmt("table_name")

	assert.Equal(t, expectedDropIfExistsSql, stmt)
}

func TestAlterTableAddColStmt(t *testing.T) {
	newColDef := "`c0` BIGINT NOT NULL"
	stmt := AlterTableAddColStmt("table_name", newColDef)

	assert.Equal(t, expectedAddColSql, stmt)
}

func TestAlterTableDropColStmt(t *testing.T) {
	stmt := AlterTableDropColStmt("table_name", "first_name")

	assert.Equal(t, expectedDropColSql, stmt)
}

func TestAlterTableRenameColStmt(t *testing.T) {
	stmt := AlterTableRenameColStmt("table_name", "id", "pk")

	assert.Equal(t, expectedRenameColSql, stmt)
}

func TestRenameTableStmt(t *testing.T) {
	stmt := RenameTableStmt("table_name", "new_table_name")

	assert.Equal(t, expectedRenameTableSql, stmt)
}

func TestRowAsInsertStmt(t *testing.T) {
	id := uuid.MustParse("00000000-0000-0000-0000-000000000000")
	tableName := "people"

	tests := []test{
		{
			name:           "simple row",
			row:            dtestutils.NewTypedRow(id, "some guy", 100, false, strPointer("normie")),
			sch:            dtestutils.TypedSchema,
			expectedOutput: "INSERT INTO `people` (`id`,`name`,`age`,`is_married`,`title`) VALUES ('00000000-0000-0000-0000-000000000000','some guy',100,0,'normie');",
		},
		{
			name:           "embedded quotes",
			row:            dtestutils.NewTypedRow(id, `It's "Mister Perfect" to you`, 100, false, strPointer("normie")),
			sch:            dtestutils.TypedSchema,
			expectedOutput: "INSERT INTO `people` (`id`,`name`,`age`,`is_married`,`title`) VALUES ('00000000-0000-0000-0000-000000000000','It\\'s \\\"Mister Perfect\\\" to you',100,0,'normie');",
		},
		{
			name:           "null values",
			row:            dtestutils.NewTypedRow(id, "some guy", 100, false, nil),
			sch:            dtestutils.TypedSchema,
			expectedOutput: "INSERT INTO `people` (`id`,`name`,`age`,`is_married`,`title`) VALUES ('00000000-0000-0000-0000-000000000000','some guy',100,0,NULL);",
		},
	}

	trickySch := dtestutils.CreateSchema(
		schema.NewColumn("a name with spaces", 0, types.FloatKind, false),
		schema.NewColumn("anotherColumn", 1, types.IntKind, true),
	)

	tests = append(tests, test{
		name:           "negative values and columns with spaces",
		row:            dtestutils.NewRow(trickySch, types.Float(-3.14), types.Int(-42)),
		sch:            trickySch,
		expectedOutput: "INSERT INTO `people` (`a name with spaces`,`anotherColumn`) VALUES (-3.14,-42);",
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := RowAsInsertStmt(tt.row, tableName, tt.sch)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedOutput, stmt)
		})
	}
}

func TestRowAsDeleteStmt(t *testing.T) {
	tableName := "tricky"
	trickySch := dtestutils.CreateSchema(
		schema.NewColumn("anotherCol", 0, types.FloatKind, false),
		schema.NewColumn("a name with spaces", 1, types.IntKind, true),
	)

	tests := []test{
		{
			name:           "negative values and columns with spaces",
			row:            dtestutils.NewRow(trickySch, types.Float(-3.14), types.Int(-42)),
			sch:            trickySch,
			expectedOutput: "DELETE FROM `tricky` WHERE (`a name with spaces`=-42);",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := RowAsDeleteStmt(tt.row, tableName, tt.sch)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedOutput, stmt)
		})
	}
}

func TestRowAsUpdateStmt(t *testing.T) {
	id := uuid.MustParse("00000000-0000-0000-0000-000000000000")
	tableName := "people"

	tests := []updateTest{
		{
			name:           "simple row",
			row:            dtestutils.NewTypedRow(id, "some guy", 100, false, strPointer("normie")),
			sch:            dtestutils.TypedSchema,
			expectedOutput: "UPDATE `people` SET `name`='some guy',`age`=100,`is_married`=0,`title`='normie' WHERE (`id`='00000000-0000-0000-0000-000000000000');",
			collDiff:       set.NewStrSet([]string{"name", "age", "is_married", "title"}),
		},
		{
			name:           "embedded quotes",
			row:            dtestutils.NewTypedRow(id, `It's "Mister Perfect" to you`, 100, false, strPointer("normie")),
			sch:            dtestutils.TypedSchema,
			expectedOutput: "UPDATE `people` SET `name`='It\\'s \\\"Mister Perfect\\\" to you',`age`=100,`is_married`=0,`title`='normie' WHERE (`id`='00000000-0000-0000-0000-000000000000');",
			collDiff:       set.NewStrSet([]string{"name", "age", "is_married", "title"}),
		},
		{
			name:           "null values",
			row:            dtestutils.NewTypedRow(id, "some guy", 100, false, nil),
			sch:            dtestutils.TypedSchema,
			expectedOutput: "UPDATE `people` SET `name`='some guy',`age`=100,`is_married`=0,`title`=NULL WHERE (`id`='00000000-0000-0000-0000-000000000000');",
			collDiff:       set.NewStrSet([]string{"name", "age", "is_married", "title"}),
		},
		{
			name:           "partial update",
			row:            dtestutils.NewTypedRow(id, "some guy", 100, false, nil),
			sch:            dtestutils.TypedSchema,
			expectedOutput: "UPDATE `people` SET `name`='some guy' WHERE (`id`='00000000-0000-0000-0000-000000000000');",
			collDiff:       set.NewStrSet([]string{"name"}),
		},
	}

	trickySch := dtestutils.CreateSchema(
		schema.NewColumn("a name with spaces", 0, types.FloatKind, false),
		schema.NewColumn("anotherColumn", 1, types.IntKind, true),
	)

	tests = append(tests, updateTest{
		name:           "negative values and columns with spaces",
		row:            dtestutils.NewRow(trickySch, types.Float(-3.14), types.Int(-42)),
		sch:            trickySch,
		expectedOutput: "UPDATE `people` SET `a name with spaces`=-3.14 WHERE (`anotherColumn`=-42);",
		collDiff:       set.NewStrSet([]string{"a name with spaces"}),
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := RowAsUpdateStmt(tt.row, tableName, tt.sch, tt.collDiff)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedOutput, stmt)
		})
	}
}

func TestValueAsSqlString(t *testing.T) {
	tu, _ := uuid.Parse("00000000-0000-0000-0000-000000000000")

	tests := []struct {
		name string
		val  types.Value
		ti   typeinfo.TypeInfo
		exp  string
	}{
		{
			name: "bool(true)",
			val:  types.Bool(true),
			ti:   typeinfo.BoolType,
			exp:  "TRUE",
		},
		{
			name: "bool(false)",
			val:  types.Bool(false),
			ti:   typeinfo.BoolType,
			exp:  "FALSE",
		},
		{
			name: "uuid",
			val:  types.UUID(tu),
			ti:   typeinfo.UuidType,
			exp:  "'00000000-0000-0000-0000-000000000000'",
		},
		{
			name: "string",
			val:  types.String("leviosa"),
			ti:   typeinfo.StringDefaultType,
			exp:  "'leviosa'",
		},
		{
			// borrowed from vitess
			name: "escape string",
			val:  types.String("\x00'\"\b\n\r\t\x1A\\"),
			ti:   typeinfo.StringDefaultType,
			exp:  "'\\0\\'\\\"\\b\\n\\r\\t\\Z\\\\'",
		},
		// using only string and int types as an example, but includes all types
		{
			name: "NULL value for typeinfo.string types",
			val:  nil,
			ti:   typeinfo.StringDefaultType,
			exp:  "NULL",
		},
		{
			name: "NULL value for typeinfo.int types",
			val:  nil,
			ti:   typeinfo.Int64Type,
			exp:  "NULL",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			act, err := valueAsSqlString(test.ti, test.val)
			require.NoError(t, err)
			assert.Equal(t, test.exp, act)
		})
	}
}

func strPointer(s string) *string {
	return &s
}
