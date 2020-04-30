// Copyright 2020 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sql/sqltestutil"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const expectedCreateSQL = "CREATE TABLE `table_name` (\n" +
	"  `id` BIGINT NOT NULL COMMENT 'tag:200',\n" +
	"  `first_name` LONGTEXT NOT NULL COMMENT 'tag:201',\n" +
	"  `last_name` LONGTEXT NOT NULL COMMENT 'tag:202',\n" +
	"  `is_married` BIT(1) COMMENT 'tag:203',\n" +
	"  `age` BIGINT COMMENT 'tag:204',\n" +
	"  `rating` DOUBLE COMMENT 'tag:206',\n" +
	"  `uuid` CHAR(36) CHARACTER SET ascii COLLATE ascii_bin COMMENT 'tag:207',\n" +
	"  `num_episodes` BIGINT UNSIGNED COMMENT 'tag:208',\n" +
	"  PRIMARY KEY (`id`)\n" +
	");"
const expectedDropSql = "DROP TABLE `table_name`;"
const expectedDropIfExistsSql = "DROP TABLE IF EXISTS `table_name`;"
const expectedAddColSql = "ALTER TABLE `table_name` ADD `c0` BIGINT NOT NULL COMMENT 'tag:9';"
const expectedDropColSql = "ALTER TABLE `table_name` DROP `first_name`;"
const expectedRenameColSql = "ALTER TABLE `table_name` RENAME COLUMN `id` TO `pk`;"
const expectedRenameTableSql = "RENAME TABLE `table_name` TO `new_table_name`;"

type test struct {
	name           string
	row            row.Row
	sch            schema.Schema
	expectedOutput string
}

func TestSchemaAsCreateStmt(t *testing.T) {
	tSchema := sqltestutil.PeopleTestSchema
	stmt := SchemaAsCreateStmt("table_name", tSchema)

	assert.Equal(t, expectedCreateSQL, stmt)
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
	newColDef := "`c0` BIGINT NOT NULL COMMENT 'tag:9'"
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
			name: "simple row",
			row:  dtestutils.NewTypedRow(id, "some guy", 100, false, strPointer("normie")),
			sch:  dtestutils.TypedSchema,
			expectedOutput: "INSERT INTO `people` (`id`,`name`,`age`,`is_married`,`title`) " +
				`VALUES ("00000000-0000-0000-0000-000000000000","some guy",100,FALSE,"normie");`,
		},
		{
			name: "embedded quotes",
			row:  dtestutils.NewTypedRow(id, `It's "Mister Perfect" to you`, 100, false, strPointer("normie")),
			sch:  dtestutils.TypedSchema,
			expectedOutput: "INSERT INTO `people` (`id`,`name`,`age`,`is_married`,`title`) " +
				`VALUES ("00000000-0000-0000-0000-000000000000","It's \"Mister Perfect\" to you",100,FALSE,"normie");`,
		},
		{
			name: "null values",
			row:  dtestutils.NewTypedRow(id, "some guy", 100, false, nil),
			sch:  dtestutils.TypedSchema,
			expectedOutput: "INSERT INTO `people` (`id`,`name`,`age`,`is_married`,`title`) " +
				`VALUES ("00000000-0000-0000-0000-000000000000","some guy",100,FALSE,NULL);`,
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

	tests := []test{
		{
			name:           "simple row",
			row:            dtestutils.NewTypedRow(id, "some guy", 100, false, strPointer("normie")),
			sch:            dtestutils.TypedSchema,
			expectedOutput: "UPDATE `people` SET `name`=\"some guy\",`age`=100,`is_married`=FALSE,`title`=\"normie\" WHERE (`id`=\"00000000-0000-0000-0000-000000000000\");",
		},
		{
			name:           "embedded quotes",
			row:            dtestutils.NewTypedRow(id, `It's "Mister Perfect" to you`, 100, false, strPointer("normie")),
			sch:            dtestutils.TypedSchema,
			expectedOutput: "UPDATE `people` SET `name`=\"It's \\\"Mister Perfect\\\" to you\",`age`=100,`is_married`=FALSE,`title`=\"normie\" WHERE (`id`=\"00000000-0000-0000-0000-000000000000\");",
		},
		{
			name:           "null values",
			row:            dtestutils.NewTypedRow(id, "some guy", 100, false, nil),
			sch:            dtestutils.TypedSchema,
			expectedOutput: "UPDATE `people` SET `name`=\"some guy\",`age`=100,`is_married`=FALSE,`title`=NULL WHERE (`id`=\"00000000-0000-0000-0000-000000000000\");",
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
		expectedOutput: "UPDATE `people` SET `a name with spaces`=-3.14 WHERE (`anotherColumn`=-42);",
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := RowAsUpdateStmt(tt.row, tableName, tt.sch)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedOutput, stmt)
		})
	}
}

func strPointer(s string) *string {
	return &s
}
