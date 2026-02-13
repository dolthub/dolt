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

package sqlfmt_test

import (
	"testing"

	_ "github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlfmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
)

const expectedDropSql = "DROP TABLE `table_name`;"
const expectedDropIfExistsSql = "DROP TABLE IF EXISTS `table_name`;"
const expectedAddColSql = "ALTER TABLE `table_name` ADD `c0` BIGINT NOT NULL;"
const expectedDropColSql = "ALTER TABLE `table_name` DROP `first_name`;"
const expectedRenameColSql = "ALTER TABLE `table_name` RENAME COLUMN `id` TO `pk`;"
const expectedRenameTableSql = "RENAME TABLE `table_name` TO `new_table_name`;"

func TestTableDropStmt(t *testing.T) {
	stmt := sqlfmt.DropTableStmt(sql.DefaultMySQLSchemaFormatter, "table_name")

	assert.Equal(t, expectedDropSql, stmt)
}

func TestTableDropIfExistsStmt(t *testing.T) {
	stmt := sqlfmt.DropTableIfExistsStmt(sql.DefaultMySQLSchemaFormatter, "table_name")

	assert.Equal(t, expectedDropIfExistsSql, stmt)
}

func TestAlterTableAddColStmt(t *testing.T) {
	newColDef := "`c0` BIGINT NOT NULL"
	stmt := sqlfmt.AlterTableAddColStmt(sql.DefaultMySQLSchemaFormatter, "table_name", newColDef)

	assert.Equal(t, expectedAddColSql, stmt)
}

func TestAlterTableDropColStmt(t *testing.T) {
	stmt := sqlfmt.AlterTableDropColStmt(sql.DefaultMySQLSchemaFormatter, "table_name", "first_name")

	assert.Equal(t, expectedDropColSql, stmt)
}

func TestAlterTableRenameColStmt(t *testing.T) {
	stmt := sqlfmt.AlterTableRenameColStmt(sql.DefaultMySQLSchemaFormatter, "table_name", "id", "pk")

	assert.Equal(t, expectedRenameColSql, stmt)
}

func TestRenameTableStmt(t *testing.T) {
	stmt := sqlfmt.RenameTableStmt(sql.DefaultMySQLSchemaFormatter, "table_name", "new_table_name")

	assert.Equal(t, expectedRenameTableSql, stmt)
}
