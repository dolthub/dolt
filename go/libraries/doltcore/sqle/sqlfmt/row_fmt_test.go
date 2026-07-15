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

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	_ "github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlfmt"
)

const expectedDropSql = "DROP TABLE `table_name`;"
const expectedDropIfExistsSql = "DROP TABLE IF EXISTS `table_name`;"
const expectedAddColSql = "ALTER TABLE `table_name` ADD `c0` BIGINT NOT NULL;"
const expectedDropColSql = "ALTER TABLE `table_name` DROP `first_name`;"
const expectedRenameColSql = "ALTER TABLE `table_name` RENAME COLUMN `id` TO `pk`;"
const expectedRenameTableSql = "RENAME TABLE `table_name` TO `new_table_name`;"

func TestSqlRowAsInsertStmtBit(t *testing.T) {
	// See https://github.com/dolthub/dolt/issues/10132
	bit3, err := typeinfo.FromSqlType(gmstypes.MustCreateBitType(3))
	require.NoError(t, err)
	bit64, err := typeinfo.FromSqlType(gmstypes.MustCreateBitType(64))
	require.NoError(t, err)
	idCol, err := schema.NewColumnWithTypeInfo("id", 0, typeinfo.Int32Type, true, "", false, "")
	require.NoError(t, err)
	b3Col, err := schema.NewColumnWithTypeInfo("b3", 1, bit3, false, "", false, "")
	require.NoError(t, err)
	b64Col, err := schema.NewColumnWithTypeInfo("b64", 2, bit64, false, "", false, "")
	require.NoError(t, err)
	sch, err := schema.SchemaFromCols(schema.NewColCollection(idCol, b3Col, b64Col))
	require.NoError(t, err)

	ctx := sql.NewEmptyContext()
	stmt, err := sqlfmt.SqlRowAsInsertStmt(ctx, sql.Row{int32(0), uint64(0), nil}, "t", sch)
	require.NoError(t, err)
	assert.Equal(t, "INSERT INTO `t` (`id`,`b3`,`b64`) VALUES (0,0x00,NULL);", stmt)

	stmt, err = sqlfmt.SqlRowAsInsertStmt(ctx, sql.Row{int32(1), uint64(2), uint64(18446744073709551615)}, "t", sch)
	require.NoError(t, err)
	assert.Equal(t, "INSERT INTO `t` (`id`,`b3`,`b64`) VALUES (1,0x02,0xffffffffffffffff);", stmt)
}

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
