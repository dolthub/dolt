// Copyright 2019 Liquidata, Inc.
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

package diff

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/alterschema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sql"
)

const name = "Jeffery Williams"
const email = "meet.me@the.london"

type StringBuilderCloser struct {
	strings.Builder
}

func (*StringBuilderCloser) Close() error {
	return nil
}

func setupSchema() (context.Context, schema.Schema, *env.DoltEnv) {
	ctx := context.Background()
	sch := dtestutils.TypedSchema
	dEnv := dtestutils.CreateTestEnv()
	_ = dEnv.DoltDB.WriteEmptyRepo(ctx, name, email)
	return ctx, sch, dEnv
}

func strPointer(s string) *string {
	return &s
}

func TestSqlTableDiffAdd(t *testing.T) {
	ctx, sch, dEnv := setupSchema()

	oldRoot, _ := dEnv.WorkingRoot(ctx)
	dtestutils.CreateTestTable(t, dEnv, "addTable", sch, []row.Row{}...)
	newRoot, _ := dEnv.WorkingRoot(ctx)
	a, _, rm, _ := newRoot.TableDiff(ctx, oldRoot)
	adds, removed, renamed, _ := findRenames(ctx, newRoot, oldRoot, a, rm)
	assert.Equal(t, []string{"addTable"}, adds)
	assert.Equal(t, []string{}, removed)
	assert.Equal(t, map[string]string{}, renamed)

	var stringWr StringBuilderCloser
	_ = PrintSqlTableDiffs(ctx, newRoot, oldRoot, &stringWr)
	expectedOutput := sql.SchemaAsCreateStmt("addTable", sch) + "\n"
	assert.Equal(t, expectedOutput, stringWr.String())
}

func TestSqlTableDiffAddThenInsert(t *testing.T) {
	id := uuid.MustParse("00000000-0000-0000-0000-000000000000")
	ctx, sch, dEnv := setupSchema()

	oldRoot, _ := dEnv.WorkingRoot(ctx)
	dtestutils.CreateTestTable(t, dEnv, "addTable", sch, []row.Row{}...)
	r := dtestutils.NewTypedRow(id, "Big Billy", 77, false, strPointer("Doctor"))
	newRoot, _ := dEnv.WorkingRoot(ctx)
	newRoot, _ = dtestutils.AddRowToRoot(dEnv, ctx, newRoot, "addTable", r)
	a, _, rm, _ := newRoot.TableDiff(ctx, oldRoot)
	added, removed, renamed, _ := findRenames(ctx, newRoot, oldRoot, a, rm)
	assert.Equal(t, []string{"addTable"}, added)
	assert.Equal(t, []string{}, removed)
	assert.Equal(t, map[string]string{}, renamed)

	var stringWr StringBuilderCloser
	_ = PrintSqlTableDiffs(ctx, newRoot, oldRoot, &stringWr)
	expectedOutput := sql.SchemaAsCreateStmt("addTable", sch) + "\n"
	expectedOutput = expectedOutput +
		"INSERT INTO `addTable` (`id`,`name`,`age`,`is_married`,`title`) " +
		"VALUES (\"00000000-0000-0000-0000-000000000000\",\"Big Billy\",77,FALSE,\"Doctor\");\n"
	assert.Equal(t, expectedOutput, stringWr.String())
}

func TestSqlTableDiffsDrop(t *testing.T) {
	ctx, sch, dEnv := setupSchema()

	dtestutils.CreateTestTable(t, dEnv, "dropTable", sch, []row.Row{}...)
	oldRoot, _ := dEnv.WorkingRoot(ctx)
	newRoot, _ := oldRoot.RemoveTables(ctx, []string{"dropTable"}...)
	a, _, rm, _ := newRoot.TableDiff(ctx, oldRoot)
	added, drops, renamed, _ := findRenames(ctx, newRoot, oldRoot, a, rm)
	assert.Equal(t, []string{"dropTable"}, drops)
	assert.Equal(t, []string{}, added)
	assert.Equal(t, map[string]string{}, renamed)

	var stringWr StringBuilderCloser
	_ = PrintSqlTableDiffs(ctx, newRoot, oldRoot, &stringWr)
	expectedOutput := "DROP TABLE `dropTable`;\n"
	assert.Equal(t, expectedOutput, stringWr.String())
}

func TestSqlTableDiffRename(t *testing.T) {
	ctx, sch, dEnv := setupSchema()
	dtestutils.CreateTestTable(t, dEnv, "renameTable", sch, []row.Row{}...)
	oldRoot, _ := dEnv.WorkingRoot(ctx)
	newRoot, _ := alterschema.RenameTable(ctx, dEnv.DoltDB, oldRoot, "renameTable", "newTableName")
	a, _, rm, _ := newRoot.TableDiff(ctx, oldRoot)
	added, removed, renames, _ := findRenames(ctx, newRoot, oldRoot, a, rm)
	assert.Equal(t, map[string]string{"renameTable": "newTableName"}, renames)
	assert.Equal(t, []string{}, removed)
	assert.Equal(t, []string{}, added)

	expectedOutput := "RENAME TABLE `renameTable` TO `newTableName`;\n"
	var stringWr StringBuilderCloser
	_ = PrintSqlTableDiffs(ctx, newRoot, oldRoot, &stringWr)
	assert.Equal(t, expectedOutput, stringWr.String())
}

func TestSqlTableDiffRenameChangedTable(t *testing.T) {
	id := uuid.MustParse("00000000-0000-0000-0000-000000000000")
	ctx, sch, dEnv := setupSchema()

	dtestutils.CreateTestTable(t, dEnv, "renameTable", sch, []row.Row{}...)
	oldRoot, _ := dEnv.WorkingRoot(ctx)
	newRoot, _ := alterschema.RenameTable(ctx, dEnv.DoltDB, oldRoot, "renameTable", "newTableName")
	r := dtestutils.NewTypedRow(id, "Big Billy", 77, false, strPointer("Doctor"))
	newRoot, _ = dtestutils.AddRowToRoot(dEnv, ctx, newRoot, "newTableName", r)
	a, _, rm, _ := newRoot.TableDiff(ctx, oldRoot)
	added, removed, renamed, _ := findRenames(ctx, newRoot, oldRoot, a, rm)
	assert.Equal(t, []string{"renameTable"}, removed)
	assert.Equal(t, []string{"newTableName"}, added)
	assert.Equal(t, map[string]string{}, renamed)

	var stringWr StringBuilderCloser
	_ = PrintSqlTableDiffs(ctx, newRoot, oldRoot, &stringWr)
	expectedOutput := "DROP TABLE `renameTable`;\n"
	expectedOutput = expectedOutput +
		sql.SchemaAsCreateStmt("newTableName", sch) + "\n" +
		"INSERT INTO `newTableName` (`id`,`name`,`age`,`is_married`,`title`) " +
		"VALUES (\"00000000-0000-0000-0000-000000000000\",\"Big Billy\",77,FALSE,\"Doctor\");\n"
	assert.Equal(t, expectedOutput, stringWr.String())
}
