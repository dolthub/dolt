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

	"github.com/stretchr/testify/assert"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/alterschema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sql"
	"github.com/liquidata-inc/dolt/go/store/types"
)

type StringBuilderCloser struct {
	strings.Builder
}

func (*StringBuilderCloser) Close() error {
	return nil
}

func TestPrintSqlTableDiffs(t *testing.T) {
	dEnv := dtestutils.CreateTestEnv()

	dropCols := []schema.Column{
		schema.NewColumn("pk", 0, types.StringKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("data", 1, types.IntKind, false),
	}
	dropColColl, _ := schema.NewColCollection(dropCols...)
	dropSch := schema.SchemaFromCols(dropColColl)

	renameCols := []schema.Column{
		schema.NewColumn("pk", 0, types.StringKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("data", 1, types.IntKind, false),
	}
	renameColColl, _ := schema.NewColCollection(renameCols...)
	renameSch := schema.SchemaFromCols(renameColColl)

	addCols := []schema.Column{
		schema.NewColumn("pk", 0, types.StringKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("data", 1, types.IntKind, false),
	}
	addColColl, _ := schema.NewColCollection(addCols...)
	addSch := schema.SchemaFromCols(addColColl)

	const name = "Jeffery Williams"
	const email = "meet.me@the.london"
	ctx := context.Background()
	_ = dEnv.DoltDB.WriteEmptyRepo(ctx, name, email)
	{
		ctx := context.Background()
		_ = dEnv.DoltDB.WriteEmptyRepo(ctx, name, email)
		oldRoot, _ := dEnv.WorkingRoot(ctx)
		dtestutils.CreateTestTable(t, dEnv, "addTable", addSch, []row.Row{}...)
		newRoot, _ := dEnv.WorkingRoot(ctx)
		added, _, removed, _ := newRoot.TableDiff(ctx, oldRoot)
		adds, _, _, _ := findRenames(ctx, newRoot, oldRoot, added, removed)
		assert.Equal(t, []string{"addTable"}, adds)

		expectedOutput := sql.SchemaAsCreateStmt("addTable", addSch) + "\n"
		var stringWr StringBuilderCloser
		_ = PrintSqlTableDiffs(ctx, newRoot, oldRoot, &stringWr)
		assert.Equal(t, expectedOutput, stringWr.String())
	}
	{
		ctx := context.Background()
		_ = dEnv.DoltDB.WriteEmptyRepo(ctx, name, email)
		dtestutils.CreateTestTable(t, dEnv, "dropTable", dropSch, []row.Row{}...)
		oldRoot, _ := dEnv.WorkingRoot(ctx)
		newRoot, _ := oldRoot.RemoveTables(ctx, []string{"dropTable"}...)
		added, _, removed, _ := newRoot.TableDiff(ctx, oldRoot)
		_, drops, _, _ := findRenames(ctx, newRoot, oldRoot, added, removed)
		assert.Equal(t, []string{"dropTable"}, drops)

		expectedOutput := "DROP TABLE `dropTable`;\n"
		var stringWr StringBuilderCloser
		_ = PrintSqlTableDiffs(ctx, newRoot, oldRoot, &stringWr)
		assert.Equal(t, expectedOutput, stringWr.String())
	}
	{
		ctx := context.Background()
		_ = dEnv.DoltDB.WriteEmptyRepo(ctx, name, email)
		dtestutils.CreateTestTable(t, dEnv, "renameTable", renameSch, []row.Row{}...)
		oldRoot, _ := dEnv.WorkingRoot(ctx)
		newRoot, _ := alterschema.RenameTable(ctx, dEnv.DoltDB, oldRoot, "renameTable", "newTableName")
		added, _, removed, _ := newRoot.TableDiff(ctx, oldRoot)
		_, _, renames, _ := findRenames(ctx, newRoot, oldRoot, added, removed)
		assert.Equal(t, map[string]string{"renameTable": "newTableName"}, renames)

		expectedOutput := "RENAME TABLE `renameTable` TO `newTableName`\n"
		var stringWr StringBuilderCloser
		_ = PrintSqlTableDiffs(ctx, newRoot, oldRoot, &stringWr)
		assert.Equal(t, expectedOutput, stringWr.String())
	}
}
