// Copyright 2026 Dolthub, Inc.
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

package sqle

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

// TestUpdateForeignKeyDuringRenameWithSchema simulates the state go-mysql-server's RenameTable node puts a table
// into mid-rename: it calls UpdateForeignKey with a ForeignKeyConstraint whose Table field already holds the new
// table name, before the table itself has actually been renamed in storage. For engines that qualify foreign keys
// by schema (i.e. Doltgres, where schema names are always populated), UpdateForeignKey must look up the existing
// foreign key by the table's *current* name, not the constraint's (already-updated) name, or the lookup fails with
// ErrForeignKeyNotFound. Dolt never populates TableName.Schema, so ForeignKeyCollection's lookups ignore the
// table name entirely and this bug is invisible to it.
func TestUpdateForeignKeyDuringRenameWithSchema(t *testing.T) {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()

	_, err := ExecuteSql(ctx, dEnv, `
CREATE TABLE parent (id BIGINT PRIMARY KEY);
CREATE TABLE child (id BIGINT PRIMARY KEY, CONSTRAINT fk1 FOREIGN KEY (id) REFERENCES parent (id));
`)
	require.NoError(t, err)

	root, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)

	fkc, err := root.GetForeignKeyCollection(ctx)
	require.NoError(t, err)

	origFk, ok := fkc.GetByNameCaseInsensitive("fk1", doltdb.TableName{Name: "child"})
	require.True(t, ok)
	require.True(t, origFk.IsResolved(), "test setup requires a resolved foreign key")

	// Rewrite fk1 as if it had been created in a schema named "public", matching what a schema-qualified engine
	// (Doltgres) would have stored.
	scopedFk := origFk
	scopedFk.TableName = doltdb.TableName{Name: "child", Schema: "public"}
	scopedFk.ReferencedTableName = doltdb.TableName{Name: "parent", Schema: "public"}
	fkc.RemoveKeys(origFk)
	require.NoError(t, fkc.AddKeys(scopedFk))

	newRoot, err := root.PutForeignKeyCollection(ctx, fkc)
	require.NoError(t, err)
	require.NoError(t, dEnv.UpdateWorkingRoot(ctx, newRoot))

	db, err := NewDatabase(ctx, "dolt", dEnv.DbData(ctx), editor.Options{})
	require.NoError(t, err)

	_, sqlCtx, err := NewTestEngine(dEnv, ctx, db)
	require.NoError(t, err)

	sqlTbl, ok, err := db.GetTableInsensitive(sqlCtx, "child")
	require.NoError(t, err)
	require.True(t, ok)

	alterableTbl, ok := sqlTbl.(*AlterableDoltTable)
	require.True(t, ok, "expected *AlterableDoltTable, got %T", sqlTbl)
	wdt := &alterableTbl.WritableDoltTable

	// Give this table's TableName() a "public" schema, as it would have if the schema-qualified table had actually
	// been resolved that way - but leave wdt.db (used to fetch/store the session root) untouched, since the
	// underlying storage in this test is still schema-less Dolt. This isolates exactly the behavior under test:
	// whether the foreign key lookup inside UpdateForeignKey uses the table's current name (t.TableName()) or the
	// new name already stashed in the incoming constraint (sqlFk.Table).
	scopedDoltTable := *wdt.DoltTable
	scopedDb := db
	scopedDb.schemaName = "public"
	scopedDoltTable.db = scopedDb
	wdt.DoltTable = &scopedDoltTable

	require.Equal(t, doltdb.TableName{Name: "child", Schema: "public"}, wdt.TableName())

	// This mirrors what sql/plan.RenameTable does: it sets fk.Table to the new name and calls UpdateForeignKey
	// before the rename itself has happened.
	renameSqlFk := sql.ForeignKeyConstraint{
		Name:           "fk1",
		Database:       "dolt",
		SchemaName:     "public",
		Table:          "renamed_child",
		ParentDatabase: "dolt",
		ParentSchema:   "public",
		ParentTable:    "parent",
		Columns:        []string{"id"},
		ParentColumns:  []string{"id"},
		IsResolved:     true,
	}

	err = wdt.UpdateForeignKey(sqlCtx, "fk1", renameSqlFk)
	require.NoError(t, err, "UpdateForeignKey should find the foreign key by the table's current name, not the "+
		"already-renamed name in the incoming constraint")

	updatedRoot, err := db.GetRoot(sqlCtx)
	require.NoError(t, err)
	updatedFkc, err := updatedRoot.GetForeignKeyCollection(ctx)
	require.NoError(t, err)

	movedFk, ok := updatedFkc.GetByNameCaseInsensitive("fk1", doltdb.TableName{Name: "renamed_child", Schema: "public"})
	require.True(t, ok)
	require.Equal(t, doltdb.TableName{Name: "renamed_child", Schema: "public"}, movedFk.TableName)

	_, stillThere := updatedFkc.GetByNameCaseInsensitive("fk1", doltdb.TableName{Name: "child", Schema: "public"})
	require.False(t, stillThere)
}
