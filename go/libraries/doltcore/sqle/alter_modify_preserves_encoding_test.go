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

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/val"
)

// TestAlterModifyTextColumn_PreservesLegacyEncoding is the load-bearing regression test
// for the schema-level half of the StringAdaptiveEnc / "invalid hash length: 19" crash.
//
// Setup: create a table where the TEXT column is persisted under the legacy
// StringAddrEnc(23) format — the same shape a Dolt 1.x repository carries forward after
// upgrade. We do this by flipping UseAdaptiveEncoding=false for the CREATE TABLE, then
// flipping it back to true (the 2.0.7+ default) before the ALTER.
//
// Pre-fix behaviour: ALTER TABLE ... MODIFY COLUMN ... LONGTEXT calls
// AlterableDoltTable.ModifyColumn, which calls sqlutil.ToDoltCol, which calls
// typeinfo.FromSqlType — returning a fresh blobStringType with enc=0. That fresh
// TypeInfo replaces the existing one in the schema and serializes under the global
// UseAdaptiveEncoding=true default, persisting StringAdaptiveEnc(135). The row data is
// never rewritten (TEXT → LONGTEXT does not trigger RewriteInserter), so the schema and
// the on-disk row layout disagree, and adaptive dispatch panics on the next read.
//
// Post-fix behaviour: PreserveAdaptiveEncoding pins the new column's encoding back onto
// the existing legacy encoding, so the persisted schema continues to say
// StringAddrEnc(23) — matching the on-disk row layout.
func TestAlterModifyTextColumn_PreservesLegacyEncoding(t *testing.T) {
	ctx := context.Background()

	// Step 1: simulate a 1.x-written schema by forcing legacy address encoding for the
	// CREATE TABLE. blobStringType.Encoding() with enc=0 will then return
	// StringAddrEnc, and that's what gets persisted.
	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = false

	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()

	_, err := ExecuteSql(ctx, dEnv,
		`CREATE TABLE issues (id INT PRIMARY KEY, description TEXT NOT NULL);`)
	require.NoError(t, err)

	// Sanity check: the legacy schema persists StringAddrEnc, not StringAdaptiveEnc.
	root, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)
	require.Equal(t, val.StringAddrEnc, descriptionColumnEncoding(t, ctx, root),
		"setup precondition: a CREATE TABLE under UseAdaptiveEncoding=false must persist StringAddrEnc(23)")

	// Step 2: flip the global flag to the 2.0.7+ default. Without the fix, the next
	// ALTER MODIFY would silently re-tag the column as StringAdaptiveEnc(135) while the
	// on-disk row data stays in the legacy format.
	typeinfo.UseAdaptiveEncoding = true
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	// Step 3: ALTER MODIFY — TEXT → LONGTEXT, exactly the shape used by a schema migration
	// 0049 (the migration shape that triggered the corruption).
	_, err = ExecuteSql(ctx, dEnv,
		`ALTER TABLE issues MODIFY COLUMN description LONGTEXT NOT NULL;`)
	require.NoError(t, err)

	// Step 4: the persisted column encoding must still be StringAddrEnc(23). If this
	// fails with StringAdaptiveEnc(135), the fix has regressed and a TEXT→LONGTEXT
	// widening on a 1.x repository will once again drift the schema away from the
	// on-disk row layout, leading to "invalid hash length: 19" panics on read.
	root, err = dEnv.WorkingRoot(ctx)
	require.NoError(t, err)
	require.Equal(t, val.StringAddrEnc, descriptionColumnEncoding(t, ctx, root),
		"regression: ALTER MODIFY must preserve the legacy StringAddrEnc on a TEXT→LONGTEXT widening — "+
			"got StringAdaptiveEnc, which means the schema-side fix is missing and downstream readers will panic")
}

// TestAlterDropColumn_SurvivingTextColumnPreservesLegacyEncoding is the rewrite-path
// sibling of TestAlterModifyTextColumn_PreservesLegacyEncoding. While the MODIFY test
// exercises AlterableDoltTable.ModifyColumn (the in-place path), this one exercises
// createSchemaForColumnChange (the rewrite path) — driven by ALTER TABLE ... DROP COLUMN.
//
// The rewrite path rebuilds the entire schema via sqlutil.ToDoltSchema, which calls
// FromSqlType for every surviving column with enc=0. Without
// preserveSurvivingColumnEncodings, every surviving TEXT/BLOB/JSON/GEOMETRY column on a
// legacy 1.x table would silently be re-tagged to its adaptive variant when an unrelated
// column on the same table was dropped — even though the persisted row data for the
// surviving column never changed.
//
// The assertion: after DROP COLUMN unrelated_text, the surviving `description` column
// still reports its original legacy encoding (StringAddrEnc), not the global adaptive
// default (StringAdaptiveEnc).
func TestAlterDropColumn_SurvivingTextColumnPreservesLegacyEncoding(t *testing.T) {
	ctx := context.Background()

	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = false

	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()

	// Create a table with two TEXT columns under legacy encoding. `description` is the
	// one we'll prove stays pinned; `notes` is the unrelated column that'll be dropped
	// (driving the rewrite path).
	_, err := ExecuteSql(ctx, dEnv,
		`CREATE TABLE issues (id INT PRIMARY KEY, description TEXT NOT NULL, notes TEXT);`)
	require.NoError(t, err)

	root, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)
	require.Equal(t, val.StringAddrEnc, descriptionColumnEncoding(t, ctx, root),
		"setup precondition: `description` must persist StringAddrEnc(23) under UseAdaptiveEncoding=false")

	// Flip the global flag to the 2.0.7+ default. Without the fix, the upcoming DROP
	// COLUMN goes through createSchemaForColumnChange → ToDoltSchema → FromSqlType,
	// and every surviving TEXT column gets a fresh TypeInfo with enc=0 that falls back
	// to StringAdaptiveEnc on serialization.
	typeinfo.UseAdaptiveEncoding = true
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	// ALTER TABLE ... DROP COLUMN drives the column-drop rewrite path (isColumnDrop →
	// ShouldRewriteTable=true → RewriteInserter → createSchemaForColumnChange with the
	// "oldColumn==nil || newColumn==nil" branch).
	_, err = ExecuteSql(ctx, dEnv, `ALTER TABLE issues DROP COLUMN notes;`)
	require.NoError(t, err)

	root, err = dEnv.WorkingRoot(ctx)
	require.NoError(t, err)
	require.Equal(t, val.StringAddrEnc, descriptionColumnEncoding(t, ctx, root),
		"regression: DROP COLUMN on a sibling must NOT silently re-tag the surviving `description` from "+
			"StringAddrEnc(23) to StringAdaptiveEnc(135) — the rewrite path needs the same encoding-preservation pass")
}

// descriptionColumnEncoding returns the persisted val.Encoding for the `description`
// column of the `issues` table at the given root. Using the live schema (rather than the
// raw flatbuffer) is sufficient here because deserializeColumns calls WithEncoding with
// the on-disk value, so the in-memory TypeInfo's Encoding() reflects the persisted enc.
func descriptionColumnEncoding(t *testing.T, ctx context.Context, root doltdb.RootValue) val.Encoding {
	t.Helper()
	table, _, ok, err := doltdb.GetTableInsensitive(ctx, root, doltdb.TableName{Name: "issues"})
	require.NoError(t, err)
	require.True(t, ok, "table `issues` not found")
	sch, err := table.GetSchema(ctx)
	require.NoError(t, err)
	col, ok := sch.GetAllCols().GetByName("description")
	require.True(t, ok, "column `description` not found")
	return col.TypeInfo.Encoding()
}
