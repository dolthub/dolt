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

package schemadrift_test

// Regression coverage for the drift scenario on ignored_log.
//
// `repair` flipped a column whose drifted rows were adaptive-INLINE (small
// [0x00]<utf8> payloads such as "example inline content payload: ...", NOT spilled to
// addressed). The schema-only flip stranded every inline row under a StringAddr
// schema: reads panic `invalid hash length: N` and any insert's node-flush
// panics in countAddresses.
//
// The pre-existing TestRecoverRows_RepairRefusesSameFixture only exercises the
// ADDRESSED adaptive shape (payload > 2KB -> FieldAdaptiveAddressed ->
// sawDefinitiveAdaptive). The INLINE shape is the gap: scanColumnFullPayload
// classifies non-empty inline as sawNonEmptyAmbiguous (not sawDefinitiveAdaptive)
// so a column with legacy witnesses + inline content falls through repair.go's
// switch to the FLIP branch — exactly what stranded ignored_log.
//
// Tests assert at the command boundary so they survive later churn in
// repair.go / recover_rows.go. They reuse helpers from
// recover_rows_integration_test.go (same package): manuallyFlipColumnEncoding,
// collectColumnContent, collectFieldFormats.

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/admin/schemadrift"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/store/val"
)

// inlineContent stays adaptive-INLINE (well under the ~2KB out-of-band spill
// threshold) and is real UTF-8 — the exact ignored_log.new_value shape
// (a 59-byte [0x00]"example inline ..." row).
const inlineContent = "example inline content payload"

// buildInlineHeterogeneousFixture creates
//
//	issues(id PK, actor VARCHAR(255) NOT NULL, description TEXT NOT NULL)
//
// with: one legacy raw-hash row (long payload) and two adaptive-INLINE-content
// rows (short payloads). The persisted schema ends tagged StringAdaptiveEnc(135)
// — the "drifted but internally readable" pre-repair state.
//
// The leading VARIABLE-width `actor` column faithfully mirrors ignored_log
// (new_value sits after VARCHAR/TEXT siblings): it pushes `description` out of
// the value tuple's fixed-width prefix so a StringAddr read uses intrinsic
// offsets and sees the FULL field bytes — reproducing the
// hash.New(len) panic. A lone leading StringAddr column would instead be read as
// a fixed 20-byte slice (silent bogus hash), masking the crash.
func buildInlineHeterogeneousFixture(t *testing.T, ctx context.Context) (*env.DoltEnv, map[int]string) {
	t.Helper()

	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = false // legacy witness row first
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	t.Cleanup(func() { dEnv.Close() })

	_, err := sqle.ExecuteSql(ctx, dEnv,
		`CREATE TABLE issues (id INT PRIMARY KEY, actor VARCHAR(255) NOT NULL, description TEXT NOT NULL);`)
	require.NoError(t, err)

	legacyPayload := strings.Repeat("L", 1500) // long -> legacy raw-hash (out-of-band)
	_, err = sqle.ExecuteSql(ctx, dEnv, fmt.Sprintf(
		`INSERT INTO issues VALUES (1, 'actor-1', '%s');`, legacyPayload))
	require.NoError(t, err)

	// Flip description to adaptive (simulating v2.0.7 ALTER drift), then insert
	// SHORT rows under the adaptive encoder so they land as inline [0x00]<content>.
	require.NoError(t, manuallyFlipColumnEncoding(ctx, dEnv, "issues", "description", val.StringAdaptiveEnc))
	typeinfo.UseAdaptiveEncoding = true
	_, err = sqle.ExecuteSql(ctx, dEnv, fmt.Sprintf(
		`INSERT INTO issues VALUES (2, 'actor-2', '%s'), (3, 'actor-3', '%s');`, inlineContent, "short note"))
	require.NoError(t, err)

	// The fixture must be heterogeneous AND contain inline (not addressed)
	// adaptive content — otherwise it doesn't reproduce the gap.
	formats := collectFieldFormats(t, ctx, dEnv, "issues", "description")
	require.True(t, formats[schemadrift.FieldLegacyRawHash], "fixture needs a legacy witness")
	require.True(t, formats[schemadrift.FieldAdaptiveInline], "fixture needs adaptive-INLINE content (the affected shape)")
	require.False(t, formats[schemadrift.FieldAdaptiveAddressed], "fixture must NOT spill to addressed (already covered elsewhere)")

	return dEnv, map[int]string{1: legacyPayload, 2: inlineContent, 3: "short note"}
}

// TestRepair_RefusesInlineHeterogeneous_REGRESSION is the load-bearing
// fail-without/pass-with test. On today's binary repair FLIPS the schema (the
// real-world bug). After the fix (non-empty inline counted as definitive
// adaptive, or refuse when ambiguous coincides with a legacy witness), repair
// must REFUSE and leave the schema untouched.
func TestRepair_RefusesInlineHeterogeneous_REGRESSION(t *testing.T) {
	ctx := context.Background()
	dEnv, _ := buildInlineHeterogeneousFixture(t, ctx)

	res, err := schemadrift.RepairColumnForTest(ctx, dEnv, "issues", "description")

	// FAILS-WITHOUT: today repair returns OutcomeFlipped + nil error.
	// PASSES-WITH: repair must refuse a legacy+adaptive-inline heterogeneous column.
	require.Error(t, err,
		"repair MUST refuse a legacy+adaptive-inline heterogeneous column (it would otherwise strand the inline rows)")
	require.NotEqual(t, schemadrift.OutcomeFlipped, res.Outcome,
		"repair MUST NOT flip the schema of an inline-heterogeneous column")
	require.Empty(t, res.CommitHash, "a refused repair must not create a commit")
	// Soft secondary: the refusal should point operators at recover-rows. Kept
	// loose so it doesn't couple to exact wording.
	require.Contains(t, strings.ToLower(err.Error()), "heterogeneous",
		"refusal should name the heterogeneous condition")
}

// TestRepair_InlineFlipStrandsRows_ReadPanics characterizes the consequence of
// the flip: it reproduces the exact stranded state (schema flipped to StringAddr
// while the inline bytes remain) and proves both the orphaning (schema/data
// mismatch) and the read crash (GetStringAddr -> hash.New ->
// "invalid hash length"). This pins the failure shape independent of the SQL
// engine's lazy materialization.
func TestRepair_InlineFlipStrandsRows_ReadPanics(t *testing.T) {
	ctx := context.Background()
	dEnv, _ := buildInlineHeterogeneousFixture(t, ctx)

	require.NoError(t, manuallyFlipColumnEncoding(ctx, dEnv, "issues", "description", val.StringAddrEnc))

	// Orphan proof (deterministic): schema now says StringAddr, yet the rows
	// still carry adaptive-inline bytes.
	formats := collectFieldFormats(t, ctx, dEnv, "issues", "description")
	require.True(t, formats[schemadrift.FieldAdaptiveInline],
		"post-flip data must still hold adaptive-inline bytes under a StringAddr schema (the orphaning)")

	// Panic proof: reading those inline fields via the StringAddr descriptor
	// calls hash.New on a non-20-byte field -> panic `invalid hash length: N`.
	require.True(t, stringAddrReadPanics(t, ctx, dEnv, "issues", "description"),
		"reading stranded adaptive-inline rows via the StringAddr descriptor MUST panic (invalid hash length)")
}

// TestOrphanedInlineColumn_IsRecoverable proves the data is NOT lost. An
// already-stranded column (StringAddr schema + adaptive-inline data — the live
// ignored_log state) is recoverable: re-tag it adaptive so recover-rows
// recognizes it as drifted, migrate, and every row reads back byte-identical.
//
// NB: recover-rows keys off the SCHEMA tag, so it no-ops on a StringAddr-tagged
// column — which is why healing the stranded columns needs either the
// re-tag step shown here OR the the forward `recover-rows --to-adaptive` command
// (see wp-tests.md). This test guards content recoverability with primitives
// available today; the --to-adaptive command gets its own test once it lands.
func TestOrphanedInlineColumn_IsRecoverable(t *testing.T) {
	ctx := context.Background()
	dEnv, expected := buildInlineHeterogeneousFixture(t, ctx)

	// Strand it exactly as the buggy schema-only flip does.
	require.NoError(t, manuallyFlipColumnEncoding(ctx, dEnv, "issues", "description", val.StringAddrEnc))

	// Re-tag to adaptive so the column is recognized as drifted (schema now
	// matches the adaptive-inline data again), then migrate to canonical legacy.
	require.NoError(t, manuallyFlipColumnEncoding(ctx, dEnv, "issues", "description", val.StringAdaptiveEnc))
	res, err := schemadrift.RecoverRowsColumnForTest(ctx, dEnv, "issues", "description")
	require.NoError(t, err, "recover-rows must heal the (re-tagged) drifted inline column")
	require.Equal(t, schemadrift.RecoverRowsMigrated, res.Outcome)
	require.NotEmpty(t, res.CommitHash)

	// Every row reads back byte-identical to what we inserted — no content loss.
	got := collectColumnContent(t, ctx, dEnv, "issues", "description")
	require.Equal(t, expected, got, "content must round-trip exactly through the heal")
}

// TestMigrateAdaptive_HealsOrphanedInlineColumn is the Approach-B (operator's
// preferred, v2-native) heal test. It takes the exact stranded real-world state
// (StringAddr schema + heterogeneous legacy/adaptive-inline data) and runs the
// new `migrate-adaptive` command: legacy rows are rewritten forward to canonical
// adaptive, already-adaptive inline rows are kept verbatim, and the schema is
// flipped to the adaptive sibling — leaving one internally-consistent adaptive
// column. Every row must read back byte-identical with no panic.
func TestMigrateAdaptive_HealsOrphanedInlineColumn(t *testing.T) {
	ctx := context.Background()
	dEnv, expected := buildInlineHeterogeneousFixture(t, ctx)

	// Strand the column exactly as the buggy schema-only repair flip does.
	require.NoError(t, manuallyFlipColumnEncoding(ctx, dEnv, "issues", "description", val.StringAddrEnc))

	res, err := schemadrift.MigrateAdaptiveColumnForTest(ctx, dEnv, "issues", "description")
	require.NoError(t, err, "migrate-adaptive must heal a stranded heterogeneous column")
	require.Equal(t, schemadrift.RecoverRowsMigrated, res.Outcome)
	require.Equal(t, val.StringAdaptiveEnc, res.NewEncoding, "schema must end on the adaptive sibling")
	require.GreaterOrEqual(t, res.RowsRewritten, 1, "the legacy row(s) must be rewritten forward to adaptive")
	require.NotEmpty(t, res.CommitHash)

	// Post-heal the column is consistent adaptive: every row reads back
	// byte-identical to what we inserted, with no invalid-hash-length panic.
	got := collectColumnContent(t, ctx, dEnv, "issues", "description")
	require.Equal(t, expected, got, "content must round-trip exactly through the forward migration")

	// And the column is now homogeneous adaptive (no legacy raw-hash bytes left).
	formats := collectFieldFormats(t, ctx, dEnv, "issues", "description")
	require.False(t, formats[schemadrift.FieldLegacyRawHash],
		"after migrate-adaptive no legacy raw-hash bytes may remain (column is canonical adaptive)")
}

// TestMigrateAdaptive_DoltIgnoredTable_PersistsRewrites is an API-level guard
// that migrate-adaptive heals a dolt_ignore'd table's content byte-identically
// (the the affected columns ignored_log.{old,new}_value are in
// dolt_ignore'd tables matching `ignored_%`).
//
// IMPORTANT — this does NOT catch the deploy-blocker found by real-data
// validation: on a real ON-DISK repo, migrate-adaptive on ignored_log fails
// with "persist working set for dolt_ignore'd table ignored_log: dangling ref".
// The in-memory dtestutils env does NOT reproduce that on-disk working-set
// persist path, so this test PASSES both before and after the fix. The on-disk
// failure is covered by the bats suite
// (repair_orphans_content_compatibility.bats, "migrate-adaptive on a
// dolt_ignore'd table") and by the real-data validation in validation notes.
// This test still has value: it guards that migrate-adaptive does not start
// REFUSING ignored tables and that content round-trips at the API level.
func TestMigrateAdaptive_DoltIgnoredTable_PersistsRewrites(t *testing.T) {
	ctx := context.Background()

	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = false
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	t.Cleanup(func() { dEnv.Close() })

	// Mark the table ignored (mirrors a database's `ignored_%` pattern), then build the
	// same legacy-witness + adaptive-inline fixture on it.
	_, err := sqle.ExecuteSql(ctx, dEnv, `INSERT INTO dolt_ignore VALUES ('ignored_%', true);`)
	require.NoError(t, err)
	_, err = sqle.ExecuteSql(ctx, dEnv,
		`CREATE TABLE ignored_log (id INT PRIMARY KEY, actor VARCHAR(255) NOT NULL, body TEXT NOT NULL);`)
	require.NoError(t, err)

	legacyPayload := strings.Repeat("L", 1500)
	_, err = sqle.ExecuteSql(ctx, dEnv, fmt.Sprintf(
		`INSERT INTO ignored_log VALUES (1, 'a-1', '%s');`, legacyPayload))
	require.NoError(t, err)

	require.NoError(t, manuallyFlipColumnEncoding(ctx, dEnv, "ignored_log", "body", val.StringAdaptiveEnc))
	typeinfo.UseAdaptiveEncoding = true
	_, err = sqle.ExecuteSql(ctx, dEnv, fmt.Sprintf(
		`INSERT INTO ignored_log VALUES (2, 'a-2', '%s'), (3, 'a-3', '%s');`, inlineContent, "short note"))
	require.NoError(t, err)

	// The legacy row (id=1) must be rewritten forward — that is what forces the
	// ignored-table working-set persist of novel chunks.
	res, err := schemadrift.MigrateAdaptiveColumnForTest(ctx, dEnv, "ignored_log", "body")
	require.NoError(t, err, "migrate-adaptive must persist a dolt_ignore'd table whose rows it rewrote (no dangling ref)")
	require.Equal(t, schemadrift.RecoverRowsMigrated, res.Outcome)
	require.GreaterOrEqual(t, res.RowsRewritten, 1)

	got := collectColumnContent(t, ctx, dEnv, "ignored_log", "body")
	require.Equal(t, map[int]string{1: legacyPayload, 2: inlineContent, 3: "short note"}, got,
		"content must round-trip exactly through the heal of an ignored table")
}

// stringAddrReadPanics reads every row's |colName| field through the table's
// CURRENT value descriptor and reports whether GetStringAddr panics — the
// lowest-level rereal-world of the read crash
// (val.TupleDesc.GetStringAddr -> GetAddr -> hash.New), independent of the SQL
// engine's lazy materialization / errguard recovery.
func stringAddrReadPanics(t *testing.T, ctx context.Context, dEnv *env.DoltEnv, tableName, colName string) (panicked bool) {
	t.Helper()
	root, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)
	tbl, _, ok, err := doltdb.GetTableInsensitive(ctx, root, doltdb.TableName{Name: tableName})
	require.NoError(t, err)
	require.True(t, ok)
	sch, err := tbl.GetSchema(ctx)
	require.NoError(t, err)

	tupleIdx := -1
	i := 0
	_ = sch.GetNonPKCols().Iter(func(tag uint64, c schema.Column) (stop bool, err error) {
		if c.Virtual {
			return false, nil
		}
		if strings.EqualFold(c.Name, colName) {
			tupleIdx = i
			return true, nil
		}
		i++
		return false, nil
	})
	require.GreaterOrEqual(t, tupleIdx, 0)

	idx, err := tbl.GetRowData(ctx)
	require.NoError(t, err)
	pm, err := durable.ProllyMapFromIndex(idx)
	require.NoError(t, err)
	vd := sch.GetValueDescriptor(pm.NodeStore())

	defer func() {
		if r := recover(); r != nil {
			panicked = true
		}
	}()
	iter, err := pm.IterAll(ctx)
	require.NoError(t, err)
	for {
		_, value, iterErr := iter.Next(ctx)
		if iterErr == io.EOF {
			return panicked
		}
		require.NoError(t, iterErr)
		// Mirrors the read path: a StringAddr field is dereferenced as
		// a 20-byte hash address; a non-20-byte inline payload panics here.
		_, _ = vd.GetStringAddr(tupleIdx, value)
	}
}
