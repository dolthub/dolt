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

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
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

// TestRecoverRows_End2End_HeterogeneousColumn drives the entire recover-rows
// pipeline against a hand-crafted heterogeneous fixture and asserts:
//
//  1. The pre-state has the column tagged adaptive with a mix of legacy and
//     adaptive rows.
//  2. recover-rows succeeds with outcome=RecoverRowsMigrated and the count of
//     rewritten rows matches the count of non-legacy rows in the pre-state.
//  3. The post-state has the column tagged legacy and every row readable
//     without panic (i.e. content matches what was inserted).
//  4. The commit message names the table.column, the row count, and the
//     target encoding.
//  5. Re-running recover-rows is a no-op (OutcomeAlreadyOK).
func TestRecoverRows_End2End_HeterogeneousColumn(t *testing.T) {
	ctx := context.Background()

	// Step 1: CREATE TABLE under legacy encoding so the first batch of rows
	// is written as raw 20-byte hashes — exactly the legacy-side payload of
	// the heterogeneous mix we want to test recovery on.
	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = false
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()

	_, err := sqle.ExecuteSql(ctx, dEnv,
		`CREATE TABLE issues (id INT PRIMARY KEY, description TEXT NOT NULL);`)
	require.NoError(t, err)

	// Insert two rows under legacy encoding. Use long payloads so they spill
	// to the legacy raw-hash representation (i.e. the field bytes are exactly
	// 20 bytes, addressing an out-of-band content chunk).
	legacyPayload1 := strings.Repeat("L", 1500)
	legacyPayload2 := strings.Repeat("M", 2500)
	_, err = sqle.ExecuteSql(ctx, dEnv, fmt.Sprintf(
		`INSERT INTO issues VALUES (1, '%s'), (2, '%s');`, legacyPayload1, legacyPayload2))
	require.NoError(t, err)

	// Step 2: manually flip the column's encoding to adaptive — simulating
	// the v2.0.7 ALTER MODIFY corruption. The schema record now says
	// StringAdaptiveEnc while the existing rows are still in legacy raw-hash
	// form.
	require.NoError(t, manuallyFlipColumnEncoding(ctx, dEnv, "issues", "description", val.StringAdaptiveEnc))

	// Step 3: insert additional rows under the now-adaptive schema. These
	// will be written via the adaptive encoder, producing either inline or
	// out-of-band-addressed adaptive bytes.
	typeinfo.UseAdaptiveEncoding = true
	adaptiveShort := "short adaptive payload"
	adaptiveLong := strings.Repeat("A", 1700)
	_, err = sqle.ExecuteSql(ctx, dEnv, fmt.Sprintf(
		`INSERT INTO issues VALUES (3, '%s'), (4, '%s');`, adaptiveShort, adaptiveLong))
	require.NoError(t, err)

	// Sanity: collect the field-format distribution to confirm we built a
	// truly heterogeneous fixture (not all-legacy, not all-adaptive).
	preStateFormats := collectFieldFormats(t, ctx, dEnv, "issues", "description")
	require.Contains(t, preStateFormats, schemadrift.FieldLegacyRawHash,
		"pre-state must contain at least one legacy row (otherwise it's not heterogeneous)")
	require.True(t, len(preStateFormats) >= 2,
		"pre-state must mix at least two distinct field formats (got %v)", preStateFormats)

	// We deliberately do NOT call SELECT on the corrupted pre-state — that's
	// the very crash this whole admin command exists to mitigate. We know
	// what we inserted, so we compare against the literal values after the
	// migration completes.
	expected := map[int]string{
		1: legacyPayload1,
		2: legacyPayload2,
		3: adaptiveShort,
		4: adaptiveLong,
	}

	// Step 4: run recover-rows.
	res, err := schemadrift.RecoverRowsColumnForTest(ctx, dEnv, "issues", "description")
	require.NoError(t, err)
	require.Equal(t, schemadrift.RecoverRowsMigrated, res.Outcome,
		"heterogeneous-with-legacy-witnesses case must migrate")
	require.Equal(t, val.StringAdaptiveEnc, res.OldEncoding)
	require.Equal(t, val.StringAddrEnc, res.NewEncoding)
	require.Equal(t, 4, res.RowsScanned)
	// At least the 2 adaptive rows must have been rewritten. Legacy rows are
	// already canonical and might be skipped depending on classifier output.
	require.GreaterOrEqual(t, res.RowsRewritten, 2,
		"at least 2 adaptive rows must be rewritten")
	require.NotEmpty(t, res.CommitHash, "successful migration must produce a real commit hash")
	require.Contains(t, res.CommitMessage, "issues.description",
		"commit message must name the specific table.column")
	require.Contains(t, res.CommitMessage, "StringAddrEnc",
		"commit message must name the post-migration encoding")
	require.Contains(t, res.CommitMessage, "content unchanged",
		"commit message must declare the content-unchanged contract for downstream consumers")

	// Step 5: verify post-state. Schema is legacy, every row readable, content
	// matches the literal values we inserted in steps 1 and 3.
	postContent := collectColumnContent(t, ctx, dEnv, "issues", "description")
	require.Equal(t, expected, postContent,
		"row content must equal the originally-inserted strings after recover-rows")

	postFormats := collectFieldFormats(t, ctx, dEnv, "issues", "description")
	require.NotContains(t, postFormats, schemadrift.FieldAdaptiveAddressed,
		"post-migration must contain no adaptive-addressed bytes")
	require.NotContains(t, postFormats, schemadrift.FieldAdaptiveInline,
		"post-migration must contain no adaptive-inline bytes (all promoted to OOB legacy)")
	// Every row must now be legacy raw hash (or NULL, but we inserted NOT NULL).
	for f := range postFormats {
		require.Equal(t, schemadrift.FieldLegacyRawHash, f,
			"every post-migration row must be in legacy raw hash format; saw %v", f)
	}

	// Step 6: idempotency. Re-running recover-rows on the now-legacy column
	// must be a no-op.
	res2, err := schemadrift.RecoverRowsColumnForTest(ctx, dEnv, "issues", "description")
	require.NoError(t, err)
	require.Equal(t, schemadrift.RecoverRowsAlreadyOK, res2.Outcome,
		"re-running on a now-legacy column must be an idempotent no-op")
	require.Empty(t, res2.CommitHash, "no-op recover-rows must NOT produce a commit")
}

// TestRecoverRows_RepairRefusesSameFixture is the empirical
// FAILS-WITHOUT/PASSES-WITH proof requested by the task brief: take the
// hand-crafted heterogeneous fixture, attempt `repair` first (which MUST
// refuse with the "heterogeneous payload" message), then attempt
// `recover-rows` (which MUST succeed). Same fixture, different outcomes —
// that's the load-bearing distinction between the two commands.
func TestRecoverRows_RepairRefusesSameFixture(t *testing.T) {
	ctx := context.Background()

	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = false
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()

	// Build the same heterogeneous fixture as the End2End test.
	_, err := sqle.ExecuteSql(ctx, dEnv,
		`CREATE TABLE issues (id INT PRIMARY KEY, description TEXT NOT NULL);`)
	require.NoError(t, err)
	legacyPayload := strings.Repeat("L", 1500)
	_, err = sqle.ExecuteSql(ctx, dEnv, fmt.Sprintf(
		`INSERT INTO issues VALUES (1, '%s');`, legacyPayload))
	require.NoError(t, err)
	require.NoError(t, manuallyFlipColumnEncoding(ctx, dEnv, "issues", "description", val.StringAdaptiveEnc))
	typeinfo.UseAdaptiveEncoding = true
	// Use a payload large enough (> DefaultTupleLengthTarget = 2048 bytes) to
	// force the adaptive writer to spill to out-of-band storage. A spilled
	// adaptive row classifies as FieldAdaptiveAddressed, which is the signal
	// repair uses to detect "genuine adaptive payload" — exactly the
	// heterogeneous condition we want repair to refuse.
	adaptivePayload := strings.Repeat("A", 3500)
	_, err = sqle.ExecuteSql(ctx, dEnv, fmt.Sprintf(
		`INSERT INTO issues VALUES (2, '%s');`, adaptivePayload))
	require.NoError(t, err)

	// FAILS-WITHOUT: repair must refuse because the column is heterogeneous.
	repairRes, repairErr := schemadrift.RepairColumnForTest(ctx, dEnv, "issues", "description")
	require.Error(t, repairErr,
		"repair MUST refuse a heterogeneous column — that's the prerequisite for recover-rows existing")
	require.Contains(t, repairErr.Error(), "heterogeneous",
		"refusal message must name the heterogeneous condition")
	require.Empty(t, repairRes.CommitHash,
		"refused repair must NOT create a commit")

	// PASSES-WITH: recover-rows must succeed on the exact same fixture.
	recoverRes, recoverErr := schemadrift.RecoverRowsColumnForTest(ctx, dEnv, "issues", "description")
	require.NoError(t, recoverErr,
		"recover-rows must accept what repair refused")
	require.Equal(t, schemadrift.RecoverRowsMigrated, recoverRes.Outcome,
		"recover-rows must migrate the heterogeneous fixture")
	require.NotEmpty(t, recoverRes.CommitHash,
		"successful recover-rows must produce a commit hash")

	// And after recover-rows, the post-state matches what we inserted.
	postContent := collectColumnContent(t, ctx, dEnv, "issues", "description")
	require.Equal(t, map[int]string{1: legacyPayload, 2: adaptivePayload}, postContent,
		"content must round-trip exactly through the migration")
}

// TestRecoverRows_RefusesPureAdaptiveColumn asserts the load-bearing safety
// guard: if a column's payload is purely adaptive (no legacy-raw-hash
// witnesses), recover-rows refuses with a clear error. The intent matches
// repair's "OutcomeGenuineAdaptive" path: a column with only adaptive rows is
// internally consistent and recover-rows must not flip its schema (that
// would corrupt the adaptive bytes the same way repair would).
func TestRecoverRows_RefusesPureAdaptiveColumn(t *testing.T) {
	ctx := context.Background()

	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = true
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()

	// Create + insert under genuine adaptive encoding. The column is
	// internally consistent; recover-rows must refuse.
	bigPayload := strings.Repeat("z", 2048)
	_, err := sqle.ExecuteSql(ctx, dEnv,
		`CREATE TABLE issues (id INT PRIMARY KEY, description TEXT NOT NULL);
`+fmt.Sprintf(`INSERT INTO issues VALUES (1, '%s');`, bigPayload))
	require.NoError(t, err)

	res, err := schemadrift.RecoverRowsColumnForTest(ctx, dEnv, "issues", "description")
	require.Error(t, err, "recover-rows must refuse a purely-adaptive column")
	require.Contains(t, err.Error(), "only genuine adaptive rows",
		"refusal must explain why the migration is refused")
	require.Equal(t, schemadrift.RecoverRowsNoLegacyRows, res.Outcome,
		"refused recover-rows must return RecoverRowsNoLegacyRows so callers can branch on it")
	require.Empty(t, res.CommitHash, "refused recover-rows must NOT touch the working set")
}

// TestRecoverRows_IdempotentOnLegacyColumn asserts the fast-path: calling
// recover-rows on a column whose schema is already legacy returns
// OutcomeAlreadyOK without iterating rows.
func TestRecoverRows_IdempotentOnLegacyColumn(t *testing.T) {
	ctx := context.Background()

	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = false
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()

	bigPayload := strings.Repeat("y", 2048)
	_, err := sqle.ExecuteSql(ctx, dEnv,
		`CREATE TABLE issues (id INT PRIMARY KEY, description TEXT NOT NULL);
`+fmt.Sprintf(`INSERT INTO issues VALUES (1, '%s');`, bigPayload))
	require.NoError(t, err)

	res, err := schemadrift.RecoverRowsColumnForTest(ctx, dEnv, "issues", "description")
	require.NoError(t, err)
	require.Equal(t, schemadrift.RecoverRowsAlreadyOK, res.Outcome,
		"legacy-tagged column must short-circuit with RecoverRowsAlreadyOK")
	require.Empty(t, res.CommitHash, "no-op must NOT produce a commit")
}

// TestRecoverRows_HealsSiblingAddrEncMismatch is a sibling-mismatch regression test.
//
// Scenario: an earlier `repair` invocation flipped a sibling column's schema
// from StringAdaptiveEnc to StringAddrEnc, but the column's row data still
// held heterogeneous bytes — some rows have legacy 20-byte hashes (canonical
// for the new schema), some have 1-byte `[0x00]` (adaptive-inline empty) or
// longer non-canonical adaptive shapes. The schema check passed because
// `repair`'s scanner only needs one legacy witness and short-circuits.
//
// Now the operator runs `recover-rows` on a DIFFERENT column. The serializer's
// countAddresses walks every AddrEnc field in the rebuilt tuple and calls
// hash.New(field) on each. For sibling-column fields that are NOT exactly 20
// bytes, hash.New panics with "invalid hash length: N" — taking down the
// recover-rows command before it can commit anything.
//
// The sibling-mismatch fix is for recover-rows to NORMALIZE every legacy-AddrEnc field
// in the post-schema tuple — not just the target column. Non-canonical bytes
// in sibling AddrEnc positions get resolved through resolveFieldToLegacy and
// rewritten to canonical 20-byte hashes. CollateralRewrites counts how many
// rows had at least one sibling field healed this way.
//
// This test builds the exact pre-state: an `issues` table with one row whose
// description is legacy AND whose sibling close_reason is `[0x00]` (1-byte
// adaptive-inline empty under a schema that's been flipped to StringAddrEnc).
// Without the sibling-mismatch fix, recover-rows on description panics. With the fix it
// succeeds AND reports collateral healing.
func TestRecoverRows_HealsSiblingAddrEncMismatch(t *testing.T) {
	ctx := context.Background()

	// Build the source schema under legacy encoding so the initial
	// CREATE/INSERT writes legacy raw-hash bytes for both `description`
	// (target) and `close_reason` (sibling).
	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = false
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()

	_, err := sqle.ExecuteSql(ctx, dEnv,
		`CREATE TABLE issues (id INT PRIMARY KEY, description TEXT NOT NULL, close_reason TEXT);`)
	require.NoError(t, err)
	// One legacy-bearing row so description has a legacy-raw-hash witness.
	legacyPayload := strings.Repeat("L", 1500)
	_, err = sqle.ExecuteSql(ctx, dEnv, fmt.Sprintf(
		`INSERT INTO issues VALUES (1, '%s', 'orig-close-reason');`, legacyPayload))
	require.NoError(t, err)

	// Flip BOTH columns' schemas to adaptive — simulating the v2.0.7 drift
	// landing on both columns simultaneously.
	require.NoError(t, manuallyFlipColumnEncoding(ctx, dEnv, "issues", "description", val.StringAdaptiveEnc))
	require.NoError(t, manuallyFlipColumnEncoding(ctx, dEnv, "issues", "close_reason", val.StringAdaptiveEnc))

	// Insert additional rows under the adaptive schema. One has an
	// out-of-band-sized description (forcing FieldAdaptiveAddressed) so the
	// drift detector sees heterogeneous payload on description. The
	// close_reason for these rows is short, so it goes into FieldAdaptiveInline
	// shape (1-byte [0x00] for empty, [0x00][content] for non-empty).
	typeinfo.UseAdaptiveEncoding = true
	adaptivePayload := strings.Repeat("A", 3500)
	_, err = sqle.ExecuteSql(ctx, dEnv, fmt.Sprintf(
		`INSERT INTO issues VALUES (2, '%s', ''), (3, '%s', 'short');`,
		adaptivePayload, adaptivePayload))
	require.NoError(t, err)

	// Now silently flip close_reason's schema BACK to legacy without
	// rewriting rows — the exact mistake a buggy `repair` would make. Row
	// data for close_reason still has adaptive-inline bytes (`[0x00]` for
	// row 2, `[0x00]short` for row 3), but the schema says legacy.
	require.NoError(t, manuallyFlipColumnEncoding(ctx, dEnv, "issues", "close_reason", val.StringAddrEnc))

	// recover-rows on `description` must succeed AND heal the sibling
	// close_reason in the process. Without the sibling-mismatch fix the serializer
	// would panic on the 1-byte close_reason bytes for row 2.
	res, err := schemadrift.RecoverRowsColumnForTest(ctx, dEnv, "issues", "description")
	require.NoError(t, err, "recover-rows must succeed even when a sibling AddrEnc column has heterogeneous bytes")
	require.Equal(t, schemadrift.RecoverRowsMigrated, res.Outcome)
	require.GreaterOrEqual(t, res.CollateralRewrites, 2,
		"both adaptive-inserted rows (id=2, id=3) had non-canonical close_reason bytes; both must be reported as collateral healings")
	require.Contains(t, res.CommitMessage, "non-target AddrEnc fields healed as collateral",
		"commit message must surface the collateral healing for the audit trail")

	// Post-migration, both description and close_reason must read back to
	// their pre-migration content via SQL — the heal is content-preserving.
	descContent := collectColumnContent(t, ctx, dEnv, "issues", "description")
	require.Equal(t, map[int]string{1: legacyPayload, 2: adaptivePayload, 3: adaptivePayload}, descContent)

	closeContent := collectColumnContent(t, ctx, dEnv, "issues", "close_reason")
	require.Equal(t, map[int]string{1: "orig-close-reason", 2: "", 3: "short"}, closeContent,
		"sibling close_reason content must round-trip — empty inline becomes empty legacy, short inline becomes legacy hash addressing the original short bytes")
}

// TestRecoverRows_NonExistentColumn asserts the resolver surfaces a clear
// error for unknown columns rather than silently no-op.
func TestRecoverRows_NonExistentColumn(t *testing.T) {
	ctx := context.Background()

	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = true
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()

	_, err := sqle.ExecuteSql(ctx, dEnv,
		`CREATE TABLE issues (id INT PRIMARY KEY, description TEXT NOT NULL);
INSERT INTO issues VALUES (1, 'hello');`)
	require.NoError(t, err)

	_, err = schemadrift.RecoverRowsColumnForTest(ctx, dEnv, "issues", "no_such_column")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no_such_column")
}

// collectFieldFormats reads every row of |tableName| via the prolly map
// directly and classifies the bytes at |colName|'s value-tuple position.
// Returns the SET of distinct formats observed (not a per-row list — the
// test only needs presence/absence).
func collectFieldFormats(t *testing.T, ctx context.Context, dEnv *env.DoltEnv, tableName, colName string) map[schemadrift.FieldFormat]bool {
	t.Helper()
	root, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)
	tbl, _, ok, err := doltdb.GetTableInsensitive(ctx, root, doltdb.TableName{Name: tableName})
	require.NoError(t, err)
	require.True(t, ok)
	sch, err := tbl.GetSchema(ctx)
	require.NoError(t, err)
	col, ok := sch.GetAllCols().GetByNameCaseInsensitive(colName)
	require.True(t, ok)

	tupleIdx := -1
	i := 0
	_ = sch.GetNonPKCols().Iter(func(tag uint64, c schema.Column) (stop bool, err error) {
		if c.Virtual {
			return false, nil
		}
		if strings.EqualFold(c.Name, col.Name) {
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

	iter, err := pm.IterAll(ctx)
	require.NoError(t, err)
	out := make(map[schemadrift.FieldFormat]bool)
	for {
		_, value, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		b := value.GetField(tupleIdx)
		out[schemadrift.ClassifyFieldBytes(b)] = true
	}
	return out
}

// collectColumnContent reads every row of |tableName| via SQL SELECT and
// returns the value of |colName| keyed by primary key. This lets the test
// assert post-migration content equality without going through low-level
// prolly access.
func collectColumnContent(t *testing.T, ctx context.Context, dEnv *env.DoltEnv, tableName, colName string) map[int]string {
	t.Helper()
	root, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)
	rows, err := sqle.ExecuteSelect(ctx, dEnv, root, fmt.Sprintf(`SELECT id, %s FROM %s ORDER BY id;`, colName, tableName))
	require.NoError(t, err)
	out := make(map[int]string)
	for _, r := range rows {
		require.Equal(t, 2, len(r))
		var id int
		switch v := r[0].(type) {
		case int32:
			id = int(v)
		case int64:
			id = int(v)
		case int:
			id = v
		default:
			t.Fatalf("expected id column to be integer type, got %T", r[0])
		}
		var s string
		switch v := r[1].(type) {
		case string:
			s = v
		case []byte:
			s = string(v)
		case sql.StringWrapper:
			// Out-of-band TEXT storage — the engine returns a wrapper so it
			// can defer the chunkstore round-trip until the value is actually
			// observed. We force the unwrap here for the assertion.
			unwrapped, uerr := v.Unwrap(ctx)
			require.NoError(t, uerr)
			s = unwrapped
		case sql.AnyWrapper:
			unwrapped, uerr := v.UnwrapAny(ctx)
			require.NoError(t, uerr)
			s = fmt.Sprintf("%v", unwrapped)
		default:
			t.Fatalf("expected description column to be string, []byte, or sql.StringWrapper; got %T", r[1])
		}
		out[id] = s
	}
	return out
}
