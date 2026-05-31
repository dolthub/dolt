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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/admin/schemadrift"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/store/val"
)

// TestSchemaEncodingDrift_End2End_CheckReportsDriftAfterManualCorruption is
// the load-bearing integration test for `dolt admin schema-encoding-drift
// check`. We:
//
//  1. Create a TEXT-bearing table under UseAdaptiveEncoding=false so the
//     persisted column encoding is legacy StringAddrEnc and the row data is
//     adaptive-inline for short content (StringAddrEnc only writes a 20-byte
//     hash on out-of-band values; short strings still encode as adaptive
//     inline through the prolly writer).
//  2. Insert one row with long content so the encoded payload is a legacy
//     raw 20-byte hash — the exact shape v2.0.7 readers crash on.
//  3. Manually flip the persisted column TypeInfo from StringAddrEnc to
//     StringAdaptiveEnc (simulating the v2.0.7 ALTER MODIFY corruption).
//     This is the smallest possible rereal-world of an real-world-like drift — we
//     bypass the SQL ALTER path so the fix in commits 4969194e2 + 09278d859
//     doesn't paper over the corruption while we test the diagnostic.
//  4. Run scanForDrift via the package's CheckCmd integration entry point and
//     assert it reports exactly one drift row for the corrupted column.
//
// This test asserts the entire check pipeline — schema walk, candidate
// collection, prolly iteration, byte classification, drift attribution —
// without ever going through the panicking adaptive reader.
func TestSchemaEncodingDrift_End2End_CheckReportsDriftAfterManualCorruption(t *testing.T) {
	ctx := context.Background()

	// Step 1: legacy-encoded CREATE.
	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = false
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()

	// Step 2: a row big enough that it spills to addressed storage in TEXT.
	// 1024 bytes is well over the 1.x inline threshold for TEXT.
	bigPayload := makeBigString(1024)
	_, err := sqle.ExecuteSql(ctx, dEnv,
		`CREATE TABLE issues (id INT PRIMARY KEY, description TEXT NOT NULL);
`+`INSERT INTO issues VALUES (1, '`+bigPayload+`');`)
	require.NoError(t, err)

	// Sanity: schema reports StringAddrEnc, no drift expected.
	driftBefore, err := schemadrift.ScanForDriftForTest(ctx, dEnv)
	require.NoError(t, err)
	require.Empty(t, driftBefore, "before corruption: schema and payload agree (StringAddrEnc on both sides)")

	// Step 3: simulate the v2.0.7 ALTER MODIFY corruption by manually rewriting
	// the persisted schema's `description` column encoding from StringAddrEnc
	// to StringAdaptiveEnc — bypassing the schema-side fix (which would refuse
	// to corrupt in the first place).
	require.NoError(t, manuallyFlipColumnEncoding(ctx, dEnv, "issues", "description", val.StringAdaptiveEnc))

	// Step 4: check should now report drift on issues.description.
	driftAfter, err := schemadrift.ScanForDriftForTest(ctx, dEnv)
	require.NoError(t, err)
	require.Len(t, driftAfter, 1, "exactly one drifted column expected")
	d := driftAfter[0]
	require.Equal(t, "issues", d.Table)
	require.Equal(t, "description", d.Column)
	require.Equal(t, "StringAdaptiveEnc", d.DeclaredEncoding,
		"check must report the post-corruption declared encoding")
	require.Equal(t, "legacy-raw-hash", d.ObservedFormat,
		"observed payload must be classified as legacy-raw-hash since the data was never rewritten")
	require.Equal(t, "StringAddrEnc", d.SuggestedEncoding,
		"check must suggest StringAddrEnc as the legacy sibling to flip back to")
}

// TestSchemaEncodingDrift_End2End_RepairFlipsTagAndCommits drives the full
// repair pipeline against the same corrupted-by-hand fixture and asserts:
//
//  1. The drift exists before repair.
//  2. RepairColumnForTest succeeds with outcome=OutcomeFlipped.
//  3. The new commit hash is non-empty and the commit message names the
//     specific table.column with the encoding transition.
//  4. A subsequent check returns no drift.
//  5. A SECOND repair call is a no-op (idempotent OutcomeAlreadyOK).
func TestSchemaEncodingDrift_End2End_RepairFlipsTagAndCommits(t *testing.T) {
	ctx := context.Background()

	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = false
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()

	bigPayload := makeBigString(1024)
	_, err := sqle.ExecuteSql(ctx, dEnv,
		`CREATE TABLE issues (id INT PRIMARY KEY, description TEXT NOT NULL);
`+`INSERT INTO issues VALUES (1, '`+bigPayload+`');`)
	require.NoError(t, err)
	require.NoError(t, manuallyFlipColumnEncoding(ctx, dEnv, "issues", "description", val.StringAdaptiveEnc))

	// Confirm we start with one drift.
	before, err := schemadrift.ScanForDriftForTest(ctx, dEnv)
	require.NoError(t, err)
	require.Len(t, before, 1)

	// Repair.
	res, err := schemadrift.RepairColumnForTest(ctx, dEnv, "issues", "description")
	require.NoError(t, err)
	require.Equal(t, schemadrift.OutcomeFlipped, res.Outcome)
	require.Equal(t, val.StringAdaptiveEnc, res.OldEncoding)
	require.Equal(t, val.StringAddrEnc, res.NewEncoding)
	require.NotEmpty(t, res.CommitHash, "repair must produce a real dolt commit hash")
	require.Contains(t, res.CommitMessage, "issues.description",
		"commit message must name the specific table.column being repaired")
	require.Contains(t, res.CommitMessage, "StringAdaptiveEnc",
		"commit message must name the encoding being repaired FROM")
	require.Contains(t, res.CommitMessage, "StringAddrEnc",
		"commit message must name the encoding being repaired TO")
	require.Contains(t, res.CommitMessage, "data unchanged",
		"commit message must declare the data-unchanged contract for downstream consumers")

	// Drift should now be clean.
	after, err := schemadrift.ScanForDriftForTest(ctx, dEnv)
	require.NoError(t, err)
	require.Empty(t, after, "repair must remove the drift entry from check")

	// Idempotent: re-running repair returns OutcomeAlreadyOK with no new
	// commit hash, because the column is already at the legacy encoding.
	res2, err := schemadrift.RepairColumnForTest(ctx, dEnv, "issues", "description")
	require.NoError(t, err)
	require.Equal(t, schemadrift.OutcomeAlreadyOK, res2.Outcome,
		"repair on a non-drifted column must be an idempotent no-op")
	require.Empty(t, res2.CommitHash, "no-op repair must NOT produce a commit")
}

// TestSchemaEncodingDrift_RefusesGenuineAdaptiveData verifies the load-bearing
// safety guard: if a column's schema record says adaptive AND its on-disk
// payload IS in adaptive format (a non-corrupted column), repair must refuse
// rather than corrupt the column in the opposite direction.
//
// We test this WITHOUT manually flipping anything — we just create the table
// under UseAdaptiveEncoding=true (the v2.0.7 default), insert a row, and
// attempt repair. The column is internally consistent; the payload is
// genuinely adaptive; repair must refuse.
func TestSchemaEncodingDrift_RefusesGenuineAdaptiveData(t *testing.T) {
	ctx := context.Background()

	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = true
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()

	// Long enough to spill to addressed storage; payload will be encoded as
	// genuine adaptive-addressed under StringAdaptiveEnc.
	bigPayload := makeBigString(2048)
	_, err := sqle.ExecuteSql(ctx, dEnv,
		`CREATE TABLE issues (id INT PRIMARY KEY, description TEXT NOT NULL);
`+`INSERT INTO issues VALUES (1, '`+bigPayload+`');`)
	require.NoError(t, err)

	// Confirm pre-state: column declares adaptive, payload is genuine adaptive.
	// No drift expected.
	drifts, err := schemadrift.ScanForDriftForTest(ctx, dEnv)
	require.NoError(t, err)
	require.Empty(t, drifts, "genuine adaptive column must not register as drift")

	// Repair attempt must refuse. We expect an error path with OutcomeGenuineAdaptive.
	res, err := schemadrift.RepairColumnForTest(ctx, dEnv, "issues", "description")
	require.Error(t, err, "repair must refuse genuinely-adaptive payloads")
	require.Contains(t, err.Error(), "genuine",
		"refusal error must explain that the column is genuinely adaptive")
	require.Equal(t, schemadrift.OutcomeGenuineAdaptive, res.Outcome,
		"refused repair must return OutcomeGenuineAdaptive so callers can branch on it")
	require.Empty(t, res.CommitHash, "refused repair must NOT touch the working set")
}

// makeBigString builds a deterministic, easily-recognized payload of |n| bytes
// that is safely inlinable as a SQL string literal (no quotes, no backslashes).
func makeBigString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + (i % 26))
	}
	return string(b)
}

// manuallyFlipColumnEncoding rewrites the persisted schema of |tableName| so
// that the column |colName|'s TypeInfo encoding becomes |newEnc|. The row
// data is not touched. This simulates the v2.0.7 ALTER MODIFY corruption
// without going through the SQL ALTER path (which the schema-side fix in this
// branch now blocks).
func manuallyFlipColumnEncoding(ctx context.Context, dEnv *env.DoltEnv, tableName, colName string, newEnc val.Encoding) error {
	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return err
	}
	tbl, _, ok, err := doltdb.GetTableInsensitive(ctx, root, doltdb.TableName{Name: tableName})
	if err != nil {
		return err
	}
	if !ok {
		return doltdb.ErrTableNotFound
	}
	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return err
	}
	existing, ok := sch.GetAllCols().GetByNameCaseInsensitive(colName)
	if !ok {
		return doltdb.ErrTableNotFound
	}
	patched := existing
	patched.TypeInfo = existing.TypeInfo.WithEncoding(newEnc)

	cols := sch.GetAllCols().GetColumns()
	for i := range cols {
		if cols[i].Tag == patched.Tag {
			cols[i] = patched
			break
		}
	}
	newCC := schema.NewColCollection(cols...)
	pkCols := sch.GetPKCols().GetColumns()
	newPKCC := schema.NewColCollection(pkCols...)
	newSch, err := schema.NewSchema(newCC, sch.GetPkOrdinals(), sch.GetCollation(), schema.NewIndexCollection(newCC, newPKCC), sch.Checks())
	if err != nil {
		return err
	}
	for _, ix := range sch.Indexes().AllIndexes() {
		_, err := newSch.Indexes().AddIndexByColTags(ix.Name(), ix.IndexedColumnTags(), ix.PrefixLengths(), schema.IndexProperties{
			IsUnique:           ix.IsUnique(),
			IsSpatial:          ix.IsSpatial(),
			IsFullText:         ix.IsFullText(),
			IsVector:           ix.IsVector(),
			IsUserDefined:      ix.IsUserDefined(),
			Comment:            ix.Comment(),
			Predicate:          ix.Predicate(),
			FullTextProperties: ix.FullTextProperties(),
			VectorProperties:   ix.VectorProperties(),
		})
		if err != nil {
			return err
		}
	}
	updated, err := tbl.UpdateSchema(ctx, newSch)
	if err != nil {
		return err
	}
	newRoot, err := root.PutTable(ctx, doltdb.TableName{Name: tableName, Schema: doltdb.DefaultSchemaName}, updated)
	if err != nil {
		return err
	}
	return dEnv.UpdateWorkingRoot(ctx, newRoot)
}
