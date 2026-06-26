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
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/store/val"
)

// TestHeterogeneousSurfacing is the regression guard.
//
// Pre-fix: `scanTableForDrift` short-circuited per column on the first
// strong-evidence row. If the first row was FieldAdaptiveAddressed, the
// column was marked clean and silently dropped — even if subsequent rows
// proved heterogeneous. the empirical real-world data (real-world TEXT columns,
// issues.description) all hit this path: their first non-NULL row happened
// to be adaptive and the legacy rows further down the prolly map were
// invisible to check.
//
// Post-fix: scanner walks every row, tracks sawLegacy + sawAdaptive
// independently, and surfaces the heterogeneous bucket separately from drift
// + clean. The hint string points at `recover-rows`.
//
// Fixture: create a TEXT column under UseAdaptiveEncoding=false, insert a
// row with a long content string (encoded as a 20-byte legacy raw hash) +
// flip to adaptive and insert a second row (encoded as a varint+hash
// adaptive-addressed value). The two rows in the same column are now of
// different shapes — the column is exactly heterogeneous.
func TestHeterogeneousSurfacing(t *testing.T) {
	ctx := context.Background()

	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = false
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()

	// 1) Insert a row under legacy encoding — produces a legacy raw-hash
	// field for long content.
	legacyBlob := makeBigString(1024)
	_, err := sqle.ExecuteSql(ctx, dEnv,
		`CREATE TABLE issues (id INT PRIMARY KEY, description TEXT NOT NULL);
INSERT INTO issues VALUES (1, '`+legacyBlob+`');`)
	require.NoError(t, err)

	// 2) Flip the schema to adaptive WITHOUT touching the existing row.
	typeinfo.UseAdaptiveEncoding = true
	require.NoError(t, manuallyFlipColumnEncoding(ctx, dEnv, "issues", "description", val.StringAdaptiveEnc))

	// 3) Insert a second row under adaptive encoding — produces an
	// adaptive-addressed field for long content. The column now mixes both
	// shapes.
	adaptiveBlob := makeBigString(2048)
	_, err = sqle.ExecuteSql(ctx, dEnv,
		`INSERT INTO issues VALUES (2, '`+adaptiveBlob+`');`)
	require.NoError(t, err)

	drifts, err := schemadrift.ScanForDriftForTest(ctx, dEnv)
	require.NoError(t, err)
	require.Len(t, drifts, 1,
		"a heterogeneous column must surface as a single entry — not silently dropped (regression)")

	d := drifts[0]
	require.Equal(t, "issues", d.Table)
	require.Equal(t, "description", d.Column)
	require.Equal(t, "StringAdaptiveEnc", d.DeclaredEncoding)
	require.Equal(t, "StringAddrEnc", d.SuggestedEncoding)
	require.Equal(t, schemadrift.SeverityHeterogeneous, d.Severity,
		"heterogeneous column must use SeverityHeterogeneous, NOT drift or safe-empty")
	require.False(t, d.SafeToRepair,
		"heterogeneous rows must report SafeToRepair=false — repair would corrupt one side of the mix")
	require.Equal(t, "mixed-legacy-and-adaptive", d.ObservedFormat,
		"the observed-format label must explicitly say mixed so JSON consumers can branch")
	require.Contains(t, d.Hint, "recover-rows",
		"heterogeneous hint must explicitly point at the recover-rows command — that's the remediation")
	require.Contains(t, d.Hint, "--table issues",
		"hint must include the exact --table flag value so it's copy-pasteable")
	require.Contains(t, d.Hint, "--column description",
		"hint must include the exact --column flag value so it's copy-pasteable")
}

// TestSingleByteInlineEmptyDisqualifiesSafeEmpty is the the fix regression
// guard for the most dangerous safe-empty edge case.
//
// Pre-fix: a 1-byte `[0x00]` field (adaptive empty-inline) was admitted into
// the safe-empty bucket. After `repair --include-empty`, the legacy reader
// did `hash.New([1 byte])` → panic `invalid hash length: 1`. forensic analysis only
// validated the 20-byte all-zero shape; an earlier version over-extended safe-empty by
// keeping `sawHomogeneousEmptyOnly=true` for 1-byte rows too.
//
// Post-fix: any non-NULL FieldAdaptiveInline row with len != hashByteLen
// disqualifies safe-empty unconditionally. Only NULL rows and 20-byte
// all-zero rows are admitted.
//
// Fixture: column under adaptive encoding, three empty-string rows
// (`empty-string` → `[0x00]` adaptive empty-inline). The column has all 1-byte
// payloads. Without the fix it would land in safe-empty; with the fix it must NOT.
func TestSingleByteInlineEmptyDisqualifiesSafeEmpty(t *testing.T) {
	ctx := context.Background()

	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = true
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()

	_, err := sqle.ExecuteSql(ctx, dEnv,
		`CREATE TABLE issues (id INT PRIMARY KEY, description TEXT NOT NULL);
INSERT INTO issues VALUES (1, ''), (2, ''), (3, '');`)
	require.NoError(t, err)

	drifts, err := schemadrift.ScanForDriftForTest(ctx, dEnv)
	require.NoError(t, err)
	require.Empty(t, drifts,
		"a column of 1-byte adaptive empty-inline rows must NOT surface in safe-empty — repair --include-empty on such a column crashes the legacy reader with `invalid hash length: 1` (regression)")
}

// TestTwentyByteAllZeroStillSafeEmpty is the complement: confirm
// the canonical 20-byte all-zero shape (the forensically-validated pattern) still
// lands in safe-empty as before. This guards against an over-broad fix
// that would have made the entire safe-empty bucket inaccessible.
func TestTwentyByteAllZeroStillSafeEmpty(t *testing.T) {
	ctx := context.Background()

	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = false
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()

	// CREATE under legacy + INSERT empty strings → the prolly writer
	// materialises each row's `description` as a 20-byte address of the
	// empty-content chunk. The empty-content hash in dolt is the all-zero
	// hash (verified by the forensic dig).
	_, err := sqle.ExecuteSql(ctx, dEnv,
		`CREATE TABLE issues (id INT PRIMARY KEY, description TEXT NOT NULL);
INSERT INTO issues VALUES (1, ''), (2, ''), (3, '');`)
	require.NoError(t, err)

	typeinfo.UseAdaptiveEncoding = true
	require.NoError(t, manuallyFlipColumnEncoding(ctx, dEnv, "issues", "description", val.StringAdaptiveEnc))

	drifts, err := schemadrift.ScanForDriftForTest(ctx, dEnv)
	require.NoError(t, err)
	require.Len(t, drifts, 1,
		"the 20-byte all-zero shape (forensically-validated) must still surface as a single safe-empty entry")
	require.Equal(t, schemadrift.SeveritySafeEmpty, drifts[0].Severity,
		"the canonical 20-byte all-zero pattern stays in safe-empty (it's the bucket the forensic verdict applies to)")
	require.Contains(t, drifts[0].Hint, "--include-empty",
		"safe-empty hint must surface the --include-empty flag")
}

// TestDriftStaysDriftWithoutAdaptiveWitness is the negative
// guard: a column with ONLY legacy witnesses (no adaptive rows at all) must
// still land in pure drift, not heterogeneous. This pairs with
// TestHeterogeneousSurfacing — together they prove the fix
// distinguishes the two cases correctly instead of conflating them.
func TestDriftStaysDriftWithoutAdaptiveWitness(t *testing.T) {
	ctx := context.Background()

	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = false
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()

	// Three legacy rows, no adaptive rows.
	_, err := sqle.ExecuteSql(ctx, dEnv,
		`CREATE TABLE issues (id INT PRIMARY KEY, description TEXT NOT NULL);
INSERT INTO issues VALUES (1, '`+makeBigString(1024)+`'), (2, '`+makeBigString(1024)+`'), (3, '`+makeBigString(1024)+`');`)
	require.NoError(t, err)

	typeinfo.UseAdaptiveEncoding = true
	require.NoError(t, manuallyFlipColumnEncoding(ctx, dEnv, "issues", "description", val.StringAdaptiveEnc))

	drifts, err := schemadrift.ScanForDriftForTest(ctx, dEnv)
	require.NoError(t, err)
	require.Len(t, drifts, 1)
	require.Equal(t, schemadrift.SeverityDrift, drifts[0].Severity,
		"pure-legacy column must stay in drift even though scanner now walks every row — only the presence of an adaptive witness should promote to heterogeneous")
	require.True(t, drifts[0].SafeToRepair, "drift bucket is safe to repair by default")
	require.Contains(t, drifts[0].Hint, "repair --table issues --column description",
		"drift hint must point at the basic repair command without --include-empty")
}
