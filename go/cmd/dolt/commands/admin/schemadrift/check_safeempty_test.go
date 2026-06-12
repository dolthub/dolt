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

// TestCheckSurfacesSafeEmptyBucket is the load-bearing test for the
// homogeneous-empty bucket added earlier. The bug it exists to prevent: a
// column whose payload is uniformly the all-zero / empty pattern (every
// scanned row is NULL, `[0x00]` inline empty, or the 20-byte all-zero shape
// that arises when a legacy raw-hash address points at an empty/sparse
// chunk) gets silently dropped from the earlier check output. the forensic
// verdict says these ARE safe to flip; the updated version surfaces them under the new
// "safe-empty" severity so operators see them and can opt into repair via
// --include-empty.
//
// Fixture: create a TEXT column under UseAdaptiveEncoding=false, leave it
// empty (no inserts), then manually flip the persisted column TypeInfo to
// StringAdaptiveEnc. The row data side stays at its initial empty state;
// the only thing that's "drifted" is the schema record. With the earlier classifier
// this would be silently dropped because there's no row to witness either
// way. In the updated version it must surface under safe-empty.
func TestCheckSurfacesSafeEmptyBucket(t *testing.T) {
	ctx := context.Background()

	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = false
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()

	// Create a 2-column table and insert rows whose descriptions encode to
	// the all-zero 20-byte shape — concretely, rows where description is an
	// empty string. With StringAddrEnc the prolly writer materialises these
	// rows as `[0x00]` inline empties; that's the safe-empty pattern.
	_, err := sqle.ExecuteSql(ctx, dEnv,
		`CREATE TABLE issues (id INT PRIMARY KEY, description TEXT NOT NULL);
INSERT INTO issues VALUES (1, ''), (2, ''), (3, '');`)
	require.NoError(t, err)

	// No drift yet — schema is legacy, payload is consistent.
	before, err := schemadrift.ScanForDriftForTest(ctx, dEnv)
	require.NoError(t, err)
	require.Empty(t, before)

	// Flip the global flag to the 2.0.7+ default and manually rewrite the
	// persisted schema's column encoding from StringAddrEnc to
	// StringAdaptiveEnc — exactly the v2.0.7 ALTER-MODIFY corruption shape,
	// but on a column whose payload is uniformly empty.
	typeinfo.UseAdaptiveEncoding = true
	require.NoError(t, manuallyFlipColumnEncoding(ctx, dEnv, "issues", "description", val.StringAdaptiveEnc))

	drifts, err := schemadrift.ScanForDriftForTest(ctx, dEnv)
	require.NoError(t, err)
	require.Len(t, drifts, 1,
		"a column with adaptive schema + homogeneous-empty payload must surface as a single safe-empty entry")
	d := drifts[0]
	require.Equal(t, "issues", d.Table)
	require.Equal(t, "description", d.Column)
	require.Equal(t, "StringAdaptiveEnc", d.DeclaredEncoding)
	require.Equal(t, "StringAddrEnc", d.SuggestedEncoding)
	require.Equal(t, schemadrift.SeveritySafeEmpty, d.Severity,
		"safe-empty bucket must use the SeveritySafeEmpty label")
	require.True(t, d.SafeToRepair,
		"safe-empty rows must report SafeToRepair=true; the repair gate is at the --include-empty flag level")
	require.Equal(t, "homogeneous-empty", d.ObservedFormat,
		"safe-empty rows must report the homogeneous-empty observed-format label so JSON consumers can branch on it")
}

// TestRepairRefusesSafeEmptyWithoutFlag confirms that the default
// repair posture stays conservative: a column in the safe-empty bucket is
// refused unless the operator explicitly opts in with --include-empty.
func TestRepairRefusesSafeEmptyWithoutFlag(t *testing.T) {
	ctx := context.Background()

	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = false
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()

	_, err := sqle.ExecuteSql(ctx, dEnv,
		`CREATE TABLE issues (id INT PRIMARY KEY, description TEXT NOT NULL);
INSERT INTO issues VALUES (1, ''), (2, '');`)
	require.NoError(t, err)

	typeinfo.UseAdaptiveEncoding = true
	require.NoError(t, manuallyFlipColumnEncoding(ctx, dEnv, "issues", "description", val.StringAdaptiveEnc))

	res, err := schemadrift.RepairColumnForTest(ctx, dEnv, "issues", "description")
	require.Error(t, err, "default repair must refuse the safe-empty bucket without --include-empty")
	require.Contains(t, err.Error(), "homogeneous-empty",
		"the refusal must name the bucket so the operator knows which flag to pass next")
	require.Contains(t, err.Error(), "--include-empty",
		"the refusal must surface the opt-in flag name verbatim so it's discoverable from the error")
	require.Equal(t, schemadrift.OutcomeGenuineAdaptive, res.Outcome,
		"safe-empty refusal still uses OutcomeGenuineAdaptive — there's no separate outcome enum and the existing one signals 'refused, no flip applied'")
	require.Empty(t, res.CommitHash, "refused repair must not touch the working set")
}

// TestRepairAcceptsSafeEmptyWithFlag verifies the --include-empty
// opt-in path: with the flag, the safe-empty bucket is treated as drift,
// the schema flips to legacy, and a real dolt commit lands.
func TestRepairAcceptsSafeEmptyWithFlag(t *testing.T) {
	ctx := context.Background()

	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = false
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()

	_, err := sqle.ExecuteSql(ctx, dEnv,
		`CREATE TABLE issues (id INT PRIMARY KEY, description TEXT NOT NULL);
INSERT INTO issues VALUES (1, ''), (2, '');`)
	require.NoError(t, err)

	typeinfo.UseAdaptiveEncoding = true
	require.NoError(t, manuallyFlipColumnEncoding(ctx, dEnv, "issues", "description", val.StringAdaptiveEnc))

	res, err := schemadrift.RepairColumnWithIncludeEmptyForTest(ctx, dEnv, "issues", "description", true)
	require.NoError(t, err, "repair with --include-empty must accept the safe-empty bucket")
	require.Equal(t, schemadrift.OutcomeFlipped, res.Outcome)
	require.Equal(t, val.StringAdaptiveEnc, res.OldEncoding)
	require.Equal(t, val.StringAddrEnc, res.NewEncoding)
	require.NotEmpty(t, res.CommitHash, "include-empty repair must still produce a real dolt commit")
	require.Contains(t, res.CommitMessage, "issues.description")
	require.Contains(t, res.CommitMessage, "data unchanged",
		"include-empty repair must still carry the data-unchanged guarantee in the audit message")

	// Subsequent check is now clean.
	after, err := schemadrift.ScanForDriftForTest(ctx, dEnv)
	require.NoError(t, err)
	require.Empty(t, after, "after the flip the column is internally consistent on both sides")
}
