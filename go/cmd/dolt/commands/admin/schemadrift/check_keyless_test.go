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

// Keyless tables prepend a cardinality field to the value tuple
// (schemaImpl.GetValueDescriptor inserts val.KeylessCardType at index 0), so
// every user column lives at value-tuple index N+1 relative to the NonPKCols
// ordering. recover-rows and migrate-adaptive refuse keyless tables because
// of exactly that ordinal mismatch — but check is the diagnostic entry point
// and must DETECT drift on keyless tables, not silently scan the wrong field
// and report the database clean. These tests pin that behavior.

// TestKeyless_CheckDetectsDrift reproduces the customer-reported false
// negative: a keyless table with a TEXT column whose persisted schema tag
// drifted to StringAdaptiveEnc while its rows still hold legacy 20-byte raw
// hashes. check must report exactly one drift row for it.
func TestKeyless_CheckDetectsDrift(t *testing.T) {
	ctx := context.Background()

	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = false
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()

	// No PRIMARY KEY → keyless table. The 1024-byte payload spills to
	// out-of-band storage, so under StringAddrEnc the row field is a legacy
	// raw 20-byte hash.
	bigPayload := makeBigString(1024)
	_, err := sqle.ExecuteSql(ctx, dEnv,
		`CREATE TABLE events (id INT, description TEXT NOT NULL);
`+`INSERT INTO events VALUES (1, '`+bigPayload+`');`)
	require.NoError(t, err)

	// Sanity: before the simulated corruption there is no drift.
	before, err := schemadrift.ScanForDriftForTest(ctx, dEnv)
	require.NoError(t, err)
	require.Empty(t, before, "before corruption: schema and payload agree")

	require.NoError(t, manuallyFlipColumnEncoding(ctx, dEnv, "events", "description", val.StringAdaptiveEnc))

	after, err := schemadrift.ScanForDriftForTest(ctx, dEnv)
	require.NoError(t, err)
	require.Len(t, after, 1, "keyless drifted column must be detected (cardinality-prefix offset)")
	d := after[0]
	require.Equal(t, "events", d.Table)
	require.Equal(t, "description", d.Column)
	require.Equal(t, schemadrift.SeverityDrift, d.Severity)
	require.Equal(t, "StringAdaptiveEnc", d.DeclaredEncoding)
	require.Equal(t, "legacy-raw-hash", d.ObservedFormat)
	require.Equal(t, "StringAddrEnc", d.SuggestedEncoding)
	require.True(t, d.SafeToRepair)
}

// TestKeyless_CheckCleanAdaptiveStaysClean guards the other direction: a
// genuinely adaptive keyless column must NOT surface as drift (i.e. the
// keyless offset is applied exactly once, not zero or two times). Without the
// offset the scanner would read the neighboring field — for this fixture the
// 4-byte INT — and misclassify.
func TestKeyless_CheckCleanAdaptiveStaysClean(t *testing.T) {
	ctx := context.Background()

	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = true
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()

	bigPayload := makeBigString(2048)
	_, err := sqle.ExecuteSql(ctx, dEnv,
		`CREATE TABLE events (id INT, description TEXT NOT NULL);
`+`INSERT INTO events VALUES (1, '`+bigPayload+`');`)
	require.NoError(t, err)

	drifts, err := schemadrift.ScanForDriftForTest(ctx, dEnv)
	require.NoError(t, err)
	require.Empty(t, drifts, "genuine adaptive keyless column must not register as drift")
}

// TestKeyless_RepairFlipsTag verifies the write side: repair shares the
// value-tuple index computation with check (valueTupleIndexForColumn), so a
// keyless drifted column must be repairable once detected — the flip itself
// is a schema-only change and is as safe on a keyless table as on a keyed
// one. Before the fix, repair scanned the wrong field, saw an ambiguous
// payload, and refused.
func TestKeyless_RepairFlipsTag(t *testing.T) {
	ctx := context.Background()

	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = false
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()

	bigPayload := makeBigString(1024)
	_, err := sqle.ExecuteSql(ctx, dEnv,
		`CREATE TABLE events (id INT, description TEXT NOT NULL);
`+`INSERT INTO events VALUES (1, '`+bigPayload+`');`)
	require.NoError(t, err)
	require.NoError(t, manuallyFlipColumnEncoding(ctx, dEnv, "events", "description", val.StringAdaptiveEnc))

	res, err := schemadrift.RepairColumnForTest(ctx, dEnv, "events", "description")
	require.NoError(t, err)
	require.Equal(t, schemadrift.OutcomeFlipped, res.Outcome)
	require.Equal(t, val.StringAdaptiveEnc, res.OldEncoding)
	require.Equal(t, val.StringAddrEnc, res.NewEncoding)

	after, err := schemadrift.ScanForDriftForTest(ctx, dEnv)
	require.NoError(t, err)
	require.Empty(t, after, "repair must clear the keyless drift entry")
}

// TestKeyless_CheckDetectsHeterogeneous covers the mixed-payload keyless
// case: legacy rows written before the corrupting ALTER plus adaptive rows
// written after it. check must classify the column heterogeneous (not clean,
// not pure drift).
func TestKeyless_CheckDetectsHeterogeneous(t *testing.T) {
	ctx := context.Background()

	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = false
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()

	bigPayload := makeBigString(1024)
	_, err := sqle.ExecuteSql(ctx, dEnv,
		`CREATE TABLE events (id INT, description TEXT NOT NULL);
`+`INSERT INTO events VALUES (1, '`+bigPayload+`');`)
	require.NoError(t, err)

	// Flip the schema tag, then write a second row THROUGH the adaptive
	// encoding so the column payload mixes legacy and adaptive rows.
	require.NoError(t, manuallyFlipColumnEncoding(ctx, dEnv, "events", "description", val.StringAdaptiveEnc))

	typeinfo.UseAdaptiveEncoding = true
	bigPayload2 := makeBigString(2048)
	_, err = sqle.ExecuteSql(ctx, dEnv,
		`INSERT INTO events VALUES (2, '`+bigPayload2+`');`)
	require.NoError(t, err)

	drifts, err := schemadrift.ScanForDriftForTest(ctx, dEnv)
	require.NoError(t, err)
	require.Len(t, drifts, 1, "heterogeneous keyless column must be surfaced")
	require.Equal(t, schemadrift.SeverityHeterogeneous, drifts[0].Severity)
	require.False(t, drifts[0].SafeToRepair)
}
