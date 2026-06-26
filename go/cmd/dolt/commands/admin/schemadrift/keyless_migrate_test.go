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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/admin/schemadrift"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/store/val"
)

// Keyless row-migration tests. Keyless tables key each row by
// xxh3.Hash128 of the value tuple's field bytes (val.HashTupleFromValue), so
// rewriting a row's field bytes invalidates its stored key, and the rewritten
// bytes must be exactly what the engine's TupleBuilder would produce for the
// same logical content — otherwise subsequent UPDATE/DELETE statements hash a
// freshly-built tuple, miss the stored key, and silently affect zero rows.
// These tests therefore verify migrations on keyless tables at the ENGINE
// level: after migrating, SQL DELETEs must actually find and remove rows.

// keylessRowCount returns SELECT COUNT(*) for the given table.
func keylessRowCount(t *testing.T, ctx context.Context, dEnv *env.DoltEnv, table string) int {
	t.Helper()
	root, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)
	rows, err := sqle.ExecuteSelect(ctx, dEnv, root, fmt.Sprintf(`SELECT COUNT(*) FROM %s;`, table))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	switch v := rows[0][0].(type) {
	case int64:
		return int(v)
	case int32:
		return int(v)
	case int:
		return v
	default:
		t.Fatalf("unexpected COUNT(*) type %T", rows[0][0])
		return -1
	}
}

// TestKeyless_MigrateAdaptive_EndToEnd drives the forward migration on a
// drifted keyless column and proves engine-level integrity afterward.
//
// The 100-byte payload is the load-bearing case: its engine-canonical adaptive
// form is INLINE (tuple well under the 2048-byte target), so a migration that
// re-encodes by a fixed content-size rule (e.g. "inline iff <= 20 bytes")
// produces out-of-band bytes the engine would never build — the row's stored
// key then never matches an engine-computed hash and DELETE silently misses.
func TestKeyless_MigrateAdaptive_EndToEnd(t *testing.T) {
	ctx := context.Background()

	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = false
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()

	smallPayload := strings.Repeat("s", 100)  // canonical adaptive: inline
	bigPayload := strings.Repeat("b", 5000)   // canonical adaptive: out-of-band
	_, err := sqle.ExecuteSql(ctx, dEnv, fmt.Sprintf(
		`CREATE TABLE events (id INT, description TEXT NOT NULL);
INSERT INTO events VALUES (1, '%s'), (2, '%s');`, smallPayload, bigPayload))
	require.NoError(t, err)

	// Simulate the v2.0.7 drift: schema says adaptive, rows are legacy.
	require.NoError(t, manuallyFlipColumnEncoding(ctx, dEnv, "events", "description", val.StringAdaptiveEnc))
	typeinfo.UseAdaptiveEncoding = true

	res, err := schemadrift.MigrateAdaptiveColumnForTest(ctx, dEnv, "events", "description")
	require.NoError(t, err, "migrate-adaptive must support keyless tables")
	require.Equal(t, schemadrift.RecoverRowsMigrated, res.Outcome)
	require.Equal(t, val.StringAdaptiveEnc, res.NewEncoding)
	require.Equal(t, 2, res.RowsScanned)
	require.Equal(t, 2, res.RowsRewritten)

	// check must be clean afterward.
	drifts, err := schemadrift.ScanForDriftForTest(ctx, dEnv)
	require.NoError(t, err)
	require.Empty(t, drifts, "post-migration keyless column must scan clean")

	// Content must be readable and byte-identical.
	content := collectColumnContent(t, ctx, dEnv, "events", "description")
	require.Equal(t, map[int]string{1: smallPayload, 2: bigPayload}, content)

	// The engine-level proof: DELETE locates keyless rows by hashing a
	// freshly-built value tuple. Both deletes must actually remove their row.
	require.Equal(t, 2, keylessRowCount(t, ctx, dEnv, "events"))
	_, err = sqle.ExecuteSql(ctx, dEnv, `DELETE FROM events WHERE id = 1;`)
	require.NoError(t, err)
	require.Equal(t, 1, keylessRowCount(t, ctx, dEnv, "events"),
		"DELETE of the inline-canonical row must find it by hash (engine-canonical bytes + rehashed key)")
	_, err = sqle.ExecuteSql(ctx, dEnv, `DELETE FROM events WHERE id = 2;`)
	require.NoError(t, err)
	require.Equal(t, 0, keylessRowCount(t, ctx, dEnv, "events"),
		"DELETE of the out-of-band-canonical row must find it by hash")
}

// TestKeyless_RecoverRows_EndToEnd drives the backward (to-legacy) migration
// on a heterogeneous keyless column — legacy rows written before the
// corrupting ALTER, adaptive rows written after it — and proves engine-level
// integrity afterward: every row readable, every row deletable.
func TestKeyless_RecoverRows_EndToEnd(t *testing.T) {
	ctx := context.Background()

	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = false
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()

	legacyPayload := strings.Repeat("L", 1500)
	_, err := sqle.ExecuteSql(ctx, dEnv, fmt.Sprintf(
		`CREATE TABLE events (id INT, description TEXT NOT NULL);
INSERT INTO events VALUES (1, '%s');`, legacyPayload))
	require.NoError(t, err)

	require.NoError(t, manuallyFlipColumnEncoding(ctx, dEnv, "events", "description", val.StringAdaptiveEnc))

	// Rows written through the drifted adaptive tag: one inline, one addressed.
	typeinfo.UseAdaptiveEncoding = true
	adaptiveShort := "short adaptive payload"
	adaptiveLong := strings.Repeat("A", 3000)
	_, err = sqle.ExecuteSql(ctx, dEnv, fmt.Sprintf(
		`INSERT INTO events VALUES (2, '%s'), (3, '%s');`, adaptiveShort, adaptiveLong))
	require.NoError(t, err)

	res, err := schemadrift.RecoverRowsColumnForTest(ctx, dEnv, "events", "description")
	require.NoError(t, err, "recover-rows must support keyless tables")
	require.Equal(t, schemadrift.RecoverRowsMigrated, res.Outcome)
	require.Equal(t, val.StringAddrEnc, res.NewEncoding)
	require.Equal(t, 3, res.RowsScanned)
	require.GreaterOrEqual(t, res.RowsRewritten, 2, "both adaptive rows must be rewritten")

	drifts, err := schemadrift.ScanForDriftForTest(ctx, dEnv)
	require.NoError(t, err)
	require.Empty(t, drifts, "post-migration keyless column must scan clean")

	content := collectColumnContent(t, ctx, dEnv, "events", "description")
	require.Equal(t, map[int]string{1: legacyPayload, 2: adaptiveShort, 3: adaptiveLong}, content)

	// Engine-level: every row must be deletable. Rows 2 and 3 were re-keyed
	// by the migration; row 1 kept its original key.
	for expected, id := 2, 1; id <= 3; id, expected = id+1, expected-1 {
		_, err = sqle.ExecuteSql(ctx, dEnv, fmt.Sprintf(`DELETE FROM events WHERE id = %d;`, id))
		require.NoError(t, err)
		require.Equal(t, expected, keylessRowCount(t, ctx, dEnv, "events"),
			"DELETE of row %d must find it by hash after recover-rows", id)
	}
}

// TestKeyless_RecoverRows_DuplicateMergesCardinality covers the collision
// case: the SAME logical row stored twice — once in legacy form (pre-drift
// insert) and once in adaptive form (post-drift insert). In a keyless table
// those are two distinct map entries (different bytes → different hash ids),
// but after canonicalization they become byte-identical, so the migration
// must MERGE them into one entry with cardinality 2 — not let the second
// rewrite silently overwrite the first (which would lose a row).
func TestKeyless_RecoverRows_DuplicateMergesCardinality(t *testing.T) {
	ctx := context.Background()

	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = false
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()

	payload := strings.Repeat("D", 1500)
	_, err := sqle.ExecuteSql(ctx, dEnv, fmt.Sprintf(
		`CREATE TABLE events (id INT, description TEXT NOT NULL);
INSERT INTO events VALUES (1, '%s');`, payload))
	require.NoError(t, err)

	require.NoError(t, manuallyFlipColumnEncoding(ctx, dEnv, "events", "description", val.StringAdaptiveEnc))

	// Insert the IDENTICAL logical row through the drifted adaptive tag.
	typeinfo.UseAdaptiveEncoding = true
	_, err = sqle.ExecuteSql(ctx, dEnv, fmt.Sprintf(
		`INSERT INTO events VALUES (1, '%s');`, payload))
	require.NoError(t, err)

	res, err := schemadrift.RecoverRowsColumnForTest(ctx, dEnv, "events", "description")
	require.NoError(t, err)
	require.Equal(t, schemadrift.RecoverRowsMigrated, res.Outcome)
	require.Equal(t, 2, res.RowsScanned)
	require.Equal(t, 1, res.RowsMerged,
		"the adaptive duplicate must merge into the legacy entry's key, not overwrite it")

	// Both logical rows must survive the merge (one entry, cardinality 2).
	require.Equal(t, 2, keylessRowCount(t, ctx, dEnv, "events"),
		"duplicate logical rows must be preserved via cardinality merge")

	// And both must be deletable in one statement.
	_, err = sqle.ExecuteSql(ctx, dEnv, `DELETE FROM events WHERE id = 1;`)
	require.NoError(t, err)
	require.Equal(t, 0, keylessRowCount(t, ctx, dEnv, "events"))
}

// TestKeyless_Migrations_RefuseSecondaryIndexes pins the honest boundary:
// keyless secondary index entries embed each row's hash id, and re-keying
// rows invalidates them. Until the migrations rebuild secondary indexes, they
// must refuse keyless tables that have any — with an error that tells the
// operator the workaround (drop, migrate, re-create).
func TestKeyless_Migrations_RefuseSecondaryIndexes(t *testing.T) {
	ctx := context.Background()

	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = false
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.Close()

	payload := strings.Repeat("X", 1500)
	_, err := sqle.ExecuteSql(ctx, dEnv, fmt.Sprintf(
		`CREATE TABLE events (id INT, description TEXT NOT NULL);
CREATE INDEX id_idx ON events (id);
INSERT INTO events VALUES (1, '%s');`, payload))
	require.NoError(t, err)

	require.NoError(t, manuallyFlipColumnEncoding(ctx, dEnv, "events", "description", val.StringAdaptiveEnc))

	_, err = schemadrift.RecoverRowsColumnForTest(ctx, dEnv, "events", "description")
	require.Error(t, err, "recover-rows must refuse a keyless table with secondary indexes")
	require.Contains(t, err.Error(), "secondary index")

	_, err = schemadrift.MigrateAdaptiveColumnForTest(ctx, dEnv, "events", "description")
	require.Error(t, err, "migrate-adaptive must refuse a keyless table with secondary indexes")
	require.Contains(t, err.Error(), "secondary index")
}
