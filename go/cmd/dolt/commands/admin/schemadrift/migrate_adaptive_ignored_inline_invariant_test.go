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

// Guards the Option-C FORCE-INLINE INVARIANT for dolt_ignore'd tables.
//
// dolt_ignore'd tables (ignored_%, ignored_meta) live ONLY in the working set; their
// content chunks are never rooted in a commit. If migrate-adaptive emits an
// out-of-line (adaptive-ADDRESSED) value for such a table, persisting the
// working set dangling-faults on the un-rooted content chunk (the affected
// deploy-blocker). the force-inline mode's fix: force-INLINE every value for an ignored table
// so there are ZERO out-of-line chunk references to dangle on.
//
// This test does NOT reproduce the non-durable-chunk dangling-ref —
// that is state-dependent and only validated on real-world data (see validation notes / deploy notes). It DOES deterministically guard the fix's
// structural invariant: if anyone reverts ignored-table migration to emit
// out-of-line refs, the invariant breaks here.
//
// FAILS-WITHOUT the force-inline mode: a >2KB value migrates to adaptive-ADDRESSED (out-of-
// line) -> FieldAdaptiveAddressed present -> assertion fails.
// PASSES-WITH it: force-inlined -> every value FieldAdaptiveInline.
// Held uncommitted until the an earlier review force-inline fix lands.

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/admin/schemadrift"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
)

func TestMigrateAdaptive_DoltIgnoredTable_ForceInlineInvariant(t *testing.T) {
	ctx := context.Background()

	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = false // seed legacy rows
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	t.Cleanup(func() { dEnv.Close() })

	_, err := sqle.ExecuteSql(ctx, dEnv, `INSERT INTO dolt_ignore VALUES ('ignored_%', true);`)
	require.NoError(t, err)
	_, err = sqle.ExecuteSql(ctx, dEnv,
		`CREATE TABLE ignored_log (id INT PRIMARY KEY, actor VARCHAR(255) NOT NULL, body LONGTEXT NOT NULL);`)
	require.NoError(t, err)

	// A >2KB value: without force-inline this migrates to adaptive-ADDRESSED
	// (out-of-line), the shape that dangles when persisting an ignored table.
	big := strings.Repeat("a", 3000)
	short := "example inline content payload"
	_, err = sqle.ExecuteSql(ctx, dEnv, fmt.Sprintf(
		`INSERT INTO ignored_log VALUES (1, 'x', '%s'), (2, 'y', '%s');`, big, short))
	require.NoError(t, err)

	typeinfo.UseAdaptiveEncoding = true // default binary behavior during migrate

	res, err := schemadrift.MigrateAdaptiveColumnForTest(ctx, dEnv, "ignored_log", "body")
	require.NoError(t, err)
	require.Equal(t, schemadrift.RecoverRowsMigrated, res.Outcome)

	// INVARIANT: every value must be adaptive-inline — ZERO out-of-line refs.
	formats := collectFieldFormats(t, ctx, dEnv, "ignored_log", "body")
	require.False(t, formats[schemadrift.FieldAdaptiveAddressed],
		"Option-C force-inline: a dolt_ignore'd table must have NO out-of-line (addressed) values after migrate-adaptive")
	require.False(t, formats[schemadrift.FieldLegacyRawHash],
		"no legacy raw-hash references may remain after migrate-adaptive")

	// Content still round-trips byte-identical.
	got := collectColumnContent(t, ctx, dEnv, "ignored_log", "body")
	require.Equal(t, map[int]string{1: big, 2: short}, got,
		"content must round-trip exactly through the force-inline heal")
}
