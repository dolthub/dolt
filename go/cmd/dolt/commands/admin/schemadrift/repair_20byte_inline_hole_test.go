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

// Regression for a review's repair hole: the 20-byte 0x00-LEADING
// adaptive-inline shape.
//
// scanColumnFullPayload's FieldAdaptiveInline branch treats a 20-byte 0x00-lead
// field as AMBIGUOUS (it could be a legacy raw hash that coincidentally starts
// 0x00, or an adaptive-inline value with 19 content bytes) and, on a chunkstore
// miss with non-zero content, only sets sawNonEmptyAmbiguous. With a legacy
// witness also present, repair's switch then falls through to FLIP — silently
// stranding that row: `[0x00]<19 bytes>` is NOT a valid 20-byte content-hash, so
// after the flip the StringAddr reader dereferences a bogus address (a QUIET
// misread, not a loud panic, since the field is exactly 20 bytes and hash.New
// accepts it). This is the same quiet-failure class the census flagged
// (BROKEN adaptive-inline ==20B) and is exactly why deploy verification must be
// content-byte-identical, not panic-based.
//
// repair MUST refuse this column. FAILS-WITHOUT the an earlier review fix (repair flips),
// PASSES-WITH it. Held uncommitted until the an earlier review repair fix lands.

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

// twentyByteInlineContent is 19 bytes -> adaptive inline encodes it as
// [0x00]<19 bytes> = exactly 20 bytes (the ambiguous "20-byte 0x00-leading"
// shape). Non-all-zero so it is not the safe-empty bucket.
const twentyByteInlineContent = "ABCDEFGHIJKLMNOPQRS" // len 19

func TestRepair_Refuses20ByteInlineWithLegacyWitness_REGRESSION(t *testing.T) {
	ctx := context.Background()

	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = false
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	dEnv := dtestutils.CreateTestEnv()
	t.Cleanup(func() { dEnv.Close() })

	_, err := sqle.ExecuteSql(ctx, dEnv,
		`CREATE TABLE issues (id INT PRIMARY KEY, actor VARCHAR(255) NOT NULL, description TEXT NOT NULL);`)
	require.NoError(t, err)

	// Legacy witness row (long -> legacy raw 20-byte hash).
	legacyPayload := strings.Repeat("L", 1500)
	_, err = sqle.ExecuteSql(ctx, dEnv, fmt.Sprintf(
		`INSERT INTO issues VALUES (1, 'a-1', '%s');`, legacyPayload))
	require.NoError(t, err)

	// Drift to adaptive, then insert the 19-byte value -> 20-byte 0x00-lead inline.
	require.NoError(t, manuallyFlipColumnEncoding(ctx, dEnv, "issues", "description", val.StringAdaptiveEnc))
	typeinfo.UseAdaptiveEncoding = true
	_, err = sqle.ExecuteSql(ctx, dEnv, fmt.Sprintf(
		`INSERT INTO issues VALUES (2, 'a-2', '%s');`, twentyByteInlineContent))
	require.NoError(t, err)

	// The fixture must actually contain a 20-byte 0x00-leading inline value AND a
	// legacy witness — otherwise it doesn't exercise the review's hole.
	require.True(t, hasTwentyByteZeroLeadInline(t, ctx, dEnv, "issues", "description"),
		"fixture must contain a 20-byte 0x00-leading adaptive-inline value (the ambiguous shape)")
	formats := collectFieldFormats(t, ctx, dEnv, "issues", "description")
	require.True(t, formats[schemadrift.FieldLegacyRawHash], "fixture must contain a legacy witness")

	// repair MUST refuse — flipping would silently strand the 20-byte inline row.
	res, err := schemadrift.RepairColumnForTest(ctx, dEnv, "issues", "description")
	require.Error(t, err,
		"repair MUST refuse a 20-byte 0x00-leading inline value with a legacy witness (flipping silently misreads it as a bogus hash)")
	require.NotEqual(t, schemadrift.OutcomeFlipped, res.Outcome,
		"repair MUST NOT flip — this is the quiet-corruption hole")
	require.Empty(t, res.CommitHash, "a refused repair must not create a commit")
}

// hasTwentyByteZeroLeadInline reports whether any row's |colName| field is
// exactly 20 bytes with a 0x00 leading byte and at least one non-zero content
// byte (i.e. a genuine adaptive-inline value of 19 content bytes, NOT the
// all-zero safe-empty sentinel).
func hasTwentyByteZeroLeadInline(t *testing.T, ctx context.Context, dEnv *env.DoltEnv, tableName, colName string) bool {
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
	iter, err := pm.IterAll(ctx)
	require.NoError(t, err)
	for {
		_, value, iterErr := iter.Next(ctx)
		if iterErr == io.EOF {
			return false
		}
		require.NoError(t, iterErr)
		b := value.GetField(tupleIdx)
		if len(b) == 20 && b[0] == 0 {
			for _, x := range b {
				if x != 0 {
					return true
				}
			}
		}
	}
}
