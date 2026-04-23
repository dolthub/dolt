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

package enginetest

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

// TestNoOpCommitAllocsAreConstant verifies that calling DOLT_COMMIT with
// nothing staged allocates a constant number of heap objects regardless of
// how many tables exist in the schema. It measures allocation counts for a
// small schema and a large one, then fails if the ratio exceeds maxAllocRatio.
//
// See https://github.com/dolthub/dolt/issues/10851
func TestNoOpCommitAllocsAreConstant(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped in -short mode")
	}

	const (
		smallTables   = 1
		largeTables   = 60
		maxAllocRatio = 5.0
	)

	small := noOpCommitAllocs(t, smallTables)
	large := noOpCommitAllocs(t, largeTables)

	t.Logf("no-op DOLT_COMMIT allocs/op: %d tables -> %.0f, %d tables -> %.0f",
		smallTables, small, largeTables, large)

	if small == 0 {
		t.Skip("alloc count is 0 - possible GC interference; rerun")
	}

	ratio := large / small
	if ratio > maxAllocRatio {
		t.Errorf(
			"DOLT_COMMIT with nothing staged should allocate O(1) objects per call\n\n"+
				"  %2d tables -> %5.0f allocs/op\n"+
				"  %2d tables -> %5.0f allocs/op  (%.1fx, want <= %.1fx)\n",
			smallTables, small,
			largeTables, large, ratio, maxAllocRatio,
		)
	}
}

// noOpCommitAllocs returns the mean heap allocations per no-op DOLT_COMMIT
// call for a database containing |numTables| committed tables. runtime.GC is
// called before measurement to avoid counting allocations from prior setup.
func noOpCommitAllocs(t *testing.T, numTables int) float64 {
	t.Helper()

	h := newDoltHarness(t)
	defer h.Close()
	engine, err := h.NewEngine(t)
	require.NoError(t, err)
	defer engine.Close()
	ctx := enginetest.NewContext(h)

	runQuery := func(q string) {
		enginetest.RunQueryWithContext(t, engine, h, ctx, q)
	}
	for i := 0; i < numTables; i++ {
		runQuery(fmt.Sprintf("CREATE TABLE t%d (pk INT PRIMARY KEY)", i))
	}
	runQuery("CALL DOLT_COMMIT('-Am', 'initial commit')")

	runtime.GC()
	return testing.AllocsPerRun(100, func() {
		_, iter, _, _ := engine.Query(ctx, "CALL DOLT_COMMIT('-m', 'no-op')")
		if iter != nil {
			_, _ = sql.RowIterToRows(ctx, iter)
			_ = iter.Close(ctx)
		}
	})
}
