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

package kvexec

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMergeJoin ensures that we trigger the merge join operator for expected query patterns.
func TestMergeJoin(t *testing.T) {
	tests := []struct {
		name   string
		query  string
		doKvex bool
	}{
		{
			name:   "accept simple merge join",
			query:  "select /*+ MERGE_JOIN(xy,ab) */ * from xy join ab on x = a",
			doKvex: true,
		},
		{
			name:   "accept merge join on a secondary index",
			query:  "select /*+ MERGE_JOIN(xy,sec) */ * from xy join sec on x = k",
			doKvex: true,
		},
		{
			name:   "accept merge join with a locking clause",
			query:  "select /*+ MERGE_JOIN(xy,ab) */ * from xy join ab on x = a for update",
			doKvex: true,
		},
		{
			name:   "accept merge join on a secondary index with a locking clause",
			query:  "select /*+ MERGE_JOIN(xy,sec) */ * from xy join sec on x = k for update of sec",
			doKvex: true,
		},
		{
			name:   "accept keyless merge join with a locking clause",
			query:  "select /*+ MERGE_JOIN(xy,keyless) */ * from xy join keyless on x = k lock in share mode",
			doKvex: true,
		},
	}

	engine, sqlCtx := setupRowLookupJoinEngine(t, rowLookupJoinSetup)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iter := buildJoinWithKvexec(t, engine, sqlCtx, tt.query)
			_, ok := iter.(*mergeJoinKvIter)
			require.Equalf(t, tt.doKvex, ok, "expected merge kvexec: %t, got %T", tt.doKvex, iter)
		})
	}
}
