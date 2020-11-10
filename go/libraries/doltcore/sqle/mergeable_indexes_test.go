// Copyright 2020 Dolthub, Inc.
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

package sqle

import (
	"context"
	"fmt"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/lookup"
)

// This tests mergeable indexes by using the SQL engine and intercepting specific calls. This way, we can verify that
// the engine is intersecting and combining the proper number of lookups, and we can also examine the ranges before
// they're converted into a format that Noms understands to verify that they were handled correctly. Lastly, we ensure
// that the final output is as expected.
func TestMergeableIndexes(t *testing.T) {
	engine, db, idxv1, idxv2v1 := setupMergeableIndexes(t)

	tests := []struct {
		whereStmt      string
		lookupsCounted int
		finalRanges    []lookup.Range
		pks            []int64
	}{
		{
			"v1 = 11",
			0,
			[]lookup.Range{
				lookup.ClosedRange(idxv1.tuple(false, 11), idxv1.tuple(true, 11)),
			},
			[]int64{1},
		},
		{
			"v1 = 11 OR v1 = 15",
			2,
			[]lookup.Range{
				lookup.ClosedRange(idxv1.tuple(false, 11), idxv1.tuple(true, 11)),
				lookup.ClosedRange(idxv1.tuple(false, 15), idxv1.tuple(true, 15)),
			},
			[]int64{1, 5},
		},
		{
			"v1 = 11 AND v1 = 15",
			2,
			[]lookup.Range{
				lookup.EmptyRange(),
			},
			[]int64{},
		},
		{
			"v1 = 11 OR v1 = 15 OR v1 = 19",
			4,
			[]lookup.Range{
				lookup.ClosedRange(idxv1.tuple(false, 11), idxv1.tuple(true, 11)),
				lookup.ClosedRange(idxv1.tuple(false, 15), idxv1.tuple(true, 15)),
				lookup.ClosedRange(idxv1.tuple(false, 19), idxv1.tuple(true, 19)),
			},
			[]int64{1, 5, 9},
		},
		{
			"v1 = 11 OR v1 = 15 AND v1 = 19",
			4,
			[]lookup.Range{
				lookup.ClosedRange(idxv1.tuple(false, 11), idxv1.tuple(true, 11)),
			},
			[]int64{1},
		},
		{
			"v1 = 11 AND v1 = 15 AND v1 = 19",
			4,
			[]lookup.Range{
				lookup.EmptyRange(),
			},
			[]int64{},
		},
		{
			"v1 = 11 OR v1 > 15",
			2,
			[]lookup.Range{
				lookup.ClosedRange(idxv1.tuple(false, 11), idxv1.tuple(true, 11)),
				lookup.GreaterThanRange(idxv1.tuple(true, 15)),
			},
			[]int64{1, 6, 7, 8, 9},
		},
		{
			"v1 = 11 AND v1 > 15",
			2,
			[]lookup.Range{
				lookup.EmptyRange(),
			},
			[]int64{},
		},
		{
			"v1 = 11 OR v1 = 15 OR v1 > 19",
			4,
			[]lookup.Range{
				lookup.ClosedRange(idxv1.tuple(false, 11), idxv1.tuple(true, 11)),
				lookup.ClosedRange(idxv1.tuple(false, 15), idxv1.tuple(true, 15)),
				lookup.GreaterThanRange(idxv1.tuple(true, 19)),
			},
			[]int64{1, 5},
		},
		{
			"v1 = 11 OR v1 = 15 AND v1 > 19",
			4,
			[]lookup.Range{
				lookup.ClosedRange(idxv1.tuple(false, 11), idxv1.tuple(true, 11)),
			},
			[]int64{1},
		},
		{
			"v1 = 11 AND v1 = 15 OR v1 > 19",
			4,
			[]lookup.Range{
				lookup.GreaterThanRange(idxv1.tuple(true, 19)),
			},
			[]int64{},
		},
		{
			"v1 = 11 AND v1 = 15 AND v1 > 19",
			4,
			[]lookup.Range{
				lookup.EmptyRange(),
			},
			[]int64{},
		},
		{
			"v1 = 11 OR v1 >= 15",
			2,
			[]lookup.Range{
				lookup.ClosedRange(idxv1.tuple(false, 11), idxv1.tuple(true, 11)),
				lookup.GreaterOrEqualRange(idxv1.tuple(false, 15)),
			},
			[]int64{1, 5, 6, 7, 8, 9},
		},
		{
			"v1 = 11 AND v1 >= 15",
			2,
			[]lookup.Range{
				lookup.EmptyRange(),
			},
			[]int64{},
		},
		{
			"v1 = 11 OR v1 = 15 OR v1 >= 19",
			4,
			[]lookup.Range{
				lookup.ClosedRange(idxv1.tuple(false, 11), idxv1.tuple(true, 11)),
				lookup.ClosedRange(idxv1.tuple(false, 15), idxv1.tuple(true, 15)),
				lookup.GreaterOrEqualRange(idxv1.tuple(false, 19)),
			},
			[]int64{1, 5, 9},
		},
		{
			"v1 = 11 OR v1 = 15 AND v1 >= 19",
			4,
			[]lookup.Range{
				lookup.ClosedRange(idxv1.tuple(false, 11), idxv1.tuple(true, 11)),
			},
			[]int64{1},
		},
		{
			"v1 = 11 AND v1 = 15 OR v1 >= 19",
			4,
			[]lookup.Range{
				lookup.GreaterOrEqualRange(idxv1.tuple(false, 19)),
			},
			[]int64{9},
		},
		{
			"v1 = 11 AND v1 = 15 AND v1 >= 19",
			4,
			[]lookup.Range{
				lookup.EmptyRange(),
			},
			[]int64{},
		},
		{
			"v1 = 11 OR v1 < 15",
			2,
			[]lookup.Range{
				lookup.LessThanRange(idxv1.tuple(false, 15)),
			},
			[]int64{0, 1, 2, 3, 4},
		},
		{
			"v1 = 11 AND v1 < 15",
			2,
			[]lookup.Range{
				lookup.ClosedRange(idxv1.tuple(false, 11), idxv1.tuple(true, 11)),
			},
			[]int64{1},
		},
		{
			"v1 = 11 OR v1 = 15 OR v1 < 19",
			4,
			[]lookup.Range{
				lookup.LessThanRange(idxv1.tuple(false, 19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			"v1 = 11 OR v1 = 15 AND v1 < 19",
			4,
			[]lookup.Range{
				lookup.ClosedRange(idxv1.tuple(false, 11), idxv1.tuple(true, 11)),
				lookup.ClosedRange(idxv1.tuple(false, 15), idxv1.tuple(true, 15)),
			},
			[]int64{1, 5},
		},
		{
			"v1 = 11 AND v1 = 15 OR v1 < 19",
			4,
			[]lookup.Range{
				lookup.LessThanRange(idxv1.tuple(false, 19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			"v1 = 11 AND v1 = 15 AND v1 < 19",
			4,
			[]lookup.Range{
				lookup.EmptyRange(),
			},
			[]int64{},
		},
		{
			"v1 = 11 OR v1 <= 15",
			2,
			[]lookup.Range{
				lookup.LessOrEqualRange(idxv1.tuple(true, 15)),
			},
			[]int64{0, 1, 2, 3, 4, 5},
		},
		{
			"v1 = 11 AND v1 <= 15",
			2,
			[]lookup.Range{
				lookup.ClosedRange(idxv1.tuple(false, 11), idxv1.tuple(true, 11)),
			},
			[]int64{1},
		},
		{
			"v1 = 11 OR v1 = 15 OR v1 <= 19",
			4,
			[]lookup.Range{
				lookup.LessOrEqualRange(idxv1.tuple(true, 19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 = 11 OR v1 = 15 AND v1 <= 19",
			4,
			[]lookup.Range{
				lookup.ClosedRange(idxv1.tuple(false, 11), idxv1.tuple(true, 11)),
				lookup.ClosedRange(idxv1.tuple(false, 15), idxv1.tuple(true, 15)),
			},
			[]int64{1, 5},
		},
		{
			"v1 = 11 AND v1 = 15 OR v1 <= 19",
			4,
			[]lookup.Range{
				lookup.LessOrEqualRange(idxv1.tuple(true, 19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 = 11 AND v1 = 15 AND v1 <= 19",
			4,
			[]lookup.Range{
				lookup.EmptyRange(),
			},
			[]int64{},
		},
		{
			"v1 > 11",
			0,
			[]lookup.Range{
				lookup.GreaterThanRange(idxv1.tuple(true, 11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 OR v1 > 15",
			2,
			[]lookup.Range{
				lookup.GreaterThanRange(idxv1.tuple(true, 11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15",
			2,
			[]lookup.Range{
				lookup.GreaterThanRange(idxv1.tuple(true, 15)),
			},
			[]int64{6, 7, 8, 9},
		},
		{
			"v1 > 11 OR v1 > 15 OR v1 > 19",
			4,
			[]lookup.Range{
				lookup.GreaterThanRange(idxv1.tuple(true, 11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 OR v1 > 15 AND v1 > 19",
			4,
			[]lookup.Range{
				lookup.GreaterThanRange(idxv1.tuple(true, 11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 AND v1 > 19",
			4,
			[]lookup.Range{
				lookup.GreaterThanRange(idxv1.tuple(true, 19)),
			},
			[]int64{},
		},
		{
			"v1 > 11 OR v1 >= 15",
			2,
			[]lookup.Range{
				lookup.GreaterThanRange(idxv1.tuple(true, 11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 >= 15",
			2,
			[]lookup.Range{
				lookup.GreaterOrEqualRange(idxv1.tuple(false, 15)),
			},
			[]int64{5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 OR v1 > 15 OR v1 >= 19",
			4,
			[]lookup.Range{
				lookup.GreaterThanRange(idxv1.tuple(true, 11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 OR v1 > 15 AND v1 >= 19",
			4,
			[]lookup.Range{
				lookup.GreaterThanRange(idxv1.tuple(true, 11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 OR v1 >= 19",
			4,
			[]lookup.Range{
				lookup.GreaterThanRange(idxv1.tuple(true, 15)),
			},
			[]int64{6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 AND v1 >= 19",
			4,
			[]lookup.Range{
				lookup.GreaterOrEqualRange(idxv1.tuple(false, 19)),
			},
			[]int64{9},
		},
		{
			"v1 > 11 OR v1 < 15",
			2,
			[]lookup.Range{
				lookup.AllRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 < 15",
			2,
			[]lookup.Range{
				lookup.OpenRange(idxv1.tuple(true, 11), idxv1.tuple(false, 15)),
			},
			[]int64{2, 3, 4},
		},
		{
			"v1 > 11 OR v1 > 15 OR v1 < 19",
			4,
			[]lookup.Range{
				lookup.AllRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 OR v1 > 15 AND v1 < 19",
			4,
			[]lookup.Range{
				lookup.GreaterThanRange(idxv1.tuple(true, 11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 OR v1 < 19",
			4,
			[]lookup.Range{
				lookup.AllRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 AND v1 < 19",
			4,
			[]lookup.Range{
				lookup.OpenRange(idxv1.tuple(true, 15), idxv1.tuple(false, 19)),
			},
			[]int64{6, 7, 8},
		},
		{
			"v1 > 11 OR v1 <= 15",
			2,
			[]lookup.Range{
				lookup.AllRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 <= 15",
			2,
			[]lookup.Range{
				lookup.CustomRange(idxv1.tuple(true, 11), idxv1.tuple(true, 15),
					lookup.Open, lookup.Closed),
			},
			[]int64{2, 3, 4, 5},
		},
		{
			"v1 > 11 OR v1 > 15 OR v1 <= 19",
			4,
			[]lookup.Range{
				lookup.AllRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 OR v1 > 15 AND v1 <= 19",
			4,
			[]lookup.Range{
				lookup.GreaterThanRange(idxv1.tuple(true, 11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 OR v1 <= 19",
			4,
			[]lookup.Range{
				lookup.AllRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 AND v1 <= 19",
			4,
			[]lookup.Range{
				lookup.CustomRange(idxv1.tuple(true, 15), idxv1.tuple(true, 19),
					lookup.Open, lookup.Closed),
			},
			[]int64{6, 7, 8, 9},
		},
		{
			"v1 >= 11",
			0,
			[]lookup.Range{
				lookup.GreaterOrEqualRange(idxv1.tuple(false, 11)),
			},
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 OR v1 >= 15",
			2,
			[]lookup.Range{
				lookup.GreaterOrEqualRange(idxv1.tuple(false, 11)),
			},
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 >= 15",
			2,
			[]lookup.Range{
				lookup.GreaterOrEqualRange(idxv1.tuple(false, 15)),
			},
			[]int64{5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 OR v1 >= 15 OR v1 >= 19",
			4,
			[]lookup.Range{
				lookup.GreaterOrEqualRange(idxv1.tuple(false, 11)),
			},
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 OR v1 >= 15 AND v1 >= 19",
			4,
			[]lookup.Range{
				lookup.GreaterOrEqualRange(idxv1.tuple(false, 11)),
			},
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 >= 15 AND v1 >= 19",
			4,
			[]lookup.Range{
				lookup.GreaterOrEqualRange(idxv1.tuple(false, 19)),
			},
			[]int64{9},
		},
		{
			"v1 >= 11 OR v1 < 15",
			2,
			[]lookup.Range{
				lookup.AllRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 < 15",
			2,
			[]lookup.Range{
				lookup.CustomRange(idxv1.tuple(false, 11), idxv1.tuple(false, 15),
					lookup.Closed, lookup.Open),
			},
			[]int64{1, 2, 3, 4},
		},
		{
			"v1 >= 11 OR v1 >= 15 OR v1 < 19",
			4,
			[]lookup.Range{
				lookup.AllRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 OR v1 >= 15 AND v1 < 19",
			4,
			[]lookup.Range{
				lookup.GreaterOrEqualRange(idxv1.tuple(false, 11)),
			},
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 >= 15 OR v1 < 19",
			4,
			[]lookup.Range{
				lookup.AllRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 >= 15 AND v1 < 19",
			4,
			[]lookup.Range{
				lookup.CustomRange(idxv1.tuple(false, 15), idxv1.tuple(false, 19),
					lookup.Closed, lookup.Open),
			},
			[]int64{5, 6, 7, 8},
		},
		{
			"v1 >= 11 OR v1 <= 15",
			2,
			[]lookup.Range{
				lookup.AllRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 <= 15",
			2,
			[]lookup.Range{
				lookup.ClosedRange(idxv1.tuple(false, 11), idxv1.tuple(true, 15)),
			},
			[]int64{1, 2, 3, 4, 5},
		},
		{
			"v1 >= 11 OR v1 >= 15 OR v1 <= 19",
			4,
			[]lookup.Range{
				lookup.AllRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 OR v1 >= 15 AND v1 <= 19",
			4,
			[]lookup.Range{
				lookup.GreaterOrEqualRange(idxv1.tuple(false, 11)),
			},
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 >= 15 OR v1 <= 19",
			4,
			[]lookup.Range{
				lookup.AllRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 >= 15 AND v1 <= 19",
			4,
			[]lookup.Range{
				lookup.ClosedRange(idxv1.tuple(false, 15), idxv1.tuple(true, 19)),
			},
			[]int64{5, 6, 7, 8, 9},
		},
		{
			"v1 < 11",
			0,
			[]lookup.Range{
				lookup.LessThanRange(idxv1.tuple(false, 11)),
			},
			[]int64{0},
		},
		{
			"v1 < 11 OR v1 < 15",
			2,
			[]lookup.Range{
				lookup.LessThanRange(idxv1.tuple(false, 15)),
			},
			[]int64{0, 1, 2, 3, 4},
		},
		{
			"v1 < 11 AND v1 < 15",
			2,
			[]lookup.Range{
				lookup.LessThanRange(idxv1.tuple(false, 11)),
			},
			[]int64{0},
		},
		{
			"v1 < 11 OR v1 < 15 OR v1 < 19",
			4,
			[]lookup.Range{
				lookup.LessThanRange(idxv1.tuple(false, 19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			"v1 < 11 OR v1 < 15 AND v1 < 19",
			4,
			[]lookup.Range{
				lookup.LessThanRange(idxv1.tuple(false, 15)),
			},
			[]int64{0, 1, 2, 3, 4},
		},
		{
			"v1 < 11 AND v1 < 15 AND v1 < 19",
			4,
			[]lookup.Range{
				lookup.LessThanRange(idxv1.tuple(false, 11)),
			},
			[]int64{0},
		},
		{
			"v1 < 11 OR v1 > 15",
			2,
			[]lookup.Range{
				lookup.LessThanRange(idxv1.tuple(false, 11)),
				lookup.GreaterThanRange(idxv1.tuple(true, 15)),
			},
			[]int64{0, 6, 7, 8, 9},
		},
		{
			"v1 < 11 AND v1 > 15",
			2,
			[]lookup.Range{
				lookup.EmptyRange(),
			},
			[]int64{},
		},
		{
			"v1 < 11 OR v1 <= 15",
			2,
			[]lookup.Range{
				lookup.LessOrEqualRange(idxv1.tuple(true, 15)),
			},
			[]int64{0, 1, 2, 3, 4, 5},
		},
		{
			"v1 < 11 AND v1 <= 15",
			2,
			[]lookup.Range{
				lookup.LessThanRange(idxv1.tuple(false, 11)),
			},
			[]int64{0},
		},
		{
			"v1 < 11 OR v1 < 15 OR v1 <= 19",
			4,
			[]lookup.Range{
				lookup.LessOrEqualRange(idxv1.tuple(true, 19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 < 11 OR v1 < 15 AND v1 <= 19",
			4,
			[]lookup.Range{
				lookup.LessThanRange(idxv1.tuple(false, 15)),
			},
			[]int64{0, 1, 2, 3, 4},
		},
		{
			"v1 < 11 AND v1 < 15 OR v1 <= 19",
			4,
			[]lookup.Range{
				lookup.LessOrEqualRange(idxv1.tuple(true, 19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 < 11 AND v1 < 15 AND v1 <= 19",
			4,
			[]lookup.Range{
				lookup.LessThanRange(idxv1.tuple(false, 11)),
			},
			[]int64{0},
		},
		{
			"v1 < 11 OR v1 >= 15",
			2,
			[]lookup.Range{
				lookup.LessThanRange(idxv1.tuple(false, 11)),
				lookup.GreaterOrEqualRange(idxv1.tuple(false, 15)),
			},
			[]int64{0, 5, 6, 7, 8, 9},
		},
		{
			"v1 < 11 AND v1 >= 15",
			2,
			[]lookup.Range{
				lookup.EmptyRange(),
			},
			[]int64{},
		},
		{
			"v1 <= 11",
			0,
			[]lookup.Range{
				lookup.LessOrEqualRange(idxv1.tuple(true, 11)),
			},
			[]int64{0, 1},
		},
		{
			"v1 <= 11 OR v1 <= 15",
			2,
			[]lookup.Range{
				lookup.LessOrEqualRange(idxv1.tuple(true, 15)),
			},
			[]int64{0, 1, 2, 3, 4, 5},
		},
		{
			"v1 <= 11 AND v1 <= 15",
			2,
			[]lookup.Range{
				lookup.LessOrEqualRange(idxv1.tuple(true, 11)),
			},
			[]int64{0, 1},
		},
		{
			"v1 <= 11 OR v1 <= 15 OR v1 <= 19",
			4,
			[]lookup.Range{
				lookup.LessOrEqualRange(idxv1.tuple(true, 19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 <= 11 OR v1 <= 15 AND v1 <= 19",
			4,
			[]lookup.Range{
				lookup.LessOrEqualRange(idxv1.tuple(true, 15)),
			},
			[]int64{0, 1, 2, 3, 4, 5},
		},
		{
			"v1 <= 11 AND v1 <= 15 AND v1 <= 19",
			4,
			[]lookup.Range{
				lookup.LessOrEqualRange(idxv1.tuple(true, 11)),
			},
			[]int64{0, 1},
		},
		{
			"v1 <= 11 OR v1 > 15",
			2,
			[]lookup.Range{
				lookup.LessOrEqualRange(idxv1.tuple(true, 11)),
				lookup.GreaterThanRange(idxv1.tuple(true, 15)),
			},
			[]int64{0, 1, 6, 7, 8, 9},
		},
		{
			"v1 <= 11 AND v1 > 15",
			2,
			[]lookup.Range{
				lookup.EmptyRange(),
			},
			[]int64{},
		},
		{
			"v1 <= 11 OR v1 >= 15",
			2,
			[]lookup.Range{
				lookup.LessOrEqualRange(idxv1.tuple(true, 11)),
				lookup.GreaterOrEqualRange(idxv1.tuple(false, 15)),
			},
			[]int64{0, 1, 5, 6, 7, 8, 9},
		},
		{
			"v1 <= 11 AND v1 >= 15",
			2,
			[]lookup.Range{
				lookup.EmptyRange(),
			},
			[]int64{},
		},
		{
			"v1 BETWEEN 11 AND 15",
			2, // TODO: BETWEEN currently calls & merges AscendRange and DescendRange, delete one of these
			[]lookup.Range{
				lookup.ClosedRange(idxv1.tuple(false, 11), idxv1.tuple(true, 15)),
			},
			[]int64{1, 2, 3, 4, 5},
		},
		{
			"v1 BETWEEN 11 AND 15 OR v1 BETWEEN 15 AND 19",
			6,
			[]lookup.Range{
				lookup.ClosedRange(idxv1.tuple(false, 11), idxv1.tuple(true, 19)),
			},
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 BETWEEN 11 AND 15 AND v1 BETWEEN 15 AND 19",
			6,
			[]lookup.Range{
				lookup.ClosedRange(idxv1.tuple(false, 15), idxv1.tuple(true, 15)),
			},
			[]int64{5},
		},
		{
			"v1 BETWEEN 11 AND 15 OR v1 = 13",
			4,
			[]lookup.Range{
				lookup.ClosedRange(idxv1.tuple(false, 11), idxv1.tuple(true, 15)),
			},
			[]int64{1, 2, 3, 4, 5},
		},
		{
			"v1 BETWEEN 11 AND 15 AND v1 <= 19",
			4,
			[]lookup.Range{
				lookup.ClosedRange(idxv1.tuple(false, 11), idxv1.tuple(true, 15)),
			},
			[]int64{1, 2, 3, 4, 5},
		},
		{
			"v2 = 21 AND v1 = 11 OR v2 > 25 AND v1 > 11",
			2,
			[]lookup.Range{
				lookup.ClosedRange(idxv2v1.tuple(false, 21, 11), idxv2v1.tuple(true, 21, 11)),
				lookup.GreaterThanRange(idxv2v1.tuple(true, 25, 11)),
			},
			[]int64{1, 6, 7, 8, 9},
		},
		{
			"v2 > 21 AND v1 > 11 AND v2 < 25 AND v1 < 15",
			2,
			[]lookup.Range{
				lookup.OpenRange(idxv2v1.tuple(true, 21, 11), idxv2v1.tuple(false, 25, 15)),
			},
			[]int64{2, 3, 4},
		},
	}

	for _, test := range tests {
		t.Run(test.whereStmt, func(t *testing.T) {
			count := 0
			var finalRanges []lookup.Range
			db.t = t
			db.countLookups = func(val int) {
				count += val
			}
			db.finalRanges = func(ranges []lookup.Range) {
				finalRanges = ranges
			}

			ctx := context.Background()
			sqlCtx := NewTestSQLCtx(ctx)
			_, iter, err := engine.Query(sqlCtx, fmt.Sprintf(`SELECT pk FROM test WHERE %s ORDER BY 1`, test.whereStmt))
			require.NoError(t, err)
			res, err := sql.RowIterToRows(iter)
			require.NoError(t, err)
			if assert.Equal(t, len(test.pks), len(res)) {
				for i, pk := range test.pks {
					if assert.Equal(t, 1, len(res[i])) {
						assert.Equal(t, pk, res[i][0])
					}
				}
			}

			assert.Equal(t, test.lookupsCounted, count)
			if assert.Equal(t, len(test.finalRanges), len(finalRanges)) {
				for i, r := range test.finalRanges {
					require.True(t, r.Equals(finalRanges[i]), fmt.Sprintf("Expected: `%v`\nActual:   `%v`", r, finalRanges[i]))
				}
			}
		})
	}
}
