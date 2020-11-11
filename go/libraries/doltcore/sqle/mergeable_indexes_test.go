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
		whereStmt   string
		finalRanges []lookup.Range
		pks         []int64
	}{
		{
			"v1 = 11",
			[]lookup.Range{
				lookup.MustClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
			},
			[]int64{1},
		},
		{
			"v1 = 11 OR v1 = 15",
			[]lookup.Range{
				lookup.MustClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
				lookup.MustClosedRange(idxv1.tuple(15), idxv1.tuple(15)),
			},
			[]int64{1, 5},
		},
		{
			"v1 = 11 AND v1 = 15",
			[]lookup.Range{
				lookup.EmptyRange(),
			},
			[]int64{},
		},
		{
			"v1 = 11 OR v1 = 15 OR v1 = 19",
			[]lookup.Range{
				lookup.MustClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
				lookup.MustClosedRange(idxv1.tuple(15), idxv1.tuple(15)),
				lookup.MustClosedRange(idxv1.tuple(19), idxv1.tuple(19)),
			},
			[]int64{1, 5, 9},
		},
		{
			"v1 = 11 OR v1 = 15 AND v1 = 19",
			[]lookup.Range{
				lookup.MustClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
			},
			[]int64{1},
		},
		{
			"v1 = 11 AND v1 = 15 AND v1 = 19",
			[]lookup.Range{
				lookup.EmptyRange(),
			},
			[]int64{},
		},
		{
			"v1 = 11 OR v1 > 15",
			[]lookup.Range{
				lookup.MustClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
				lookup.MustGreaterThanRange(idxv1.tuple(15)),
			},
			[]int64{1, 6, 7, 8, 9},
		},
		{
			"v1 = 11 AND v1 > 15",
			[]lookup.Range{
				lookup.EmptyRange(),
			},
			[]int64{},
		},
		{
			"v1 = 11 OR v1 = 15 OR v1 > 19",
			[]lookup.Range{
				lookup.MustClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
				lookup.MustClosedRange(idxv1.tuple(15), idxv1.tuple(15)),
				lookup.MustGreaterThanRange(idxv1.tuple(19)),
			},
			[]int64{1, 5},
		},
		{
			"v1 = 11 OR v1 = 15 AND v1 > 19",
			[]lookup.Range{
				lookup.MustClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
			},
			[]int64{1},
		},
		{
			"v1 = 11 AND v1 = 15 OR v1 > 19",
			[]lookup.Range{
				lookup.MustGreaterThanRange(idxv1.tuple(19)),
			},
			[]int64{},
		},
		{
			"v1 = 11 AND v1 = 15 AND v1 > 19",
			[]lookup.Range{
				lookup.EmptyRange(),
			},
			[]int64{},
		},
		{
			"v1 = 11 OR v1 >= 15",
			[]lookup.Range{
				lookup.MustClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
				lookup.GreaterOrEqualRange(idxv1.tuple(15)),
			},
			[]int64{1, 5, 6, 7, 8, 9},
		},
		{
			"v1 = 11 AND v1 >= 15",
			[]lookup.Range{
				lookup.EmptyRange(),
			},
			[]int64{},
		},
		{
			"v1 = 11 OR v1 = 15 OR v1 >= 19",
			[]lookup.Range{
				lookup.MustClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
				lookup.MustClosedRange(idxv1.tuple(15), idxv1.tuple(15)),
				lookup.GreaterOrEqualRange(idxv1.tuple(19)),
			},
			[]int64{1, 5, 9},
		},
		{
			"v1 = 11 OR v1 = 15 AND v1 >= 19",
			[]lookup.Range{
				lookup.MustClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
			},
			[]int64{1},
		},
		{
			"v1 = 11 AND v1 = 15 OR v1 >= 19",
			[]lookup.Range{
				lookup.GreaterOrEqualRange(idxv1.tuple(19)),
			},
			[]int64{9},
		},
		{
			"v1 = 11 AND v1 = 15 AND v1 >= 19",
			[]lookup.Range{
				lookup.EmptyRange(),
			},
			[]int64{},
		},
		{
			"v1 = 11 OR v1 < 15",
			[]lookup.Range{
				lookup.LessThanRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 2, 3, 4},
		},
		{
			"v1 = 11 AND v1 < 15",
			[]lookup.Range{
				lookup.MustClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
			},
			[]int64{1},
		},
		{
			"v1 = 11 OR v1 = 15 OR v1 < 19",
			[]lookup.Range{
				lookup.LessThanRange(idxv1.tuple(19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			"v1 = 11 OR v1 = 15 AND v1 < 19",
			[]lookup.Range{
				lookup.MustClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
				lookup.MustClosedRange(idxv1.tuple(15), idxv1.tuple(15)),
			},
			[]int64{1, 5},
		},
		{
			"v1 = 11 AND v1 = 15 OR v1 < 19",
			[]lookup.Range{
				lookup.LessThanRange(idxv1.tuple(19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			"v1 = 11 AND v1 = 15 AND v1 < 19",
			[]lookup.Range{
				lookup.EmptyRange(),
			},
			[]int64{},
		},
		{
			"v1 = 11 OR v1 <= 15",
			[]lookup.Range{
				lookup.MustLessOrEqualRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 2, 3, 4, 5},
		},
		{
			"v1 = 11 AND v1 <= 15",
			[]lookup.Range{
				lookup.MustClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
			},
			[]int64{1},
		},
		{
			"v1 = 11 OR v1 = 15 OR v1 <= 19",
			[]lookup.Range{
				lookup.MustLessOrEqualRange(idxv1.tuple(19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 = 11 OR v1 = 15 AND v1 <= 19",
			[]lookup.Range{
				lookup.MustClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
				lookup.MustClosedRange(idxv1.tuple(15), idxv1.tuple(15)),
			},
			[]int64{1, 5},
		},
		{
			"v1 = 11 AND v1 = 15 OR v1 <= 19",
			[]lookup.Range{
				lookup.MustLessOrEqualRange(idxv1.tuple(19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 = 11 AND v1 = 15 AND v1 <= 19",
			[]lookup.Range{
				lookup.EmptyRange(),
			},
			[]int64{},
		},
		{
			"v1 > 11",
			[]lookup.Range{
				lookup.MustGreaterThanRange(idxv1.tuple(11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 OR v1 > 15",
			[]lookup.Range{
				lookup.MustGreaterThanRange(idxv1.tuple(11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15",
			[]lookup.Range{
				lookup.MustGreaterThanRange(idxv1.tuple(15)),
			},
			[]int64{6, 7, 8, 9},
		},
		{
			"v1 > 11 OR v1 > 15 OR v1 > 19",
			[]lookup.Range{
				lookup.MustGreaterThanRange(idxv1.tuple(11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 OR v1 > 15 AND v1 > 19",
			[]lookup.Range{
				lookup.MustGreaterThanRange(idxv1.tuple(11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 AND v1 > 19",
			[]lookup.Range{
				lookup.MustGreaterThanRange(idxv1.tuple(19)),
			},
			[]int64{},
		},
		{
			"v1 > 11 OR v1 >= 15",
			[]lookup.Range{
				lookup.MustGreaterThanRange(idxv1.tuple(11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 >= 15",
			[]lookup.Range{
				lookup.GreaterOrEqualRange(idxv1.tuple(15)),
			},
			[]int64{5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 OR v1 > 15 OR v1 >= 19",
			[]lookup.Range{
				lookup.MustGreaterThanRange(idxv1.tuple(11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 OR v1 > 15 AND v1 >= 19",
			[]lookup.Range{
				lookup.MustGreaterThanRange(idxv1.tuple(11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 OR v1 >= 19",
			[]lookup.Range{
				lookup.MustGreaterThanRange(idxv1.tuple(15)),
			},
			[]int64{6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 AND v1 >= 19",
			[]lookup.Range{
				lookup.GreaterOrEqualRange(idxv1.tuple(19)),
			},
			[]int64{9},
		},
		{
			"v1 > 11 OR v1 < 15",
			[]lookup.Range{
				lookup.AllRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 < 15",
			[]lookup.Range{
				lookup.MustOpenRange(idxv1.tuple(11), idxv1.tuple(15)),
			},
			[]int64{2, 3, 4},
		},
		{
			"v1 > 11 OR v1 > 15 OR v1 < 19",
			[]lookup.Range{
				lookup.AllRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 OR v1 > 15 AND v1 < 19",
			[]lookup.Range{
				lookup.MustGreaterThanRange(idxv1.tuple(11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 OR v1 < 19",
			[]lookup.Range{
				lookup.AllRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 AND v1 < 19",
			[]lookup.Range{
				lookup.MustOpenRange(idxv1.tuple(15), idxv1.tuple(19)),
			},
			[]int64{6, 7, 8},
		},
		{
			"v1 > 11 OR v1 <= 15",
			[]lookup.Range{
				lookup.AllRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 <= 15",
			[]lookup.Range{
				lookup.MustCustomRange(idxv1.tuple(11), idxv1.tuple(15),
					lookup.Open, lookup.Closed),
			},
			[]int64{2, 3, 4, 5},
		},
		{
			"v1 > 11 OR v1 > 15 OR v1 <= 19",
			[]lookup.Range{
				lookup.AllRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 OR v1 > 15 AND v1 <= 19",
			[]lookup.Range{
				lookup.MustGreaterThanRange(idxv1.tuple(11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 OR v1 <= 19",
			[]lookup.Range{
				lookup.AllRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 AND v1 <= 19",
			[]lookup.Range{
				lookup.MustCustomRange(idxv1.tuple(15), idxv1.tuple(19),
					lookup.Open, lookup.Closed),
			},
			[]int64{6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 AND v1 <= 19",
			[]lookup.Range{
				lookup.MustCustomRange(idxv1.tuple(15), idxv1.tuple(19),
					lookup.Open, lookup.Closed),
			},
			[]int64{6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 < 15 OR v1 > 15 AND v1 < 19",
			[]lookup.Range{
				lookup.MustOpenRange(idxv1.tuple(11), idxv1.tuple(15)),
				lookup.MustOpenRange(idxv1.tuple(15), idxv1.tuple(19)),
			},
			[]int64{2, 3, 4, 6, 7, 8},
		},
		{
			"v1 >= 11",
			[]lookup.Range{
				lookup.GreaterOrEqualRange(idxv1.tuple(11)),
			},
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 OR v1 >= 15",
			[]lookup.Range{
				lookup.GreaterOrEqualRange(idxv1.tuple(11)),
			},
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 >= 15",
			[]lookup.Range{
				lookup.GreaterOrEqualRange(idxv1.tuple(15)),
			},
			[]int64{5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 OR v1 >= 15 OR v1 >= 19",
			[]lookup.Range{
				lookup.GreaterOrEqualRange(idxv1.tuple(11)),
			},
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 OR v1 >= 15 AND v1 >= 19",
			[]lookup.Range{
				lookup.GreaterOrEqualRange(idxv1.tuple(11)),
			},
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 >= 15 AND v1 >= 19",
			[]lookup.Range{
				lookup.GreaterOrEqualRange(idxv1.tuple(19)),
			},
			[]int64{9},
		},
		{
			"v1 >= 11 OR v1 < 15",
			[]lookup.Range{
				lookup.AllRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 < 15",
			[]lookup.Range{
				lookup.MustCustomRange(idxv1.tuple(11), idxv1.tuple(15),
					lookup.Closed, lookup.Open),
			},
			[]int64{1, 2, 3, 4},
		},
		{
			"v1 >= 11 OR v1 >= 15 OR v1 < 19",
			[]lookup.Range{
				lookup.AllRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 OR v1 >= 15 AND v1 < 19",
			[]lookup.Range{
				lookup.GreaterOrEqualRange(idxv1.tuple(11)),
			},
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 >= 15 OR v1 < 19",
			[]lookup.Range{
				lookup.AllRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 >= 15 AND v1 < 19",
			[]lookup.Range{
				lookup.MustCustomRange(idxv1.tuple(15), idxv1.tuple(19),
					lookup.Closed, lookup.Open),
			},
			[]int64{5, 6, 7, 8},
		},
		{
			"v1 >= 11 OR v1 <= 15",
			[]lookup.Range{
				lookup.AllRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 <= 15",
			[]lookup.Range{
				lookup.MustClosedRange(idxv1.tuple(11), idxv1.tuple(15)),
			},
			[]int64{1, 2, 3, 4, 5},
		},
		{
			"v1 >= 11 OR v1 >= 15 OR v1 <= 19",
			[]lookup.Range{
				lookup.AllRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 OR v1 >= 15 AND v1 <= 19",
			[]lookup.Range{
				lookup.GreaterOrEqualRange(idxv1.tuple(11)),
			},
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 >= 15 OR v1 <= 19",
			[]lookup.Range{
				lookup.AllRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 >= 15 AND v1 <= 19",
			[]lookup.Range{
				lookup.MustClosedRange(idxv1.tuple(15), idxv1.tuple(19)),
			},
			[]int64{5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 <= 14 OR v1 >= 16 AND v1 <= 19",
			[]lookup.Range{
				lookup.MustClosedRange(idxv1.tuple(11), idxv1.tuple(14)),
				lookup.MustClosedRange(idxv1.tuple(16), idxv1.tuple(19)),
			},
			[]int64{1, 2, 3, 4, 6, 7, 8, 9},
		},
		{
			"v1 < 11",
			[]lookup.Range{
				lookup.LessThanRange(idxv1.tuple(11)),
			},
			[]int64{0},
		},
		{
			"v1 < 11 OR v1 < 15",
			[]lookup.Range{
				lookup.LessThanRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 2, 3, 4},
		},
		{
			"v1 < 11 AND v1 < 15",
			[]lookup.Range{
				lookup.LessThanRange(idxv1.tuple(11)),
			},
			[]int64{0},
		},
		{
			"v1 < 11 OR v1 < 15 OR v1 < 19",
			[]lookup.Range{
				lookup.LessThanRange(idxv1.tuple(19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			"v1 < 11 OR v1 < 15 AND v1 < 19",
			[]lookup.Range{
				lookup.LessThanRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 2, 3, 4},
		},
		{
			"v1 < 11 AND v1 < 15 AND v1 < 19",
			[]lookup.Range{
				lookup.LessThanRange(idxv1.tuple(11)),
			},
			[]int64{0},
		},
		{
			"v1 < 11 OR v1 > 15",
			[]lookup.Range{
				lookup.LessThanRange(idxv1.tuple(11)),
				lookup.MustGreaterThanRange(idxv1.tuple(15)),
			},
			[]int64{0, 6, 7, 8, 9},
		},
		{
			"v1 < 11 AND v1 > 15",
			[]lookup.Range{
				lookup.EmptyRange(),
			},
			[]int64{},
		},
		{
			"v1 < 11 OR v1 <= 15",
			[]lookup.Range{
				lookup.MustLessOrEqualRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 2, 3, 4, 5},
		},
		{
			"v1 < 11 AND v1 <= 15",
			[]lookup.Range{
				lookup.LessThanRange(idxv1.tuple(11)),
			},
			[]int64{0},
		},
		{
			"v1 < 11 OR v1 < 15 OR v1 <= 19",
			[]lookup.Range{
				lookup.MustLessOrEqualRange(idxv1.tuple(19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 < 11 OR v1 < 15 AND v1 <= 19",
			[]lookup.Range{
				lookup.LessThanRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 2, 3, 4},
		},
		{
			"v1 < 11 AND v1 < 15 OR v1 <= 19",
			[]lookup.Range{
				lookup.MustLessOrEqualRange(idxv1.tuple(19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 < 11 AND v1 < 15 AND v1 <= 19",
			[]lookup.Range{
				lookup.LessThanRange(idxv1.tuple(11)),
			},
			[]int64{0},
		},
		{
			"v1 < 11 OR v1 >= 15",
			[]lookup.Range{
				lookup.LessThanRange(idxv1.tuple(11)),
				lookup.GreaterOrEqualRange(idxv1.tuple(15)),
			},
			[]int64{0, 5, 6, 7, 8, 9},
		},
		{
			"v1 < 11 AND v1 >= 15",
			[]lookup.Range{
				lookup.EmptyRange(),
			},
			[]int64{},
		},
		{
			"(v1 < 13 OR v1 > 16) AND (v1 > 10 OR v1 < 19)",
			[]lookup.Range{
				lookup.LessThanRange(idxv1.tuple(13)),
				lookup.MustGreaterThanRange(idxv1.tuple(16)),
			},
			[]int64{0, 1, 2, 7, 8, 9},
		},
		{
			"v1 <= 11",
			[]lookup.Range{
				lookup.MustLessOrEqualRange(idxv1.tuple(11)),
			},
			[]int64{0, 1},
		},
		{
			"v1 <= 11 OR v1 <= 15",
			[]lookup.Range{
				lookup.MustLessOrEqualRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 2, 3, 4, 5},
		},
		{
			"v1 <= 11 AND v1 <= 15",
			[]lookup.Range{
				lookup.MustLessOrEqualRange(idxv1.tuple(11)),
			},
			[]int64{0, 1},
		},
		{
			"v1 <= 11 OR v1 <= 15 OR v1 <= 19",
			[]lookup.Range{
				lookup.MustLessOrEqualRange(idxv1.tuple(19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 <= 11 OR v1 <= 15 AND v1 <= 19",
			[]lookup.Range{
				lookup.MustLessOrEqualRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 2, 3, 4, 5},
		},
		{
			"v1 <= 11 AND v1 <= 15 AND v1 <= 19",
			[]lookup.Range{
				lookup.MustLessOrEqualRange(idxv1.tuple(11)),
			},
			[]int64{0, 1},
		},
		{
			"v1 <= 11 OR v1 > 15",
			[]lookup.Range{
				lookup.MustLessOrEqualRange(idxv1.tuple(11)),
				lookup.MustGreaterThanRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 6, 7, 8, 9},
		},
		{
			"v1 <= 11 AND v1 > 15",
			[]lookup.Range{
				lookup.EmptyRange(),
			},
			[]int64{},
		},
		{
			"v1 <= 11 OR v1 >= 15",
			[]lookup.Range{
				lookup.MustLessOrEqualRange(idxv1.tuple(11)),
				lookup.GreaterOrEqualRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 5, 6, 7, 8, 9},
		},
		{
			"v1 <= 11 AND v1 >= 15",
			[]lookup.Range{
				lookup.EmptyRange(),
			},
			[]int64{},
		},
		{
			"v1 BETWEEN 11 AND 15",
			[]lookup.Range{
				lookup.MustClosedRange(idxv1.tuple(11), idxv1.tuple(15)),
			},
			[]int64{1, 2, 3, 4, 5},
		},
		{
			"v1 BETWEEN 11 AND 15 OR v1 BETWEEN 15 AND 19",
			[]lookup.Range{
				lookup.MustClosedRange(idxv1.tuple(11), idxv1.tuple(19)),
			},
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 BETWEEN 11 AND 15 AND v1 BETWEEN 15 AND 19",
			[]lookup.Range{
				lookup.MustClosedRange(idxv1.tuple(15), idxv1.tuple(15)),
			},
			[]int64{5},
		},
		{
			"v1 BETWEEN 11 AND 15 OR v1 = 13",
			[]lookup.Range{
				lookup.MustClosedRange(idxv1.tuple(11), idxv1.tuple(15)),
			},
			[]int64{1, 2, 3, 4, 5},
		},
		{
			"v1 BETWEEN 11 AND 15 AND v1 <= 19",
			[]lookup.Range{
				lookup.MustClosedRange(idxv1.tuple(11), idxv1.tuple(15)),
			},
			[]int64{1, 2, 3, 4, 5},
		},
		{
			"v1 BETWEEN 11 AND 15 AND v1 <= 19",
			[]lookup.Range{
				lookup.MustClosedRange(idxv1.tuple(11), idxv1.tuple(15)),
			},
			[]int64{1, 2, 3, 4, 5},
		},
		{
			"v1 IN (11, 12, 13)",
			[]lookup.Range{
				lookup.MustClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
				lookup.MustClosedRange(idxv1.tuple(12), idxv1.tuple(12)),
				lookup.MustClosedRange(idxv1.tuple(13), idxv1.tuple(13)),
			},
			[]int64{1, 2, 3},
		},
		{
			"v1 IN (11, 12, 13) OR v1 BETWEEN 11 and 13",
			[]lookup.Range{
				lookup.MustClosedRange(idxv1.tuple(11), idxv1.tuple(13)),
			},
			[]int64{1, 2, 3},
		},
		{
			"v1 IN (11, 12, 13) AND v1 > 11",
			[]lookup.Range{
				lookup.MustClosedRange(idxv1.tuple(12), idxv1.tuple(12)),
				lookup.MustClosedRange(idxv1.tuple(13), idxv1.tuple(13)),
			},
			[]int64{2, 3},
		},
		{
			"v1 IN (11, 12, 13) OR v1 >= 13 AND v1 < 15",
			[]lookup.Range{
				lookup.MustClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
				lookup.MustClosedRange(idxv1.tuple(12), idxv1.tuple(12)),
				lookup.MustCustomRange(idxv1.tuple(13), idxv1.tuple(15),
					lookup.Closed, lookup.Open),
			},
			[]int64{1, 2, 3, 4},
		},
		{
			"v2 = 21 AND v1 = 11 OR v2 > 25 AND v1 > 11",
			[]lookup.Range{
				lookup.MustClosedRange(idxv2v1.tuple(21, 11), idxv2v1.tuple(21, 11)),
				lookup.MustGreaterThanRange(idxv2v1.tuple(25, 11)),
			},
			[]int64{1, 6, 7, 8, 9},
		},
		{
			"v2 > 21 AND v1 > 11 AND v2 < 25 AND v1 < 15",
			[]lookup.Range{
				lookup.MustOpenRange(idxv2v1.tuple(21, 11), idxv2v1.tuple(25, 15)),
			},
			[]int64{2, 3, 4},
		},
	}

	for _, test := range tests {
		t.Run(test.whereStmt, func(t *testing.T) {
			var finalRanges []lookup.Range
			db.t = t
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

			if assert.Equal(t, len(test.finalRanges), len(finalRanges)) {
				for i, r := range test.finalRanges {
					require.True(t, r.Equals(finalRanges[i]), fmt.Sprintf("Expected: `%v`\nActual:   `%v`", r, finalRanges[i]))
				}
			}
		})
	}
}
