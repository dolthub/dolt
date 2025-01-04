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

package index_test

import (
	"fmt"
	"testing"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/types"
)

// This tests mergeable indexes by using the SQL engine and intercepting specific calls. This way, we can verify that
// the engine is intersecting and combining the proper number of lookups, and we can also examine the ranges before
// they're converted into a format that Noms understands to verify that they were handled correctly. Lastly, we ensure
// that the final output is as expected.
func TestMergeableIndexes(t *testing.T) {
	if types.Format_Default != types.Format_LD_1 {
		t.Skip() // this test is specific to Noms ranges
	}

	engine, sqlCtx, indexTuples := setupIndexes(t, "test", `INSERT INTO test VALUES
		(-3, NULL, NULL),
		(-2, NULL, NULL),
		(-1, NULL, NULL),
		(0, 10, 20),
		(1, 11, 21),
		(2, 12, 22),
		(3, 13, 23),
		(4, 14, 24),
		(5, 15, 25),
		(6, 16, 26),
		(7, 17, 27),
		(8, 18, 28),
		(9, 19, 29);`)
	idxv1, idxv2v1, idxv2v1Gen := indexTuples[0], indexTuples[1], indexTuples[2]

	tests := []struct {
		whereStmt   string
		finalRanges []*noms.ReadRange
		pks         []int64
	}{
		{
			"v1 = 11",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
			},
			[]int64{1},
		},
		{
			"v1 = 11 OR v1 = 15",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
				index.ClosedRange(idxv1.tuple(15), idxv1.tuple(15)),
			},
			[]int64{1, 5},
		},
		{
			"v1 = 11 AND v1 = 15",
			nil,
			[]int64{},
		},
		{
			"v1 = 11 OR v1 = 15 OR v1 = 19",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
				index.ClosedRange(idxv1.tuple(15), idxv1.tuple(15)),
				index.ClosedRange(idxv1.tuple(19), idxv1.tuple(19)),
			},
			[]int64{1, 5, 9},
		},
		{
			"v1 = 11 OR v1 = 15 AND v1 = 19",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
			},
			[]int64{1},
		},
		{
			"v1 = 11 AND v1 = 15 AND v1 = 19",
			nil,
			[]int64{},
		},
		{
			"v1 = 11 OR v1 != 11",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 = 11 OR v1 != 15",
			[]*noms.ReadRange{
				index.GreaterThanRange(idxv1.tuple(15)),
				index.LessThanRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 2, 3, 4, 6, 7, 8, 9},
		},
		{
			"v1 = 11 AND v1 != 15",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
			},
			[]int64{1},
		},
		{
			"v1 = 11 OR v1 = 15 OR v1 != 19",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(19)),
				index.GreaterThanRange(idxv1.tuple(19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			"v1 = 11 OR v1 = 15 AND v1 != 19",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
				index.ClosedRange(idxv1.tuple(15), idxv1.tuple(15)),
			},
			[]int64{1, 5},
		},
		{
			"v1 = 11 AND v1 = 15 OR v1 != 19",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(19)),
				index.GreaterThanRange(idxv1.tuple(19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			"v1 = 11 AND v1 = 15 AND v1 != 19",
			nil,
			[]int64{},
		},
		{
			"v1 = 11 OR v1 > 15",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
				index.GreaterThanRange(idxv1.tuple(15)),
			},
			[]int64{1, 6, 7, 8, 9},
		},
		{
			"v1 = 11 AND v1 > 15",
			nil,
			[]int64{},
		},
		{
			"v1 = 11 OR v1 = 15 OR v1 > 19",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
				index.ClosedRange(idxv1.tuple(15), idxv1.tuple(15)),
				index.GreaterThanRange(idxv1.tuple(19)),
			},
			[]int64{1, 5},
		},
		{
			"v1 = 11 OR v1 = 15 AND v1 > 19",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
			},
			[]int64{1},
		},
		{
			"v1 = 11 AND v1 = 15 OR v1 > 19",
			[]*noms.ReadRange{
				index.GreaterThanRange(idxv1.tuple(19)),
			},
			[]int64{},
		},
		{
			"v1 = 11 AND v1 = 15 AND v1 > 19",
			nil,
			[]int64{},
		},
		{
			"v1 = 11 OR v1 >= 15",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
				index.GreaterOrEqualRange(idxv1.tuple(15)),
			},
			[]int64{1, 5, 6, 7, 8, 9},
		},
		{
			"v1 = 11 AND v1 >= 15",
			nil,
			[]int64{},
		},
		{
			"v1 = 11 OR v1 = 15 OR v1 >= 19",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
				index.ClosedRange(idxv1.tuple(15), idxv1.tuple(15)),
				index.GreaterOrEqualRange(idxv1.tuple(19)),
			},
			[]int64{1, 5, 9},
		},
		{
			"v1 = 11 OR v1 = 15 AND v1 >= 19",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
			},
			[]int64{1},
		},
		{
			"v1 = 11 AND v1 = 15 OR v1 >= 19",
			[]*noms.ReadRange{
				index.GreaterOrEqualRange(idxv1.tuple(19)),
			},
			[]int64{9},
		},
		{
			"v1 = 11 AND v1 = 15 AND v1 >= 19",
			nil,
			[]int64{},
		},
		{
			"v1 = 11 OR v1 < 15",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 2, 3, 4},
		},
		{
			"v1 = 11 AND v1 < 15",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
			},
			[]int64{1},
		},
		{
			"v1 = 11 OR v1 = 15 OR v1 < 19",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			"v1 = 11 OR v1 = 15 AND v1 < 19",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
				index.ClosedRange(idxv1.tuple(15), idxv1.tuple(15)),
			},
			[]int64{1, 5},
		},
		{
			"v1 = 11 AND v1 = 15 OR v1 < 19",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			"v1 = 11 AND v1 = 15 AND v1 < 19",
			nil,
			[]int64{},
		},
		{
			"v1 = 11 OR v1 <= 15",
			[]*noms.ReadRange{
				index.LessOrEqualRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 2, 3, 4, 5},
		},
		{
			"v1 = 11 AND v1 <= 15",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
			},
			[]int64{1},
		},
		{
			"v1 = 11 OR v1 = 15 OR v1 <= 19",
			[]*noms.ReadRange{
				index.LessOrEqualRange(idxv1.tuple(19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 = 11 OR v1 = 15 AND v1 <= 19",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
				index.ClosedRange(idxv1.tuple(15), idxv1.tuple(15)),
			},
			[]int64{1, 5},
		},
		{
			"v1 = 11 AND v1 = 15 OR v1 <= 19",
			[]*noms.ReadRange{
				index.LessOrEqualRange(idxv1.tuple(19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 = 11 AND v1 = 15 AND v1 <= 19",
			nil,
			[]int64{},
		},
		{
			"v1 != 11",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(11)),
				index.GreaterThanRange(idxv1.tuple(11)),
			},
			[]int64{0, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 <> 11",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(11)),
				index.GreaterThanRange(idxv1.tuple(11)),
			},
			[]int64{0, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 OR v1 != 15",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 <> 11 OR v1 <> 15",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 AND v1 != 15",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(11)),
				index.OpenRange(idxv1.tuple(11), idxv1.tuple(15)),
				index.GreaterThanRange(idxv1.tuple(15)),
			},
			[]int64{0, 2, 3, 4, 6, 7, 8, 9},
		},
		{
			"v1 <> 11 AND v1 <> 15",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(11)),
				index.OpenRange(idxv1.tuple(11), idxv1.tuple(15)),
				index.GreaterThanRange(idxv1.tuple(15)),
			},
			[]int64{0, 2, 3, 4, 6, 7, 8, 9},
		},
		{
			"v1 != 11 OR v1 != 15 OR v1 != 19",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 <> 11 OR v1 <> 15 OR v1 <> 19",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 OR v1 != 15 AND v1 != 19",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 <> 11 OR v1 <> 15 AND v1 <> 19",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 AND v1 != 15 AND v1 != 19",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(11)),
				index.OpenRange(idxv1.tuple(11), idxv1.tuple(15)),
				index.OpenRange(idxv1.tuple(15), idxv1.tuple(19)),
				index.GreaterThanRange(idxv1.tuple(19)),
			},
			[]int64{0, 2, 3, 4, 6, 7, 8},
		},
		{
			"v1 <> 11 AND v1 <> 15 AND v1 <> 19",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(11)),
				index.OpenRange(idxv1.tuple(11), idxv1.tuple(15)),
				index.OpenRange(idxv1.tuple(15), idxv1.tuple(19)),
				index.GreaterThanRange(idxv1.tuple(19)),
			},
			[]int64{0, 2, 3, 4, 6, 7, 8},
		},
		{
			"v1 != 11 OR v1 > 15",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(11)),
				index.GreaterThanRange(idxv1.tuple(11)),
			},
			[]int64{0, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 AND v1 > 15",
			[]*noms.ReadRange{
				index.GreaterThanRange(idxv1.tuple(15)),
			},
			[]int64{6, 7, 8, 9},
		},
		{
			"v1 != 11 OR v1 != 15 OR v1 > 19",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 OR v1 != 15 AND v1 > 19",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(11)),
				index.GreaterThanRange(idxv1.tuple(11)),
			},
			[]int64{0, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 AND v1 != 15 OR v1 > 19",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(11)),
				index.OpenRange(idxv1.tuple(11), idxv1.tuple(15)),
				index.GreaterThanRange(idxv1.tuple(15)),
			},
			[]int64{0, 2, 3, 4, 6, 7, 8, 9},
		},
		{
			"v1 != 11 AND v1 != 15 AND v1 > 19",
			[]*noms.ReadRange{
				index.GreaterThanRange(idxv1.tuple(19)),
			},
			[]int64{},
		},
		{
			"v1 != 11 OR v1 >= 15",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(11)),
				index.GreaterThanRange(idxv1.tuple(11)),
			},
			[]int64{0, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 AND v1 >= 15",
			[]*noms.ReadRange{
				index.GreaterOrEqualRange(idxv1.tuple(15)),
			},
			[]int64{5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 OR v1 != 15 OR v1 >= 19",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 OR v1 != 15 AND v1 >= 19",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(11)),
				index.GreaterThanRange(idxv1.tuple(11)),
			},
			[]int64{0, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 AND v1 != 15 OR v1 >= 19",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(11)),
				index.OpenRange(idxv1.tuple(11), idxv1.tuple(15)),
				index.GreaterThanRange(idxv1.tuple(15)),
			},
			[]int64{0, 2, 3, 4, 6, 7, 8, 9},
		},
		{
			"v1 != 11 AND v1 != 15 AND v1 >= 19",
			[]*noms.ReadRange{
				index.GreaterOrEqualRange(idxv1.tuple(19)),
			},
			[]int64{9},
		},
		{
			"v1 != 11 OR v1 < 15",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 AND v1 < 15",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(11)),
				index.OpenRange(idxv1.tuple(11), idxv1.tuple(15)),
			},
			[]int64{0, 2, 3, 4},
		},
		{
			"v1 != 11 OR v1 != 15 OR v1 < 19",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 OR v1 != 15 AND v1 < 19",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 AND v1 != 15 OR v1 < 19",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 AND v1 != 15 AND v1 < 19",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(11)),
				index.OpenRange(idxv1.tuple(11), idxv1.tuple(15)),
				index.OpenRange(idxv1.tuple(15), idxv1.tuple(19)),
			},
			[]int64{0, 2, 3, 4, 6, 7, 8},
		},
		{
			"v1 != 11 OR v1 <= 15",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 AND v1 <= 15",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(11)),
				index.CustomRange(idxv1.tuple(11), idxv1.tuple(15), sql.Open, sql.Closed),
			},
			[]int64{0, 2, 3, 4, 5},
		},
		{
			"v1 != 11 OR v1 != 15 OR v1 <= 19",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 OR v1 != 15 AND v1 <= 19",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 AND v1 != 15 OR v1 <= 19",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 AND v1 != 15 AND v1 <= 19",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(11)),
				index.OpenRange(idxv1.tuple(11), idxv1.tuple(15)),
				index.CustomRange(idxv1.tuple(15), idxv1.tuple(19), sql.Open, sql.Closed),
			},
			[]int64{0, 2, 3, 4, 6, 7, 8, 9},
		},
		{
			"v1 > 11",
			[]*noms.ReadRange{
				index.GreaterThanRange(idxv1.tuple(11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 OR v1 > 15",
			[]*noms.ReadRange{
				index.GreaterThanRange(idxv1.tuple(11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15",
			[]*noms.ReadRange{
				index.GreaterThanRange(idxv1.tuple(15)),
			},
			[]int64{6, 7, 8, 9},
		},
		{
			"v1 > 11 OR v1 > 15 OR v1 > 19",
			[]*noms.ReadRange{
				index.GreaterThanRange(idxv1.tuple(11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 OR v1 > 15 AND v1 > 19",
			[]*noms.ReadRange{
				index.GreaterThanRange(idxv1.tuple(11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 AND v1 > 19",
			[]*noms.ReadRange{
				index.GreaterThanRange(idxv1.tuple(19)),
			},
			[]int64{},
		},
		{
			"v1 > 11 OR v1 >= 15",
			[]*noms.ReadRange{
				index.GreaterThanRange(idxv1.tuple(11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 >= 15",
			[]*noms.ReadRange{
				index.GreaterOrEqualRange(idxv1.tuple(15)),
			},
			[]int64{5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 OR v1 > 15 OR v1 >= 19",
			[]*noms.ReadRange{
				index.GreaterThanRange(idxv1.tuple(11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 OR v1 > 15 AND v1 >= 19",
			[]*noms.ReadRange{
				index.GreaterThanRange(idxv1.tuple(11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 OR v1 >= 19",
			[]*noms.ReadRange{
				index.GreaterThanRange(idxv1.tuple(15)),
			},
			[]int64{6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 AND v1 >= 19",
			[]*noms.ReadRange{
				index.GreaterOrEqualRange(idxv1.tuple(19)),
			},
			[]int64{9},
		},
		{
			"v1 > 11 OR v1 < 15",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 < 15",
			[]*noms.ReadRange{
				index.OpenRange(idxv1.tuple(11), idxv1.tuple(15)),
			},
			[]int64{2, 3, 4},
		},
		{
			"v1 > 11 OR v1 > 15 OR v1 < 19",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 OR v1 > 15 AND v1 < 19",
			[]*noms.ReadRange{
				index.GreaterThanRange(idxv1.tuple(11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 OR v1 < 19",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 AND v1 < 19",
			[]*noms.ReadRange{
				index.OpenRange(idxv1.tuple(15), idxv1.tuple(19)),
			},
			[]int64{6, 7, 8},
		},
		{
			"v1 > 11 OR v1 <= 15",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 <= 15",
			[]*noms.ReadRange{
				index.CustomRange(idxv1.tuple(11), idxv1.tuple(15), sql.Open, sql.Closed),
			},
			[]int64{2, 3, 4, 5},
		},
		{
			"v1 > 11 OR v1 > 15 OR v1 <= 19",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 OR v1 > 15 AND v1 <= 19",
			[]*noms.ReadRange{
				index.GreaterThanRange(idxv1.tuple(11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 OR v1 <= 19",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 AND v1 <= 19",
			[]*noms.ReadRange{
				index.CustomRange(idxv1.tuple(15), idxv1.tuple(19), sql.Open, sql.Closed),
			},
			[]int64{6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 AND v1 <= 19",
			[]*noms.ReadRange{
				index.CustomRange(idxv1.tuple(15), idxv1.tuple(19), sql.Open, sql.Closed),
			},
			[]int64{6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 < 15 OR v1 > 15 AND v1 < 19",
			[]*noms.ReadRange{
				index.OpenRange(idxv1.tuple(11), idxv1.tuple(15)),
				index.OpenRange(idxv1.tuple(15), idxv1.tuple(19)),
			},
			[]int64{2, 3, 4, 6, 7, 8},
		},
		{
			"v1 >= 11",
			[]*noms.ReadRange{
				index.GreaterOrEqualRange(idxv1.tuple(11)),
			},
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 OR v1 >= 15",
			[]*noms.ReadRange{
				index.GreaterOrEqualRange(idxv1.tuple(11)),
			},
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 >= 15",
			[]*noms.ReadRange{
				index.GreaterOrEqualRange(idxv1.tuple(15)),
			},
			[]int64{5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 OR v1 >= 15 OR v1 >= 19",
			[]*noms.ReadRange{
				index.GreaterOrEqualRange(idxv1.tuple(11)),
			},
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 OR v1 >= 15 AND v1 >= 19",
			[]*noms.ReadRange{
				index.GreaterOrEqualRange(idxv1.tuple(11)),
			},
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 >= 15 AND v1 >= 19",
			[]*noms.ReadRange{
				index.GreaterOrEqualRange(idxv1.tuple(19)),
			},
			[]int64{9},
		},
		{
			"v1 >= 11 OR v1 < 15",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 < 15",
			[]*noms.ReadRange{
				index.CustomRange(idxv1.tuple(11), idxv1.tuple(15), sql.Closed, sql.Open),
			},
			[]int64{1, 2, 3, 4},
		},
		{
			"v1 >= 11 OR v1 >= 15 OR v1 < 19",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 OR v1 >= 15 AND v1 < 19",
			[]*noms.ReadRange{
				index.GreaterOrEqualRange(idxv1.tuple(11)),
			},
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 >= 15 OR v1 < 19",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 >= 15 AND v1 < 19",
			[]*noms.ReadRange{
				index.CustomRange(idxv1.tuple(15), idxv1.tuple(19), sql.Closed, sql.Open),
			},
			[]int64{5, 6, 7, 8},
		},
		{
			"v1 >= 11 OR v1 <= 15",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 <= 15",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(11), idxv1.tuple(15)),
			},
			[]int64{1, 2, 3, 4, 5},
		},
		{
			"v1 >= 11 OR v1 >= 15 OR v1 <= 19",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 OR v1 >= 15 AND v1 <= 19",
			[]*noms.ReadRange{
				index.GreaterOrEqualRange(idxv1.tuple(11)),
			},
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 >= 15 OR v1 <= 19",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 >= 15 AND v1 <= 19",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(15), idxv1.tuple(19)),
			},
			[]int64{5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 <= 14 OR v1 >= 16 AND v1 <= 19",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(11), idxv1.tuple(14)),
				index.ClosedRange(idxv1.tuple(16), idxv1.tuple(19)),
			},
			[]int64{1, 2, 3, 4, 6, 7, 8, 9},
		},
		{
			"v1 < 11",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(11)),
			},
			[]int64{0},
		},
		{
			"v1 < 11 OR v1 < 15",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 2, 3, 4},
		},
		{
			"v1 < 11 AND v1 < 15",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(11)),
			},
			[]int64{0},
		},
		{
			"v1 < 11 OR v1 < 15 OR v1 < 19",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			"v1 < 11 OR v1 < 15 AND v1 < 19",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 2, 3, 4},
		},
		{
			"v1 < 11 AND v1 < 15 AND v1 < 19",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(11)),
			},
			[]int64{0},
		},
		{
			"v1 < 11 OR v1 > 15",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(11)),
				index.GreaterThanRange(idxv1.tuple(15)),
			},
			[]int64{0, 6, 7, 8, 9},
		},
		{
			"v1 < 11 AND v1 > 15",
			nil,
			[]int64{},
		},
		{
			"v1 < 11 OR v1 <= 15",
			[]*noms.ReadRange{
				index.LessOrEqualRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 2, 3, 4, 5},
		},
		{
			"v1 < 11 AND v1 <= 15",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(11)),
			},
			[]int64{0},
		},
		{
			"v1 < 11 OR v1 < 15 OR v1 <= 19",
			[]*noms.ReadRange{
				index.LessOrEqualRange(idxv1.tuple(19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 < 11 OR v1 < 15 AND v1 <= 19",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 2, 3, 4},
		},
		{
			"v1 < 11 AND v1 < 15 OR v1 <= 19",
			[]*noms.ReadRange{
				index.LessOrEqualRange(idxv1.tuple(19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 < 11 AND v1 < 15 AND v1 <= 19",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(11)),
			},
			[]int64{0},
		},
		{
			"v1 < 11 OR v1 >= 15",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(11)),
				index.GreaterOrEqualRange(idxv1.tuple(15)),
			},
			[]int64{0, 5, 6, 7, 8, 9},
		},
		{
			"v1 < 11 AND v1 >= 15",
			nil,
			[]int64{},
		},
		{
			"(v1 < 13 OR v1 > 16) AND (v1 > 10 OR v1 < 19)",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(13)),
				index.GreaterThanRange(idxv1.tuple(16)),
			},
			[]int64{0, 1, 2, 7, 8, 9},
		},
		{
			"v1 <= 11",
			[]*noms.ReadRange{
				index.LessOrEqualRange(idxv1.tuple(11)),
			},
			[]int64{0, 1},
		},
		{
			"v1 <= 11 OR v1 <= 15",
			[]*noms.ReadRange{
				index.LessOrEqualRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 2, 3, 4, 5},
		},
		{
			"v1 <= 11 AND v1 <= 15",
			[]*noms.ReadRange{
				index.LessOrEqualRange(idxv1.tuple(11)),
			},
			[]int64{0, 1},
		},
		{
			"v1 <= 11 OR v1 <= 15 OR v1 <= 19",
			[]*noms.ReadRange{
				index.LessOrEqualRange(idxv1.tuple(19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 <= 11 OR v1 <= 15 AND v1 <= 19",
			[]*noms.ReadRange{
				index.LessOrEqualRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 2, 3, 4, 5},
		},
		{
			"v1 <= 11 AND v1 <= 15 AND v1 <= 19",
			[]*noms.ReadRange{
				index.LessOrEqualRange(idxv1.tuple(11)),
			},
			[]int64{0, 1},
		},
		{
			"v1 <= 11 OR v1 > 15",
			[]*noms.ReadRange{
				index.LessOrEqualRange(idxv1.tuple(11)),
				index.GreaterThanRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 6, 7, 8, 9},
		},
		{
			"v1 <= 11 AND v1 > 15",
			nil,
			[]int64{},
		},
		{
			"v1 <= 11 OR v1 >= 15",
			[]*noms.ReadRange{
				index.LessOrEqualRange(idxv1.tuple(11)),
				index.GreaterOrEqualRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 5, 6, 7, 8, 9},
		},
		{
			"v1 <= 11 AND v1 >= 15",
			nil,
			[]int64{},
		},
		{
			"v1 BETWEEN 11 AND 15",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(11), idxv1.tuple(15)),
			},
			[]int64{1, 2, 3, 4, 5},
		},
		{
			"v1 BETWEEN 11 AND 15 OR v1 BETWEEN 15 AND 19",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(11), idxv1.tuple(19)),
			},
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 BETWEEN 11 AND 15 AND v1 BETWEEN 15 AND 19",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(15), idxv1.tuple(15)),
			},
			[]int64{5},
		},
		{
			"v1 BETWEEN 11 AND 15 OR v1 = 13",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(11), idxv1.tuple(15)),
			},
			[]int64{1, 2, 3, 4, 5},
		},
		{
			"v1 BETWEEN 11 AND 15 OR v1 != 13",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 BETWEEN 11 AND 15 AND v1 != 13",
			[]*noms.ReadRange{
				index.CustomRange(idxv1.tuple(11), idxv1.tuple(13), sql.Closed, sql.Open),
				index.CustomRange(idxv1.tuple(13), idxv1.tuple(15), sql.Open, sql.Closed),
			},
			[]int64{1, 2, 4, 5},
		},
		{
			"v1 BETWEEN 11 AND 15 AND v1 <= 19",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(11), idxv1.tuple(15)),
			},
			[]int64{1, 2, 3, 4, 5},
		},
		{
			"v1 BETWEEN 11 AND 15 AND v1 <= 19",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(11), idxv1.tuple(15)),
			},
			[]int64{1, 2, 3, 4, 5},
		},
		{
			"v1 IN (11, 12, 13)",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
				index.ClosedRange(idxv1.tuple(12), idxv1.tuple(12)),
				index.ClosedRange(idxv1.tuple(13), idxv1.tuple(13)),
			},
			[]int64{1, 2, 3},
		},
		{
			"v1 IN (11, 12, 13) OR v1 BETWEEN 11 and 13",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(11), idxv1.tuple(13)),
			},
			[]int64{1, 2, 3},
		},
		{
			"v1 IN (11, 12, 13) AND v1 > 11",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(12), idxv1.tuple(12)),
				index.ClosedRange(idxv1.tuple(13), idxv1.tuple(13)),
			},
			[]int64{2, 3},
		},
		{
			"v1 IN (11, 12, 13) OR v1 != 12",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 IN (11, 12, 13) AND v1 != 12",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
				index.ClosedRange(idxv1.tuple(13), idxv1.tuple(13)),
			},
			[]int64{1, 3},
		},
		{
			"v1 IN (11, 12, 13) OR v1 >= 13 AND v1 < 15",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
				index.ClosedRange(idxv1.tuple(12), idxv1.tuple(12)),
				index.CustomRange(idxv1.tuple(13), idxv1.tuple(15), sql.Closed, sql.Open),
			},
			[]int64{1, 2, 3, 4},
		},
		{
			"v2 = 21 AND v1 = 11 OR v2 > 25 AND v1 > 11",
			[]*noms.ReadRange{
				index.ClosedRange(idxv2v1.tuple(21, 11), idxv2v1.tuple(21, 11)),
				index.GreaterThanRange(idxv2v1.tuple(25, 11)),
			},
			[]int64{1, 6, 7, 8, 9},
		},
		{
			"v2 > 21 AND v1 > 11 AND v2 < 25 AND v1 < 15",
			[]*noms.ReadRange{
				index.OpenRange(idxv2v1.tuple(21, 11), idxv2v1.tuple(25, 15)),
			},
			[]int64{2, 3, 4},
		},
		{
			"v2 = 21",
			[]*noms.ReadRange{
				index.ClosedRange(idxv2v1Gen.tuple(21), idxv2v1Gen.tuple(21)),
			},
			[]int64{1},
		},
		{
			"v2 = 21 OR v2 = 25",
			[]*noms.ReadRange{
				index.ClosedRange(idxv2v1Gen.tuple(21), idxv2v1Gen.tuple(21)),
				index.ClosedRange(idxv2v1Gen.tuple(25), idxv2v1Gen.tuple(25)),
			},
			[]int64{1, 5},
		},
		{
			"v2 = 21 AND v2 = 25",
			nil,
			[]int64{},
		},
		{
			"v2 = 21 OR v2 = 25 OR v2 = 29",
			[]*noms.ReadRange{
				index.ClosedRange(idxv2v1Gen.tuple(21), idxv2v1Gen.tuple(21)),
				index.ClosedRange(idxv2v1Gen.tuple(25), idxv2v1Gen.tuple(25)),
				index.ClosedRange(idxv2v1Gen.tuple(29), idxv2v1Gen.tuple(29)),
			},
			[]int64{1, 5, 9},
		},
		{
			"v2 = 21 OR v2 = 25 AND v2 = 29",
			[]*noms.ReadRange{
				index.ClosedRange(idxv2v1Gen.tuple(21), idxv2v1Gen.tuple(21)),
			},
			[]int64{1},
		},
		{
			"v2 = 21 AND v2 = 25 AND v2 = 29",
			nil,
			[]int64{},
		},
		{
			"v2 = 21 OR v2 != 21",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v2 = 21 OR v2 != 25",
			[]*noms.ReadRange{
				index.LessThanRange(idxv2v1Gen.tuple(25)),
				index.GreaterThanRange(idxv2v1Gen.tuple(25)),
			},
			[]int64{0, 1, 2, 3, 4, 6, 7, 8, 9},
		},
		{
			"v2 = 21 AND v2 != 25",
			[]*noms.ReadRange{
				index.ClosedRange(idxv2v1Gen.tuple(21), idxv2v1Gen.tuple(21)),
			},
			[]int64{1},
		},
		{
			"v2 = 21 OR v2 = 25 OR v2 != 29",
			[]*noms.ReadRange{
				index.LessThanRange(idxv2v1Gen.tuple(29)),
				index.GreaterThanRange(idxv2v1Gen.tuple(29)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			"v2 = 21 OR v2 = 25 AND v2 != 29",
			[]*noms.ReadRange{
				index.ClosedRange(idxv2v1Gen.tuple(21), idxv2v1Gen.tuple(21)),
				index.ClosedRange(idxv2v1Gen.tuple(25), idxv2v1Gen.tuple(25)),
			},
			[]int64{1, 5},
		},
		{
			"v2 = 21 AND v2 = 25 OR v2 != 29",
			[]*noms.ReadRange{
				index.LessThanRange(idxv2v1Gen.tuple(29)),
				index.GreaterThanRange(idxv2v1Gen.tuple(29)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			"v2 = 21 AND v2 = 25 AND v2 != 29",
			nil,
			[]int64{},
		},
	}

	for _, test := range tests {
		t.Run(test.whereStmt, func(t *testing.T) {
			query := fmt.Sprintf(`SELECT pk FROM test WHERE %s ORDER BY 1`, test.whereStmt)

			finalRanges, err := ReadRangesFromQuery(sqlCtx, engine, query)
			require.NoError(t, err)

			_, iter, _, err := engine.Query(sqlCtx, query)
			require.NoError(t, err)
			res, err := sql.RowIterToRows(sqlCtx, iter)
			require.NoError(t, err)

			if assert.Equal(t, len(test.pks), len(res)) {
				for i, pk := range test.pks {
					if assert.Equal(t, 1, res[i].Len()) {
						assert.Equal(t, pk, res[i].GetValue(0))
					}
				}
			}

			if assert.Equal(t, len(test.finalRanges), len(finalRanges)) {
				finalRangeMatches := make([]bool, len(finalRanges))
				for i, testFinalRange := range test.finalRanges {
					for _, finalRange := range finalRanges {
						if index.ReadRangesEqual(finalRange, testFinalRange) {
							if finalRangeMatches[i] {
								require.FailNow(t, fmt.Sprintf("Duplicate ReadRange: `%v`", finalRange))
							} else {
								finalRangeMatches[i] = true
							}
						}
					}
				}
				for _, finalRangeMatch := range finalRangeMatches {
					if !finalRangeMatch {
						require.FailNow(t, fmt.Sprintf("Expected: `%v`\nActual:   `%v`", test.finalRanges, finalRanges))
					}
				}
			} else {
				t.Logf("%v != %v", test.finalRanges, finalRanges)
			}
		})
	}
}

// TestMergeableIndexesNulls is based on TestMergeableIndexes, but specifically handles IS NULL and IS NOT NULL.
// For now, some of these tests are broken, but they return the correct end result. As NULL is encoded as being a value
// larger than all integers, == NULL becomes a subset of > x and >= x, thus the intersection returns == NULL.
// The correct behavior would be to return the empty range in that example. However, as the SQL engine still filters the
// returned results, we end up with zero values actually being returned, just like we'd expect from the empty range.
// As a consequence, I'm leaving these tests in to verify that the overall result is correct, but the intermediate
// ranges may be incorrect.
// TODO: disassociate NULL ranges from value ranges and fix the intermediate ranges (finalRanges).
func TestMergeableIndexesNulls(t *testing.T) {
	if types.Format_Default != types.Format_LD_1 {
		t.Skip() // this test is specific to Noms ranges
	}

	engine, sqlCtx, indexTuples := setupIndexes(t, "test", `INSERT INTO test VALUES
		(0, 10, 20),
		(1, 11, 21),
		(2, NULL, NULL),
		(3, 13, 23),
		(4, NULL, NULL),
		(5, 15, 25),
		(6, NULL, NULL),
		(7, 17, 27),
		(8, 18, 28),
		(9, 19, 29);`)
	idxv1 := indexTuples[0]

	tests := []struct {
		whereStmt   string
		finalRanges []*noms.ReadRange
		pks         []int64
	}{
		{
			"v1 IS NULL",
			[]*noms.ReadRange{
				index.NullRange(),
			},
			[]int64{2, 4, 6},
		},
		{
			"v1 IS NULL OR v1 IS NULL",
			[]*noms.ReadRange{
				index.NullRange(),
			},
			[]int64{2, 4, 6},
		},
		{
			"v1 IS NULL AND v1 IS NULL",
			[]*noms.ReadRange{
				index.NullRange(),
			},
			[]int64{2, 4, 6},
		},
		{
			"v1 IS NULL OR v1 = 11",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(11), idxv1.tuple(11)),
				index.NullRange(),
			},
			[]int64{1, 2, 4, 6},
		},
		{
			"v1 IS NULL OR v1 < 16",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(16)),
				index.NullRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6},
		},
		{
			"v1 IS NULL OR v1 > 16",
			[]*noms.ReadRange{
				index.NullRange(),
				index.GreaterThanRange(idxv1.tuple(16)),
			},
			[]int64{2, 4, 6, 7, 8, 9},
		},
		{
			"v1 IS NULL AND v1 < 16",
			nil,
			[]int64{},
		},
		{
			"v1 IS NULL AND v1 > 16",
			[]*noms.ReadRange{},
			[]int64{},
		},
		{
			"v1 IS NULL OR v1 IS NOT NULL",
			[]*noms.ReadRange{
				index.AllRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 IS NULL AND v1 IS NOT NULL",
			nil,
			[]int64{},
		},
		{
			"v1 IS NOT NULL",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 3, 5, 7, 8, 9},
		},
		{
			"v1 IS NOT NULL OR v1 IS NULL",
			[]*noms.ReadRange{
				index.AllRange(),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 IS NOT NULL AND v1 IS NULL",
			nil,
			[]int64{},
		},
		{
			"v1 IS NOT NULL OR v1 = 15",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 3, 5, 7, 8, 9},
		},
		{
			"v1 IS NOT NULL OR v1 > 16",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 3, 5, 7, 8, 9},
		},
		{
			"v1 IS NOT NULL OR v1 < 16",
			[]*noms.ReadRange{
				index.NotNullRange(),
			},
			[]int64{0, 1, 3, 5, 7, 8, 9},
		},
		{
			"v1 IS NOT NULL AND v1 = 15",
			[]*noms.ReadRange{
				index.ClosedRange(idxv1.tuple(15), idxv1.tuple(15)),
			},
			[]int64{5},
		},
		{
			"v1 IS NOT NULL AND v1 > 16",
			[]*noms.ReadRange{
				index.GreaterThanRange(idxv1.tuple(16)),
			},
			[]int64{7, 8, 9},
		},
		{
			"v1 IS NOT NULL AND v1 < 16",
			[]*noms.ReadRange{
				index.LessThanRange(idxv1.tuple(16)),
			},
			[]int64{0, 1, 3, 5},
		},
	}

	for _, test := range tests {
		t.Run(test.whereStmt, func(t *testing.T) {
			query := fmt.Sprintf(`SELECT pk FROM test WHERE %s ORDER BY 1`, test.whereStmt)

			finalRanges, err := ReadRangesFromQuery(sqlCtx, engine, query)
			require.NoError(t, err)

			_, iter, _, err := engine.Query(sqlCtx, query)
			require.NoError(t, err)

			res, err := sql.RowIterToRows(sqlCtx, iter)
			require.NoError(t, err)
			if assert.Equal(t, len(test.pks), len(res)) {
				for i, pk := range test.pks {
					if assert.Equal(t, 1, res[i].Len()) {
						assert.Equal(t, pk, res[i].GetValue(0))
					}
				}
			}

			if assert.Equal(t, len(test.finalRanges), len(finalRanges)) {
				finalRangeMatches := make([]bool, len(finalRanges))
				for _, finalRange := range finalRanges {
					for i, testFinalRange := range test.finalRanges {
						if index.ReadRangesEqual(finalRange, testFinalRange) {
							if finalRangeMatches[i] {
								require.FailNow(t, fmt.Sprintf("Duplicate ReadRange: `%v`", finalRange))
							} else {
								finalRangeMatches[i] = true
							}
						}
					}
				}
				for _, finalRangeMatch := range finalRangeMatches {
					if !finalRangeMatch {
						require.FailNow(t, fmt.Sprintf("Expected: `%v`\nActual:   `%v`", test.finalRanges, finalRanges))
					}
				}
			} else {
				t.Logf("%v != %v", test.finalRanges, finalRanges)
			}
		})
	}
}

func ReadRangesFromQuery(ctx *sql.Context, eng *sqle.Engine, query string) ([]*noms.ReadRange, error) {
	binder := planbuilder.New(ctx, eng.Analyzer.Catalog, eng.EventScheduler, eng.Parser)
	parsed, _, _, qFlags, err := binder.Parse(query, nil, false)
	if err != nil {
		return nil, err
	}

	analyzed, err := eng.Analyzer.Analyze(ctx, parsed, nil, qFlags)
	if err != nil {
		return nil, err
	}

	var lookup sql.IndexLookup
	transform.Inspect(analyzed, func(n sql.Node) bool {
		switch node := n.(type) {
		case *plan.IndexedTableAccess:
			lookup = plan.GetIndexLookup(node)
		}
		return true
	})

	return index.NomsRangesFromIndexLookup(ctx, lookup)
}
