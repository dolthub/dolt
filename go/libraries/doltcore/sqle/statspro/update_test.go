// Copyright 2023 Dolthub, Inc.
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

package statspro

import (
	"container/heap"
	"context"
	"fmt"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

func TestMcvHeap(t *testing.T) {
	h := new(mcvHeap)
	for i := 0; i < 10; i++ {
		heap.Push(h, mcv{val.Tuple{byte(i)}, i})
		if i > 2 {
			heap.Pop(h)
		}
	}
	require.Equal(t, 3, h.Len())
	require.Equal(t, 3, len(h.Counts()))
	for _, cnt := range h.Counts() {
		switch int(cnt) {
		case 7, 8, 9:
		default:
			t.Errorf("unexpected value in mcvHeap: %d", cnt)
		}
	}
	cmp := []int{7, 8, 9}
	var res []int
	for i := 0; h.Len() > 0; i++ {
		next := heap.Pop(h)
		res = append(res, next.(mcv).cnt)
	}
	require.Equal(t, cmp, res)
}

func TestBucketBuilder(t *testing.T) {
	tests := []struct {
		name    string
		keys    []sql.UntypedSqlRow
		keyDesc val.TupleDesc
		bucket  DoltBucket
	}{
		{
			name:    "ints",
			keys:    []sql.UntypedSqlRow{{1}, {1}, {1}, {2}, {2}, {2}, {2}, {3}, {3}, {3}, {4}, {4}, {4}, {5}, {5}},
			keyDesc: val.NewTupleDescriptor(val.Type{Enc: val.Int64Enc, Nullable: false}),
			bucket: DoltBucket{Bucket: &stats.Bucket{
				RowCnt:      15,
				DistinctCnt: 5,
				McvVals:     []sql.Row{},
				McvsCnt:     []uint64{},
				BoundVal:    sql.UntypedSqlRow{int64(5)},
				BoundCnt:    2,
			}},
		},
		{
			// technically nulls should be at beginning
			name:    "ints with middle nulls",
			keys:    []sql.UntypedSqlRow{{1}, {1}, {1}, {2}, {2}, {2}, {2}, {nil}, {nil}, {nil}, {3}, {4}, {4}, {4}, {5}, {5}},
			keyDesc: val.NewTupleDescriptor(val.Type{Enc: val.Int64Enc, Nullable: true}),
			bucket: DoltBucket{Bucket: &stats.Bucket{
				RowCnt:      16,
				DistinctCnt: 6,
				NullCnt:     3,
				McvVals:     []sql.Row{},
				McvsCnt:     []uint64{},
				BoundVal:    sql.UntypedSqlRow{int64(5)},
				BoundCnt:    2,
			}},
		},
		{
			name:    "ints with beginning nulls",
			keys:    []sql.UntypedSqlRow{{nil}, {nil}, {1}, {2}, {2}, {2}, {2}, {3}, {3}, {3}, {4}, {4}, {4}, {5}, {5}},
			keyDesc: val.NewTupleDescriptor(val.Type{Enc: val.Int64Enc, Nullable: true}),
			bucket: DoltBucket{Bucket: &stats.Bucket{
				RowCnt:      15,
				DistinctCnt: 6,
				NullCnt:     2,
				McvVals:     []sql.Row{},
				McvsCnt:     []uint64{},
				BoundVal:    sql.UntypedSqlRow{int64(5)},
				BoundCnt:    2,
			}},
		},
		{
			name:    "more ints",
			keys:    []sql.UntypedSqlRow{{1}, {1}, {1}, {2}, {2}, {2}, {2}, {3}, {3}, {3}, {4}, {4}, {4}, {5}, {5}, {5}, {5}, {6}, {6}, {6}, {6}, {7}},
			keyDesc: val.NewTupleDescriptor(val.Type{Enc: val.Int64Enc, Nullable: false}),
			bucket: DoltBucket{Bucket: &stats.Bucket{
				RowCnt:      22,
				DistinctCnt: 7,
				BoundCnt:    1,
				McvVals:     []sql.Row{},
				McvsCnt:     []uint64{},
				BoundVal:    sql.UntypedSqlRow{int64(7)},
			}},
		},
		{
			name:    "2-ints",
			keys:    []sql.UntypedSqlRow{{1, 1}, {1, 1}, {1, 2}, {2, 1}, {2, 2}, {2, 3}, {2, 3}, {3, 1}, {3, 2}, {3, 3}, {4, 1}, {4, 1}, {4, 1}, {5, 1}, {5, 2}},
			keyDesc: val.NewTupleDescriptor(val.Type{Enc: val.Int64Enc, Nullable: false}, val.Type{Enc: val.Int64Enc, Nullable: false}),
			bucket: DoltBucket{Bucket: &stats.Bucket{
				RowCnt:      15,
				DistinctCnt: 11,
				McvVals:     []sql.Row{sql.UntypedSqlRow{int64(4), int64(1)}},
				McvsCnt:     []uint64{3},
				BoundVal:    sql.UntypedSqlRow{int64(5), int64(2)},
				BoundCnt:    1,
			}},
		},
		{
			name:    "2-ints with nulls",
			keys:    []sql.UntypedSqlRow{{nil, 1}, {1, nil}, {1, 2}, {2, nil}, {2, 2}},
			keyDesc: val.NewTupleDescriptor(val.Type{Enc: val.Int64Enc, Nullable: true}, val.Type{Enc: val.Int64Enc, Nullable: true}),
			bucket: DoltBucket{Bucket: &stats.Bucket{
				RowCnt:      5,
				DistinctCnt: 5,
				NullCnt:     3,
				McvVals:     []sql.Row{},
				McvsCnt:     []uint64{},
				BoundVal:    sql.UntypedSqlRow{int64(2), int64(2)},
				BoundCnt:    1},
			},
		},
		{
			name:    "varchars",
			keys:    []sql.UntypedSqlRow{{"a"}, {"b"}, {"c"}, {"d"}, {"e"}, {"e"}, {"f"}, {"g"}, {"g"}, {"g"}, {"h"}, {"h"}, {"h"}, {"i"}, {"i"}},
			keyDesc: val.NewTupleDescriptor(val.Type{Enc: val.StringEnc, Nullable: false}),
			bucket: DoltBucket{Bucket: &stats.Bucket{
				RowCnt:      15,
				DistinctCnt: 9,
				McvVals:     []sql.Row{},
				McvsCnt:     []uint64{},
				BoundVal:    sql.UntypedSqlRow{"i"},
				BoundCnt:    2,
			}},
		},
		{
			name:    "varchar-ints",
			keys:    []sql.UntypedSqlRow{{"a", 1}, {"b", 1}, {"c", 1}, {"d", 1}, {"e", 1}, {"e", 2}, {"f", 1}, {"g", 1}, {"g", 2}, {"g", 2}, {"h", 1}, {"h", 1}, {"h", 2}, {"i", 1}, {"i", 1}},
			keyDesc: val.NewTupleDescriptor(val.Type{Enc: val.StringEnc, Nullable: false}, val.Type{Enc: val.Int64Enc, Nullable: false}),
			bucket: DoltBucket{Bucket: &stats.Bucket{
				RowCnt:      15,
				DistinctCnt: 12,
				McvVals:     []sql.Row{},
				McvsCnt:     []uint64{},
				BoundVal:    sql.UntypedSqlRow{"i", int64(1)},
				BoundCnt:    2,
			}},
		},
		{
			name:    "mcvs",
			keys:    []sql.UntypedSqlRow{{1}, {2}, {3}, {4}, {5}, {6}, {7}, {7}, {7}, {7}, {8}, {9}, {10}, {10}, {10}, {11}, {12}, {13}, {14}, {15}, {20}, {21}, {22}},
			keyDesc: val.NewTupleDescriptor(val.Type{Enc: val.Int64Enc, Nullable: false}),
			bucket: DoltBucket{Bucket: &stats.Bucket{
				RowCnt:      23,
				DistinctCnt: 18,
				McvVals:     []sql.Row{sql.UntypedSqlRow{int64(10)}, sql.UntypedSqlRow{int64(7)}},
				McvsCnt:     []uint64{3, 4},
				BoundVal:    sql.UntypedSqlRow{int64(22)},
				BoundCnt:    1,
			}},
		},
	}

	ctx := context.Background()
	pool := pool.NewBuffPool()
	for _, tt := range tests {
		t.Run(fmt.Sprintf("build bucket: %s", tt.name), func(t *testing.T) {
			b := newBucketBuilder(sql.StatQualifier{}, tt.keyDesc.Count(), tt.keyDesc)
			kb := val.NewTupleBuilder(tt.keyDesc)
			for _, k := range tt.keys {
				for i, v := range k {
					// |ns| only needed for out of band tuples
					err := tree.PutField(ctx, nil, kb, i, v)
					assert.NoError(t, err)
				}
				b.add(kb.Build(pool))
			}
			// |ns| only needed for out of band tuples
			bucket, err := b.finalize(ctx, nil)
			require.NoError(t, err)

			require.Equal(t, int(tt.bucket.RowCount()), int(bucket.RowCount()))
			require.Equal(t, int(tt.bucket.NullCount()), int(bucket.NullCount()))
			require.Equal(t, int(tt.bucket.DistinctCount()), int(bucket.DistinctCount()))
			require.Equal(t, int(tt.bucket.BoundCount()), int(bucket.BoundCount()))
			require.Equal(t, tt.bucket.UpperBound(), bucket.UpperBound())
			require.Equal(t, tt.bucket.McvCounts(), bucket.McvCounts())
			require.Equal(t, tt.bucket.Mcvs(), bucket.Mcvs())
		})
	}
}
