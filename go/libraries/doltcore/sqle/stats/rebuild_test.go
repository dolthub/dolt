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

package stats

import (
	"container/heap"
	"context"
	"fmt"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/pool"
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
		keys    []sql.Row
		keyDesc val.TupleDesc
		bucket  DoltBucket
	}{
		{
			name:    "ints",
			keys:    []sql.Row{{1}, {1}, {1}, {2}, {2}, {2}, {2}, {3}, {3}, {3}, {4}, {4}, {4}, {5}, {5}},
			keyDesc: val.NewTupleDescriptor(val.Type{Enc: val.Int64Enc, Nullable: false}),
			bucket: DoltBucket{
				Count:      15,
				Distinct:   5,
				Mcv:        []sql.Row{{int64(4)}, {int64(2)}, {int64(3)}},
				McvCount:   []uint64{3, 4, 3},
				UpperBound: sql.Row{int64(5)},
				BoundCount: 2,
			},
		},
		{
			// technically nulls should be at beginning
			name:    "ints with middle nulls",
			keys:    []sql.Row{{1}, {1}, {1}, {2}, {2}, {2}, {2}, {nil}, {nil}, {nil}, {3}, {4}, {4}, {4}, {5}, {5}},
			keyDesc: val.NewTupleDescriptor(val.Type{Enc: val.Int64Enc, Nullable: true}),
			bucket: DoltBucket{
				Count:      16,
				Distinct:   6,
				Null:       3,
				Mcv:        []sql.Row{{int64(4)}, {int64(2)}, {nil}},
				McvCount:   []uint64{3, 4, 3},
				UpperBound: sql.Row{int64(5)},
				BoundCount: 2,
			},
		},
		{
			name:    "ints with beginning nulls",
			keys:    []sql.Row{{nil}, {nil}, {1}, {2}, {2}, {2}, {2}, {3}, {3}, {3}, {4}, {4}, {4}, {5}, {5}},
			keyDesc: val.NewTupleDescriptor(val.Type{Enc: val.Int64Enc, Nullable: true}),
			bucket: DoltBucket{
				Count:      15,
				Distinct:   6,
				Null:       2,
				Mcv:        []sql.Row{{int64(3)}, {int64(4)}, {int64(2)}},
				McvCount:   []uint64{3, 3, 4},
				UpperBound: sql.Row{int64(5)},
				BoundCount: 2,
			},
		},
		{
			name:    "more ints",
			keys:    []sql.Row{{1}, {1}, {1}, {2}, {2}, {2}, {2}, {3}, {3}, {3}, {4}, {4}, {4}, {5}, {5}, {5}, {5}, {6}, {6}, {6}, {6}, {7}},
			keyDesc: val.NewTupleDescriptor(val.Type{Enc: val.Int64Enc, Nullable: false}),
			bucket: DoltBucket{
				Count:      22,
				Distinct:   7,
				BoundCount: 1,
				Mcv:        []sql.Row{{int64(2)}, {int64(6)}, {int64(5)}},
				McvCount:   []uint64{4, 4, 4},
				UpperBound: sql.Row{int64(7)},
			},
		},
		{
			name:    "2-ints",
			keys:    []sql.Row{{1, 1}, {1, 1}, {1, 2}, {2, 1}, {2, 2}, {2, 3}, {2, 3}, {3, 1}, {3, 2}, {3, 3}, {4, 1}, {4, 1}, {4, 1}, {5, 1}, {5, 2}},
			keyDesc: val.NewTupleDescriptor(val.Type{Enc: val.Int64Enc, Nullable: false}, val.Type{Enc: val.Int64Enc, Nullable: false}),
			bucket: DoltBucket{
				Count:      15,
				Distinct:   11,
				Mcv:        []sql.Row{{int64(1), int64(1)}, {int64(4), int64(1)}, {int64(2), int64(3)}},
				McvCount:   []uint64{2, 3, 2},
				UpperBound: sql.Row{int64(5), int64(2)},
				BoundCount: 1,
			},
		},
		{
			name:    "2-ints with nulls",
			keys:    []sql.Row{{nil, 1}, {1, nil}, {1, 2}, {2, nil}, {2, 2}},
			keyDesc: val.NewTupleDescriptor(val.Type{Enc: val.Int64Enc, Nullable: true}, val.Type{Enc: val.Int64Enc, Nullable: true}),
			bucket: DoltBucket{
				Count:      5,
				Distinct:   5,
				Null:       3,
				Mcv:        []sql.Row{{int64(2), int64(2)}, {int64(1), nil}, {int64(1), int64(2)}},
				McvCount:   []uint64{1, 1, 1},
				UpperBound: sql.Row{int64(2), int64(2)},
				BoundCount: 1,
			},
		},
		{
			name:    "varchars",
			keys:    []sql.Row{{"a"}, {"b"}, {"c"}, {"d"}, {"e"}, {"e"}, {"f"}, {"g"}, {"g"}, {"g"}, {"h"}, {"h"}, {"h"}, {"i"}, {"i"}},
			keyDesc: val.NewTupleDescriptor(val.Type{Enc: val.StringEnc, Nullable: false}),
			bucket: DoltBucket{
				Count:      15,
				Distinct:   9,
				Mcv:        []sql.Row{{"i"}, {"h"}, {"g"}},
				McvCount:   []uint64{2, 3, 3},
				UpperBound: sql.Row{"i"},
				BoundCount: 2,
			},
		},
		{
			name:    "varchar-ints",
			keys:    []sql.Row{{"a", 1}, {"b", 1}, {"c", 1}, {"d", 1}, {"e", 1}, {"e", 2}, {"f", 1}, {"g", 1}, {"g", 2}, {"g", 2}, {"h", 1}, {"h", 1}, {"h", 2}, {"i", 1}, {"i", 1}},
			keyDesc: val.NewTupleDescriptor(val.Type{Enc: val.StringEnc, Nullable: false}, val.Type{Enc: val.Int64Enc, Nullable: false}),
			bucket: DoltBucket{
				Count:      15,
				Distinct:   12,
				Mcv:        []sql.Row{{"i", int64(1)}, {"g", int64(2)}, {"h", int64(1)}},
				McvCount:   []uint64{2, 2, 2},
				UpperBound: sql.Row{"i", int64(1)},
				BoundCount: 2,
			},
		},
	}

	ctx := context.Background()
	pool := pool.NewBuffPool()
	for _, tt := range tests {
		t.Run(fmt.Sprintf("build bucket: %s", tt.name), func(t *testing.T) {
			b := newBucketBuilder(statsMeta{}, tt.keyDesc.Count(), tt.keyDesc)
			kb := val.NewTupleBuilder(tt.keyDesc)
			for _, k := range tt.keys {
				for i, v := range k {
					// |ns| only needed for out of band tuples
					err := index.PutField(ctx, nil, kb, i, v)
					assert.NoError(t, err)
				}
				b.add(kb.Build(pool))
			}
			// |ns| only needed for out of band tuples
			bucket, err := b.finalize(ctx, nil)
			require.NoError(t, err)

			require.Equal(t, int(tt.bucket.Count), int(bucket.Count))
			require.Equal(t, int(tt.bucket.Null), int(bucket.Null))
			require.Equal(t, int(tt.bucket.Distinct), int(bucket.Distinct))
			require.Equal(t, int(tt.bucket.BoundCount), int(bucket.BoundCount))
			require.Equal(t, tt.bucket.UpperBound, bucket.UpperBound)
			require.Equal(t, tt.bucket.McvCount, bucket.McvCount)
			require.Equal(t, tt.bucket.Mcv, bucket.Mcv)
		})
	}
}
