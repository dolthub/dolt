// Copyright 2023-2025 Dolthub, Inc.
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
	"sort"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"

	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

const (
	bucketLowCnt = 20
	mcvCnt       = 3
)

func firstRowForIndex(ctx *sql.Context, idxLen int, prollyMap prolly.Map, keyBuilder *val.TupleBuilder) (sql.Row, error) {
	if cnt, err := prollyMap.Count(); err != nil {
		return nil, err
	} else if cnt == 0 {
		return nil, nil
	}

	buffPool := prollyMap.NodeStore().Pool()

	// first row is ordinal 0
	firstIter, err := prollyMap.IterOrdinalRange(ctx, 0, 1)
	if err != nil {
		return nil, err
	}
	keyBytes, _, err := firstIter.Next(ctx)
	if err != nil {
		return nil, err
	}
	for i := range keyBuilder.Desc.Types {
		keyBuilder.PutRaw(i, keyBytes.GetField(i))
	}

	firstKey, _ := keyBuilder.Build(buffPool)
	firstRow := make(sql.Row, idxLen)
	for i := range firstRow {
		firstRow[i], err = tree.GetField(ctx, prollyMap.KeyDesc(), i, firstKey, prollyMap.NodeStore())
		if err != nil {
			return nil, err
		}
	}
	return firstRow, nil
}

func newBucketBuilder(qual sql.StatQualifier, prefixLen int, tupleDesc val.TupleDesc) *bucketBuilder {
	return &bucketBuilder{
		qual:      qual,
		prefixLen: prefixLen,
		mcvs:      new(mcvHeap),
		tupleDesc: tupleDesc.PrefixDesc(prefixLen),
	}
}

// bucketBuilder performs an aggregation on a sorted series of keys to
// collect statistics for a single histogram bucket. DistinctCount is fuzzy,
// we might double count a key that crosses bucket boundaries.
type bucketBuilder struct {
	qual      sql.StatQualifier
	tupleDesc val.TupleDesc
	prefixLen int

	count    int
	distinct int
	nulls    int
	mcvs     *mcvHeap

	currentKey val.Tuple
	currentCnt int

	globalDistinct int
	globalCount    int
	prevBound      val.Tuple
}

// newBucket zeroes aggregation statistics. Global counters are not reset for
// new buckets. Updaters should only be reused between buckets for the same
// column statistic.
func (u *bucketBuilder) newBucket() {
	u.count = 0
	u.distinct = 0
	u.nulls = 0
	u.currentKey = nil
	u.currentCnt = 0

	oldMcvs := *u.mcvs
	oldMcvs = oldMcvs[:0]
	u.mcvs = &oldMcvs
}

// finalize converts the current aggregation stats into a histogram bucket,
// which includes deserializing most common value tuples into sql.Rows.
func (u *bucketBuilder) finalize(ctx context.Context, ns tree.NodeStore) (*stats.Bucket, error) {
	// update MCV in case we've ended on a run of many identical keys
	u.updateMcv()

	u.mcvs.Sort()
	u.mcvs.Truncate(2 * float64(u.count) / float64(u.distinct)) // only keep MCVs that are > twice as common as average

	// convert the MCV tuples into SQL rows (most efficient to only do this once)
	mcvRows, err := u.mcvs.Values(ctx, u.tupleDesc, ns, u.prefixLen)
	if err != nil {
		return nil, err
	}
	upperBound := make(sql.Row, u.prefixLen)
	if u.currentKey != nil {
		for i := 0; i < u.prefixLen; i++ {
			upperBound[i], err = tree.GetField(ctx, u.tupleDesc, i, u.currentKey, ns)
			if err != nil {
				return nil, err
			}
		}
	}
	return &stats.Bucket{
		RowCnt:      uint64(u.count),
		DistinctCnt: uint64(u.distinct),
		BoundCnt:    uint64(u.currentCnt),
		McvVals:     mcvRows,
		McvsCnt:     u.mcvs.Counts(),
		BoundVal:    upperBound,
		NullCnt:     uint64(u.nulls),
	}, nil
}

// add inputs a new row for a histogram bucket aggregation. We assume
// the key has already been truncated to the appropriate prefix length.
func (u *bucketBuilder) add(ctx context.Context, key val.Tuple) {
	newKey := u.currentKey == nil || u.tupleDesc.Compare(ctx, u.currentKey, key) != 0
	if newKey {
		u.newKey(ctx, key)
	} else {
		u.currentCnt++
	}

	u.count++
	u.globalCount++
	for i := 0; i < u.prefixLen; i++ {
		if key.FieldIsNull(i) {
			u.nulls++
			break
		}
	}
}

// newKey updates state for a new key in the rolling stream.
func (u *bucketBuilder) newKey(ctx context.Context, key val.Tuple) {
	u.updateMcv()
	if u.prevBound != nil {
		if u.tupleDesc.Compare(ctx, u.prevBound, key) != 0 {
			u.globalDistinct++
			u.prevBound = nil
		} else {
			// not a globally unique key
		}
	} else {
		u.globalDistinct++
	}
	u.distinct++
	u.currentCnt = 1
	u.currentKey = key
}

// updateMcv updates the most common value heap when we've demarked the
// end of a sequence of key repeats.
func (u *bucketBuilder) updateMcv() {
	if u.count == 0 && u.nulls == 0 {
		return
	}
	key := u.currentKey
	cnt := u.currentCnt
	u.mcvs.Add(mcv{key, cnt})
}

type mcv struct {
	val val.Tuple
	cnt int
}

type mcvHeap []mcv

var _ heap.Interface = (*mcvHeap)(nil)

func (m *mcvHeap) Add(i mcv) {
	heap.Push(m, i)
	if m.Len() > mcvCnt {
		heap.Pop(m)
	}
}

func (m mcvHeap) Counts() []uint64 {
	ret := make([]uint64, len(m))
	for i, mcv := range m {
		ret[i] = uint64(mcv.cnt)
	}
	return ret
}

func (m mcvHeap) Sort() {
	sort.Slice(m, m.Less)
}

func (m *mcvHeap) Truncate(cutoff float64) {
	start := m.Len()
	for i, v := range *m {
		if float64(v.cnt) >= cutoff {
			start = i
			break
		}
	}
	old := *m
	*m = old[start:]
}

func (m mcvHeap) Values(ctx context.Context, keyDesc val.TupleDesc, ns tree.NodeStore, prefixLen int) ([]sql.Row, error) {
	ret := make([]sql.Row, len(m))
	for i, mcv := range m {
		// todo build sql.Row
		row := make(sql.Row, prefixLen)
		var err error
		for i := 0; i < prefixLen; i++ {
			row[i], err = tree.GetField(ctx, keyDesc, i, mcv.val, ns)
			if err != nil {
				return nil, err
			}
		}
		ret[i] = row
	}
	return ret, nil
}

func (m mcvHeap) Len() int {
	return len(m)
}

func (m mcvHeap) Less(i, j int) bool {
	return m[i].cnt < m[j].cnt
}

func (m mcvHeap) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

func (m *mcvHeap) Push(x any) {
	*m = append(*m, x.(mcv))
}

func (m *mcvHeap) Pop() any {
	old := *m
	n := len(old)
	ret := old[n-1]
	*m = old[0 : n-1]
	return ret
}
