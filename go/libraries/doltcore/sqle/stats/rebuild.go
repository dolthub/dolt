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
	"errors"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
	"io"
	"strings"
	"time"
)

const (
	bucketLowCnt = 20
	mcvCnt       = 3
)

// rebuildStats builds histograms for each index statistic metadata
// indicated in |newStats|.
func rebuildStats(ctx *sql.Context, indexes []sql.Index, idxMetas []indexMeta, newStats map[indexMeta][]statsMeta) (map[statsMeta]*DoltStats, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	prov := dSess.Provider()
	db, err := prov.Database(ctx, idxMetas[0].db)
	if err != nil {
		return nil, err
	}
	tab, ok, err := db.GetTableInsensitive(ctx, idxMetas[0].table)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("error creating statistics for table: %s; table not found", idxMetas[0].table)
	}

	var dTab *doltdb.Table
	switch t := tab.(type) {
	case *sqle.AlterableDoltTable:
		dTab, err = t.DoltTable.DoltTable(ctx)
	case *sqle.WritableDoltTable:
		dTab, err = t.DoltTable.DoltTable(ctx)
	case *sqle.DoltTable:
		dTab, err = t.DoltTable(ctx)
	default:
		return nil, fmt.Errorf("failed to unwrap dolt table from type: %T", tab)
	}
	if err != nil {
		return nil, err
	}

	ret := make(map[statsMeta]*DoltStats)

	for i, meta := range idxMetas {
		var idx durable.Index
		var err error
		if meta.index == "PRIMARY" {
			idx, err = dTab.GetRowData(ctx)
		} else {
			idx, err = dTab.GetIndexRowData(ctx, meta.index)
		}
		if err != nil {
			return nil, err
		}
		prollyMap := durable.ProllyMapFromIndex(idx)
		keyBuilder := val.NewTupleBuilder(prollyMap.KeyDesc())
		buffPool := prollyMap.NodeStore().Pool()

		var types []sql.Type
		for _, cet := range indexes[i].ColumnExpressionTypes() {
			types = append(types, cet.Type)
		}

		// find level
		levelNodes, err := tree.GetHistogramLevel(ctx, prollyMap.Tuples(), bucketLowCnt)
		if err != nil {
			return nil, err
		}
		var addrs []hash.Hash
		for _, n := range levelNodes {
			addrs = append(addrs, n.HashOf())
		}

		updaters := make([]*bucketBuilder, len(newStats[meta]))
		for i, statsMeta := range newStats[meta] {
			cols := strings.Split(statsMeta.pref, ",")
			updaters[i] = newBucketBuilder(statsMeta, len(cols), prollyMap.KeyDesc())
			ret[statsMeta] = &DoltStats{
				level:     levelNodes[0].Level(),
				chunks:    addrs,
				CreatedAt: time.Now(),
				Columns:   cols,
				Types:     types[:len(cols)],
			}
		}

		// read leaf rows for each bucket
		for i, _ := range levelNodes {
			// each node is a bucket
			for _, u := range updaters {
				// reset stats updaters for the new bucket
				u.newBucket()
			}

			// we read exclusive range [node first key, next node first key)
			var start, stop val.Tuple
			if i > 0 {
				start = val.Tuple(levelNodes[i].GetKey(0))
			}
			if i+1 < len(levelNodes) {
				stop = val.Tuple(levelNodes[i+1].GetKey(0))
			}
			iter, err := prollyMap.IterKeyRange(ctx, start, stop)
			if err != nil {
				return nil, err
			}
			for {
				// stats key will be a prefix of the index key
				keyBytes, _, err := iter.Next(ctx)
				if errors.Is(err, io.EOF) {
					break
				} else if err != nil {
					return nil, err
				}
				// build full key
				for i := range keyBuilder.Desc.Types {
					keyBuilder.PutRaw(i, keyBytes.GetField(i))
				}

				for _, u := range updaters {
					// update with prefix of the key
					u.add(keyBuilder.BuildPrefixNoRecycle(buffPool, u.prefixLen))
				}
				keyBuilder.Recycle()
			}
			for _, u := range updaters {
				// finalize the aggregation
				bucket, err := u.finalize(ctx, prollyMap.NodeStore())
				if err != nil {
					return nil, err
				}
				ret[u.meta].Histogram = append(ret[u.meta].Histogram, bucket)
			}
		}
		for _, u := range updaters {
			ret[u.meta].DistinctCount = uint64(u.globalDistinct)
			ret[u.meta].RowCount = uint64(u.globalCount)
		}
	}
	return ret, nil
}

func newBucketBuilder(meta statsMeta, prefixLen int, tupleDesc val.TupleDesc) *bucketBuilder {
	return &bucketBuilder{
		meta:      meta,
		prefixLen: prefixLen,
		mcvs:      new(mcvHeap),
		tupleDesc: tupleDesc.PrefixDesc(prefixLen),
	}
}

// bucketBuilder performs an aggregation on a sorted series of keys to
// collect statistics for a single histogram bucket. Distinct is fuzzy,
// we might double count a key that crosses bucket boundaries.
type bucketBuilder struct {
	meta      statsMeta
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
func (u *bucketBuilder) finalize(ctx context.Context, ns tree.NodeStore) (DoltBucket, error) {
	// update MCV in case we've ended on a run of many identical keys
	u.updateMcv()
	// convert the MCV tuples into SQL rows (most efficient to only do this once)
	mcvRows, err := u.mcvs.Values(ctx, u.tupleDesc, ns, u.prefixLen)
	if err != nil {
		return DoltBucket{}, err
	}
	upperBound := make(sql.Row, u.prefixLen)
	for i := 0; i < u.prefixLen; i++ {
		upperBound[i], err = index.GetField(ctx, u.tupleDesc, i, u.currentKey, ns)
		if err != nil {
			return DoltBucket{}, err
		}
	}
	return DoltBucket{
		Count:      uint64(u.count),
		Distinct:   uint64(u.distinct),
		BoundCount: uint64(u.currentCnt),
		Mcv:        mcvRows,
		McvCount:   u.mcvs.Counts(),
		UpperBound: upperBound,
		Null:       uint64(u.nulls),
	}, nil
}

// add inputs a new row for a histogram bucket aggregation. We assume
// the key has already been truncated to the appropriate prefix length.
func (u *bucketBuilder) add(key val.Tuple) {
	newKey := u.currentKey == nil || u.tupleDesc.Compare(u.currentKey, key) != 0
	if newKey {
		u.newKey(key)
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
func (u *bucketBuilder) newKey(key val.Tuple) {
	u.updateMcv()
	if u.prevBound != nil {
		if u.tupleDesc.Compare(u.prevBound, key) != 0 {
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
	heap.Push(u.mcvs, mcv{key, cnt})
	if u.mcvs.Len() > mcvCnt {
		heap.Pop(u.mcvs)
	}
}

type mcv struct {
	val val.Tuple
	cnt int
}

type mcvHeap []mcv

var _ heap.Interface = (*mcvHeap)(nil)

func (m mcvHeap) Counts() []uint64 {
	ret := make([]uint64, len(m))
	for i, mcv := range m {
		ret[i] = uint64(mcv.cnt)
	}
	return ret
}

func (m mcvHeap) Values(ctx context.Context, keyDesc val.TupleDesc, ns tree.NodeStore, prefixLen int) ([]sql.Row, error) {
	ret := make([]sql.Row, len(m))
	for i, mcv := range m {
		// todo build sql.Row
		row := make(sql.Row, prefixLen)
		var err error
		for i := 0; i < prefixLen; i++ {
			row[i], err = index.GetField(ctx, keyDesc, i, mcv.val, ns)
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
