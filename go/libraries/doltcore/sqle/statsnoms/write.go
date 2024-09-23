// Copyright 2024 Dolthub, Inc.
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

package statsnoms

import (
	"context"
	"errors"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/go-mysql-server/sql/types"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/statspro"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

// About ~200 20 byte address fit in a ~4k chunk. Chunk sizes
// are approximate, but certainly shouldn't reach the square
// of the expected size.
const maxBucketFanout = 200 * 200

var mcvsTypes = []sql.Type{types.Int64, types.Int64, types.Int64}

func (n *NomsStatsDatabase) replaceStats(ctx context.Context, statsMap *prolly.MutableMap, dStats *statspro.DoltStats) error {
	if err := deleteIndexRows(ctx, statsMap, dStats); err != nil {
		return err
	}
	return putIndexRows(ctx, statsMap, dStats)
}

func deleteIndexRows(ctx context.Context, statsMap *prolly.MutableMap, dStats *statspro.DoltStats) error {
	sch := schema.StatsTableDoltSchema
	kd, _ := sch.GetMapDescriptors()

	keyBuilder := val.NewTupleBuilder(kd)

	qual := dStats.Qualifier()
	pool := statsMap.NodeStore().Pool()

	// delete previous entries for this index -> (db, table, index, pos)
	keyBuilder.PutString(0, qual.Database)
	keyBuilder.PutString(1, qual.Table())
	keyBuilder.PutString(2, qual.Index())
	keyBuilder.PutInt64(3, 0)
	firstKey := keyBuilder.Build(pool)
	keyBuilder.PutString(0, qual.Database)
	keyBuilder.PutString(1, qual.Table())
	keyBuilder.PutString(2, qual.Index())
	keyBuilder.PutInt64(3, maxBucketFanout+1)
	maxKey := keyBuilder.Build(pool)

	// there is a limit on the number of buckets for a given index, iter
	// will terminate before maxBucketFanout
	iter, err := statsMap.IterKeyRange(ctx, firstKey, maxKey)
	if err != nil {
		return err
	}

	for {
		k, _, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		err = statsMap.Put(ctx, k, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func putIndexRows(ctx context.Context, statsMap *prolly.MutableMap, dStats *statspro.DoltStats) error {
	sch := schema.StatsTableDoltSchema
	kd, vd := sch.GetMapDescriptors()

	keyBuilder := val.NewTupleBuilder(kd)
	valueBuilder := val.NewTupleBuilder(vd)

	qual := dStats.Qualifier()
	pool := statsMap.NodeStore().Pool()

	// now add new buckets
	typesB := strings.Builder{}
	sep := ""
	for _, t := range dStats.Statistic.Typs {
		typesB.WriteString(sep + t.String())
		sep = "\n"
	}
	typesStr := typesB.String()

	var pos int64
	for _, h := range dStats.Hist {
		keyBuilder.PutString(0, qual.Database)
		keyBuilder.PutString(1, qual.Tab)
		keyBuilder.PutString(2, qual.Idx)
		keyBuilder.PutInt64(3, pos)

		valueBuilder.PutInt64(0, schema.StatsVersion)
		valueBuilder.PutString(1, statspro.DoltBucketChunk(h).String())
		valueBuilder.PutInt64(2, int64(h.RowCount()))
		valueBuilder.PutInt64(3, int64(h.DistinctCount()))
		valueBuilder.PutInt64(4, int64(h.NullCount()))
		valueBuilder.PutString(5, strings.Join(dStats.Columns(), ","))
		valueBuilder.PutString(6, typesStr)
		boundRow, err := EncodeRow(ctx, statsMap.NodeStore(), h.UpperBound(), dStats.Tb)
		if err != nil {
			return err
		}
		valueBuilder.PutString(7, string(boundRow))
		valueBuilder.PutInt64(8, int64(h.BoundCount()))
		valueBuilder.PutDatetime(9, statspro.DoltBucketCreated(h))
		for i, r := range h.Mcvs() {
			valueBuilder.PutString(10+i, stats.StringifyKey(r, dStats.Statistic.Typs))
		}
		var mcvCntsRow sql.Row
		for _, v := range h.McvCounts() {
			mcvCntsRow = append(mcvCntsRow, int(v))
		}
		valueBuilder.PutString(14, stats.StringifyKey(mcvCntsRow, mcvsTypes))

		key := keyBuilder.Build(pool)
		value := valueBuilder.Build(pool)
		statsMap.Put(ctx, key, value)
		pos++
	}
	return nil
}

func EncodeRow(ctx context.Context, ns tree.NodeStore, r sql.Row, tb *val.TupleBuilder) ([]byte, error) {
	for i, v := range r {
		if v == nil {
			continue
		}
		if err := tree.PutField(ctx, ns, tb, i, v); err != nil {
			return nil, err
		}
	}
	return tb.Build(ns.Pool()), nil
}

func DecodeRow(ctx context.Context, ns tree.NodeStore, s string, tb *val.TupleBuilder) (sql.Row, error) {
	tup := []byte(s)
	r := make(sql.Row, tb.Desc.Count())
	var err error
	for i, _ := range r {
		r[i], err = tree.GetField(ctx, tb.Desc, i, tup, ns)
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}
