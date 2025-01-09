// Copyright 2025 Dolthub, Inc.
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
	"context"
	"errors"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"
	"github.com/dolthub/go-mysql-server/sql/types"
	"strconv"
	"strings"
)

var ErrIncompatibleVersion = errors.New("client stats version mismatch")

type StatsKv interface {
	Put(ctx context.Context, h hash.Hash, b *stats.Bucket, tupB *val.TupleBuilder) error
	Get(ctx context.Context, h hash.Hash, tupB *val.TupleBuilder) (*stats.Bucket, bool, error)
	Flush(ctx context.Context) error
	NewEmpty(ctx *sql.Context) (StatsKv, error)
}

var _ StatsKv = (*prollyStats)(nil)
var _ StatsKv = (*memStats)(nil)

func NewMemStats() *memStats {
	return &memStats{m: make(map[hash.Hash]*stats.Bucket)}
}

type memStats struct {
	m map[hash.Hash]*stats.Bucket
}

func (m *memStats) Put(_ context.Context, h hash.Hash, b *stats.Bucket, _ *val.TupleBuilder) error {
	m.m[h] = b
	return nil
}

func (m *memStats) Get(_ context.Context, h hash.Hash, _ *val.TupleBuilder) (*stats.Bucket, bool, error) {
	b, ok := m.m[h]
	return b, ok, nil
}

func (m *memStats) Flush(_ context.Context) error {
	return nil
}

func (m *memStats) NewEmpty(ctx *sql.Context) (StatsKv, error) {
	return &memStats{m: make(map[hash.Hash]*stats.Bucket)}, nil
}

func NewProllyStats(ctx context.Context, destDb dsess.SqlDatabase) (*prollyStats, error) {
	sch := schema.StatsTableDoltSchema
	kd, vd := sch.GetMapDescriptors()

	keyBuilder := val.NewTupleBuilder(kd)
	valueBuilder := val.NewTupleBuilder(vd)
	newMap, err := prolly.NewMapFromTuples(ctx, destDb.DbData().Ddb.NodeStore(), kd, vd)
	if err != nil {
		return nil, err
	}

	return &prollyStats{
		destDb: destDb,
		kb:     keyBuilder,
		vb:     valueBuilder,
		m:      newMap.Mutate(),
	}, nil
}

type prollyStats struct {
	destDb dsess.SqlDatabase
	kb, vb *val.TupleBuilder
	m      *prolly.MutableMap
}

func (p *prollyStats) Put(ctx context.Context, h hash.Hash, b *stats.Bucket, tupB *val.TupleBuilder) error {
	k, err := p.encodeHash(h)
	if err != nil {
		return err
	}
	v, err := p.encodeBucket(ctx, b, tupB)
	if err != nil {
		return err
	}
	return p.m.Put(ctx, k, v)
}

func (p *prollyStats) Get(ctx context.Context, h hash.Hash, tupB *val.TupleBuilder) (*stats.Bucket, bool, error) {
	k, err := p.encodeHash(h)
	if err != nil {
		return nil, false, err
	}

	var v val.Tuple
	var ok bool
	err = p.m.Get(ctx, k, func(key val.Tuple, value val.Tuple) error {
		if key != nil {
			v = value
		} else {
			ok = false
		}
		return nil
	})
	if !ok || err != nil {
		return nil, false, err
	}

	b, err := p.decodeBucketTuple(ctx, v, tupB)
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

func (p *prollyStats) encodeHash(h hash.Hash) (val.Tuple, error) {
	if err := p.kb.PutString(0, h.String()); err != nil {
		return nil, err
	}
	return p.vb.Build(p.m.NodeStore().Pool()), nil
}

func (p *prollyStats) decodeHashTuple(v val.Tuple) (hash.Hash, error) {
	hStr, ok := p.kb.Desc.GetString(0, v)
	if !ok {
		return hash.Hash{}, fmt.Errorf("unexpected null hash")
	}
	return hash.Parse(hStr), nil
}

func (p *prollyStats) decodeBucketTuple(ctx context.Context, v val.Tuple, tupB *val.TupleBuilder) (*stats.Bucket, error) {
	var row []interface{}
	for i := 0; i < p.vb.Desc.Count(); i++ {
		f, err := tree.GetField(ctx, p.vb.Desc, i, v, p.m.NodeStore())
		if err != nil {
			return nil, err
		}
		row = append(row, f)
	}

	version := row[0]
	if version != schema.StatsVersion {
		return nil, fmt.Errorf("%w: write version %d does not match read version %d", ErrIncompatibleVersion, version, schema.StatsVersion)
	}
	rowCount := row[1].(int64)
	distinctCount := row[2].(int64)
	nullCount := row[3].(int64)
	boundRowStr := row[4].(string)
	upperBoundCnt := row[5].(uint64)
	mcvCountsStr := row[10].(string)

	boundRow, err := DecodeRow(ctx, p.m.NodeStore(), boundRowStr, tupB)
	if err != nil {
		return nil, err
	}

	var mcvCnts []uint64
	for _, c := range strings.Split(mcvCountsStr, ",") {
		cnt, err := strconv.ParseInt(c, 10, 64)
		if err != nil {
			return nil, err
		}
		mcvCnts = append(mcvCnts, uint64(cnt))
	}

	mcvs := make([]sql.Row, 4)
	for i, v := range row[6:10] {
		if v != nil && v != "" {
			row, err := DecodeRow(ctx, p.m.NodeStore(), v.(string), tupB)
			if err != nil {
				return nil, err
			}
			mcvs[i] = row
		}
	}

	return &stats.Bucket{
		RowCnt:      uint64(rowCount),
		DistinctCnt: uint64(distinctCount),
		NullCnt:     uint64(nullCount),
		McvsCnt:     mcvCnts,
		BoundCnt:    upperBoundCnt,
		BoundVal:    boundRow,
		McvVals:     mcvs,
	}, nil
}

var mcvTypes = []sql.Type{types.Int16, types.Int16, types.Int16, types.Int16}

func (p *prollyStats) encodeBucket(ctx context.Context, b *stats.Bucket, tupB *val.TupleBuilder) (val.Tuple, error) {
	p.vb.PutInt64(0, schema.StatsVersion)
	p.vb.PutInt64(1, int64(b.RowCount()))
	p.vb.PutInt64(2, int64(b.DistinctCount()))
	p.vb.PutInt64(3, int64(b.NullCount()))
	boundRow, err := EncodeRow(ctx, p.m.NodeStore(), b.UpperBound(), tupB)
	if err != nil {
		return nil, err
	}
	p.vb.PutString(4, string(boundRow))
	p.vb.PutInt64(5, int64(b.BoundCount()))
	for i, r := range b.Mcvs() {
		mcvRow, err := EncodeRow(ctx, p.m.NodeStore(), r, tupB)
		if err != nil {
			return nil, err
		}
		p.vb.PutString(6+i, string(mcvRow))
	}
	var mcvCntsRow sql.Row
	for _, v := range b.McvCounts() {
		mcvCntsRow = append(mcvCntsRow, int(v))
	}
	p.vb.PutString(10, stats.StringifyKey(mcvCntsRow, mcvTypes))

	return p.vb.Build(p.m.NodeStore().Pool()), nil
}

func (p *prollyStats) Flush(ctx context.Context) error {
	flushedMap, err := p.m.Map(ctx)
	if err != nil {
		return err
	}
	return p.destDb.DbData().Ddb.SetStatisics(ctx, "main", flushedMap.HashOf())
}

func (p *prollyStats) NewEmpty(ctx *sql.Context) (StatsKv, error) {
	kd, vd := schema.StatsTableDoltSchema.GetMapDescriptors()
	newMap, err := prolly.NewMapFromTuples(ctx, p.destDb.DbData().Ddb.NodeStore(), kd, vd)
	if err != nil {
		return nil, err
	}
	m := newMap.Mutate()
	return &prollyStats{m: m, destDb: p.destDb, kb: p.kb, vb: p.vb}, nil
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
