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
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

var ErrIncompatibleVersion = errors.New("client stats version mismatch")

type StatsKv interface {
	PutBucket(ctx context.Context, h hash.Hash, b *stats.Bucket, tupB *val.TupleBuilder) error
	GetBucket(ctx context.Context, h hash.Hash, tupB *val.TupleBuilder) (*stats.Bucket, bool, error)
	GetTemplate(key templateCacheKey) (stats.Statistic, bool)
	PutTemplate(key templateCacheKey, stat stats.Statistic)
	GetBound(h hash.Hash, len int) (sql.Row, bool)
	PutBound(h hash.Hash, r sql.Row, l int)
	Flush(ctx context.Context) (int, error)
	Len() int
	GcGen() uint64
}

var _ StatsKv = (*prollyStats)(nil)
var _ StatsKv = (*memStats)(nil)
var _ StatsKv = (*StatsController)(nil)

func NewMemStats() *memStats {
	return &memStats{
		mu:        sync.Mutex{},
		buckets:   make(map[bucketKey]*stats.Bucket),
		templates: make(map[templateCacheKey]stats.Statistic),
		bounds:    make(map[bucketKey]sql.Row),
		gcFlusher: make(map[*val.TupleBuilder][]bucketKey),
	}
}

type memStats struct {
	mu    sync.Mutex
	gcGen uint64

	buckets   map[bucketKey]*stats.Bucket
	templates map[templateCacheKey]stats.Statistic
	bounds    map[bucketKey]sql.Row

	// gcFlusher tracks state require to lazily swap from
	// a *memStats to *prollyStats
	gcFlusher map[*val.TupleBuilder][]bucketKey
}

func (m *memStats) StorageCnt(context.Context) (int, error) {
	return 0, nil
}

func (m *memStats) GetTemplate(key templateCacheKey) (stats.Statistic, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.templates[key]
	if !ok {
		return stats.Statistic{}, false
	}
	return t, true
}

func (m *memStats) PutTemplate(key templateCacheKey, stat stats.Statistic) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.templates[key] = stat
}

type bucketKey [22]byte

func getBucketKey(h hash.Hash, l int) bucketKey {
	var k bucketKey
	copy(k[:hash.ByteLen], h[:])
	binary.BigEndian.PutUint16(k[hash.ByteLen:], uint16(l))
	return k
}

func (m *memStats) GetBound(h hash.Hash, l int) (sql.Row, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := getBucketKey(h, l)
	r, ok := m.bounds[k]
	if !ok {
		return nil, false
	}
	return r, true
}

func (m *memStats) PutBound(h hash.Hash, r sql.Row, l int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := getBucketKey(h, l)
	m.bounds[k] = r
}

func (m *memStats) GcMark(from StatsKv, nodes []tree.Node, buckets []*stats.Bucket, idxLen int, tb *val.TupleBuilder) bool {
	if from.GcGen() > m.GcGen() {
		return false
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for i, b := range buckets {
		h := nodes[i].HashOf()
		k := getBucketKey(h, idxLen)
		if i == 0 {
			m.bounds[k], _ = from.GetBound(h, idxLen)
		}
		m.buckets[k] = b
		m.gcFlusher[tb] = append(m.gcFlusher[tb], k)
	}
	return true
}

func (m *memStats) GcGen() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.gcGen
}

func (m *memStats) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.buckets)
}

func (m *memStats) PutBucket(_ context.Context, h hash.Hash, b *stats.Bucket, _ *val.TupleBuilder) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := getBucketKey(h, len(b.BoundVal))
	m.buckets[k] = b
	return nil
}

func (m *memStats) GetBucket(_ context.Context, h hash.Hash, tupB *val.TupleBuilder) (*stats.Bucket, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if h.IsEmpty() {
		return nil, false, nil
	}
	k := getBucketKey(h, tupB.Desc.Count())
	b, ok := m.buckets[k]
	return b, ok, nil
}

func (m *memStats) Flush(_ context.Context) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.gcFlusher != nil {
		m.gcFlusher = nil
	}
	return 0, nil
}

func NewProllyStats(ctx context.Context, destDb dsess.SqlDatabase) (*prollyStats, error) {
	sch := schema.StatsTableDoltSchema
	kd, vd := sch.GetMapDescriptors(nil)

	nodeStore := destDb.DbData().Ddb.NodeStore()
	keyBuilder := val.NewTupleBuilder(kd, nodeStore)
	valueBuilder := val.NewTupleBuilder(vd, nodeStore)
	newMap, err := prolly.NewMapFromTuples(ctx, nodeStore, kd, vd)
	if err != nil {
		return nil, err
	}

	return &prollyStats{
		mu:     sync.Mutex{},
		destDb: destDb,
		kb:     keyBuilder,
		vb:     valueBuilder,
		m:      newMap.Mutate(),
		mem:    NewMemStats(),
	}, nil
}

type prollyStats struct {
	mu     sync.Mutex
	destDb dsess.SqlDatabase
	kb, vb *val.TupleBuilder
	m      *prolly.MutableMap
	newM   *prolly.MutableMap
	mem    *memStats
}

func (p *prollyStats) Len() int {
	return p.mem.Len()
}

func (p *prollyStats) GetTemplate(key templateCacheKey) (stats.Statistic, bool) {
	return p.mem.GetTemplate(key)
}

func (p *prollyStats) PutTemplate(key templateCacheKey, stat stats.Statistic) {
	p.mem.PutTemplate(key, stat)
}

func (p *prollyStats) GetBound(h hash.Hash, l int) (sql.Row, bool) {
	return p.mem.GetBound(h, l)
}

func (p *prollyStats) PutBound(h hash.Hash, r sql.Row, l int) {
	p.mem.PutBound(h, r, l)
}

func (p *prollyStats) PutBucket(ctx context.Context, h hash.Hash, b *stats.Bucket, tupB *val.TupleBuilder) error {
	if err := p.mem.PutBucket(ctx, h, b, tupB); err != nil {
		return err
	}

	k, err := p.encodeHash(h, tupB.Desc.Count())
	if err != nil {
		return err
	}
	v, err := p.encodeBucket(ctx, b, tupB)
	if err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	return p.m.Put(ctx, k, v)
}

func (p *prollyStats) GetBucket(ctx context.Context, h hash.Hash, tupB *val.TupleBuilder) (*stats.Bucket, bool, error) {
	if h.IsEmpty() {
		return nil, false, nil
	}
	b, ok, err := p.mem.GetBucket(ctx, h, tupB)
	if err != nil {
		return nil, false, err
	}
	if ok {
		return b, true, nil
	}

	// missing bucket and not GC'ing, try disk
	k, err := p.encodeHash(h, tupB.Desc.Count())
	if err != nil {
		return nil, false, err
	}

	var v val.Tuple
	err = p.m.Get(ctx, k, func(key val.Tuple, value val.Tuple) error {
		if key != nil {
			ok = true
			v = value
		}
		return nil
	})
	if !ok || err != nil {
		return nil, false, err
	}

	b, err = p.decodeBucketTuple(ctx, v, tupB)
	if err != nil {
		return nil, false, err
	}

	p.mem.PutBucket(ctx, h, b, tupB)
	return b, true, nil
}

func (p *prollyStats) GcGen() uint64 {
	return p.mem.gcGen
}

func (p *prollyStats) LoadFromMem(ctx context.Context) error {
	p.mem.mu.Lock()
	defer p.mem.mu.Unlock()
	for tb, keys := range p.mem.gcFlusher {
		for _, key := range keys {
			b, ok := p.mem.buckets[key]
			if !ok {
				return fmt.Errorf("memory KV inconsistent, missing bucket for: %s", key)
			}
			tupK, err := p.encodeHash(hash.New(key[:hash.ByteLen]), tb.Desc.Count())
			tupV, err := p.encodeBucket(ctx, b, tb)
			if err != nil {
				return err
			}
			if err := p.m.Put(ctx, tupK, tupV); err != nil {
				return err
			}
		}
	}
	p.mem.gcFlusher = nil
	return nil
}

func (p *prollyStats) Flush(ctx context.Context) (int, error) {
	if err := p.LoadFromMem(ctx); err != nil {
		return 0, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	flushedMap, err := p.m.Map(ctx)
	if err != nil {
		return 0, err
	}
	if err := p.destDb.DbData().Ddb.SetStatistics(ctx, "main", flushedMap.HashOf()); err != nil {
		return 0, err
	}

	p.m = flushedMap.Mutate()

	cnt, err := flushedMap.Count()
	return cnt, err
}

func (p *prollyStats) encodeHash(h hash.Hash, len int) (val.Tuple, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.kb.PutInt64(0, int64(len))
	if err := p.kb.PutString(1, h.String()); err != nil {
		return nil, err
	}
	return p.kb.Build(p.m.NodeStore().Pool())
}

func (p *prollyStats) decodeHashTuple(v val.Tuple) (int, hash.Hash, error) {
	l, ok := p.kb.Desc.GetInt64(0, v)
	hStr, ok := p.kb.Desc.GetString(1, v)
	if !ok {
		return 0, hash.Hash{}, fmt.Errorf("unexpected null hash")
	}
	return int(l), hash.Parse(hStr), nil
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
	upperBoundCnt := row[5].(int64)
	mcvCountsStr := row[10].(string)

	boundRow, err := DecodeRow(ctx, p.m.NodeStore(), boundRowStr, tupB)
	if err != nil {
		return nil, err
	}

	var mcvCnts []uint64
	if len(mcvCountsStr) > 0 {
		for _, c := range strings.Split(mcvCountsStr, ",") {
			cnt, err := strconv.ParseInt(c, 10, 64)
			if err != nil {
				return nil, err
			}
			mcvCnts = append(mcvCnts, uint64(cnt))
		}
	}

	mcvs := make([]sql.Row, len(mcvCnts))
	for i, v := range row[6 : 6+len(mcvCnts)] {
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
		BoundCnt:    uint64(upperBoundCnt),
		BoundVal:    boundRow,
		McvVals:     mcvs,
	}, nil
}

var mcvTypes = []sql.Type{types.Int16, types.Int16, types.Int16, types.Int16}

func (p *prollyStats) encodeBucket(ctx context.Context, b *stats.Bucket, tupB *val.TupleBuilder) (val.Tuple, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

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
	p.vb.PutString(10, stats.StringifyKey(mcvCntsRow, mcvTypes[:len(mcvCntsRow)]))

	return p.vb.Build(p.m.NodeStore().Pool())
}

func (p *prollyStats) NewEmpty(ctx context.Context) (StatsKv, error) {
	kd, vd := schema.StatsTableDoltSchema.GetMapDescriptors(nil)
	newMap, err := prolly.NewMapFromTuples(ctx, p.destDb.DbData().Ddb.NodeStore(), kd, vd)
	if err != nil {
		return nil, err
	}
	m := newMap.Mutate()
	return &prollyStats{m: m, destDb: p.destDb, kb: p.kb, vb: p.vb}, nil
}

func EncodeRow(ctx context.Context, ns tree.NodeStore, r sql.Row, tb *val.TupleBuilder) ([]byte, error) {
	for i := range tb.Desc.Count() {
		v := r[i]
		if v == nil {
			continue
		}
		if err := tree.PutField(ctx, ns, tb, i, v); err != nil {
			return nil, err
		}
	}
	return tb.Build(ns.Pool())
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

func (sc *StatsController) PutBucket(ctx context.Context, h hash.Hash, b *stats.Bucket, tupB *val.TupleBuilder) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.kv.PutBucket(ctx, h, b, tupB)
}

func (sc *StatsController) GetBucket(ctx context.Context, h hash.Hash, tupB *val.TupleBuilder) (*stats.Bucket, bool, error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.kv.GetBucket(ctx, h, tupB)
}

func (sc *StatsController) GetTemplate(key templateCacheKey) (stats.Statistic, bool) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.kv.GetTemplate(key)
}

func (sc *StatsController) PutTemplate(key templateCacheKey, stat stats.Statistic) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.kv.PutTemplate(key, stat)
}

func (sc *StatsController) GetBound(h hash.Hash, len int) (sql.Row, bool) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.kv.GetBound(h, len)
}

func (sc *StatsController) PutBound(h hash.Hash, r sql.Row, l int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.kv.PutBound(h, r, l)
}

func (sc *StatsController) Flush(ctx context.Context) (int, error) {
	sqlCtx, err := sc.ctxGen(ctx)
	if err != nil {
		return 0, err
	}
	defer sql.SessionEnd(sqlCtx.Session)
	sql.SessionCommandBegin(sqlCtx.Session)
	defer sql.SessionCommandEnd(sqlCtx.Session)

	sc.mu.Lock()
	defer sc.mu.Unlock()
	defer sc.signalListener(leFlush)
	return sc.kv.Flush(sqlCtx)
}

func (sc *StatsController) Len() int {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.kv.Len()
}

func (sc *StatsController) GcGen() uint64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.kv.GcGen()
}
