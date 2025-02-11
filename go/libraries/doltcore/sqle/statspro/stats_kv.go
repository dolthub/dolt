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
	"sync"
)

var ErrIncompatibleVersion = errors.New("client stats version mismatch")

const defaultBucketSize = 1024 // must be > 0 to avoid panic

type StatsKv interface {
	PutBucket(ctx context.Context, h hash.Hash, b *stats.Bucket, tupB *val.TupleBuilder) error
	GetBucket(ctx context.Context, h hash.Hash, tupB *val.TupleBuilder) (*stats.Bucket, bool, error)
	GetTemplate(key templateCacheKey) (stats.Statistic, bool)
	PutTemplate(key templateCacheKey, stat stats.Statistic)
	GetBound(h hash.Hash, len int) (sql.Row, bool)
	PutBound(h hash.Hash, r sql.Row, l int)
	Flush(ctx context.Context) (int, error)
	StartGc(ctx context.Context, sz int) error
	MarkBucket(ctx context.Context, h hash.Hash, tupB *val.TupleBuilder) error
	FinishGc(context.Context) error
	Len() int
}

var _ StatsKv = (*prollyStats)(nil)
var _ StatsKv = (*memStats)(nil)

func NewMemStats() *memStats {
	return &memStats{
		mu:        sync.Mutex{},
		buckets:   make(map[bucketKey]*stats.Bucket),
		templates: make(map[templateCacheKey]stats.Statistic),
		bounds:    make(map[bucketKey]sql.Row),
	}
}

type memStats struct {
	mu   sync.Mutex
	doGc bool

	//buckets     *lru.Cache[bucketKey, *stats.Bucket]
	//nextBuckets *lru.Cache[bucketKey, *stats.Bucket]
	buckets     map[bucketKey]*stats.Bucket
	nextBuckets map[bucketKey]*stats.Bucket

	templates     map[templateCacheKey]stats.Statistic
	nextTemplates map[templateCacheKey]stats.Statistic

	bounds     map[bucketKey]sql.Row
	nextBounds map[bucketKey]sql.Row

	epochCnt int
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
	if m.doGc {
		m.nextTemplates[key] = t
	}
	return t, true
}

func (m *memStats) PutTemplate(key templateCacheKey, stat stats.Statistic) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.templates[key] = stat
	if m.doGc {
		m.nextTemplates[key] = stat
	}
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
	if m.doGc {
		m.nextBounds[k] = r
	}
	return r, true
}

func (m *memStats) PutBound(h hash.Hash, r sql.Row, l int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := getBucketKey(h, l)
	m.bounds[k] = r
	if m.doGc {
		m.nextBounds[k] = r
	}
}

func (m *memStats) StartGc(ctx context.Context, sz int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.doGc = true
	if sz == 0 {
		sz = len(m.buckets) * 2
	}
	var err error
	//m.nextBuckets, err = lru.New[bucketKey, *stats.Bucket](sz)
	m.nextBuckets = make(map[bucketKey]*stats.Bucket, sz)
	if err != nil {
		return err
	}
	m.nextBounds = make(map[bucketKey]sql.Row)
	m.nextTemplates = make(map[templateCacheKey]stats.Statistic)
	return nil
}

func (m *memStats) RestartEpoch() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.epochCnt = 0
}

func (m *memStats) FinishGc(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.buckets = m.nextBuckets
	m.templates = m.nextTemplates
	m.bounds = m.nextBounds
	m.nextBuckets = nil
	m.nextTemplates = nil
	m.nextBounds = nil
	m.doGc = false
	return nil
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

func (m *memStats) MarkBucket(_ context.Context, h hash.Hash, tupB *val.TupleBuilder) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := getBucketKey(h, tupB.Desc.Count())
	b, ok := m.buckets[k]
	if ok {
		m.nextBuckets[k] = b
	}
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
	return 0, nil
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

	if tupB == nil {
		// still function if treating like memStats
		return nil, true, nil
	}

	b, err = p.decodeBucketTuple(ctx, v, tupB)
	if err != nil {
		return nil, false, err
	}

	p.mem.PutBucket(ctx, h, b, tupB)
	return b, true, nil
}

func (p *prollyStats) Flush(ctx context.Context) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	flushedMap, err := p.m.Map(ctx)
	if err != nil {
		return 0, err
	}
	if err := p.destDb.DbData().Ddb.SetStatistics(ctx, "main", flushedMap.HashOf()); err != nil {
		return 0, err
	}

	cnt, err := flushedMap.Count()
	return cnt, err
}

func (p *prollyStats) StartGc(ctx context.Context, sz int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.mem.StartGc(ctx, sz); err != nil {
		return err
	}
	kd, vd := schema.StatsTableDoltSchema.GetMapDescriptors()
	newMap, err := prolly.NewMapFromTuples(ctx, p.destDb.DbData().Ddb.NodeStore(), kd, vd)
	if err != nil {
		return err
	}
	p.newM = newMap.Mutate()

	return nil
}

func (p *prollyStats) MarkBucket(ctx context.Context, h hash.Hash, tupB *val.TupleBuilder) error {
	p.mem.MarkBucket(ctx, h, tupB)

	// try disk
	k, err := p.encodeHash(h, tupB.Desc.Count())
	if err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	var v val.Tuple
	var ok bool
	err = p.m.Get(ctx, k, func(key val.Tuple, value val.Tuple) error {
		if key != nil {
			ok = true
			v = value
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	return p.newM.Put(ctx, k, v)
}

func (p *prollyStats) FinishGc(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mem.FinishGc(nil)
	m, err := p.newM.Map(context.Background())
	if err != nil {
		return err
	}
	p.m = m.Mutate()
	p.newM = nil

	return nil
}

func (p *prollyStats) encodeHash(h hash.Hash, len int) (val.Tuple, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.kb.PutInt64(0, int64(len))
	if err := p.kb.PutString(1, h.String()); err != nil {
		return nil, err
	}
	return p.kb.Build(p.m.NodeStore().Pool()), nil
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

	return p.vb.Build(p.m.NodeStore().Pool()), nil
}

func (p *prollyStats) NewEmpty(ctx context.Context) (StatsKv, error) {
	kd, vd := schema.StatsTableDoltSchema.GetMapDescriptors()
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
