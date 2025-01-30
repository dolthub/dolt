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
	lru "github.com/hashicorp/golang-lru/v2"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

var ErrIncompatibleVersion = errors.New("client stats version mismatch")

const defaultBucketSize = 1024 // must be > 0 to avoid panic

type StatsKv interface {
	PutBucket(ctx context.Context, h hash.Hash, b *stats.Bucket, tupB *val.TupleBuilder) error
	GetBucket(ctx context.Context, h hash.Hash, tupB *val.TupleBuilder) (*stats.Bucket, bool, error)
	MarkBucket(ctx context.Context, h hash.Hash, tupB *val.TupleBuilder) error
	GetTemplate(key templateCacheKey) (stats.Statistic, bool)
	PutTemplate(key templateCacheKey, stat stats.Statistic)
	GetBound(h hash.Hash) (sql.Row, bool)
	PutBound(h hash.Hash, r sql.Row)
	Flush(ctx context.Context) error
	StartGc(ctx context.Context, sz int) error
	FinishGc()
	Len() int
	Cap() int64
}

var _ StatsKv = (*prollyStats)(nil)
var _ StatsKv = (*memStats)(nil)

func NewMemStats() *memStats {
	buckets, _ := lru.New[hash.Hash, *stats.Bucket](defaultBucketSize)
	gcCap := atomic.Int64{}
	gcCap.Store(defaultBucketSize)
	return &memStats{
		mu:        sync.Mutex{},
		buckets:   buckets,
		templates: make(map[templateCacheKey]stats.Statistic),
		bounds:    make(map[hash.Hash]sql.Row),
		gcCap:     gcCap,
	}
}

type memStats struct {
	mu    sync.Mutex
	doGc  bool
	gcCap atomic.Int64

	buckets     *lru.Cache[hash.Hash, *stats.Bucket]
	nextBuckets *lru.Cache[hash.Hash, *stats.Bucket]

	templates     map[templateCacheKey]stats.Statistic
	nextTemplates map[templateCacheKey]stats.Statistic

	bounds     map[hash.Hash]sql.Row
	nextBounds map[hash.Hash]sql.Row
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

func (m *memStats) GetBound(h hash.Hash) (sql.Row, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.bounds[h]
	if !ok {
		return nil, false
	}
	if m.doGc {
		//log.Println("copy bound ", h.String()[:5])
		m.nextBounds[h] = r
	}
	return r, true
}

func (m *memStats) PutBound(h hash.Hash, r sql.Row) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bounds[h] = r
	if m.doGc {
		m.nextBounds[h] = r
	}
}

func (m *memStats) StartGc(ctx context.Context, sz int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.doGc = true
	m.gcCap.Store(int64(sz))
	if sz == 0 {
		sz = m.buckets.Len() * 2
	}
	var err error
	m.nextBuckets, err = lru.New[hash.Hash, *stats.Bucket](sz)
	if err != nil {
		return err
	}
	m.nextBounds = make(map[hash.Hash]sql.Row)
	m.nextTemplates = make(map[templateCacheKey]stats.Statistic)
	return nil
}

func (m *memStats) FinishGc() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.buckets = m.nextBuckets
	m.templates = m.nextTemplates
	m.bounds = m.nextBounds

	var hashes []string
	for _, k := range m.buckets.Keys() {
		hashes = append(hashes, k.String()[:5])
	}
	log.Println("hashes after GC: ", strings.Join(hashes, ", "))
	var templates []string
	for k, _ := range m.templates {
		templates = append(templates, k.String())
	}
	log.Println("templates after GC: ", strings.Join(templates, ", "))

	var bounds []string
	for k, _ := range m.bounds {
		bounds = append(bounds, k.String())
	}
	log.Println("bounds after GC: ", strings.Join(bounds, ", "))

	m.nextBuckets = nil
	m.nextTemplates = nil
	m.nextBounds = nil
	m.doGc = false
}

func (m *memStats) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.buckets.Len()
}

func (m *memStats) Cap() int64 {
	return m.gcCap.Load()
}

func (m *memStats) PutBucket(_ context.Context, h hash.Hash, b *stats.Bucket, _ *val.TupleBuilder) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.buckets.Add(h, b)
	log.Println("put ", h.String()[:5], m.buckets.Len())
	return nil
}

func (m *memStats) MarkBucket(ctx context.Context, h hash.Hash, _ *val.TupleBuilder) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.buckets.Get(h)
	//log.Printf("mark %s, %t\n", h.String()[:5], ok)
	if ok {
		m.nextBuckets.Add(h, b)
		gcCap := int(m.gcCap.Load())
		if m.nextBuckets.Len() >= gcCap {
			m.gcCap.Store(int64(gcCap) * 2)
			m.nextBuckets.Resize(gcCap * 2)
		}
	}
	return nil
}

func (m *memStats) GetBucket(_ context.Context, h hash.Hash, _ *val.TupleBuilder) (*stats.Bucket, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if h.IsEmpty() {
		return nil, false, nil
	}
	b, ok := m.buckets.Get(h)
	return b, ok, nil
}

func (m *memStats) Flush(_ context.Context) error {
	return nil
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

func (p *prollyStats) Cap() int64 {
	return p.mem.Cap()
}

func (p *prollyStats) GetTemplate(key templateCacheKey) (stats.Statistic, bool) {
	return p.mem.GetTemplate(key)
}

func (p *prollyStats) PutTemplate(key templateCacheKey, stat stats.Statistic) {
	p.mem.PutTemplate(key, stat)
}

func (p *prollyStats) GetBound(h hash.Hash) (sql.Row, bool) {
	return p.mem.GetBound(h)
}

func (p *prollyStats) PutBound(h hash.Hash, r sql.Row) {
	p.mem.PutBound(h, r)
}

func (p *prollyStats) PutBucket(ctx context.Context, h hash.Hash, b *stats.Bucket, tupB *val.TupleBuilder) error {
	if err := p.mem.PutBucket(ctx, h, b, tupB); err != nil {
		return err
	}

	k, err := p.encodeHash(h)
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
	k, err := p.encodeHash(h)
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

	// missing bucket and not GC'ing, try disk
	k, err := p.encodeHash(h)
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

func (p *prollyStats) FinishGc() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mem.FinishGc()
	p.m = p.newM
	p.newM = nil
}

func (p *prollyStats) encodeHash(h hash.Hash) (val.Tuple, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.kb.PutString(0, h.String()); err != nil {
		return nil, err
	}
	return p.kb.Build(p.m.NodeStore().Pool()), nil
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
