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

package statspro

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"

	"github.com/dolthub/dolt/go/store/hash"
)

type DoltStats struct {
	Statistic *stats.Statistic
	mu        *sync.Mutex
	// Chunks is a list of addresses for the histogram fanout level
	Chunks []hash.Hash
	// Active maps a chunk/bucket address to its position in
	// the histogram. 1-indexed to differentiate from an empty
	// field on disk
	Active map[hash.Hash]int
	Hist   sql.Histogram
}

func (s *DoltStats) Clone(_ context.Context) sql.JSONWrapper {
	return s
}

var _ sql.Statistic = (*DoltStats)(nil)

func (s *DoltStats) WithColSet(set sql.ColSet) sql.Statistic {
	ret := *s
	ret.Statistic = ret.Statistic.WithColSet(set).(*stats.Statistic)
	return &ret
}

func (s *DoltStats) WithFuncDeps(set *sql.FuncDepSet) sql.Statistic {
	ret := *s
	ret.Statistic = ret.Statistic.WithFuncDeps(set).(*stats.Statistic)
	return &ret
}

func (s *DoltStats) WithDistinctCount(u uint64) sql.Statistic {
	ret := *s
	ret.Statistic = ret.Statistic.WithDistinctCount(u).(*stats.Statistic)
	return &ret
}

func (s *DoltStats) WithRowCount(u uint64) sql.Statistic {
	ret := *s
	ret.Statistic = ret.Statistic.WithRowCount(u).(*stats.Statistic)
	return &ret
}

func (s *DoltStats) WithNullCount(u uint64) sql.Statistic {
	ret := *s
	ret.Statistic = ret.Statistic.WithNullCount(u).(*stats.Statistic)
	return &ret
}

func (s *DoltStats) WithAvgSize(u uint64) sql.Statistic {
	ret := *s
	ret.Statistic = ret.Statistic.WithAvgSize(u).(*stats.Statistic)
	return &ret
}

func (s *DoltStats) WithLowerBound(row sql.Row) sql.Statistic {
	ret := *s
	ret.Statistic = ret.Statistic.WithLowerBound(row).(*stats.Statistic)
	return &ret
}

func (s *DoltStats) RowCount() uint64 {
	return s.Statistic.RowCount()
}

func (s *DoltStats) DistinctCount() uint64 {
	return s.Statistic.DistinctCount()
}

func (s *DoltStats) NullCount() uint64 {
	return s.Statistic.NullCount()

}

func (s *DoltStats) AvgSize() uint64 {
	return s.Statistic.AvgSize()

}

func (s *DoltStats) CreatedAt() time.Time {
	return s.Statistic.CreatedAt()

}

func (s *DoltStats) Columns() []string {
	return s.Statistic.Columns()
}

func (s *DoltStats) Types() []sql.Type {
	return s.Statistic.Types()
}

func (s *DoltStats) Qualifier() sql.StatQualifier {
	return s.Statistic.Qualifier()
}

func (s *DoltStats) IndexClass() sql.IndexClass {
	return s.Statistic.IndexClass()
}

func (s *DoltStats) FuncDeps() *sql.FuncDepSet {
	return s.Statistic.FuncDeps()
}

func (s *DoltStats) ColSet() sql.ColSet {
	return s.Statistic.ColSet()
}

func (s *DoltStats) LowerBound() sql.Row {
	return s.Statistic.LowerBound()
}

func NewDoltStats() *DoltStats {
	return &DoltStats{mu: &sync.Mutex{}, Active: make(map[hash.Hash]int), Statistic: &stats.Statistic{}}
}

func (s *DoltStats) ToInterface() (interface{}, error) {
	statVal, err := s.Statistic.ToInterface()
	if err != nil {
		return nil, err
	}
	ret := statVal.(map[string]interface{})

	var hist sql.Histogram
	for _, b := range s.Hist {
		hist = append(hist, b)
	}
	histVal, err := hist.ToInterface()
	if err != nil {
		return nil, err
	}
	ret["statistic"].(map[string]interface{})["buckets"] = histVal
	return ret, nil
}

func (s *DoltStats) WithHistogram(h sql.Histogram) (sql.Statistic, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ret := *s
	ret.Hist = nil
	for _, b := range h {
		doltB, ok := b.(DoltBucket)
		if !ok {
			return nil, fmt.Errorf("invalid bucket type: %T, %s", b, h.DebugString())
		}
		ret.Hist = append(ret.Hist, doltB)
	}
	return &ret, nil
}

func (s *DoltStats) Histogram() sql.Histogram {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Hist
}

func DoltStatsFromSql(stat sql.Statistic) (*DoltStats, error) {
	hist, err := DoltHistFromSql(stat.Histogram(), stat.Types())
	if err != nil {
		return nil, err
	}
	ret := &DoltStats{
		mu:        &sync.Mutex{},
		Hist:      hist,
		Statistic: stats.NewStatistic(stat.RowCount(), stat.DistinctCount(), stat.NullCount(), stat.AvgSize(), stat.CreatedAt(), stat.Qualifier(), stat.Columns(), stat.Types(), nil, stat.IndexClass(), stat.LowerBound()),
		Active:    make(map[hash.Hash]int),
	}
	ret.Statistic.Fds = stat.FuncDeps()
	ret.Statistic.Colset = stat.ColSet()
	return ret, nil
}

func (s *DoltStats) UpdateActive() {
	s.mu.Lock()
	defer s.mu.Unlock()
	newActive := make(map[hash.Hash]int)
	for i, hash := range s.Chunks {
		newActive[hash] = i
	}
	s.Active = newActive
}

type DoltHistogram []DoltBucket

type DoltBucket struct {
	Bucket  *stats.Bucket
	Chunk   hash.Hash
	Created time.Time
}

func (d DoltBucket) RowCount() uint64 {
	return d.Bucket.RowCount()
}

func (d DoltBucket) DistinctCount() uint64 {
	return d.Bucket.DistinctCount()
}

func (d DoltBucket) NullCount() uint64 {
	return d.Bucket.NullCount()
}

func (d DoltBucket) BoundCount() uint64 {
	return d.Bucket.BoundCount()
}

func (d DoltBucket) UpperBound() sql.Row {
	return d.Bucket.UpperBound()
}

func (d DoltBucket) McvCounts() []uint64 {
	return d.Bucket.McvCounts()
}

func (d DoltBucket) Mcvs() []sql.Row {
	return d.Bucket.Mcvs()
}

func DoltBucketChunk(b sql.HistogramBucket) hash.Hash {
	return b.(DoltBucket).Chunk
}

func DoltBucketCreated(b sql.HistogramBucket) time.Time {
	return b.(DoltBucket).Created
}

var _ sql.HistogramBucket = (*DoltBucket)(nil)

func DoltHistFromSql(hist sql.Histogram, types []sql.Type) (sql.Histogram, error) {
	ret := make(sql.Histogram, len(hist))
	var err error
	for i, b := range hist {
		upperBound := make(sql.Row, len(b.UpperBound()))
		for i, v := range b.UpperBound() {
			upperBound[i], _, err = types[i].Convert(v)
			if err != nil {
				return nil, fmt.Errorf("failed to convert %v to type %s", v, types[i].String())
			}
		}
		mcvs := make([]sql.Row, len(b.Mcvs()))
		for i, mcv := range b.Mcvs() {
			for _, v := range mcv {
				conv, _, err := types[i].Convert(v)
				if err != nil {
					return nil, fmt.Errorf("failed to convert %v to type %s", v, types[i].String())
				}
				mcvs[i] = append(mcvs[i], conv)
			}
		}
		ret[i] = DoltBucket{
			Bucket: stats.NewHistogramBucket(b.RowCount(), b.DistinctCount(), b.NullCount(), b.BoundCount(), upperBound, b.McvCounts(), mcvs).(*stats.Bucket),
		}
	}
	return ret, nil
}
